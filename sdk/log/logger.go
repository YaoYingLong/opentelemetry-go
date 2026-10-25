// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package log

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/embedded"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
)

var now = time.Now

const (
	exceptionTypeKey    = semconv.ExceptionTypeKey
	exceptionMessageKey = semconv.ExceptionMessageKey
)

// This is a compile-time check that logger implements log.Logger.
var _ log.Logger = (*logger)(nil)

type logger struct {
	embedded.Logger

	provider             *LoggerProvider
	instrumentationScope instrumentation.Scope

	// recCntIncr increments the count of log records created. It will be nil
	// if observability is disabled.
	recCntIncr func(context.Context)
}

func newLogger(p *LoggerProvider, scope instrumentation.Scope) *logger {
	l := &logger{
		provider:             p,
		instrumentationScope: scope,
	}

	var err error
	l.recCntIncr, err = newRecordCounterIncr()
	if err != nil {
		otel.Handle(err)
	}
	return l
}

func (l *logger) Emit(ctx context.Context, r log.Record) {
	// 中文导读：日志桥把 API 层的 log.Record 交到这里，SDK 再补充资源、scope、
	// 关联 ID 和属性限制。按值传参不等于递归深拷贝；异步保存记录仍需 Clone。
	// Emit 没有返回 error，处理器失败会交给全局错误处理器，也不会中止后续处理器。
	processors := l.provider.processors
	if len(processors) == 0 {
		if l.provider.stopped.Load() {
			return
		}
		// Emit remains observable without processors, but no lifecycle
		// admission or SDK record construction is needed.
		l.recordCreated(ctx)
		return
	}

	// 先退出 Provider 的活跃操作计数，再调用应用可替换的错误处理器。
	// 否则错误处理器若调用 Shutdown，就可能与当前 Emit 互相等待。
	for _, err := range l.emit(ctx, r, processors) {
		otel.Handle(err)
	}
}

func (l *logger) emit(ctx context.Context, r log.Record, processors []Processor) []error {
	if !l.provider.beginProcessorOperation() {
		return nil
	}
	defer l.provider.endProcessorOperation()

	l.recordCreated(ctx)
	newRecord := l.newRecord(ctx, r)
	var errs []error
	// 同一次 Emit 内按注册顺序共享一个 *Record；前面的脱敏或补充属性操作
	// 会被后面的处理器看到。不同 Emit 可并发进入同一处理器，实现者仍需同步。
	for _, processor := range processors {
		if err := processor.OnEmit(ctx, &newRecord); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

func (l *logger) recordCreated(ctx context.Context) {
	if l.recCntIncr != nil {
		l.recCntIncr(ctx)
	}
}

// Enabled returns true if at least one Processor held by the LoggerProvider
// that created the logger will process a record for the provided context and
// param.
//
// Enabled returns false after the LoggerProvider that created l starts shutting
// down.
//
// If it is not possible to definitively determine whether the record will be
// processed, true will be returned by default. A value of false will only be
// returned if it can be positively verified that no Processor will process it.
func (l *logger) Enabled(ctx context.Context, param log.EnabledParameters) bool {
	// 中文导读：Enabled 是桥接层构造昂贵日志内容前的预判，多个处理器取逻辑“或”。
	// Emit 不会先调用它，也不按先前的结果跳过某个处理器；需要过滤的处理器
	// 必须在 OnEmit 根据完整记录再次判断，不能把 Enabled 当成强制过滤关卡。
	p := EnabledParameters{
		InstrumentationScope: l.instrumentationScope,
		Severity:             param.Severity,
		EventName:            param.EventName,
	}

	processors := l.provider.processors
	if len(processors) == 0 || !l.provider.beginProcessorOperation() {
		return false
	}
	defer l.provider.endProcessorOperation()

	for _, processor := range processors {
		if processor.Enabled(ctx, p) {
			// At least one Processor will process the Record.
			return true
		}
	}
	// No Processor will process the record.
	return false
}

func (l *logger) newRecord(ctx context.Context, r log.Record) Record {
	// 中文导读：Go 在调用参数中显式传递 context；这里没有隐式读取线程当前 Span。
	// 只读取 SpanContext 就能关联日志，无需该 Span 正在记录或已被采样。
	// 因而 traceFlags 的 sampled 位不是日志开关；没有关联上下文时 ID 为零值。
	// 记录路径不会因请求 context 取消就丢弃日志，导出阶段另有自己的取消边界。
	sc := trace.SpanContextFromContext(ctx)

	newRecord := Record{
		eventName:         r.EventName(),
		timestamp:         r.Timestamp(),
		observedTimestamp: r.ObservedTimestamp(),
		severity:          r.Severity(),
		severityText:      r.SeverityText(),

		traceID:    sc.TraceID(),
		spanID:     sc.SpanID(),
		traceFlags: sc.TraceFlags(),

		resource:                  l.provider.resource,
		scope:                     &l.instrumentationScope,
		attributeValueLengthLimit: l.provider.attributeValueLengthLimit,
		attributeValueDepthLimit:  l.provider.attrValueDepthLimit(),
		attributeCountLimit:       l.provider.attributeCountLimit,
		allowDupKeys:              l.provider.allowDupKeys,
	}
	// This ensures we deduplicate key-value collections in the log body
	newRecord.SetBody(r.Body())

	// This field SHOULD be set once the event is observed by OpenTelemetry.
	// Timestamp 保留事件发生时间；ObservedTimestamp 补 SDK 观察到事件的时间，
	// 这样延迟转发的日志仍能区分“何时发生”与“何时采集”。
	if newRecord.observedTimestamp.IsZero() {
		newRecord.observedTimestamp = now()
	}

	// User-provided exception attributes MUST take precedence. Track message
	// and type independently so a supplied value suppresses only its own
	// derivation.
	var hasExceptionMessage, hasExceptionType bool
	r.WalkAttributes(func(kv attribute.KeyValue) bool {
		switch kv.Key {
		case exceptionMessageKey:
			hasExceptionMessage = true
		case exceptionTypeKey:
			hasExceptionType = true
		}
		newRecord.AddAttributes(kv)
		return true
	})

	// Avoid inspecting the error for attributes when the caller has
	// already supplied the attributes.
	// exception.message 与 exception.type 分别判断是否需要推导；显式属性优先。
	// 即使剩余额度容不下推导值，仍要计入被限制丢弃的数量，便于解释字段缺失。
	if err := r.Err(); err != nil && (!hasExceptionMessage || !hasExceptionType) {
		// Derive missing exception attributes by default, as required by the
		// Logs SDK specification. Attribute limits may constrain generation,
		// but attributes omitted due to those limits are still counted.
		var attrs [2]attribute.KeyValue
		n := 0

		// Derived attributes are buffered until flush, so the current attribute
		// count stays unchanged while missing values are prepared.
		hasLimit := newRecord.hasAttributeCountLimit()
		var remaining int
		if hasLimit {
			remaining = newRecord.attributeCountLimit - newRecord.AttributesLen()
		}
		if !hasExceptionMessage {
			if msg := err.Error(); msg != "" {
				if hasLimit && remaining <= n {
					dropped := 1
					if !hasExceptionType {
						// Every non-nil error has a concrete type, so avoid
						// resolving it when it cannot be retained.
						dropped++
					}
					newRecord.addDropped(dropped)
					goto flush
				}
				attrs[n] = exceptionMessageKey.String(msg)
				n++
			}
		}
		if !hasExceptionType {
			if hasLimit && remaining <= n {
				newRecord.addDropped(1)
				goto flush
			}
			if errType := errorType(err); errType != "" {
				attrs[n] = exceptionTypeKey.String(errType)
				n++
			}
		}

	flush:
		if n > 0 {
			newRecord.addAttrs(attrs[:n])
		}
	}

	return newRecord
}

func errorType(err error) string {
	// 这个局部小接口是 Go 的能力探测：错误类型只要具备 ErrorType 方法就可
	// 提供稳定类型名，无需依赖 SDK 类型。普通错误再使用反射作后备解析。
	if et, ok := err.(interface{ ErrorType() string }); ok {
		if s := et.ErrorType(); s != "" {
			return s
		}
	}

	t := reflect.TypeOf(unwrapFmtWrapped(err))
	if t == nil {
		return ""
	}

	pkg, name := t.PkgPath(), t.Name()
	if pkg != "" && name != "" {
		return pkg + "." + name
	}

	// The type has no package path or name (predeclared, not-defined,
	// or alias for a not-defined type).
	//
	// The type has no package path or name (predeclared, not-defined,
	// or alias for a not-defined type).
	//
	// This is not guaranteed to be unique, but is a best effort.
	return t.String()
}

var fmtWrapErrorType = reflect.TypeOf(fmt.Errorf("wrapped: %w", errors.New("err")))

func unwrapFmtWrapped(err error) error {
	for reflect.TypeOf(err) == fmtWrapErrorType {
		u := errors.Unwrap(err)
		if u == nil {
			return err // When the wrapped error is nil, use the concrete type of the wrapper.
		}
		err = u
	}
	return err
}
