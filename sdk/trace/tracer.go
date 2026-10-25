// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package trace

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/sdk/instrumentation"
	"go.opentelemetry.io/otel/sdk/trace/internal/observ"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/embedded"
)

type tracer struct {
	embedded.Tracer

	provider             *TracerProvider
	instrumentationScope instrumentation.Scope

	inst observ.Tracer
}

var _ trace.Tracer = &tracer{}

// Start starts a Span and returns it along with a context containing it.
//
// The Span is created with the provided name and as a child of any existing
// span context found in the passed context. The created Span will be
// configured appropriately by any SpanOption passed.
func (tr *tracer) Start(
	ctx context.Context,
	name string,
	options ...trace.SpanStartOption,
) (context.Context, trace.Span) {
	// 中文导读：主线从这里进入：生成 Span/采样 -> 放入新 Context -> OnStart -> 返回。
	// Go 通过显式传递 ctx 传播当前 Span；应把返回的新 Context 继续传给下游调用。
	// ContextWithSpan 派生新 Context，不会改写调用方传入的 Context，也不会设置线程局部变量。
	// ctx 取消不阻止记录遥测；取消请求和关闭遥测管线分别由 Context 与 Provider 管理。
	config := trace.NewSpanStartConfig(options...)

	if ctx == nil {
		// Prevent trace.ContextWithSpan from panicking.
		ctx = context.Background()
	}

	// For local spans created by this SDK, track child span count.
	if p := trace.SpanFromContext(ctx); p != nil {
		if sdkSpan, ok := p.(*recordingSpan); ok {
			sdkSpan.addChild()
		}
	}

	s := tr.newSpan(ctx, name, &config)
	newCtx := trace.ContextWithSpan(ctx, s)
	if tr.inst.Enabled() {
		if o, ok := s.(interface{ setOrigCtx(context.Context) }); ok {
			// If this is a recording span, store the original context.
			// This allows later retrieval of baggage and other information
			// that may have been stored in the context at span start time and
			// to avoid the allocation of repeatedly calling
			// trace.ContextWithSpan.
			o.setOrigCtx(newCtx)
		}
		psc := trace.SpanContextFromContext(ctx)
		tr.inst.SpanStarted(newCtx, psc, s)
	}

	// s.(ReadWriteSpan) 是带 ok 的类型断言：仅在具体实现具备读写能力时调用处理器。
	// OnStart 在调用 Start 的 goroutine 中同步执行，传入的仍是原始父 Context。
	// 处理器可在此补属性，但这些属性产生得比采样决策晚，不能反过来影响本次头部采样。
	if rw, ok := s.(ReadWriteSpan); ok && s.IsRecording() {
		sps := tr.provider.getSpanProcessors()
		for _, sp := range sps {
			// Use original context.
			sp.sp.OnStart(ctx, rw)
		}
	}
	if rtt, ok := s.(runtimeTracer); ok {
		newCtx = rtt.runtimeTrace(newCtx)
	}

	return newCtx, s
}

type runtimeTracer interface {
	// runtimeTrace starts a "runtime/trace".Task for the span and
	// returns a context containing the task.
	runtimeTrace(ctx context.Context) context.Context
}

// newSpan returns a new configured span.
func (tr *tracer) newSpan(ctx context.Context, name string, config *trace.SpanConfig) trace.Span {
	// 中文导读：SpanContext 是传播用的身份数据；Context 则是携带它及其他请求信息的容器。
	// 零值 SpanContext 没有有效 ID。WithNewRoot 在这里屏蔽原父 Span，让采样器也按根判断。
	// 这只替换 Context 中的 Span 值，不会删除该 Context 携带的其他信息。
	// If told explicitly to make this a new root use a zero value SpanContext
	// as a parent which contains an invalid trace ID and is not remote.
	var psc trace.SpanContext
	if config.NewRoot() {
		ctx = trace.ContextWithSpanContext(ctx, psc)
	} else {
		psc = trace.SpanContextFromContext(ctx)
	}

	// If there is a valid parent trace ID, use it to ensure the continuity of
	// the trace. Always generate a new span ID so other components can rely
	// on a unique span ID, even if the Span is non-recording.
	var tid trace.TraceID
	var sid trace.SpanID
	if !psc.TraceID().IsValid() {
		tid, sid = tr.provider.idGenerator.NewIDs(ctx)
	} else {
		tid = psc.TraceID()
		sid = tr.provider.idGenerator.NewSpanID(ctx, tid)
	}

	// 采样器在 Start 时拿到 ID、初始属性和 Links；End 时不会因后来发生错误而重新采样。
	// 所以需要参与采样的属性应通过 Start 选项提供，而不是 Start 返回后再 SetAttributes。
	samplingResult := tr.provider.sampler.ShouldSample(SamplingParameters{
		ParentContext: ctx,
		TraceID:       tid,
		Name:          name,
		Kind:          config.SpanKind(),
		Attributes:    config.Attributes(),
		Links:         config.Links(),
	})

	scc := trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     sid,
		TraceState: samplingResult.Tracestate,
	}
	if isSampled(samplingResult) {
		scc.TraceFlags = psc.TraceFlags() | trace.FlagsSampled
	} else {
		// &^ 是 Go 的“按位清除”：仅清除 Sampled 位，保留父上下文的其他标志位。
		scc.TraceFlags = psc.TraceFlags() &^ trace.FlagsSampled
	}
	sc := trace.NewSpanContext(scc)

	if !isRecording(samplingResult) {
		// Drop 仍返回带有新 SpanID 的 Span，后续子调用或跨服务传播仍能关联这条 Trace。
		// 它不分配完整记录结构，也不会收到本 SDK 的 OnStart/OnEnd 回调。
		return tr.newNonRecordingSpan(sc)
	}
	return tr.newRecordingSpan(ctx, psc, sc, name, samplingResult, config)
}

// newRecordingSpan returns a new configured recordingSpan.
func (tr *tracer) newRecordingSpan(
	ctx context.Context,
	psc, sc trace.SpanContext,
	name string,
	sr SamplingResult,
	config *trace.SpanConfig,
) *recordingSpan {
	// 中文导读：RecordOnly 与 RecordAndSample 都进入此分支，是否记录与是否导出是两层判断。
	// Provider 提供共享配置；这里创建的结构只保存本次操作的数据，并由自己的锁保护。
	startTime := config.Timestamp()
	if startTime.IsZero() {
		startTime = time.Now()
	}

	s := &recordingSpan{
		// Do not pre-allocate the attributes slice here! Doing so will
		// allocate memory that is likely never going to be used, or if used,
		// will be over-sized. The default Go compiler has been tested to
		// dynamically allocate needed space very well. Benchmarking has shown
		// it to be more performant than what we can predetermine here,
		// especially for the common use case of few to no added
		// attributes.

		parent:      psc,
		spanContext: sc,
		spanKind:    trace.ValidateSpanKind(config.SpanKind()),
		name:        name,
		startTime:   startTime,
		events:      newEvictedQueueEvent(tr.provider.spanLimits.EventCountLimit),
		links:       newEvictedQueueLink(tr.provider.spanLimits.LinkCountLimit),
		tracer:      tr,
	}

	for _, l := range config.Links() {
		s.AddLink(l)
	}

	// 切片后的 ... 将切片展开成可变参数。先写采样器属性，再写 Start 属性，后者同键覆盖。
	// 统一走 SetAttributes/AddLink，保证构造时的数据也遵守去重与资源限制。
	s.SetAttributes(sr.Attributes...)
	s.SetAttributes(config.Attributes()...)

	if tr.inst.Enabled() {
		// Propagate any existing values from the context with the new span to
		// the measurement context.
		ctx = trace.ContextWithSpan(ctx, s)
		tr.inst.SpanLive(ctx, s)
	}

	return s
}

// newNonRecordingSpan returns a new configured nonRecordingSpan.
func (tr *tracer) newNonRecordingSpan(sc trace.SpanContext) nonRecordingSpan {
	return nonRecordingSpan{tracer: tr, sc: sc}
}
