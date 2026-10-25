// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package global

/*
This file contains the forwarding implementation of the TracerProvider used as
the default global instance. Prior to initialization of an SDK, Tracers
returned by the global TracerProvider will provide no-op functionality. This
means that all Span created prior to initialization are no-op Spans.

Once an SDK has been initialized, all provided no-op Tracers are swapped for
Tracers provided by the SDK defined TracerProvider. However, any Span started
prior to this initialization does not change its behavior. Meaning, the Span
remains a no-op Span.

The implementation to track and swap Tracers locks all new Tracer creation
until the swap is complete. This assumes that this operation is not
performance-critical. If that assumption is incorrect, be sure to configure an
SDK prior to any Tracer creation.
*/

import (
	"context"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/auto/sdk"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/embedded"
)

// tracerProvider is a placeholder for a configured SDK TracerProvider.
//
// All TracerProvider functionality is forwarded to a delegate once
// configured.
type tracerProvider struct {
	// 中文导读：嵌入 embedded.TracerProvider 是 API 演进的编译期约束，
	// 不会自动提供 SDK 的业务实现。真正的 Tracer 方法仍由这个结构体实现。
	embedded.TracerProvider

	mtx      sync.Mutex
	tracers  map[il]*tracer
	delegate trace.TracerProvider
}

// Compile-time guarantee that tracerProvider implements the TracerProvider
// interface.
var _ trace.TracerProvider = &tracerProvider{}

// setDelegate configures p to delegate all TracerProvider functionality to
// provider.
//
// All Tracers provided prior to this function call are switched out to be
// Tracers provided by provider.
//
// It is guaranteed by the caller that this happens only once.
func (p *tracerProvider) setDelegate(provider trace.TracerProvider) {
	// 这里与 Tracer 共用互斥锁，确保安装 SDK 与创建占位 Tracer 不能交错漏接。
	// defer 在函数退出时释放锁，包含提前 return 的路径；它不是启动异步任务。
	p.mtx.Lock()
	defer p.mtx.Unlock()

	p.delegate = provider

	if len(p.tracers) == 0 {
		return
	}

	for _, t := range p.tracers {
		t.setDelegate(provider)
	}

	// 已返回的 *tracer 仍由调用者持有，并已获得 delegate；清空缓存只是释放登记表。
	// 后续 Tracer 调用走 provider，不再需要这张占位对象表。
	p.tracers = nil
}

// Tracer implements TracerProvider.
func (p *tracerProvider) Tracer(name string, opts ...trace.TracerOption) trace.Tracer {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	if p.delegate != nil {
		return p.delegate.Tracer(name, opts...)
	}

	// At this moment it is guaranteed that no sdk is installed, save the tracer in the tracers map.

	c := trace.NewTracerConfig(opts...)
	// 一个埋点库的身份包含名称、版本、schema 与 scope 属性。
	// 缓存的是可复用的 Tracer，而不是一次请求的 Span；不要用请求 ID 动态命名 Tracer。
	key := il{
		name:    name,
		version: c.InstrumentationVersion(),
		schema:  c.SchemaURL(),
		attrs:   c.InstrumentationAttributes(),
	}

	if p.tracers == nil {
		p.tracers = make(map[il]*tracer)
	}

	if val, ok := p.tracers[key]; ok {
		return val
	}

	t := &tracer{name: name, opts: opts, provider: p}
	p.tracers[key] = t
	return t
}

type il struct {
	name    string
	version string
	schema  string
	attrs   attribute.Set
}

// tracer is a placeholder for a trace.Tracer.
//
// All Tracer functionality is forwarded to a delegate once configured.
// Otherwise, all functionality is forwarded to a NoopTracer.
type tracer struct {
	embedded.Tracer

	name     string
	opts     []trace.TracerOption
	provider *tracerProvider

	delegate atomic.Value
}

// Compile-time guarantee that tracer implements the trace.Tracer interface.
var _ trace.Tracer = &tracer{}

// setDelegate configures t to delegate all Tracer functionality to Tracers
// created by provider.
//
// All subsequent calls to the Tracer methods will be passed to the delegate.
//
// It is guaranteed by the caller that this happens only once.
func (t *tracer) setDelegate(provider trace.TracerProvider) {
	t.delegate.Store(provider.Tracer(t.name, t.opts...))
}

// Start implements trace.Tracer by forwarding the call to t.delegate if
// set, otherwise it forwards the call to a NoopTracer.
func (t *tracer) Start(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	// Start 位于每次请求都会经过的路径。安装 SDK 时写入一次 delegate，
	// 此后通过原子读取转发，避免为了检查全局配置而每次获取 Provider 的互斥锁。
	// 接口类型断言只取出先前存入的实现；真正的 ID、采样与 processor 在 SDK tracer 中处理。
	delegate := t.delegate.Load()
	if delegate != nil {
		return delegate.(trace.Tracer).Start(ctx, name, opts...)
	}

	return t.newSpan(ctx, autoInstEnabled, name, opts)
}

// autoInstEnabled determines if the auto-instrumentation SDK span is returned
// from the tracer when not backed by a delegate and auto-instrumentation has
// attached to this process.
//
// The auto-instrumentation is expected to overwrite this value to true when it
// attaches. By default, this will point to false and mean a tracer will return
// a nonRecordingSpan by default.
var autoInstEnabled = new(bool)

// newSpan is called by tracer.Start so auto-instrumentation can attach an eBPF
// uprobe to this code.
//
// "noinline" pragma prevents the method from ever being inlined.
//
//go:noinline
func (t *tracer) newSpan(
	ctx context.Context,
	autoSpan *bool,
	name string,
	opts []trace.SpanStartOption,
) (context.Context, trace.Span) {
	// autoInstEnabled is passed to newSpan via the autoSpan parameter. This is
	// so the auto-instrumentation can define a uprobe for (*t).newSpan and be
	// provided with the address of the bool autoInstEnabled points to. It
	// needs to be a parameter so that pointer can be reliably determined, it
	// should not be read from the global.

	if *autoSpan {
		tracer := sdk.TracerProvider().Tracer(t.name, t.opts...)
		return tracer.Start(ctx, name, opts...)
	}

	// 未接入普通 SDK 且未启用上方自动埋点分支时，保留传入的 SpanContext。
	// 这里不会为本次操作生成新的 ID：既不采集本地数据，也尽量保住已有链路的传播能力。
	// 注意已有 no-op span 不会在 SDK 安装后“升级”；只有之后的 Start 才会使用 delegate。
	s := nonRecordingSpan{sc: trace.SpanContextFromContext(ctx), tracer: t}
	ctx = trace.ContextWithSpan(ctx, s)
	return ctx, s
}

// nonRecordingSpan is a minimal implementation of a Span that wraps a
// SpanContext. It performs no operations other than to return the wrapped
// SpanContext.
type nonRecordingSpan struct {
	embedded.Span

	sc     trace.SpanContext
	tracer *tracer
}

var _ trace.Span = nonRecordingSpan{}

// SpanContext returns the wrapped SpanContext.
func (s nonRecordingSpan) SpanContext() trace.SpanContext { return s.sc }

// IsRecording always returns false.
func (nonRecordingSpan) IsRecording() bool { return false }

// SetStatus does nothing.
func (nonRecordingSpan) SetStatus(codes.Code, string) {}

// SetError does nothing.
func (nonRecordingSpan) SetError(bool) {}

// SetAttributes does nothing.
func (nonRecordingSpan) SetAttributes(...attribute.KeyValue) {}

// End does nothing.
func (nonRecordingSpan) End(...trace.SpanEndOption) {}

// RecordError does nothing.
func (nonRecordingSpan) RecordError(error, ...trace.EventOption) {}

// AddEvent does nothing.
func (nonRecordingSpan) AddEvent(string, ...trace.EventOption) {}

// AddLink does nothing.
func (nonRecordingSpan) AddLink(trace.Link) {}

// SetName does nothing.
func (nonRecordingSpan) SetName(string) {}

func (s nonRecordingSpan) TracerProvider() trace.TracerProvider { return s.tracer.provider }
