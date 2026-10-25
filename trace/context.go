// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package trace

import "context"

// 中文导读：context.Context 是沿调用链显式传递的容器；SpanContext 是 trace ID、span ID、
// flags 等链路元数据。两者不是同一个类型。这里用包私有的 key 类型隔离存储空间，
// 避免其他包也使用字符串 "span" 作 key 时互相覆盖。
type traceContextKeyType int

const currentSpanKey traceContextKeyType = iota

// ContextWithSpan returns a copy of parent with span set as the current Span.
func ContextWithSpan(parent context.Context, span Span) context.Context {
	// WithValue 派生一个新 context，不修改 parent。调用者必须接住返回值并传给下游；
	// 仅调用此函数不会像 Java 中 makeCurrent/Scope 的常见用法那样安装“线程当前值”。
	// context 本身可并发传递，但其中存的 span 是同一个对象，不会随 context 自动深拷贝。
	return context.WithValue(parent, currentSpanKey, span)
}

// ContextWithSpanContext returns a copy of parent with sc as the current
// Span. The Span implementation that wraps sc is non-recording and performs
// no operations other than to return sc as the SpanContext from the
// SpanContext method.
func ContextWithSpanContext(parent context.Context, sc SpanContext) context.Context {
	// 只有远端标识或只需传播上下文时，不需要构造可记录的 SDK span。
	// 这个轻量包装仍实现 Span 接口，供后续 Start 读取父标识、Inject 写出传播头；
	// 对它调用 SetAttributes/End 没有记录效果。因此“不记录”不等于“无法继续链路”。
	return ContextWithSpan(parent, nonRecordingSpan{sc: sc})
}

// ContextWithRemoteSpanContext returns a copy of parent with rsc set explicitly
// as a remote SpanContext and as the current Span. The Span implementation
// that wraps rsc is non-recording and performs no operations other than to
// return rsc as the SpanContext from the SpanContext method.
func ContextWithRemoteSpanContext(parent context.Context, rsc SpanContext) context.Context {
	// Remote 标记表示父上下文来自进程外，ParentBased 采样会区分远端父和本地父。
	// 这里只建立父上下文，不创建服务端 span；接收方仍需以返回的 context 调用 Tracer.Start。
	return ContextWithSpanContext(parent, rsc.WithRemote(true))
}

// SpanFromContext returns the current Span from ctx.
//
// If no Span is currently set in ctx an implementation of a Span that
// performs no operations is returned.
func SpanFromContext(ctx context.Context) Span {
	if ctx == nil {
		return noopSpanInstance
	}
	// “value, ok := x.(接口)”是 Go 的安全类型断言：缺值或类型不符时 ok 为 false，
	// 不会因断言失败 panic。统一返回 no-op span，让未启用 SDK 的宿主也能执行相同埋点代码。
	if span, ok := ctx.Value(currentSpanKey).(Span); ok {
		return span
	}
	return noopSpanInstance
}

// SpanContextFromContext returns the current Span's SpanContext.
func SpanContextFromContext(ctx context.Context) SpanContext {
	return SpanFromContext(ctx).SpanContext()
}
