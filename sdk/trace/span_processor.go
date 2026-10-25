// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package trace

import (
	"context"
	"sync"
)

// SpanProcessor is a processing pipeline for spans in the trace signal.
// SpanProcessors registered with a TracerProvider and are called at the start
// and end of a Span's lifecycle, and are called in the order they are
// registered.
//
// 中文导读：Processor 连接 Span 生命周期与导出管线；多个处理器按注册顺序分别接收回调。
// OnStart 得到活跃的读写 Span，OnEnd 得到结束后的读取快照，权限与生命周期对应。
// 两类回调都由业务 goroutine 同步调用；不同 Span 的回调可以并发进入同一处理器。
// 自定义处理器应保护自己的共享状态，并把耗时工作交给受控的后台流程。
type SpanProcessor interface {
	// DO NOT CHANGE: any modification will not be backwards compatible and
	// must never be done outside of a new major release.

	// OnStart is called when a span is started. It is called synchronously
	// and should not block.
	OnStart(parent context.Context, s ReadWriteSpan)
	// DO NOT CHANGE: any modification will not be backwards compatible and
	// must never be done outside of a new major release.

	// OnEnd is called when span is finished. It is called synchronously and
	// hence not block.
	OnEnd(s ReadOnlySpan)
	// DO NOT CHANGE: any modification will not be backwards compatible and
	// must never be done outside of a new major release.

	// Shutdown is called when the SDK shuts down. Any cleanup or release of
	// resources held by the processor should be done in this call.
	//
	// Calls to OnStart, OnEnd, or ForceFlush after this has been called
	// should be ignored.
	//
	// All timeouts and cancellations contained in ctx must be honored, this
	// should not block indefinitely.
	Shutdown(ctx context.Context) error
	// DO NOT CHANGE: any modification will not be backwards compatible and
	// must never be done outside of a new major release.

	// ForceFlush exports all ended spans to the configured Exporter that have not yet
	// been exported.  It should only be called when absolutely necessary, such as when
	// using a FaaS provider that may suspend the process after an invocation, but before
	// the Processor can export the completed spans.
	// Flush 是显式的生命周期操作，可以等待导出，但必须服从调用方给定的取消与期限。
	// 它不会结束业务仍在使用的 Span，也不意味着之后不能再接收新的 Span。
	ForceFlush(ctx context.Context) error
	// DO NOT CHANGE: any modification will not be backwards compatible and
	// must never be done outside of a new major release.
}

type spanProcessorState struct {
	// 状态对象的指针在列表快照之间共享，保证注销和 Provider.Shutdown 使用同一个 Once。
	// sync.Once 的语义是执行一次操作，不会缓存并自动重放那次操作返回的 error。
	sp    SpanProcessor
	state sync.Once
}

func newSpanProcessorState(sp SpanProcessor) *spanProcessorState {
	return &spanProcessorState{sp: sp}
}

type spanProcessorStates []*spanProcessorState
