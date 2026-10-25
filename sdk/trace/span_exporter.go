// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package trace

import "context"

// SpanExporter handles the delivery of spans to external receivers. This is
// the final component in the trace export pipeline.
//
// 中文导读：Exporter 是传输边界，具体实现可使用 OTLP gRPC、HTTP、标准输出或其他接收端。
// Processor 管理调度和缓冲，Exporter 管理序列化、传输以及协议相关的重试。
type SpanExporter interface {
	// DO NOT CHANGE: any modification will not be backwards compatible and
	// must never be done outside of a new major release.

	// ExportSpans exports a batch of spans.
	//
	// This function is called synchronously, so there is no concurrency
	// safety requirement. However, due to the synchronous calling pattern,
	// it is critical that all timeouts and cancellations contained in the
	// passed context must be honored.
	//
	// Any retry logic must be contained in this function. The SDK that
	// calls this function will not implement any retry logic. All errors
	// returned by this function are considered unrecoverable and will be
	// reported to a configured error Handler.
	// 内置处理器串行调用其 ExportSpans；若手动把同一实例共享给多个处理器，需另行考虑并发。
	// 批处理器在本调用返回后会清空并复用 spans 的底层数组，Exporter 不应保留该切片异步使用。
	// 如实现内部必须延迟消费，应先建立自己拥有的数据副本，并维持同步导出接口的完成语义。
	// Context 取消是协作机制，不会强制终止 goroutine；网络请求与重试等待都需要主动遵守它。
	ExportSpans(ctx context.Context, spans []ReadOnlySpan) error
	// DO NOT CHANGE: any modification will not be backwards compatible and
	// must never be done outside of a new major release.

	// Shutdown notifies the exporter of a pending halt to operations. The
	// exporter is expected to perform any cleanup or synchronization it
	// requires while honoring all timeouts and cancellations contained in
	// the passed context.
	Shutdown(ctx context.Context) error
	// DO NOT CHANGE: any modification will not be backwards compatible and
	// must never be done outside of a new major release.
}
