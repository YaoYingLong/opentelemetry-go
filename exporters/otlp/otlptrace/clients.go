// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package otlptrace

import (
	"context"

	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// Client manages connections to the collector, handles the
// transformation of data into wire format, and the transmission of that
// data to the collector.
//
// 中文导读：Client 收到的已经是 OTLP protobuf 对象，具体协议实现再把它编码
// 成请求并发送。Exporter 对接口的依赖允许 gRPC 与 HTTP 共用同一上层实现；
// Go 类型只要具备这些方法就满足接口，无需显式声明 implements。
// 内置客户端在 UploadTraces 内处理可重试状态、退避及请求超时；修改协议行为
// 应继续阅读各协议的 client.go 和 internal/retry，而不是在此抽象层重复重试。
type Client interface {
	// DO NOT CHANGE: any modification will not be backwards compatible and
	// must never be done outside of a new major release.

	// Start should establish connection(s) to endpoint(s). It is
	// called just once by the exporter, so the implementation
	// does not need to worry about idempotence and locking.
	Start(ctx context.Context) error
	// DO NOT CHANGE: any modification will not be backwards compatible and
	// must never be done outside of a new major release.

	// Stop should close the connections. The function is called
	// only once by the exporter, so the implementation does not
	// need to worry about idempotence, but it may be called
	// concurrently with UploadTraces, so proper
	// locking is required. The function serves as a
	// synchronization point - after the function returns, the
	// process of closing connections is assumed to be finished.
	Stop(ctx context.Context) error
	// DO NOT CHANGE: any modification will not be backwards compatible and
	// must never be done outside of a new major release.

	// UploadTraces should transform the passed traces to the wire
	// format and send it to the collector. May be called
	// concurrently.
	UploadTraces(ctx context.Context, protoSpans []*tracepb.ResourceSpans) error
	// DO NOT CHANGE: any modification will not be backwards compatible and
	// must never be done outside of a new major release.
}
