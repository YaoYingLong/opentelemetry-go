// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package otlptrace

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/internal/tracetransform"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
)

var errAlreadyStarted = errors.New("already started")

// Exporter exports trace data in the OTLP wire format.
//
// 中文导读：此层把 Trace SDK 的 SpanExporter 接口适配到 OTLP Client。
// 批处理与排队由上游 SpanProcessor 决定；这里没有自己的后台发送队列。
// gRPC/HTTP 的连接、请求编码及重试细节留给 Client，以便共用数据转换逻辑。
type Exporter struct {
	client Client

	mu      sync.RWMutex
	started bool

	startOnce sync.Once
	stopOnce  sync.Once
}

// ExportSpans exports a batch of spans.
func (e *Exporter) ExportSpans(ctx context.Context, ss []tracesdk.ReadOnlySpan) error {
	// 中文导读：SDK 的只读 span 先按 Resource、InstrumentationScope 分组，
	// 转成 ResourceSpans -> ScopeSpans -> Span 的 OTLP 消息树；空输入不发请求。
	// UploadTraces 在本调用中同步执行，重试等待也属于这次导出耗时。
	// %w 保留底层错误身份，调用方仍能用 errors.Is 判断取消等具体原因。
	protoSpans := tracetransform.Spans(ss)
	if len(protoSpans) == 0 {
		return nil
	}

	err := e.client.UploadTraces(ctx, protoSpans)
	if err != nil {
		return fmt.Errorf("traces export: %w", err)
	}
	return nil
}

// Start establishes a connection to the receiving endpoint.
func (e *Exporter) Start(ctx context.Context) error {
	// sync.Once 控制的是“只尝试一次”，并非“直到成功”；首次 Client.Start
	// 失败后也不能在同一 Exporter 上重新 Start，后续调用返回 errAlreadyStarted。
	err := errAlreadyStarted
	e.startOnce.Do(func() {
		e.mu.Lock()
		e.started = true
		e.mu.Unlock()
		err = e.client.Start(ctx)
	})

	return err
}

// Shutdown flushes all exports and closes all connections to the receiving endpoint.
func (e *Exporter) Shutdown(ctx context.Context) error {
	// mu 保护 started 状态，stopOnce 限制 Stop 的调用次数；这把锁不包住网络
	// 上传。Stop 与 UploadTraces 的并发协调必须由 Client 自身实现。
	e.mu.RLock()
	started := e.started
	e.mu.RUnlock()

	if !started {
		return nil
	}

	var err error

	e.stopOnce.Do(func() {
		err = e.client.Stop(ctx)
		e.mu.Lock()
		e.started = false
		e.mu.Unlock()
	})

	return err
}

var _ tracesdk.SpanExporter = (*Exporter)(nil)

// New constructs a new Exporter and starts it.
func New(ctx context.Context, client Client) (*Exporter, error) {
	exp := NewUnstarted(client)
	if err := exp.Start(ctx); err != nil {
		return nil, err
	}
	return exp, nil
}

// NewUnstarted constructs a new Exporter and does not start it.
func NewUnstarted(client Client) *Exporter {
	return &Exporter{
		client: client,
	}
}

// MarshalLog is the marshaling function used by the logging system to represent this Exporter.
func (e *Exporter) MarshalLog() any {
	return struct {
		Type   string
		Client string
	}{
		Type:   "otlptrace",
		Client: fmt.Sprintf("%T", e.client),
	}
}
