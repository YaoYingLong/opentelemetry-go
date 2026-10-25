// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package propagation

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"

	"go.opentelemetry.io/otel/propagation/internal/hextable"
	"go.opentelemetry.io/otel/trace"
)

const (
	supportedVersion  = 0
	maxVersion        = 254
	traceparentHeader = "traceparent"
	tracestateHeader  = "tracestate"
	delimiter         = "-"
)

// TraceContext is a propagator that supports the W3C Trace Context format
// (https://www.w3.org/TR/trace-context/)
//
// This propagator will propagate the traceparent and tracestate headers to
// guarantee traces are not broken. It is up to the users of this propagator
// to choose if they want to participate in a trace by modifying the
// traceparent header and relevant parts of the tracestate header containing
// their proprietary information.
type TraceContext struct{}

var (
	_           TextMapPropagator = TraceContext{}
	versionPart                   = fmt.Sprintf("%.2X", supportedVersion)
)

// Inject injects the trace context from ctx into carrier.
func (TraceContext) Inject(ctx context.Context, carrier TextMapCarrier) {
	// 中文导读：进程内通过 context 传对象，跨进程只能传可序列化的标识。
	// Inject 读取当前 span 的 SpanContext 写入载体，不会导出 span 的属性、事件或时间。
	// 客户端集成通常先 Start 出站 span，再用 Start 返回的 context 注入请求头，
	// 这样下游看到的 parent ID 才是这个出站 span 的 ID。
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return
	}

	if ts := sc.TraceState().String(); ts != "" {
		carrier.Set(tracestateHeader, ts)
	}

	// Preserve only the spec-defined flags: sampled (0x01) and random (0x02).
	// 当前 checkout 同时支持 sampled 与 random 标志；这里的位与是在保留已支持的位，
	// 不是“只传采样位”。阅读 flags 时应以本版本的常量和测试为准。
	flags := sc.TraceFlags() & (trace.FlagsSampled | trace.FlagsRandom)

	var sb strings.Builder
	sb.Grow(2 + 32 + 16 + 2 + 3)
	_, _ = sb.WriteString(versionPart)
	traceID := sc.TraceID()
	spanID := sc.SpanID()
	flagByte := [1]byte{byte(flags)}
	var buf [32]byte
	for _, src := range [][]byte{traceID[:], spanID[:], flagByte[:]} {
		_ = sb.WriteByte(delimiter[0])
		n := hex.Encode(buf[:], src)
		_, _ = sb.Write(buf[:n])
	}
	carrier.Set(traceparentHeader, sb.String())
}

// Extract reads tracecontext from the carrier into a returned Context.
//
// The returned Context will be a copy of ctx and contain the extracted
// tracecontext as the remote SpanContext. If the extracted tracecontext is
// invalid, the passed ctx will be returned directly instead.
func (tc TraceContext) Extract(ctx context.Context, carrier TextMapCarrier) context.Context {
	// 接收方先解析远端父上下文，再用返回值 Start 服务端/消费者 span。
	// 无效的外部 header 被忽略，保留调用者原有 context；遥测输入不应破坏正常业务处理。
	sc := tc.extract(carrier)
	if !sc.IsValid() {
		return ctx
	}
	return trace.ContextWithRemoteSpanContext(ctx, sc)
}

func (TraceContext) extract(carrier TextMapCarrier) trace.SpanContext {
	h := carrier.Get(traceparentHeader)
	if h == "" {
		return trace.SpanContext{}
	}

	var ver [1]byte
	if !extractPart(ver[:], &h, 2) {
		return trace.SpanContext{}
	}
	version := int(ver[0])
	if version > maxVersion {
		return trace.SpanContext{}
	}

	var scc trace.SpanContextConfig
	if !extractPart(scc.TraceID[:], &h, 32) {
		return trace.SpanContext{}
	}
	if !extractPart(scc.SpanID[:], &h, 16) {
		return trace.SpanContext{}
	}

	var opts [1]byte
	if !extractPart(opts[:], &h, 2) {
		return trace.SpanContext{}
	}
	if version == 0 && (h != "" || opts[0] > 3) {
		// version 0 does not allow extra fields or reserved flag bits.
		return trace.SpanContext{}
	}

	scc.TraceFlags = trace.TraceFlags(opts[0]) & //nolint:gosec // slice size already checked.
		(trace.FlagsSampled | trace.FlagsRandom)

	// Ignore the error returned here. Failure to parse tracestate MUST NOT
	// affect the parsing of traceparent according to the W3C tracecontext
	// specification.
	// traceparent 决定链路身份，tracestate 是附加的供应商状态。
	// 附加状态解析失败不应连带丢失有效父链路，因此这里有意忽略这一处错误。
	scc.TraceState, _ = trace.ParseTraceState(carrier.Get(tracestateHeader))
	scc.Remote = true

	sc := trace.NewSpanContext(scc)
	if !sc.IsValid() {
		return trace.SpanContext{}
	}

	return sc
}

func extractPart(dst []byte, h *string, n int) bool {
	// *string 让函数能更新调用者的“剩余输入”；dst 切片则指向调用者提供的字节缓冲区。
	// 先验证固定长度，再两字符一组解码，避免在不可信输入上越界。
	// 返回 bool 表示整个片段是否有效；即使缓冲区写了一部分，失败结果也不会成为父上下文。
	part, left, _ := strings.Cut(*h, delimiter)
	*h = left
	if len(part) != n {
		return false
	}
	// hextable.Rev maps every invalid character to 0xff, including the
	// upper-case A-F the specification disallows. OR-ing every looked-up value
	// together lets a single check detect any invalid character, because no
	// valid value has the upper 4 bits set.
	invalidMark := byte(0)
	for i := 0; i < n; i += 2 {
		hi, lo := hextable.Rev[part[i]], hextable.Rev[part[i+1]]
		dst[i/2] = (hi << 4) | lo
		invalidMark |= hi | lo
	}
	return invalidMark&0xf0 == 0
}

// Fields returns the keys who's values are set with Inject.
func (TraceContext) Fields() []string {
	return []string{traceparentHeader, tracestateHeader}
}
