// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package trace

import (
	"context"
	"encoding/binary"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Sampler decides whether a trace should be sampled and exported.
//
// 中文导读：采样器属于 Start 热路径，多个 goroutine 可以通过同一 Provider 并发调用它。
// 自定义实现应快速返回并自行保护共享可变状态；此接口不会替实现加锁或发起后台任务。
type Sampler interface {
	// DO NOT CHANGE: any modification will not be backwards compatible and
	// must never be done outside of a new major release.

	// ShouldSample returns a SamplingResult based on a decision made from the
	// passed parameters.
	ShouldSample(parameters SamplingParameters) SamplingResult
	// DO NOT CHANGE: any modification will not be backwards compatible and
	// must never be done outside of a new major release.

	// Description returns information describing the Sampler.
	Description() string
	// DO NOT CHANGE: any modification will not be backwards compatible and
	// must never be done outside of a new major release.
}

// SamplingParameters contains the values passed to a Sampler.
type SamplingParameters struct {
	ParentContext context.Context
	TraceID       trace.TraceID
	Name          string
	Kind          trace.SpanKind
	Attributes    []attribute.KeyValue
	Links         []trace.Link
}

// SamplingDecision indicates whether a span is dropped, recorded and/or sampled.
type SamplingDecision uint8

// Valid sampling decisions.
//
// 中文导读：三种决策对应“不记录”“记录但不采样”“记录且采样”。
// 记录决策决定是否积累数据并进入处理器，Sampled 位决定内置导出处理器是否接收。
// RecordOnly 可以用于本地统计；它不会被内置 Simple/BatchSpanProcessor 发给 Exporter。
const (
	// Drop will not record the span and all attributes/events will be dropped.
	Drop SamplingDecision = iota

	// RecordOnly indicates the span's IsRecording method returns true, but trace.FlagsSampled flag
	// must not be set.
	RecordOnly

	// RecordAndSample indicates the span's IsRecording method returns true and trace.FlagsSampled flag
	// must be set.
	RecordAndSample
)

// SamplingResult conveys a SamplingDecision, set of Attributes and a Tracestate.
type SamplingResult struct {
	Decision   SamplingDecision
	Attributes []attribute.KeyValue
	Tracestate trace.TraceState
}

type traceIDRatioSampler struct {
	traceIDUpperBound uint64
	description       string
}

func (ts traceIDRatioSampler) ShouldSample(p SamplingParameters) SamplingResult {
	// 中文导读：这里不为每个 Span 再抽一次随机数，而是把 TraceID 的一部分映射到阈值区间。
	// 相同 TraceID 和相同阈值会得出相同结果；比例含义依赖 TraceID 具备合适的随机性。
	// 取后 8 字节再右移 1 位得到 63 位数，与构造时 fraction * 2^63 的上界对应。
	state := trace.SpanContextFromContext(p.ParentContext).TraceState()
	x := binary.BigEndian.Uint64(p.TraceID[8:16]) >> 1
	if x < ts.traceIDUpperBound {
		return SamplingResult{
			Decision:   RecordAndSample,
			Tracestate: state,
		}
	}
	return SamplingResult{
		Decision:   Drop,
		Tracestate: state,
	}
}

func (ts traceIDRatioSampler) Description() string {
	return ts.description
}

// TraceIDRatioBased samples a given fraction of traces. Fractions >= 1 will
// always sample. Fractions < 0 are treated as zero. To respect the
// parent trace's `SampledFlag`, the `TraceIDRatioBased` sampler should be used
// as a delegate of a `Parent` sampler.
//
//nolint:revive // revive complains about stutter of `trace.TraceIDRatioBased`
func TraceIDRatioBased(fraction float64) Sampler {
	// Cannot use AlwaysSample() and NeverSample(), must return spec-compliant descriptions.
	// See https://opentelemetry.io/docs/specs/otel/trace/sdk/#traceidratiobased.
	if fraction >= 1 {
		return predeterminedSampler{
			description: "TraceIDRatioBased{1}",
			decision:    RecordAndSample,
		}
	}

	if fraction <= 0 {
		return predeterminedSampler{
			description: "TraceIDRatioBased{0}",
			decision:    Drop,
		}
	}

	return &traceIDRatioSampler{
		traceIDUpperBound: uint64(fraction * (1 << 63)),
		description:       fmt.Sprintf("TraceIDRatioBased{%g}", fraction),
	}
}

type alwaysOnSampler struct{}

func (alwaysOnSampler) ShouldSample(p SamplingParameters) SamplingResult {
	return SamplingResult{
		Decision:   RecordAndSample,
		Tracestate: trace.SpanContextFromContext(p.ParentContext).TraceState(),
	}
}

func (alwaysOnSampler) Description() string {
	// https://opentelemetry.io/docs/specs/otel/trace/sdk/#alwayson
	return "AlwaysOnSampler"
}

// AlwaysSample returns a Sampler that samples every trace.
// Be careful about using this sampler in a production application with
// significant traffic: a new trace will be started and exported for every
// request.
func AlwaysSample() Sampler {
	return alwaysOnSampler{}
}

type alwaysOffSampler struct{}

func (alwaysOffSampler) ShouldSample(p SamplingParameters) SamplingResult {
	return SamplingResult{
		Decision:   Drop,
		Tracestate: trace.SpanContextFromContext(p.ParentContext).TraceState(),
	}
}

func (alwaysOffSampler) Description() string {
	// https://opentelemetry.io/docs/specs/otel/trace/sdk/#alwaysoff
	return "AlwaysOffSampler"
}

// NeverSample returns a Sampler that samples no traces.
func NeverSample() Sampler {
	return alwaysOffSampler{}
}

type predeterminedSampler struct {
	description string
	decision    SamplingDecision
}

func (s predeterminedSampler) ShouldSample(p SamplingParameters) SamplingResult {
	return SamplingResult{
		Decision:   s.decision,
		Tracestate: trace.SpanContextFromContext(p.ParentContext).TraceState(),
	}
}

func (s predeterminedSampler) Description() string {
	return s.description
}

// ParentBased returns a sampler decorator which behaves differently,
// based on the parent of the span. If the span has no parent,
// the decorated sampler is used to make sampling decision. If the span has
// a parent, depending on whether the parent is remote and whether it
// is sampled, one of the following samplers will apply:
//   - remoteParentSampled(Sampler) (default: AlwaysOn)
//   - remoteParentNotSampled(Sampler) (default: AlwaysOff)
//   - localParentSampled(Sampler) (default: AlwaysOn)
//   - localParentNotSampled(Sampler) (default: AlwaysOff)
func ParentBased(root Sampler, samplers ...ParentBasedSamplerOption) Sampler {
	// 中文导读：装饰器保存另一个 Sampler，在满足条件时把决策委托给它，不依赖类继承。
	// root 只处理没有有效父 Span 的情况；默认的四个父分支都跟随父 Span 的 Sampled 位。
	// 若希望根按比例抽样、子 Span 跟随上游，组合 ParentBased(TraceIDRatioBased(...))。
	return parentBased{
		root:   root,
		config: configureSamplersForParentBased(samplers),
	}
}

type parentBased struct {
	root   Sampler
	config samplerConfig
}

func configureSamplersForParentBased(samplers []ParentBasedSamplerOption) samplerConfig {
	c := samplerConfig{
		remoteParentSampled:    AlwaysSample(),
		remoteParentNotSampled: NeverSample(),
		localParentSampled:     AlwaysSample(),
		localParentNotSampled:  NeverSample(),
	}

	for _, so := range samplers {
		c = so.apply(c)
	}

	return c
}

// samplerConfig is a group of options for parentBased sampler.
type samplerConfig struct {
	remoteParentSampled, remoteParentNotSampled Sampler
	localParentSampled, localParentNotSampled   Sampler
}

// ParentBasedSamplerOption configures the sampler for a particular sampling case.
type ParentBasedSamplerOption interface {
	apply(samplerConfig) samplerConfig
}

// WithRemoteParentSampled sets the sampler for the case of sampled remote parent.
// If s is nil, this option has no effect.
func WithRemoteParentSampled(s Sampler) ParentBasedSamplerOption {
	return remoteParentSampledOption{s}
}

type remoteParentSampledOption struct {
	s Sampler
}

func (o remoteParentSampledOption) apply(config samplerConfig) samplerConfig {
	if o.s != nil {
		config.remoteParentSampled = o.s
	}
	return config
}

// WithRemoteParentNotSampled sets the sampler for the case of remote parent
// which is not sampled.
// If s is nil, this option has no effect.
func WithRemoteParentNotSampled(s Sampler) ParentBasedSamplerOption {
	return remoteParentNotSampledOption{s}
}

type remoteParentNotSampledOption struct {
	s Sampler
}

func (o remoteParentNotSampledOption) apply(config samplerConfig) samplerConfig {
	if o.s != nil {
		config.remoteParentNotSampled = o.s
	}
	return config
}

// WithLocalParentSampled sets the sampler for the case of sampled local parent.
// If s is nil, this option has no effect.
func WithLocalParentSampled(s Sampler) ParentBasedSamplerOption {
	return localParentSampledOption{s}
}

type localParentSampledOption struct {
	s Sampler
}

func (o localParentSampledOption) apply(config samplerConfig) samplerConfig {
	if o.s != nil {
		config.localParentSampled = o.s
	}
	return config
}

// WithLocalParentNotSampled sets the sampler for the case of local parent
// which is not sampled.
// If s is nil, this option has no effect.
func WithLocalParentNotSampled(s Sampler) ParentBasedSamplerOption {
	return localParentNotSampledOption{s}
}

type localParentNotSampledOption struct {
	s Sampler
}

func (o localParentNotSampledOption) apply(config samplerConfig) samplerConfig {
	if o.s != nil {
		config.localParentNotSampled = o.s
	}
	return config
}

func (pb parentBased) ShouldSample(p SamplingParameters) SamplingResult {
	// Remote 表示父身份来自跨进程传播，不表示网络地址或父 SpanKind。
	// 本地/远端以及采样/未采样共四种路径，允许分别配置；有效父身份不要求本地有记录对象。
	psc := trace.SpanContextFromContext(p.ParentContext)
	if psc.IsValid() {
		if psc.IsRemote() {
			if psc.IsSampled() {
				return pb.config.remoteParentSampled.ShouldSample(p)
			}
			return pb.config.remoteParentNotSampled.ShouldSample(p)
		}

		if psc.IsSampled() {
			return pb.config.localParentSampled.ShouldSample(p)
		}
		return pb.config.localParentNotSampled.ShouldSample(p)
	}
	return pb.root.ShouldSample(p)
}

func (pb parentBased) Description() string {
	return fmt.Sprintf(
		"ParentBased{root:%s,remoteParentSampled:%s,"+
			"remoteParentNotSampled:%s,localParentSampled:%s,localParentNotSampled:%s}",
		pb.root.Description(),
		pb.config.remoteParentSampled.Description(),
		pb.config.remoteParentNotSampled.Description(),
		pb.config.localParentSampled.Description(),
		pb.config.localParentNotSampled.Description(),
	)
}

// AlwaysRecord returns a sampler decorator which ensures that every span
// is passed to the SpanProcessor, even those that would be normally dropped.
// It converts `Drop` decisions from the root sampler into `RecordOnly` decisions,
// allowing processors to see all spans without sending them to exporters. This is
// typically used to enable accurate span-to-metrics processing.
func AlwaysRecord(root Sampler) Sampler {
	return alwaysRecord{root}
}

type alwaysRecord struct {
	root Sampler
}

func (ar alwaysRecord) ShouldSample(p SamplingParameters) SamplingResult {
	// 中文导读：AlwaysRecord 只把 Drop 提升为 RecordOnly，保留“未采样”的传播标志。
	// 这样处理器能观察更多操作，同时内置导出处理器仍按 Sampled 位过滤。
	// 当前 Drop 分支构造新的结果并沿用父 TraceState；它不会透传原 Drop 结果的附加属性。
	rootSamplerSamplingResult := ar.root.ShouldSample(p)
	if rootSamplerSamplingResult.Decision == Drop {
		return SamplingResult{
			Decision:   RecordOnly,
			Tracestate: trace.SpanContextFromContext(p.ParentContext).TraceState(),
		}
	}
	return rootSamplerSamplingResult
}

func (ar alwaysRecord) Description() string {
	return "AlwaysRecord{root:" + ar.root.Description() + "}"
}
