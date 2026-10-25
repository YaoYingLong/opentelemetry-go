// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package trace

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/internal/global"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	"go.opentelemetry.io/otel/sdk/internal/attrnorm"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace/internal/observ"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/embedded"
	"go.opentelemetry.io/otel/trace/noop"
)

const defaultTracerName = "go.opentelemetry.io/otel/sdk/tracer"

// tracerProviderConfig.
type tracerProviderConfig struct {
	// processors contains collection of SpanProcessors that are processing pipeline
	// for spans in the trace signal.
	// SpanProcessors registered with a TracerProvider and are called at the start
	// and end of a Span's lifecycle, and are called in the order they are
	// registered.
	processors []SpanProcessor

	// sampler is the default sampler used when creating new spans.
	sampler Sampler

	// idGenerator is used to generate all Span and Trace IDs when needed.
	idGenerator IDGenerator

	// spanLimits defines the attribute, event, and link limits for spans.
	spanLimits SpanLimits

	// resource contains attributes representing an entity that produces telemetry.
	resource *resource.Resource

	// panicRecordingDisabled disables recording exception events from panics.
	panicRecordingDisabled bool
}

// MarshalLog is the marshaling function used by the logging system to represent this Provider.
func (cfg tracerProviderConfig) MarshalLog() any {
	return struct {
		SpanProcessors         []SpanProcessor
		SamplerType            string
		IDGeneratorType        string
		SpanLimits             SpanLimits
		Resource               *resource.Resource
		PanicRecordingDisabled bool
	}{
		SpanProcessors:         cfg.processors,
		SamplerType:            fmt.Sprintf("%T", cfg.sampler),
		IDGeneratorType:        fmt.Sprintf("%T", cfg.idGenerator),
		SpanLimits:             cfg.spanLimits,
		Resource:               cfg.resource,
		PanicRecordingDisabled: cfg.panicRecordingDisabled,
	}
}

// TracerProvider is an OpenTelemetry TracerProvider. It provides Tracers to
// instrumentation so it can trace operational flow through a system.
type TracerProvider struct {
	embedded.TracerProvider

	// 中文导读：Provider 持有整条 Trace 管线的公共配置，Tracer 按埋点作用域区分。
	// 同一 Provider 创建的 Tracer 共享采样器、资源和处理器；通常不应为每个请求新建 Provider。
	// mu 保护 Tracer 缓存及处理器列表的更新；Span 热路径通过原子指针读取列表快照。
	// atomic.Pointer[T] 是带类型的原子指针，不意味着它指向的任意可变对象都自动线程安全。
	mu             sync.Mutex
	namedTracer    map[instrumentation.Scope]*tracer
	spanProcessors atomic.Pointer[spanProcessorStates]

	isShutdown atomic.Bool

	// These fields are not protected by the lock mu. They are assumed to be
	// immutable after creation of the TracerProvider.
	sampler                Sampler
	idGenerator            IDGenerator
	spanLimits             SpanLimits
	resource               *resource.Resource
	panicRecordingDisabled bool
}

var _ trace.TracerProvider = &TracerProvider{}

type experimentalOption interface {
	Experimental()
}

// NewTracerProvider returns a new and configured TracerProvider.
//
// By default the returned TracerProvider is configured with:
//   - a ParentBased(AlwaysSample) Sampler
//   - a random number IDGenerator
//   - the resource.Default() Resource
//   - the default SpanLimits.
//
// The passed opts are used to override these default values and configure the
// returned TracerProvider appropriately.
// 这里一般是入口.
func NewTracerProvider(opts ...TracerProviderOption) *TracerProvider {
	// 中文导读：构造顺序是默认限制、环境变量、调用方选项，最后为缺省组件补齐默认值。
	// opts ...T 表示可变参数；每个选项把配置值传入 apply，再接收更新后的配置值。
	// 这是本包替代 Java Builder 的主要方式，选项按传入顺序生效。
	o := tracerProviderConfig{
		spanLimits: NewSpanLimits(),
	}
	o = applyTracerProviderEnvConfigs(o)

	for _, opt := range opts {
		if _, ok := opt.(experimentalOption); ok {
			continue
		}
		o = opt.apply(o)
	}

	o = ensureValidTracerProviderConfig(o)

	tp := &TracerProvider{
		namedTracer:            make(map[instrumentation.Scope]*tracer),
		sampler:                o.sampler,
		idGenerator:            o.idGenerator,
		spanLimits:             o.spanLimits,
		resource:               o.resource,
		panicRecordingDisabled: o.panicRecordingDisabled,
	}
	global.Info("TracerProvider created", "config", o)

	spss := make(spanProcessorStates, 0, len(o.processors))
	// 切片的长度从 0 开始，容量预留给已知数量的处理器；append 才把元素纳入长度。
	// 发布后的列表不原地修改，读者可以继续遍历旧列表，而无需占用 Provider 的锁。
	for _, sp := range o.processors {
		spss = append(spss, newSpanProcessorState(sp))
	}
	tp.spanProcessors.Store(&spss)

	return tp
}

// Tracer returns a Tracer with the given name and options. If a Tracer for
// the given name and options does not exist it is created, otherwise the
// existing Tracer is returned.
//
// If name is empty, DefaultTracerName is used instead.
//
// This method is safe to be called concurrently.
func (p *TracerProvider) Tracer(name string, opts ...trace.TracerOption) trace.Tracer {
	// 中文导读：缓存键是完整 instrumentation.Scope，包括名称、版本、Schema URL 和属性。
	// 名称标识产生遥测的库或组件，Span 名称则描述一次操作；两者具有不同生命周期。
	// This check happens before the mutex is acquired to avoid deadlocking if Tracer() is called from within Shutdown().
	if p.isShutdown.Load() {
		return noop.NewTracerProvider().Tracer(name, opts...)
	}
	c := trace.NewTracerConfig(opts...)
	attrs, _, _ := attrnorm.SetDedupLimitDepth(
		c.InstrumentationAttributes(),
		p.spanLimits.AttributeValueDepthLimit,
	)
	if name == "" {
		name = defaultTracerName
	}
	is := instrumentation.Scope{
		Name:       name,
		Version:    c.InstrumentationVersion(),
		SchemaURL:  c.SchemaURL(),
		Attributes: attrs,
	}

	// 立即调用的匿名函数把 defer Unlock 的作用域收窄到缓存查找。
	// defer 在所在函数返回时执行，因此下方记录日志时已经释放锁。
	t, ok := func() (trace.Tracer, bool) {
		p.mu.Lock()
		defer p.mu.Unlock()
		// Must check the flag after acquiring the mutex to avoid returning a valid tracer if Shutdown() ran
		// after the first check above but before we acquired the mutex.
		if p.isShutdown.Load() {
			return noop.NewTracerProvider().Tracer(name, opts...), true
		}
		t, ok := p.namedTracer[is]
		if !ok {
			t = &tracer{
				provider:             p,
				instrumentationScope: is,
			}

			var err error
			t.inst, err = observ.NewTracer()
			if err != nil {
				otel.Handle(err)
			}

			p.namedTracer[is] = t
		}
		return t, ok
	}()
	if !ok {
		// This code is outside the mutex to not hold the lock while calling third party logging code:
		// - That code may do slow things like I/O, which would prolong the duration the lock is held,
		//   slowing down all tracing consumers.
		// - Logging code may be instrumented with tracing and deadlock because it could try
		//   acquiring the same non-reentrant mutex.
		global.Info(
			"Tracer created",
			"name",
			name,
			"version",
			is.Version,
			"schemaURL",
			is.SchemaURL,
			"attributes",
			is.Attributes,
		)
	}
	return t
}

// RegisterSpanProcessor adds the given SpanProcessor to the list of SpanProcessors.
// If sp is nil, this method has no effect.
func (p *TracerProvider) RegisterSpanProcessor(sp SpanProcessor) {
	if sp == nil {
		return
	}
	// This check prevents calls during a shutdown.
	if p.isShutdown.Load() {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	// This check prevents calls after a shutdown.
	if p.isShutdown.Load() {
		return
	}

	current := p.getSpanProcessors()
	// 复制切片元素再原子替换整个列表，是这里的写时复制策略。
	// 只复制切片变量会共享底层数组，无法保证正在遍历旧列表的 Start/End 不受干扰。
	newSPS := make(spanProcessorStates, 0, len(current)+1)
	newSPS = append(newSPS, current...)
	newSPS = append(newSPS, newSpanProcessorState(sp))
	p.spanProcessors.Store(&newSPS)
}

// UnregisterSpanProcessor removes the given SpanProcessor from the list of SpanProcessors.
func (p *TracerProvider) UnregisterSpanProcessor(sp SpanProcessor) {
	// This check prevents calls during a shutdown.
	if p.isShutdown.Load() {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	// This check prevents calls after a shutdown.
	if p.isShutdown.Load() {
		return
	}
	old := p.getSpanProcessors()
	if len(old) == 0 {
		return
	}
	spss := make(spanProcessorStates, len(old))
	copy(spss, old)

	// stop the span processor if it is started and remove it from the list
	var stopOnce *spanProcessorState
	var idx int
	for i, sps := range spss {
		if sps.sp == sp {
			stopOnce = sps
			idx = i
		}
	}
	if stopOnce == nil {
		return
	}
	stopOnce.state.Do(func() {
		// sync.Once 属于处理器的注册状态，使注销与 Provider 关闭共用一次关闭机会。
		// 此接口没有 ctx 参数，关闭使用 Background；定制处理器仍应避免无限等待。
		if err := sp.Shutdown(context.Background()); err != nil {
			otel.Handle(err)
		}
	})
	if len(spss) > 1 {
		copy(spss[idx:], spss[idx+1:])
	}
	spss[len(spss)-1] = nil
	spss = spss[:len(spss)-1]

	p.spanProcessors.Store(&spss)
}

// ForceFlush immediately exports all spans that have not yet been exported for
// all the registered span processors.
func (p *TracerProvider) ForceFlush(ctx context.Context) error {
	// 中文导读：Flush 只要求处理器送出已经结束的 Span，不会替业务代码调用 Span.End。
	// errors.Join 汇总多个处理器的错误，某个处理器失败后仍可继续尝试后续处理器。
	spss := p.getSpanProcessors()
	if len(spss) == 0 {
		return nil
	}

	var err error
	for _, sps := range spss {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		err = errors.Join(err, sps.sp.ForceFlush(ctx))
	}
	return err
}

// Shutdown shuts down TracerProvider. All registered span processors are shut down
// in the order they were registered and any held computational resources are released.
// After Shutdown is called, all methods are no-ops.
func (p *TracerProvider) Shutdown(ctx context.Context) error {
	// 中文导读：应用拥有 Provider 的生命周期，应在退出前给关闭过程留出时间。
	// CompareAndSwap 只允许一个调用把状态从运行改为关闭；状态先变更，再关闭处理器。
	// 关闭是单向过程；若 ctx 取消导致提前返回，后续 Shutdown 不会重新执行这一流程。
	// This check prevents deadlocks in case of recursive shutdown.
	if p.isShutdown.Load() {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	// This check prevents calls after a shutdown has already been done concurrently.
	if !p.isShutdown.CompareAndSwap(false, true) { // did toggle?
		return nil
	}

	var retErr error
	for _, sps := range p.getSpanProcessors() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		var err error
		sps.state.Do(func() {
			err = sps.sp.Shutdown(ctx)
		})
		retErr = errors.Join(retErr, err)
	}
	p.spanProcessors.Store(&spanProcessorStates{})
	return retErr
}

func (p *TracerProvider) getSpanProcessors() spanProcessorStates {
	// Start 和 End 分别读取当时的列表；它们不会为同一个 Span 固定同一组处理器。
	// 动态注册或注销处理器时，不能假设每次 OnEnd 都一定对应自身收到过的 OnStart。
	return *p.spanProcessors.Load()
}

// TracerProviderOption configures a TracerProvider.
type TracerProviderOption interface {
	apply(tracerProviderConfig) tracerProviderConfig
}

type traceProviderOptionFunc func(tracerProviderConfig) tracerProviderConfig

func (fn traceProviderOptionFunc) apply(cfg tracerProviderConfig) tracerProviderConfig {
	// Go 的具名函数类型也能拥有方法，因此函数值可以满足 TracerProviderOption 接口。
	return fn(cfg)
}

// WithSyncer registers the exporter with the TracerProvider using a
// SimpleSpanProcessor.
//
// This is not recommended for production use. The synchronous nature of the
// SimpleSpanProcessor that will wrap the exporter make it good for testing,
// debugging, or showing examples of other feature, but it will be slow and
// have a high computation resource usage overhead. The WithBatcher option is
// recommended for production use instead.
func WithSyncer(e SpanExporter) TracerProviderOption {
	return WithSpanProcessor(NewSimpleSpanProcessor(e))
}

// WithBatcher registers the exporter with the TracerProvider using a
// BatchSpanProcessor configured with the passed opts.
func WithBatcher(e SpanExporter, opts ...BatchSpanProcessorOption) TracerProviderOption {
	return WithSpanProcessor(NewBatchSpanProcessor(e, opts...))
}

// WithSpanProcessor registers the SpanProcessor with a TracerProvider.
// If sp is nil, this option has no effect.
func WithSpanProcessor(sp SpanProcessor) TracerProviderOption {
	return traceProviderOptionFunc(func(cfg tracerProviderConfig) tracerProviderConfig {
		if sp != nil {
			cfg.processors = append(cfg.processors, sp)
		}
		return cfg
	})
}

// WithoutPanicRecording configures the TracerProvider to not record exception
// events when a Span is ended while panicking. The panic continues to
// propagate, and the span is ended without adding the event.
func WithoutPanicRecording() TracerProviderOption {
	return traceProviderOptionFunc(func(cfg tracerProviderConfig) tracerProviderConfig {
		cfg.panicRecordingDisabled = true
		return cfg
	})
}

// WithResource returns a TracerProviderOption that will configure the
// Resource r as a TracerProvider's Resource. The configured Resource is
// referenced by all the Tracers the TracerProvider creates. It represents the
// entity producing telemetry.
//
// If this option is not used, the TracerProvider will use the
// resource.Default() Resource by default.
func WithResource(r *resource.Resource) TracerProviderOption {
	return traceProviderOptionFunc(func(cfg tracerProviderConfig) tracerProviderConfig {
		var err error
		cfg.resource, err = resource.Merge(resource.Environment(), r)
		if err != nil {
			otel.Handle(err)
		}
		return cfg
	})
}

// WithIDGenerator returns a TracerProviderOption that will configure the
// IDGenerator g as a TracerProvider's IDGenerator. The configured IDGenerator
// is used by the Tracers the TracerProvider creates to generate new Span and
// Trace IDs.
//
// If this option is not used, the TracerProvider will use a random number
// IDGenerator by default.
func WithIDGenerator(g IDGenerator) TracerProviderOption {
	return traceProviderOptionFunc(func(cfg tracerProviderConfig) tracerProviderConfig {
		if g != nil {
			cfg.idGenerator = g
		}
		return cfg
	})
}

// WithSampler returns a TracerProviderOption that will configure the Sampler
// s as a TracerProvider's Sampler. The configured Sampler is used by the
// Tracers the TracerProvider creates to make their sampling decisions for the
// Spans they create.
//
// This option overrides the Sampler configured through the OTEL_TRACES_SAMPLER
// and OTEL_TRACES_SAMPLER_ARG environment variables. If this option is not used
// and the sampler is not configured through environment variables or the environment
// contains invalid/unsupported configuration, the TracerProvider will use a
// ParentBased(AlwaysSample) Sampler by default.
func WithSampler(s Sampler) TracerProviderOption {
	return traceProviderOptionFunc(func(cfg tracerProviderConfig) tracerProviderConfig {
		if s != nil {
			cfg.sampler = s
		}
		return cfg
	})
}

// WithAttributeValueDepthLimit sets the maximum allowed depth for attribute
// values. Depth starts at one for the top-level value and increments when
// descending into an array element or map value. An array or map beyond this
// depth is replaced by an empty value.
//
// This limit applies to span, event, link, and instrumentation scope
// attributes processed by this TracerProvider. It does not apply to Resource
// attributes.
//
// Setting this to zero means the default limit of 64 is used. Setting this to
// a negative value means no limit is applied.
//
// There is no environment variable for this limit.
func WithAttributeValueDepthLimit(limit int) TracerProviderOption {
	if limit == 0 {
		limit = DefaultAttributeValueDepthLimit
	}
	return traceProviderOptionFunc(func(cfg tracerProviderConfig) tracerProviderConfig {
		cfg.spanLimits.AttributeValueDepthLimit = limit
		return cfg
	})
}

// WithSpanLimits returns a TracerProviderOption that configures a
// TracerProvider to use the SpanLimits sl. These SpanLimits bound any Span
// created by a Tracer from the TracerProvider.
//
// If any field of sl is zero or negative it will be replaced with the default
// value for that field.
//
// If this or WithRawSpanLimits are not provided, the TracerProvider will use
// the limits defined by environment variables, or the defaults if unset.
// Refer to the NewSpanLimits documentation for information about this
// relationship.
//
// Deprecated: Use WithRawSpanLimits instead which allows setting unlimited
// and zero limits. This option will be kept until the next major version
// incremented release.
func WithSpanLimits(sl SpanLimits) TracerProviderOption {
	if sl.AttributeValueLengthLimit <= 0 {
		sl.AttributeValueLengthLimit = DefaultAttributeValueLengthLimit
	}
	if sl.AttributeValueDepthLimit <= 0 {
		sl.AttributeValueDepthLimit = DefaultAttributeValueDepthLimit
	}
	if sl.AttributeCountLimit <= 0 {
		sl.AttributeCountLimit = DefaultAttributeCountLimit
	}
	if sl.EventCountLimit <= 0 {
		sl.EventCountLimit = DefaultEventCountLimit
	}
	if sl.AttributePerEventCountLimit <= 0 {
		sl.AttributePerEventCountLimit = DefaultAttributePerEventCountLimit
	}
	if sl.LinkCountLimit <= 0 {
		sl.LinkCountLimit = DefaultLinkCountLimit
	}
	if sl.AttributePerLinkCountLimit <= 0 {
		sl.AttributePerLinkCountLimit = DefaultAttributePerLinkCountLimit
	}
	return traceProviderOptionFunc(func(cfg tracerProviderConfig) tracerProviderConfig {
		cfg.spanLimits = sl
		return cfg
	})
}

// WithRawSpanLimits returns a TracerProviderOption that configures a
// TracerProvider to use these limits. These limits bound any Span created by
// a Tracer from the TracerProvider.
//
// The limits will be used as-is, except that an AttributeValueDepthLimit of
// zero means the default limit is used. Other zero or negative values will not
// be changed to the default value like WithSpanLimits does. Setting a limit to
// zero will effectively disable the related resource it limits and setting to
// a negative value will mean that resource is unlimited. Consequentially, the
// zero-value SpanLimits will disable all span resources except the attribute
// value depth limit. Because of this, limits should be constructed using
// NewSpanLimits and updated accordingly.
//
// If this or WithSpanLimits are not provided, the TracerProvider will use the
// limits defined by environment variables, or the defaults if unset. Refer to
// the NewSpanLimits documentation for information about this relationship.
func WithRawSpanLimits(limits SpanLimits) TracerProviderOption {
	return traceProviderOptionFunc(func(cfg tracerProviderConfig) tracerProviderConfig {
		cfg.spanLimits = limits
		return cfg
	})
}

func applyTracerProviderEnvConfigs(cfg tracerProviderConfig) tracerProviderConfig {
	for _, opt := range tracerProviderOptionsFromEnv() {
		cfg = opt.apply(cfg)
	}

	return cfg
}

func tracerProviderOptionsFromEnv() []TracerProviderOption {
	var opts []TracerProviderOption

	// 获取采样类型Sampler
	sampler, err := samplerFromEnv()
	if err != nil {
		otel.Handle(err)
	}

	if sampler != nil {
		opts = append(opts, WithSampler(sampler))
	}

	return opts
}

// ensureValidTracerProviderConfig ensures that given TracerProviderConfig is valid.
func ensureValidTracerProviderConfig(cfg tracerProviderConfig) tracerProviderConfig {
	if cfg.sampler == nil {
		cfg.sampler = ParentBased(AlwaysSample())
	}
	if cfg.idGenerator == nil {
		cfg.idGenerator = defaultIDGenerator()
	}
	if cfg.resource == nil {
		cfg.resource = resource.Default()
	}
	if cfg.spanLimits.AttributeValueDepthLimit == 0 {
		cfg.spanLimits.AttributeValueDepthLimit = DefaultAttributeValueDepthLimit
	}
	return cfg
}
