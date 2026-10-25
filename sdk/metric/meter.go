// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package metric

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/internal/global"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/embedded"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	"go.opentelemetry.io/otel/sdk/metric/internal/aggregate"
)

// ErrInstrumentName indicates the created instrument has an invalid name.
// Valid names must consist of 255 or fewer characters including alphanumeric, _, ., -, / and start with a letter.
var ErrInstrumentName = errors.New("invalid instrument name")

// meter handles the creation and coordination of all metric instruments. A
// meter represents a single instrumentation scope; all metric telemetry
// produced by an instrumentation scope will use metric instruments from a
// single meter.
//
// 中文导读：Meter 负责把 API instrument 绑定到具体的聚合函数，不负责定时导出。
// int64 和 float64 分开缓存，resolver[N] 用类型参数复用相同的绑定流程；
// N 在这里是编译期确定的数值类型，记录时不需要把每个数值装入 any 再分派。
// 同步 instrument 保存所有目标流的 Measure 函数，异步 instrument 则在采集回调中观察值。
type meter struct {
	embedded.Meter

	scope instrumentation.Scope
	pipes pipelines

	int64Insts             *cacheWithErr[instID, *int64Inst]
	float64Insts           *cacheWithErr[instID, *float64Inst]
	int64ObservableInsts   *cacheWithErr[instID, int64Observable]
	float64ObservableInsts *cacheWithErr[instID, float64Observable]

	int64Resolver   resolver[int64]
	float64Resolver resolver[float64]
}

func newMeter(s instrumentation.Scope, p pipelines) *meter {
	// viewCache ensures instrument conflicts, including number conflicts, this
	// meter is asked to create are logged to the user.
	var viewCache cache[string, instID]

	var int64Insts cacheWithErr[instID, *int64Inst]
	var float64Insts cacheWithErr[instID, *float64Inst]
	var int64ObservableInsts cacheWithErr[instID, int64Observable]
	var float64ObservableInsts cacheWithErr[instID, float64Observable]

	return &meter{
		scope:                  s,
		pipes:                  p,
		int64Insts:             &int64Insts,
		float64Insts:           &float64Insts,
		int64ObservableInsts:   &int64ObservableInsts,
		float64ObservableInsts: &float64ObservableInsts,
		int64Resolver:          newResolver[int64](p, &viewCache),
		float64Resolver:        newResolver[float64](p, &viewCache),
	}
}

// Compile-time check meter implements metric.Meter.
var _ metric.Meter = (*meter)(nil)

// Int64Counter returns a new instrument identified by name and configured with
// options. The instrument is used to synchronously record increasing int64
// measurements during a computational operation.
func (m *meter) Int64Counter(name string, options ...metric.Int64CounterOption) (metric.Int64Counter, error) {
	// 用这个入口追踪同步记录：lookup -> resolver.Aggregators -> inserter -> Builder.Sum。
	// lookup 在创建阶段准备好 Measure 列表，之后 int64Inst.Add 可直接遍历该列表。
	// 返回的 instrument 与 error 是两个独立结果；名称校验失败不意味着 instrument 必为 nil。
	cfg := metric.NewInt64CounterConfig(options...)
	const kind = InstrumentKindCounter
	p := int64InstProvider{m}
	i, err := p.lookup(kind, name, cfg.Description(), cfg.Unit(), defaultAttributes(options))
	if err != nil {
		return i, err
	}

	return i, validateInstrumentName(name)
}

// Int64UpDownCounter returns a new instrument identified by name and
// configured with options. The instrument is used to synchronously record
// int64 measurements during a computational operation.
func (m *meter) Int64UpDownCounter(
	name string,
	options ...metric.Int64UpDownCounterOption,
) (metric.Int64UpDownCounter, error) {
	cfg := metric.NewInt64UpDownCounterConfig(options...)
	const kind = InstrumentKindUpDownCounter
	p := int64InstProvider{m}
	i, err := p.lookup(kind, name, cfg.Description(), cfg.Unit(), defaultAttributes(options))
	if err != nil {
		return i, err
	}

	return i, validateInstrumentName(name)
}

// Int64Histogram returns a new instrument identified by name and configured
// with options. The instrument is used to synchronously record the
// distribution of int64 measurements during a computational operation.
func (m *meter) Int64Histogram(name string, options ...metric.Int64HistogramOption) (metric.Int64Histogram, error) {
	cfg := metric.NewInt64HistogramConfig(options...)
	p := int64InstProvider{m}
	i, err := p.lookupHistogram(name, cfg, defaultAttributes(options))
	if err != nil {
		return i, err
	}

	return i, validateInstrumentName(name)
}

// Int64Gauge returns a new instrument identified by name and configured
// with options. The instrument is used to synchronously record the
// distribution of int64 measurements during a computational operation.
func (m *meter) Int64Gauge(name string, options ...metric.Int64GaugeOption) (metric.Int64Gauge, error) {
	cfg := metric.NewInt64GaugeConfig(options...)
	const kind = InstrumentKindGauge
	p := int64InstProvider{m}
	i, err := p.lookup(kind, name, cfg.Description(), cfg.Unit(), defaultAttributes(options))
	if err != nil {
		return i, err
	}

	return i, validateInstrumentName(name)
}

// int64ObservableInstrument returns a new observable identified by the Instrument.
// It registers callbacks for each reader's pipeline.
func (m *meter) int64ObservableInstrument(
	id Instrument,
	allowedKeys []attribute.Key,
	callbacks []metric.Int64Callback,
) (int64Observable, error) {
	// 中文导读：“异步”指 SDK 采集时调用应用提供的函数，不是每个 instrument 自带 goroutine。
	// 每个 Reader 的回调只写本 pipeline 的聚合器；否则 Reader A 的采集会污染 Reader B。
	// 同一个应用回调可能因多个 Reader 而被多次调用，应读取当前状态而不是消费一次性事件。
	// 下方闭包捕获 fn 和 observer，把用户函数与本 pipeline 的 Measure 列表绑定起来。
	// float64ObservableInstrument 采用同样的绑定方式，区别只是数值类型。
	key := instID{
		Name:        id.Name,
		Description: id.Description,
		Unit:        id.Unit,
		Kind:        id.Kind,
	}
	if m.int64ObservableInsts.HasKey(key) && len(callbacks) > 0 {
		warnRepeatedObservableCallbacks(id)
	}
	return m.int64ObservableInsts.Lookup(key, func() (int64Observable, error) {
		inst := newInt64Observable(m, id.Kind, id.Name, id.Description, id.Unit)
		for _, insert := range m.int64Resolver.inserters {
			// Connect the measure functions for instruments in this pipeline with the
			// callbacks for this pipeline.
			in, err := insert.Instrument(id, allowedKeys, insert.readerDefaultAggregation(id.Kind))
			if err != nil {
				return inst, err
			}
			// Drop aggregation
			if len(in) == 0 {
				inst.dropAggregation = true
				continue
			}
			inst.appendMeasures(in)

			// Add the measures to the pipeline. It is required to maintain
			// measures per pipeline to avoid calling the measure that
			// is not part of the pipeline.
			insert.pipeline.addInt64Measure(inst.observableID, in)
			for _, cback := range callbacks {
				inst := int64Observer{measures: in}
				fn := cback
				insert.addCallback(func(ctx context.Context) error { return fn(ctx, inst) })
			}
		}
		return inst, validateInstrumentName(id.Name)
	})
}

// Int64ObservableCounter returns a new instrument identified by name and
// configured with options. The instrument is used to asynchronously record
// increasing int64 measurements once per a measurement collection cycle.
// Only the measurements recorded during the collection cycle are exported.
//
// If Int64ObservableCounter is invoked repeatedly with the same Name,
// Description, and Unit, only the first set of callbacks provided are used.
// Use meter.RegisterCallback and Registration.Unregister to manage callbacks
// if instrumentation can be created multiple times with different callbacks.
func (m *meter) Int64ObservableCounter(
	name string,
	options ...metric.Int64ObservableCounterOption,
) (metric.Int64ObservableCounter, error) {
	cfg := metric.NewInt64ObservableCounterConfig(options...)
	id := Instrument{
		Name:        name,
		Description: cfg.Description(),
		Unit:        cfg.Unit(),
		Kind:        InstrumentKindObservableCounter,
		Scope:       m.scope,
	}
	return m.int64ObservableInstrument(id, defaultAttributes(options), cfg.Callbacks())
}

// Int64ObservableUpDownCounter returns a new instrument identified by name and
// configured with options. The instrument is used to asynchronously record
// int64 measurements once per a measurement collection cycle. Only the
// measurements recorded during the collection cycle are exported.
//
// If Int64ObservableUpDownCounter is invoked repeatedly with the same Name,
// Description, and Unit, only the first set of callbacks provided are used.
// Use meter.RegisterCallback and Registration.Unregister to manage callbacks
// if instrumentation can be created multiple times with different callbacks.
func (m *meter) Int64ObservableUpDownCounter(
	name string,
	options ...metric.Int64ObservableUpDownCounterOption,
) (metric.Int64ObservableUpDownCounter, error) {
	cfg := metric.NewInt64ObservableUpDownCounterConfig(options...)
	id := Instrument{
		Name:        name,
		Description: cfg.Description(),
		Unit:        cfg.Unit(),
		Kind:        InstrumentKindObservableUpDownCounter,
		Scope:       m.scope,
	}
	return m.int64ObservableInstrument(id, defaultAttributes(options), cfg.Callbacks())
}

// Int64ObservableGauge returns a new instrument identified by name and
// configured with options. The instrument is used to asynchronously record
// instantaneous int64 measurements once per a measurement collection cycle.
// Only the measurements recorded during the collection cycle are exported.
//
// If Int64ObservableGauge is invoked repeatedly with the same Name,
// Description, and Unit, only the first set of callbacks provided are used.
// Use meter.RegisterCallback and Registration.Unregister to manage callbacks
// if instrumentation can be created multiple times with different callbacks.
func (m *meter) Int64ObservableGauge(
	name string,
	options ...metric.Int64ObservableGaugeOption,
) (metric.Int64ObservableGauge, error) {
	cfg := metric.NewInt64ObservableGaugeConfig(options...)
	id := Instrument{
		Name:        name,
		Description: cfg.Description(),
		Unit:        cfg.Unit(),
		Kind:        InstrumentKindObservableGauge,
		Scope:       m.scope,
	}
	return m.int64ObservableInstrument(id, defaultAttributes(options), cfg.Callbacks())
}

// Float64Counter returns a new instrument identified by name and configured
// with options. The instrument is used to synchronously record increasing
// float64 measurements during a computational operation.
func (m *meter) Float64Counter(name string, options ...metric.Float64CounterOption) (metric.Float64Counter, error) {
	cfg := metric.NewFloat64CounterConfig(options...)
	const kind = InstrumentKindCounter
	p := float64InstProvider{m}
	i, err := p.lookup(kind, name, cfg.Description(), cfg.Unit(), defaultAttributes(options))
	if err != nil {
		return i, err
	}

	return i, validateInstrumentName(name)
}

// Float64UpDownCounter returns a new instrument identified by name and
// configured with options. The instrument is used to synchronously record
// float64 measurements during a computational operation.
func (m *meter) Float64UpDownCounter(
	name string,
	options ...metric.Float64UpDownCounterOption,
) (metric.Float64UpDownCounter, error) {
	cfg := metric.NewFloat64UpDownCounterConfig(options...)
	const kind = InstrumentKindUpDownCounter
	p := float64InstProvider{m}
	i, err := p.lookup(kind, name, cfg.Description(), cfg.Unit(), defaultAttributes(options))
	if err != nil {
		return i, err
	}

	return i, validateInstrumentName(name)
}

// Float64Histogram returns a new instrument identified by name and configured
// with options. The instrument is used to synchronously record the
// distribution of float64 measurements during a computational operation.
func (m *meter) Float64Histogram(
	name string,
	options ...metric.Float64HistogramOption,
) (metric.Float64Histogram, error) {
	cfg := metric.NewFloat64HistogramConfig(options...)
	p := float64InstProvider{m}
	i, err := p.lookupHistogram(name, cfg, defaultAttributes(options))
	if err != nil {
		return i, err
	}

	return i, validateInstrumentName(name)
}

// Float64Gauge returns a new instrument identified by name and configured
// with options. The instrument is used to synchronously record the
// distribution of float64 measurements during a computational operation.
func (m *meter) Float64Gauge(name string, options ...metric.Float64GaugeOption) (metric.Float64Gauge, error) {
	cfg := metric.NewFloat64GaugeConfig(options...)
	const kind = InstrumentKindGauge
	p := float64InstProvider{m}
	i, err := p.lookup(kind, name, cfg.Description(), cfg.Unit(), defaultAttributes(options))
	if err != nil {
		return i, err
	}

	return i, validateInstrumentName(name)
}

// float64ObservableInstrument returns a new observable identified by the Instrument.
// It registers callbacks for each reader's pipeline.
func (m *meter) float64ObservableInstrument(
	id Instrument,
	allowedKeys []attribute.Key,
	callbacks []metric.Float64Callback,
) (float64Observable, error) {
	key := instID{
		Name:        id.Name,
		Description: id.Description,
		Unit:        id.Unit,
		Kind:        id.Kind,
	}
	if m.float64ObservableInsts.HasKey(key) && len(callbacks) > 0 {
		warnRepeatedObservableCallbacks(id)
	}
	return m.float64ObservableInsts.Lookup(key, func() (float64Observable, error) {
		inst := newFloat64Observable(m, id.Kind, id.Name, id.Description, id.Unit)
		for _, insert := range m.float64Resolver.inserters {
			// Connect the measure functions for instruments in this pipeline with the
			// callbacks for this pipeline.
			in, err := insert.Instrument(id, allowedKeys, insert.readerDefaultAggregation(id.Kind))
			if err != nil {
				return inst, err
			}
			// Drop aggregation
			if len(in) == 0 {
				inst.dropAggregation = true
				continue
			}
			inst.appendMeasures(in)

			// Add the measures to the pipeline. It is required to maintain
			// measures per pipeline to avoid calling the measure that
			// is not part of the pipeline.
			insert.pipeline.addFloat64Measure(inst.observableID, in)
			for _, cback := range callbacks {
				inst := float64Observer{measures: in}
				fn := cback
				insert.addCallback(func(ctx context.Context) error { return fn(ctx, inst) })
			}
		}
		return inst, validateInstrumentName(id.Name)
	})
}

// Float64ObservableCounter returns a new instrument identified by name and
// configured with options. The instrument is used to asynchronously record
// increasing float64 measurements once per a measurement collection cycle.
// Only the measurements recorded during the collection cycle are exported.
//
// If Float64ObservableCounter is invoked repeatedly with the same Name,
// Description, and Unit, only the first set of callbacks provided are used.
// Use meter.RegisterCallback and Registration.Unregister to manage callbacks
// if instrumentation can be created multiple times with different callbacks.
func (m *meter) Float64ObservableCounter(
	name string,
	options ...metric.Float64ObservableCounterOption,
) (metric.Float64ObservableCounter, error) {
	cfg := metric.NewFloat64ObservableCounterConfig(options...)
	id := Instrument{
		Name:        name,
		Description: cfg.Description(),
		Unit:        cfg.Unit(),
		Kind:        InstrumentKindObservableCounter,
		Scope:       m.scope,
	}
	return m.float64ObservableInstrument(id, defaultAttributes(options), cfg.Callbacks())
}

// Float64ObservableUpDownCounter returns a new instrument identified by name
// and configured with options. The instrument is used to asynchronously record
// float64 measurements once per a measurement collection cycle. Only the
// measurements recorded during the collection cycle are exported.
//
// If Float64ObservableUpDownCounter is invoked repeatedly with the same Name,
// Description, and Unit, only the first set of callbacks provided are used.
// Use meter.RegisterCallback and Registration.Unregister to manage callbacks
// if instrumentation can be created multiple times with different callbacks.
func (m *meter) Float64ObservableUpDownCounter(
	name string,
	options ...metric.Float64ObservableUpDownCounterOption,
) (metric.Float64ObservableUpDownCounter, error) {
	cfg := metric.NewFloat64ObservableUpDownCounterConfig(options...)
	id := Instrument{
		Name:        name,
		Description: cfg.Description(),
		Unit:        cfg.Unit(),
		Kind:        InstrumentKindObservableUpDownCounter,
		Scope:       m.scope,
	}
	return m.float64ObservableInstrument(id, defaultAttributes(options), cfg.Callbacks())
}

// Float64ObservableGauge returns a new instrument identified by name and
// configured with options. The instrument is used to asynchronously record
// instantaneous float64 measurements once per a measurement collection cycle.
// Only the measurements recorded during the collection cycle are exported.
//
// If Float64ObservableGauge is invoked repeatedly with the same Name,
// Description, and Unit, only the first set of callbacks provided are used.
// Use meter.RegisterCallback and Registration.Unregister to manage callbacks
// if instrumentation can be created multiple times with different callbacks.
func (m *meter) Float64ObservableGauge(
	name string,
	options ...metric.Float64ObservableGaugeOption,
) (metric.Float64ObservableGauge, error) {
	cfg := metric.NewFloat64ObservableGaugeConfig(options...)
	id := Instrument{
		Name:        name,
		Description: cfg.Description(),
		Unit:        cfg.Unit(),
		Kind:        InstrumentKindObservableGauge,
		Scope:       m.scope,
	}
	return m.float64ObservableInstrument(id, defaultAttributes(options), cfg.Callbacks())
}

func validateInstrumentName(name string) error {
	if name == "" {
		return fmt.Errorf("%w: %s: is empty", ErrInstrumentName, name)
	}
	if len(name) > 255 {
		return fmt.Errorf("%w: %s: longer than 255 characters", ErrInstrumentName, name)
	}
	if !isAlpha([]rune(name)[0]) {
		return fmt.Errorf("%w: %s: must start with a letter", ErrInstrumentName, name)
	}
	if len(name) == 1 {
		return nil
	}
	for _, c := range name[1:] {
		if !isAlphanumeric(c) && c != '_' && c != '.' && c != '-' && c != '/' {
			return fmt.Errorf("%w: %s: must only contain [A-Za-z0-9_.-/]", ErrInstrumentName, name)
		}
	}
	return nil
}

func isAlpha(c rune) bool {
	return ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z')
}

func isAlphanumeric(c rune) bool {
	return isAlpha(c) || ('0' <= c && c <= '9')
}

func warnRepeatedObservableCallbacks(id Instrument) {
	inst := fmt.Sprintf(
		"Instrument{Name: %q, Description: %q, Kind: %q, Unit: %q}",
		id.Name, id.Description, "InstrumentKind"+id.Kind.String(), id.Unit,
	)
	global.Warn(
		"Repeated observable instrument creation with callbacks. Ignoring new callbacks. Use meter.RegisterCallback and Registration.Unregister to manage callbacks.",
		"instrument",
		inst,
	)
}

// RegisterCallback registers f to be called each collection cycle so it will
// make observations for insts during those cycles.
//
// The only instruments f can make observations for are insts. All other
// observations will be dropped and an error will be logged.
//
// Only instruments from this meter can be registered with f, an error is
// returned if other instrument are provided.
//
// Only observations made in the callback will be exported. Unlike synchronous
// instruments, asynchronous callbacks can "forget" attribute sets that are no
// longer relevant by omitting the observation during the callback.
//
// The returned Registration can be used to unregister f.
// If f is nil, no callback is registered.
func (m *meter) RegisterCallback(f metric.Callback, insts ...metric.Observable) (metric.Registration, error) {
	// 中文导读：RegisterCallback 把一个用户回调关联到一组允许观察的 instrument。
	// observer 的 map 是这次注册的白名单，避免回调向未声明或其他 Meter 的 instrument 写入。
	// 每个 pipeline 得到各自的 observer 和注销函数，返回的 Registration 汇总这些注销动作。
	// 回调由 pipeline.produce 在锁内同步执行；不要在回调里注销自身或再触发同一管线的采集，
	// 这些操作也需要同一把锁。需要阻塞等待的外部读取应主动检查传入的采集 ctx。
	if f == nil || len(insts) == 0 {
		// Don't allocate an observer if there is no callback or instrument.
		return noopRegister{}, nil
	}

	var err error
	validInstruments := make([]metric.Observable, 0, len(insts))
	for _, inst := range insts {
		switch o := inst.(type) {
		case int64Observable:
			if e := o.registerable(m); e != nil {
				if !errors.Is(e, errEmptyAgg) {
					err = errors.Join(err, e)
				}
				continue
			}

			validInstruments = append(validInstruments, inst)
		case float64Observable:
			if e := o.registerable(m); e != nil {
				if !errors.Is(e, errEmptyAgg) {
					err = errors.Join(err, e)
				}
				continue
			}

			validInstruments = append(validInstruments, inst)
		default:
			// Instrument external to the SDK.
			return nil, errors.New("invalid observable: from different implementation")
		}
	}

	if len(validInstruments) == 0 {
		// All insts use drop aggregation or are invalid.
		return noopRegister{}, err
	}

	unregs := make([]func(), len(m.pipes))
	for ix, pipe := range m.pipes {
		reg := newObserver(pipe)
		for _, inst := range validInstruments {
			switch o := inst.(type) {
			case int64Observable:
				reg.registerInt64(o.observableID)
			case float64Observable:
				reg.registerFloat64(o.observableID)
			}
		}

		// Some or all instruments were valid.
		cBack := func(ctx context.Context) error { return f(ctx, reg) }
		unregs[ix] = pipe.addMultiCallback(cBack)
	}

	return unregisterFuncs{f: unregs}, err
}

type observer struct {
	embedded.Observer

	pipe    *pipeline
	float64 map[observableID[float64]]struct{}
	int64   map[observableID[int64]]struct{}
}

func newObserver(p *pipeline) observer {
	return observer{
		pipe:    p,
		float64: make(map[observableID[float64]]struct{}),
		int64:   make(map[observableID[int64]]struct{}),
	}
}

func (r observer) registerFloat64(id observableID[float64]) {
	r.float64[id] = struct{}{}
}

func (r observer) registerInt64(id observableID[int64]) {
	r.int64[id] = struct{}{}
}

var (
	errUnknownObserver = errors.New("unknown observable instrument")
	errUnregObserver   = errors.New("observable instrument not registered for callback")
)

func (r observer) ObserveFloat64(o metric.Float64Observable, v float64, opts ...metric.ObserveOption) {
	var oImpl float64Observable
	switch conv := o.(type) {
	case float64Observable:
		oImpl = conv
	default:
		global.Error(errUnknownObserver, "failed to record")
		return
	}

	if _, registered := r.float64[oImpl.observableID]; !registered {
		if !oImpl.dropAggregation {
			global.Error(
				errUnregObserver, "failed to record",
				"name", oImpl.name,
				"description", oImpl.description,
				"unit", oImpl.unit,
				"number", fmt.Sprintf("%T", float64(0)),
			)
		}
		return
	}
	c := metric.NewObserveConfig(opts)
	rawKVs := extractRawKVs(opts)
	set := resolveAttributes(c.Attributes(), rawKVs)
	// Access to r.pipe.float64Measure is already guarded by a lock in pipeline.produce.
	// TODO (#5946): Refactor pipeline and observable measures.
	measures := r.pipe.float64Measures[oImpl.observableID]
	for _, m := range measures {
		m(context.Background(), v, set)
	}
}

func (r observer) ObserveInt64(o metric.Int64Observable, v int64, opts ...metric.ObserveOption) {
	// Observe 的 API 没有 context 参数；这里传给 Measure 的是 Background。
	// 用户回调收到的 ctx 用来控制采集工作，不会自动成为每个观测值的 trace 上下文。
	// observer 依赖 produce 持有的锁读取管线映射，因此不应保留到回调结束后继续使用。
	var oImpl int64Observable
	switch conv := o.(type) {
	case int64Observable:
		oImpl = conv
	default:
		global.Error(errUnknownObserver, "failed to record")
		return
	}

	if _, registered := r.int64[oImpl.observableID]; !registered {
		if !oImpl.dropAggregation {
			global.Error(
				errUnregObserver, "failed to record",
				"name", oImpl.name,
				"description", oImpl.description,
				"unit", oImpl.unit,
				"number", fmt.Sprintf("%T", int64(0)),
			)
		}
		return
	}
	c := metric.NewObserveConfig(opts)
	rawKVs := extractRawKVs(opts)
	set := resolveAttributes(c.Attributes(), rawKVs)
	// Access to r.pipe.int64Measures is already guarded b a lock in pipeline.produce.
	// TODO (#5946): Refactor pipeline and observable measures.
	measures := r.pipe.int64Measures[oImpl.observableID]
	for _, m := range measures {
		m(context.Background(), v, set)
	}
}

type noopRegister struct{ embedded.Registration }

func (noopRegister) Unregister() error {
	return nil
}

// int64InstProvider provides int64 OpenTelemetry instruments.
type int64InstProvider struct{ *meter }

func (p int64InstProvider) aggs(
	kind InstrumentKind,
	name, desc, u string,
	allowedKeys []attribute.Key,
) ([]aggregate.Measure[int64], error) {
	inst := Instrument{
		Name:        name,
		Description: desc,
		Unit:        u,
		Kind:        kind,
		Scope:       p.scope,
	}
	return p.int64Resolver.Aggregators(inst, allowedKeys)
}

func (p int64InstProvider) histogramAggs(
	name string,
	cfg metric.Int64HistogramConfig,
	allowedKeys []attribute.Key,
) ([]aggregate.Measure[int64], error) {
	boundaries := cfg.ExplicitBucketBoundaries()
	aggError := AggregationExplicitBucketHistogram{Boundaries: boundaries}.err()
	if aggError != nil {
		// If boundaries are invalid, ignore them.
		boundaries = nil
	}
	inst := Instrument{
		Name:        name,
		Description: cfg.Description(),
		Unit:        cfg.Unit(),
		Kind:        InstrumentKindHistogram,
		Scope:       p.scope,
	}
	measures, err := p.int64Resolver.HistogramAggregators(inst, allowedKeys, boundaries)
	return measures, errors.Join(aggError, err)
}

// lookup returns the resolved instrumentImpl.
func (p int64InstProvider) lookup(
	kind InstrumentKind,
	name, desc, u string,
	allowedKeys []attribute.Key,
) (*int64Inst, error) {
	return p.int64Insts.Lookup(instID{
		Name:        name,
		Description: desc,
		Unit:        u,
		Kind:        kind,
	}, func() (*int64Inst, error) {
		aggs, err := p.aggs(kind, name, desc, u, allowedKeys)
		return &int64Inst{measures: aggs}, err
	})
}

// lookupHistogram returns the resolved instrumentImpl.
func (p int64InstProvider) lookupHistogram(
	name string,
	cfg metric.Int64HistogramConfig,
	allowedKeys []attribute.Key,
) (*int64Inst, error) {
	return p.int64Insts.Lookup(instID{
		Name:        name,
		Description: cfg.Description(),
		Unit:        cfg.Unit(),
		Kind:        InstrumentKindHistogram,
	}, func() (*int64Inst, error) {
		aggs, err := p.histogramAggs(name, cfg, allowedKeys)
		return &int64Inst{measures: aggs}, err
	})
}

// float64InstProvider provides float64 OpenTelemetry instruments.
type float64InstProvider struct{ *meter }

func (p float64InstProvider) aggs(
	kind InstrumentKind,
	name, desc, u string,
	allowedKeys []attribute.Key,
) ([]aggregate.Measure[float64], error) {
	inst := Instrument{
		Name:        name,
		Description: desc,
		Unit:        u,
		Kind:        kind,
		Scope:       p.scope,
	}
	return p.float64Resolver.Aggregators(inst, allowedKeys)
}

func (p float64InstProvider) histogramAggs(
	name string,
	cfg metric.Float64HistogramConfig,
	allowedKeys []attribute.Key,
) ([]aggregate.Measure[float64], error) {
	boundaries := cfg.ExplicitBucketBoundaries()
	aggError := AggregationExplicitBucketHistogram{Boundaries: boundaries}.err()
	if aggError != nil {
		// If boundaries are invalid, ignore them.
		boundaries = nil
	}
	inst := Instrument{
		Name:        name,
		Description: cfg.Description(),
		Unit:        cfg.Unit(),
		Kind:        InstrumentKindHistogram,
		Scope:       p.scope,
	}
	measures, err := p.float64Resolver.HistogramAggregators(inst, allowedKeys, boundaries)
	return measures, errors.Join(aggError, err)
}

// lookup returns the resolved instrumentImpl.
func (p float64InstProvider) lookup(
	kind InstrumentKind,
	name, desc, u string,
	allowedKeys []attribute.Key,
) (*float64Inst, error) {
	return p.float64Insts.Lookup(instID{
		Name:        name,
		Description: desc,
		Unit:        u,
		Kind:        kind,
	}, func() (*float64Inst, error) {
		aggs, err := p.aggs(kind, name, desc, u, allowedKeys)
		return &float64Inst{measures: aggs}, err
	})
}

// lookupHistogram returns the resolved instrumentImpl.
func (p float64InstProvider) lookupHistogram(
	name string,
	cfg metric.Float64HistogramConfig,
	allowedKeys []attribute.Key,
) (*float64Inst, error) {
	return p.float64Insts.Lookup(instID{
		Name:        name,
		Description: cfg.Description(),
		Unit:        cfg.Unit(),
		Kind:        InstrumentKindHistogram,
	}, func() (*float64Inst, error) {
		aggs, err := p.histogramAggs(name, cfg, allowedKeys)
		return &float64Inst{measures: aggs}, err
	})
}

type int64Observer struct {
	embedded.Int64Observer
	measures[int64]
}

func (o int64Observer) Observe(val int64, opts ...metric.ObserveOption) {
	c := metric.NewObserveConfig(opts)
	rawKVs := extractRawKVs(opts)
	o.observe(val, resolveAttributes(c.Attributes(), rawKVs))
}

type float64Observer struct {
	embedded.Float64Observer
	measures[float64]
}

func (o float64Observer) Observe(val float64, opts ...metric.ObserveOption) {
	c := metric.NewObserveConfig(opts)
	rawKVs := extractRawKVs(opts)
	o.observe(val, resolveAttributes(c.Attributes(), rawKVs))
}

func defaultAttributes[T any](opts []T) []attribute.Key {
	// nil 与非 nil 的空切片在这里有意保留不同含义：前者表示没有声明默认属性限制，
	// 后者表示显式声明不保留任何属性键。pipeline 只在 View 未设置过滤器时采用此建议。
	// any(o) 后的接口断言按能力识别实验选项，让稳定 SDK 不必依赖实验模块的具体类型。
	var keys []attribute.Key
	var found bool
	for _, o := range opts {
		if exp, ok := any(o).(interface{ AllowedKeys() []attribute.Key }); ok {
			found = true
			keys = append(keys, exp.AllowedKeys()...)
		}
	}
	if found && keys == nil {
		return []attribute.Key{}
	}
	return keys
}
