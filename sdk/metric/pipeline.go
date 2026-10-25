// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package metric

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/internal/global"
	"go.opentelemetry.io/otel/metric/embedded"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	"go.opentelemetry.io/otel/sdk/metric/exemplar"
	"go.opentelemetry.io/otel/sdk/metric/internal"
	"go.opentelemetry.io/otel/sdk/metric/internal/aggregate"
	"go.opentelemetry.io/otel/sdk/metric/internal/attrnorm"
	"go.opentelemetry.io/otel/sdk/metric/internal/finish"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
)

var (
	errCreatingAggregators     = errors.New("could not create all aggregators")
	errIncompatibleAggregation = errors.New("incompatible aggregation")
	errUnknownAggregation      = errors.New("unrecognized aggregation")
)

// instrumentSync is a synchronization point between a pipeline and an
// instrument's aggregate function.
type instrumentSync struct {
	name        string
	description string
	unit        string
	compAgg     aggregate.ComputeAggregation
}

func newPipeline(
	res *resource.Resource,
	reader Reader,
	views []View,
	exemplarFilter exemplar.Filter,
	cardinalityLimit int,
) *pipeline {
	if res == nil {
		res = resource.Empty()
	}
	return &pipeline{
		resource:         res,
		reader:           reader,
		views:            views,
		int64Measures:    map[observableID[int64]][]aggregate.Measure[int64]{},
		float64Measures:  map[observableID[float64]][]aggregate.Measure[float64]{},
		exemplarFilter:   exemplarFilter,
		cardinalityLimit: cardinalityLimit,
		// aggregations is lazy allocated when needed.
	}
}

// pipeline connects all of the instruments created by a meter provider to a Reader.
// This is the object that will be `Reader.register()` when a meter provider is created.
//
// As instruments are created the instrument should be checked if it exists in
// the views of a the Reader, and if so each aggregate function should be added
// to the pipeline.
//
// 中文导读：一个 pipeline 对应一个 Reader，保存该 Reader 的回调和聚合输出入口。
// instrument 持有写入侧 Measure，aggregations 保存读取侧 ComputeAggregation，
// 两侧函数引用同一聚合器状态。这里没有按测量次数增长的原始事件队列。
// pipeline 的锁保护注册表和完整采集流程；同步测量使用聚合器自己的并发机制，
// 不会为了每次 Add/Record 都获取这把管线锁。
type pipeline struct {
	resource *resource.Resource

	reader Reader
	views  []View

	sync.Mutex
	int64Measures    map[observableID[int64]][]aggregate.Measure[int64]
	float64Measures  map[observableID[float64]][]aggregate.Measure[float64]
	aggregations     map[instrumentation.Scope][]instrumentSync
	callbacks        []func(context.Context) error
	multiCallbacks   list.List
	exemplarFilter   exemplar.Filter
	cardinalityLimit int
}

// addInt64Measure adds a new int64 measure to the pipeline for each observer.
func (p *pipeline) addInt64Measure(id observableID[int64], m []aggregate.Measure[int64]) {
	p.Lock()
	defer p.Unlock()
	p.int64Measures[id] = m
}

// addFloat64Measure adds a new float64 measure to the pipeline for each observer.
func (p *pipeline) addFloat64Measure(id observableID[float64], m []aggregate.Measure[float64]) {
	p.Lock()
	defer p.Unlock()
	p.float64Measures[id] = m
}

// addSync adds the instrumentSync to pipeline p with scope. This method is not
// idempotent. Duplicate calls will result in duplicate additions, it is the
// callers responsibility to ensure this is called with unique values.
func (p *pipeline) addSync(scope instrumentation.Scope, iSync instrumentSync) {
	p.Lock()
	defer p.Unlock()
	if p.aggregations == nil {
		p.aggregations = map[instrumentation.Scope][]instrumentSync{
			scope: {iSync},
		}
		return
	}
	p.aggregations[scope] = append(p.aggregations[scope], iSync)
}

type multiCallback func(context.Context) error

// addMultiCallback registers a multi-instrument callback to be run when
// `produce()` is called.
func (p *pipeline) addMultiCallback(c multiCallback) (unregister func()) {
	p.Lock()
	defer p.Unlock()
	e := p.multiCallbacks.PushBack(c)
	return func() {
		p.Lock()
		p.multiCallbacks.Remove(e)
		p.Unlock()
	}
}

// produce returns aggregated metrics from a single collection.
//
// This method is safe to call concurrently.
func (p *pipeline) produce(ctx context.Context, rm *metricdata.ResourceMetrics) error {
	// 中文导读：一次采集先运行所有异步回调，再把所有聚合器转成 metricdata。
	// 回调和聚合必须走完同一轮：部分回调执行后提前退出，可能把本轮值留到下一轮重算。
	// 因此这里只在入口拒绝已取消的 ctx，中途的取消交给回调协作处理，回调错误则合并返回。
	// 即使回调报错，聚合器的周期状态仍会推进；Reader 可能因此不导出本轮结果。
	// 锁也使同一 Reader 的并发采集串行化，但不同 Reader 的回调仍可能同时执行。
	// Only check if context is already cancelled before starting, not inside or after callback loops.
	// If this method returns after executing some callbacks but before running all aggregations,
	// internal aggregation state can be corrupted and result in incorrect data returned
	// by future produce calls.
	if err := ctx.Err(); err != nil {
		return err
	}

	p.Lock()
	defer p.Unlock()

	var err error
	for _, c := range p.callbacks {
		// TODO make the callbacks parallel. ( #3034 )
		if e := c(ctx); e != nil {
			err = errors.Join(err, e)
		}
	}
	for e := p.multiCallbacks.Front(); e != nil; e = e.Next() {
		// TODO make the callbacks parallel. ( #3034 )
		f := e.Value.(multiCallback)
		if e := f(ctx); e != nil {
			err = errors.Join(err, e)
		}
	}

	rm.Resource = p.resource
	// 按 Resource -> Scope -> Metrics 组装输出，并复用调用方提供的切片容量。
	// compAgg 的结果可能在下轮被覆盖，消费者需要明确数据所有权；不要把复用当作历史存储。
	// 没有数据点的 instrument 和没有指标的 Scope 都会从本次结果中省略。
	rm.ScopeMetrics = internal.ReuseSlice(rm.ScopeMetrics, len(p.aggregations))

	i := 0
	for scope, instruments := range p.aggregations {
		rm.ScopeMetrics[i].Metrics = internal.ReuseSlice(rm.ScopeMetrics[i].Metrics, len(instruments))
		j := 0
		for _, inst := range instruments {
			data := rm.ScopeMetrics[i].Metrics[j].Data
			if n := inst.compAgg(&data); n > 0 {
				rm.ScopeMetrics[i].Metrics[j].Name = inst.name
				rm.ScopeMetrics[i].Metrics[j].Description = inst.description
				rm.ScopeMetrics[i].Metrics[j].Unit = inst.unit
				rm.ScopeMetrics[i].Metrics[j].Data = data
				j++
			}
		}
		rm.ScopeMetrics[i].Metrics = rm.ScopeMetrics[i].Metrics[:j]
		if len(rm.ScopeMetrics[i].Metrics) > 0 {
			rm.ScopeMetrics[i].Scope = scope
			i++
		}
	}

	rm.ScopeMetrics = rm.ScopeMetrics[:i]

	return err
}

// inserter facilitates inserting of new instruments from a single scope into a
// pipeline.
type inserter[N int64 | float64] struct {
	// aggregators is a cache that holds aggregate function inputs whose
	// outputs have been inserted into the underlying reader pipeline. This
	// cache ensures no duplicate aggregate functions are inserted into the
	// reader pipeline and if a new request during an instrument creation asks
	// for the same aggregate function input the same instance is returned.
	aggregators *cache[instID, aggVal[N]]

	// views is a cache that holds instrument identifiers for all the
	// instruments a Meter has created, it is provided from the Meter that owns
	// this inserter. This cache ensures during the creation of instruments
	// with the same name but different options (e.g. description, unit) a
	// warning message is logged.
	views *cache[string, instID]

	pipeline *pipeline
}

func newInserter[N int64 | float64](p *pipeline, vc *cache[string, instID]) *inserter[N] {
	if vc == nil {
		vc = &cache[string, instID]{}
	}
	return &inserter[N]{
		aggregators: &cache[instID, aggVal[N]]{},
		views:       vc,
		pipeline:    p,
	}
}

// Instrument inserts the instrument inst with instUnit into a pipeline. All
// views the pipeline contains are matched against, and any matching view that
// creates a unique aggregate function will have its output inserted into the
// pipeline and its input included in the returned slice.
//
// The returned aggregate function inputs are ensured to be deduplicated and
// unique. If another view in another pipeline that is cached by this
// inserter's cache has already inserted the same aggregate function for the
// same instrument, that functions input instance is returned.
//
// If another instrument has already been inserted by this inserter, or any
// other using the same cache, and it conflicts with the instrument being
// inserted in this call, an aggregate function input matching the arguments
// will still be returned but an Info level log message will also be logged to
// the OTel global logger.
//
// If the passed instrument would result in an incompatible aggregate function,
// an error is returned and that aggregate function output is not inserted nor
// is its input returned.
//
// If an instrument is determined to use a Drop aggregation, that instrument is
// not inserted nor returned.
func (i *inserter[N]) Instrument(
	inst Instrument,
	allowedKeys []attribute.Key,
	readerAggregation Aggregation,
) ([]aggregate.Measure[N], error) {
	var measures []aggregate.Measure[N]
	err := i.forEachStream(
		inst,
		allowedKeys,
		readerAggregation,
		i.newStreamAggregation,
		func(agg aggVal[N]) { measures = append(measures, agg.Measure) },
	)
	return measures, err
}

func (i *inserter[N]) forEachStream(
	inst Instrument,
	allowedKeys []attribute.Key,
	readerAggregation Aggregation,
	newAggregation streamAggregationFactory[N],
	yield func(aggVal[N]),
) error {
	// 中文导读：所有匹配的 View 都有机会生成流，不是“第一个匹配就停止”。
	// 解析成同一缓存聚合器的多个匹配通过 agg.ID 去重，防止一次测量被同一状态重复累加。
	// 只有完全没有显式 View 匹配时才补默认流；匹配到 Drop 也算匹配，不能再偷偷补回来。
	// View 的匹配发生在 instrument 解析阶段，数值记录时直接调用解析好的函数。
	var (
		matched bool
		err     error
	)

	seen := make(map[uint64]struct{})
	for _, v := range i.pipeline.views {
		stream, match := v(inst)
		if !match {
			continue
		}
		matched = true
		agg := i.cachedAggregator(
			inst.Scope, inst.Kind, stream, allowedKeys, readerAggregation, newAggregation,
		)
		if agg.Err != nil {
			err = errors.Join(err, agg.Err)
		}
		if agg.Measure == nil { // Drop aggregation.
			continue
		}
		if _, ok := seen[agg.ID]; ok {
			// This aggregate function has already been added.
			continue
		}
		seen[agg.ID] = struct{}{}
		yield(agg)
	}

	if err != nil {
		err = errors.Join(errCreatingAggregators, err)
	}

	if matched {
		return err
	}

	// Apply implicit default view if no explicit matched.
	stream := Stream{
		Name:        inst.Name,
		Description: inst.Description,
		Unit:        inst.Unit,
	}
	agg := i.cachedAggregator(
		inst.Scope, inst.Kind, stream, allowedKeys, readerAggregation, newAggregation,
	)
	if agg.Err != nil {
		if err == nil {
			err = errCreatingAggregators
		}
		err = errors.Join(err, agg.Err)
	}
	if agg.Measure != nil {
		// Ensured to have not seen given matched was false.
		yield(agg)
	}
	return err
}

// addCallback registers a single instrument callback to be run when
// `produce()` is called.
func (i *inserter[N]) addCallback(cback func(context.Context) error) {
	i.pipeline.Lock()
	defer i.pipeline.Unlock()
	i.pipeline.callbacks = append(i.pipeline.callbacks, cback)
}

var aggIDCount atomic.Uint64

// aggVal is the instrument-facing value cached for a resolved metric stream.
type aggVal[N int64 | float64] struct {
	ID      uint64
	Measure aggregate.Measure[N]
	Finish  finish.Func
	Err     error
}

// streamAggregation contains the operations of one resolved metric stream.
// The pipeline retains compute; the instrument retains measure and, when
// present, finish.
type streamAggregation[N int64 | float64] struct {
	measure aggregate.Measure[N]
	compute aggregate.ComputeAggregation
	finish  func(attribute.Distinct, time.Time)
}

// streamAggregationFactory constructs a stream's operations after its Builder
// has been configured with the stream's temporality, limits, and filters.
type streamAggregationFactory[N int64 | float64] func(
	aggregate.Builder[N],
	Aggregation,
	InstrumentKind,
) (streamAggregation[N], error)

// readerDefaultAggregation returns the default aggregation for the instrument
// kind based on the reader's aggregation preferences. This is used unless the
// aggregation is overridden with a view.
func (i *inserter[N]) readerDefaultAggregation(kind InstrumentKind) Aggregation {
	aggregation := i.pipeline.reader.aggregation(kind)
	switch aggregation.(type) {
	case nil, AggregationDefault:
		// If the reader returns default or nil use the default selector.
		aggregation = DefaultAggregationSelector(kind)
	default:
		// Deep copy and validate before using.
		aggregation = aggregation.copy()
		if err := aggregation.err(); err != nil {
			orig := aggregation
			aggregation = DefaultAggregationSelector(kind)
			global.Error(
				err, "using default aggregation instead",
				"aggregation", orig,
				"replacement", aggregation,
			)
		}
	}
	return aggregation
}

// cachedAggregator returns the instrument-facing operations for a resolved
// metric stream. On a cache miss, newAggregation constructs the stream's
// operations and its compute operation is registered with the pipeline.
//
// If the instrument configuration conflicts with an instrument that has
// already been created (e.g. description, unit, data type) a warning will be
// logged at the "Info" level with the global OTel logger. Valid new aggregate
// functions for the instrument configuration will still be returned without an
// error.
//
// If the instrument defines an unknown or incompatible aggregation, an error
// is returned.
func (i *inserter[N]) cachedAggregator(
	scope instrumentation.Scope,
	kind InstrumentKind,
	stream Stream,
	allowedKeys []attribute.Key,
	readerAggregation Aggregation,
	newAggregation streamAggregationFactory[N],
) aggVal[N] {
	// 中文导读：这里把 View、Reader 和 instrument 的配置合并成一个可执行的流。
	// View 未指定聚合时沿用 readerAggregation；显式 AggregationDefault 则选 SDK 默认聚合。
	// 属性过滤器先于聚合键和基数限制生效；删除某个维度后，原来不同的属性组合可能合并。
	// View 的 AttributeFilter 优先于 instrument 的 allowedKeys，nil 与空键集不能混为一谈。
	switch stream.Aggregation.(type) {
	case nil:
		// The aggregation was not overridden with a view. Use the aggregation
		// provided by the reader.
		stream.Aggregation = readerAggregation
	case AggregationDefault:
		// The view explicitly requested the default aggregation.
		stream.Aggregation = DefaultAggregationSelector(kind)
	}
	if stream.ExemplarReservoirProviderSelector == nil {
		stream.ExemplarReservoirProviderSelector = DefaultExemplarReservoirProviderSelector
	}
	// If attribute_keys is not provided on the view (AttributeFilter == nil),
	// use the advisory Attributes configured on the instrument (allowedKeys).
	// allowedKeys == nil indicates that the WithDefaultAttributes option was not passed,
	// and all keys are allowed. An empty (non-nil) slice indicates that the option was passed
	// with an empty set of keys, and no keys are allowed.
	if stream.AttributeFilter == nil && allowedKeys != nil {
		stream.AttributeFilter = attribute.NewAllowKeysFilter(allowedKeys...)
	}

	if err := isAggregatorCompatible(kind, stream.Aggregation); err != nil {
		return aggVal[N]{Err: fmt.Errorf(
			"creating aggregator with instrumentKind: %d, aggregation %v: %w",
			kind, stream.Aggregation, err,
		)}
	}

	id := i.instID(kind, stream)
	// If there is a conflict, the specification says the view should
	// still be applied and a warning should be logged.
	i.logConflict(id)

	// If there are requests for the same instrument with different name
	// casing, the first-seen needs to be returned. Use a normalize ID for the
	// cache lookup to ensure the correct comparison.
	normID := id.normalize()
	cv := i.aggregators.Lookup(normID, func() aggVal[N] {
		// 此缓存属于当前 Meter、当前数值类型和当前 Reader 的 inserter。
		// 缓存命中会复用已建好的流；未命中才创建聚合器并向 pipeline 注册一次输出函数。
		// temporality 由当前 Reader 选择，所以另一个 Reader 的采集不会清空这里的 Delta 状态。
		b := aggregate.Builder[N]{
			Temporality: i.pipeline.reader.temporality(kind),
			ReservoirFunc: reservoirFunc[N](
				kind,
				stream.ExemplarReservoirProviderSelector(stream.Aggregation),
				i.pipeline.exemplarFilter,
			),
		}
		b.Filter = stream.AttributeFilter
		// A value less than or equal to zero will disable the aggregation
		// limits for the builder (an all the created aggregates).
		b.AggregationLimit = i.getCardinalityLimit(kind)
		funcs, err := newAggregation(b, stream.Aggregation, kind)
		if err != nil {
			return aggVal[N]{Err: err}
		}
		if funcs.measure == nil { // Drop aggregator.
			return aggVal[N]{}
		}
		i.pipeline.addSync(scope, instrumentSync{
			// Use the first-seen name casing for this and all subsequent
			// requests of this instrument.
			name:        stream.Name,
			description: stream.Description,
			unit:        stream.Unit,
			compAgg:     funcs.compute,
		})
		var finishFunc finish.Func
		if funcs.finish != nil {
			finishDistinct := funcs.finish
			filter := stream.AttributeFilter
			finishFunc = func(kvs []attribute.KeyValue, t time.Time) {
				// Canonicalize at the stream boundary so each resolved stream
				// applies its own View filter before aggregate lookup.
				finishDistinct(attrnorm.NewDistinct(kvs, filter), t)
			}
		}
		id := aggIDCount.Add(1)
		return aggVal[N]{
			ID:      id,
			Measure: funcs.measure,
			Finish:  finishFunc,
		}
	})
	return cv
}

// getCardinalityLimit returns the cardinality limit for the given instrument kind.
// When the reader's selector returns fallback = true, the pipeline's global
// limit is used, then the default if global is unset. When fallback is false,
// the selector's limit is used (0 or less means unlimited).
func (i *inserter[N]) getCardinalityLimit(kind InstrumentKind) int {
	// 限制针对每个聚合器内的不同属性集合数量，不是 Meter 总数或每秒测量次数。
	// Reader 可以按 instrument kind 覆盖 Provider 的限制；超限测量的去向由聚合器处理。
	// 对 Sum 而言，超限的新属性集合会汇入 overflow 序列，值仍累加，但属性细节不再保留。
	limit, fallback := i.pipeline.reader.cardinalityLimit(kind)
	if fallback {
		return i.pipeline.cardinalityLimit
	}
	return limit
}

// logConflict validates if an instrument with the same case-insensitive name
// as id has already been created. If that instrument conflicts with id, a
// warning is logged.
func (i *inserter[N]) logConflict(id instID) {
	// The API specification defines names as case-insensitive. If there is a
	// different casing of a name it needs to be a conflict.
	name := id.normalize().Name
	existing := i.views.Lookup(name, func() instID { return id })
	if id == existing {
		return
	}

	const msg = "duplicate metric stream definitions"
	args := []any{
		"names", fmt.Sprintf("%q, %q", existing.Name, id.Name),
		"descriptions", fmt.Sprintf("%q, %q", existing.Description, id.Description),
		"kinds", fmt.Sprintf("%s, %s", existing.Kind, id.Kind),
		"units", fmt.Sprintf("%s, %s", existing.Unit, id.Unit),
		"numbers", fmt.Sprintf("%s, %s", existing.Number, id.Number),
	}

	// The specification recommends logging a suggested view to resolve
	// conflicts if possible.
	//
	// https://github.com/open-telemetry/opentelemetry-specification/blob/v1.21.0/specification/metrics/sdk.md#duplicate-instrument-registration
	if id.Unit != existing.Unit || id.Number != existing.Number {
		// There is no view resolution for these, don't make a suggestion.
		global.Warn(msg, args...)
		return
	}

	var stream string
	if id.Name != existing.Name || id.Kind != existing.Kind {
		stream = `Stream{Name: "{{NEW_NAME}}"}`
	} else if id.Description != existing.Description {
		stream = fmt.Sprintf("Stream{Description: %q}", existing.Description)
	}

	inst := fmt.Sprintf(
		"Instrument{Name: %q, Description: %q, Kind: %q, Unit: %q}",
		id.Name, id.Description, "InstrumentKind"+id.Kind.String(), id.Unit,
	)
	args = append(args, "suggested.view", fmt.Sprintf("NewView(%s, %s)", inst, stream))

	global.Warn(msg, args...)
}

func (*inserter[N]) instID(kind InstrumentKind, stream Stream) instID {
	var zero N
	return instID{
		Name:        stream.Name,
		Description: stream.Description,
		Unit:        stream.Unit,
		Kind:        kind,
		Number:      fmt.Sprintf("%T", zero),
	}
}

// newStreamAggregation constructs the operations matching agg and kind. If
// agg is unknown, an error is returned.
func (i *inserter[N]) newStreamAggregation(
	b aggregate.Builder[N],
	agg Aggregation,
	kind InstrumentKind,
) (result streamAggregation[N], err error) {
	switch a := agg.(type) {
	case AggregationDefault:
		return i.newStreamAggregation(b, DefaultAggregationSelector(kind), kind)
	case AggregationDrop:
		// Return an empty stream to signify the drop aggregator.
	case AggregationLastValue:
		switch kind {
		case InstrumentKindGauge:
			result.measure, result.compute = b.LastValue()
		case InstrumentKindObservableGauge:
			result.measure, result.compute = b.PrecomputedLastValue()
		}
	case AggregationSum:
		switch kind {
		case InstrumentKindObservableCounter:
			result.measure, result.compute = b.PrecomputedSum(true)
		case InstrumentKindObservableUpDownCounter:
			result.measure, result.compute = b.PrecomputedSum(false)
		case InstrumentKindCounter, InstrumentKindHistogram:
			result.measure, result.compute = b.Sum(true)
		default:
			// InstrumentKindUpDownCounter, InstrumentKindObservableGauge, and
			// instrumentKindUndefined or other invalid instrument kinds.
			result.measure, result.compute = b.Sum(false)
		}
	case AggregationExplicitBucketHistogram:
		var noSum bool
		switch kind {
		case InstrumentKindUpDownCounter,
			InstrumentKindObservableUpDownCounter,
			InstrumentKindObservableGauge,
			InstrumentKindGauge:
			// The sum should not be collected for any instrument that can make
			// negative measurements:
			// https://github.com/open-telemetry/opentelemetry-specification/blob/v1.21.0/specification/metrics/sdk.md#histogram-aggregations
			noSum = true
		}
		result.measure, result.compute = b.ExplicitBucketHistogram(a.Boundaries, a.NoMinMax, noSum)
	case AggregationBase2ExponentialHistogram:
		var noSum bool
		switch kind {
		case InstrumentKindUpDownCounter,
			InstrumentKindObservableUpDownCounter,
			InstrumentKindObservableGauge,
			InstrumentKindGauge:
			// The sum should not be collected for any instrument that can make
			// negative measurements:
			// https://github.com/open-telemetry/opentelemetry-specification/blob/v1.21.0/specification/metrics/sdk.md#histogram-aggregations
			noSum = true
		}
		result.measure, result.compute = b.ExponentialBucketHistogram(a.MaxSize, a.MaxScale, a.NoMinMax, noSum)

	default:
		err = errUnknownAggregation
	}

	return result, err
}

// isAggregatorCompatible checks if the aggregation can be used by the instrument.
// Current compatibility:
//
// | Instrument Kind          | Drop | LastValue | Sum | Histogram | Exponential Histogram |
// |--------------------------|------|-----------|-----|-----------|-----------------------|
// | Counter                  | ✓    |           | ✓   | ✓         | ✓                     |
// | UpDownCounter            | ✓    |           | ✓   | ✓         | ✓                     |
// | Histogram                | ✓    |           | ✓   | ✓         | ✓                     |
// | Gauge                    | ✓    | ✓         |     | ✓         | ✓                     |
// | Observable Counter       | ✓    |           | ✓   | ✓         | ✓                     |
// | Observable UpDownCounter | ✓    |           | ✓   | ✓         | ✓                     |
// | Observable Gauge         | ✓    | ✓         |     | ✓         | ✓                     |.
func isAggregatorCompatible(kind InstrumentKind, agg Aggregation) error {
	switch agg.(type) {
	case AggregationDefault:
		return nil
	case AggregationExplicitBucketHistogram, AggregationBase2ExponentialHistogram:
		switch kind {
		case InstrumentKindCounter,
			InstrumentKindUpDownCounter,
			InstrumentKindHistogram,
			InstrumentKindGauge,
			InstrumentKindObservableCounter,
			InstrumentKindObservableUpDownCounter,
			InstrumentKindObservableGauge:
			return nil
		default:
			return errIncompatibleAggregation
		}
	case AggregationSum:
		switch kind {
		case InstrumentKindObservableCounter,
			InstrumentKindObservableUpDownCounter,
			InstrumentKindCounter,
			InstrumentKindHistogram,
			InstrumentKindUpDownCounter:
			return nil
		default:
			// TODO: review need for aggregation check after
			// https://github.com/open-telemetry/opentelemetry-specification/issues/2710
			return errIncompatibleAggregation
		}
	case AggregationLastValue:
		switch kind {
		case InstrumentKindObservableGauge, InstrumentKindGauge:
			return nil
		}
		// TODO: review need for aggregation check after
		// https://github.com/open-telemetry/opentelemetry-specification/issues/2710
		return errIncompatibleAggregation
	case AggregationDrop:
		return nil
	default:
		// This is used passed checking for default, it should be an error at this point.
		return fmt.Errorf("%w: %v", errUnknownAggregation, agg)
	}
}

// pipelines is the group of pipelines connecting Readers with instrument
// measurement.
type pipelines []*pipeline

func newPipelines(
	res *resource.Resource,
	readers []Reader,
	views []View,
	exemplarFilter exemplar.Filter,
	cardinalityLimit int,
) pipelines {
	// 给每个 Reader 建立独立的生产端，再把 produce 方法值注册给 Reader。
	// Go 的方法值会绑定接收者 p；Reader 之后调用它，就能回到对应管线采集。
	// Reader 的注册只能成功一次，不能把同一个 Reader 实例当作多个 Provider 的共享出口。
	pipes := make([]*pipeline, 0, len(readers))
	for _, r := range readers {
		p := newPipeline(res, r, views, exemplarFilter, cardinalityLimit)
		r.register(p)
		pipes = append(pipes, p)
	}
	return pipes
}

type unregisterFuncs struct {
	embedded.Registration
	f []func()
}

func (u unregisterFuncs) Unregister() error {
	for _, f := range u.f {
		f()
	}
	return nil
}

// resolver facilitates resolving aggregate functions an instrument calls to
// aggregate measurements with while updating all pipelines that need to pull
// from those aggregations.
type resolver[N int64 | float64] struct {
	inserters []*inserter[N]
}

func newResolver[N int64 | float64](p pipelines, vc *cache[string, instID]) resolver[N] {
	in := make([]*inserter[N], len(p))
	for i := range in {
		in[i] = newInserter[N](p[i], vc)
	}
	return resolver[N]{in}
}

// Aggregators returns the Aggregators that must be updated by the instrument
// defined by key.
func (r resolver[N]) Aggregators(id Instrument, allowedKeys []attribute.Key) ([]aggregate.Measure[N], error) {
	// 一个 instrument 的一次记录会扇出到各 Reader、各匹配流的 Measure。
	// 返回的切片是函数值列表，既连接写入目标，也把 View 解析从频繁执行的记录路径中移走。
	// 没有 Reader 时切片为空，后续记录自然不会更新任何 SDK 聚合状态。
	var measures []aggregate.Measure[N]

	var err error
	for _, i := range r.inserters {
		in, e := i.Instrument(id, allowedKeys, i.readerDefaultAggregation(id.Kind))
		if e != nil {
			err = errors.Join(err, e)
		}
		measures = append(measures, in...)
	}
	return measures, err
}

// HistogramAggregators returns the histogram Aggregators that must be updated by the instrument
// defined by key. If boundaries were provided on instrument instantiation, those take precedence
// over boundaries provided by the reader.
func (r resolver[N]) HistogramAggregators(
	id Instrument,
	allowedKeys []attribute.Key,
	boundaries []float64,
) ([]aggregate.Measure[N], error) {
	var measures []aggregate.Measure[N]

	var err error
	for _, i := range r.inserters {
		agg := i.readerDefaultAggregation(id.Kind)
		if histAgg, ok := agg.(AggregationExplicitBucketHistogram); ok && len(boundaries) > 0 {
			histAgg.Boundaries = boundaries
			agg = histAgg
		}
		in, e := i.Instrument(id, allowedKeys, agg)
		if e != nil {
			err = errors.Join(err, e)
		}
		measures = append(measures, in...)
	}
	return measures, err
}
