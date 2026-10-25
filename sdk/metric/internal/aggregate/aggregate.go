// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package aggregate

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// now is used to return the current local time while allowing tests to
// override the default time.Now function.
var now = time.Now

// Measure receives measurements to be aggregated.
//
// 中文导读：聚合器向外暴露两个函数值，Measure 是写入口，ComputeAggregation 是采集入口。
// N int64 | float64 是 Go 的类型约束，表示此处仅支持这两种数值类型。
// 函数值可以被 instrument 和 pipeline 分别持有，并通过绑定的接收者共享一个聚合状态。
// 因而不需要让 API instrument 了解 Sum、Histogram 等聚合器的内部结构。
type Measure[N int64 | float64] func(context.Context, N, attribute.Set)

// ComputeAggregation stores the aggregate of measurements into dest and
// returns the number of aggregate data-points output.
//
// dest 是接口值的指针，聚合器可以替换其中的具体 Sum、Gauge 或 Histogram，
// 也可以在类型匹配时复用已有数据点切片。返回数量用于让 pipeline 排除空流。
// 采集可能同时重置周期状态，因此调用者必须按聚合器要求串行调用；pipeline 的锁负责这一点。
type ComputeAggregation func(dest *metricdata.Aggregation) int

// Builder builds an aggregate function.
type Builder[N int64 | float64] struct {
	// Temporality is the temporality used for the returned aggregate function.
	//
	// If this is not provided a default of cumulative will be used (except for
	// the last-value aggregate function where delta is the only appropriate
	// temporality).
	Temporality metricdata.Temporality
	// Filter is the attribute filter the aggregate function will use on the
	// input of measurements.
	Filter attribute.Filter
	// ReservoirFunc is the factory function used by aggregate functions to
	// create new exemplar reservoirs for a new seen attribute set.
	//
	// If this is not provided a default factory function that returns a
	// DropReservoir reservoir will be used.
	ReservoirFunc func(attribute.Set) FilteredExemplarReservoir[N]
	// AggregationLimit is the cardinality limit of measurement attributes. Any
	// measurement for new attributes once the limit has been reached will be
	// aggregated into a single aggregate for the "otel.metric.overflow"
	// attribute.
	//
	// If AggregationLimit is less than or equal to zero there will not be an
	// aggregation limit imposed (i.e. unlimited attribute sets).
	AggregationLimit int
}

func (b Builder[N]) resFunc() func(attribute.Set) FilteredExemplarReservoir[N] {
	if b.ReservoirFunc != nil {
		return b.ReservoirFunc
	}

	return DropReservoir
}

type fltrMeasure[N int64 | float64] func(ctx context.Context, value N, lazy lazyFilteredAttributes)

func (b Builder[N]) filter(f fltrMeasure[N]) Measure[N] {
	// 中文导读：过滤器被包装进写入函数，确保每个流按自己的 View 维度建立聚合键。
	// lazyFilteredAttributes 延迟构造完整 Set 和被丢弃属性列表，重复属性可走更轻的查找路径。
	// 被过滤的属性仍保留给 exemplar 使用，所以降低指标基数与删掉所有原始属性是两件事。
	// 闭包捕获当前过滤器函数值；调用方之后给 Builder.Filter 重新赋值不会替换这个绑定。
	if b.Filter != nil {
		fltr := b.Filter // Copy to make it immutable after assignment.
		return func(ctx context.Context, n N, a attribute.Set) {
			f(ctx, n, newLazyFilteredAttributes(a, fltr))
		}
	}
	return func(ctx context.Context, n N, a attribute.Set) {
		f(ctx, n, newLazyFilteredAttributes(a, nil))
	}
}

// LastValue returns a last-value aggregate function input and output.
func (b Builder[N]) LastValue() (Measure[N], ComputeAggregation) {
	switch b.Temporality {
	case metricdata.DeltaTemporality:
		lv := newDeltaLastValue[N](b.AggregationLimit, b.resFunc())
		return b.filter(lv.measure), lv.collect
	default:
		lv := newCumulativeLastValue[N](b.AggregationLimit, b.resFunc())
		return b.filter(lv.measure), lv.collect
	}
}

// PrecomputedLastValue returns a last-value aggregate function input and
// output. The aggregation returned from the returned ComputeAggregation
// function will always only return values from the previous collection cycle.
func (b Builder[N]) PrecomputedLastValue() (Measure[N], ComputeAggregation) {
	lv := newPrecomputedLastValue[N](b.AggregationLimit, b.resFunc())
	switch b.Temporality {
	case metricdata.DeltaTemporality:
		return b.filter(lv.measure), lv.delta
	default:
		return b.filter(lv.measure), lv.cumulative
	}
}

// PrecomputedSum returns a sum aggregate function input and output. The
// arguments passed to the input are expected to be the precomputed sum values.
func (b Builder[N]) PrecomputedSum(monotonic bool) (Measure[N], ComputeAggregation) {
	// ObservableCounter 等回调提交的是已计算的累计读数，例如进程当前总请求数。
	// 同步 Counter.Add 提交的是增量，两者不能因为最终都输出 Sum 就复用同样的周期换算。
	// PrecomputedSum 的 Delta 输出需要记住上次读数并相减，详见 sum.go 的 reported。
	s := newPrecomputedSum[N](monotonic, b.AggregationLimit, b.resFunc())
	switch b.Temporality {
	case metricdata.DeltaTemporality:
		return b.filter(s.measure), s.delta
	default:
		return b.filter(s.measure), s.cumulative
	}
}

// Sum returns a sum aggregate function input and output.
func (b Builder[N]) Sum(monotonic bool) (Measure[N], ComputeAggregation) {
	// Builder 在创建时选定状态模型，而不是每次记录时判断 temporality。
	// 返回的 s.measure 和 s.collect 都是绑定到 s 的方法值，包装过滤后仍操作同一个 s。
	// monotonic 用来标记输出 Sum 的单调性；这里不会把负值自动钳制为零。
	switch b.Temporality {
	case metricdata.DeltaTemporality:
		s := newDeltaSum[N](monotonic, b.AggregationLimit, b.resFunc())
		return b.filter(s.measure), s.collect
	default:
		s := newCumulativeSum[N](monotonic, b.AggregationLimit, b.resFunc())
		return b.filter(s.measure), s.collect
	}
}

// ExplicitBucketHistogram returns a histogram aggregate function input and
// output.
func (b Builder[N]) ExplicitBucketHistogram(
	boundaries []float64,
	noMinMax, noSum bool,
) (Measure[N], ComputeAggregation) {
	switch b.Temporality {
	case metricdata.DeltaTemporality:
		h := newDeltaHistogram[N](boundaries, noMinMax, noSum, b.AggregationLimit, b.resFunc())
		return b.filter(h.measure), h.collect
	default:
		h := newCumulativeHistogram[N](boundaries, noMinMax, noSum, b.AggregationLimit, b.resFunc())
		return b.filter(h.measure), h.collect
	}
}

// ExponentialBucketHistogram returns a histogram aggregate function input and
// output.
func (b Builder[N]) ExponentialBucketHistogram(
	maxSize, maxScale int32,
	noMinMax, noSum bool,
) (Measure[N], ComputeAggregation) {
	switch b.Temporality {
	case metricdata.DeltaTemporality:
		h := newDeltaExpoHistogram[N](maxSize, maxScale, noMinMax, noSum, b.AggregationLimit, b.resFunc())
		return b.filter(h.measure), h.collect
	default:
		measure, collect := newCumulativeExpoHistogram[N](
			maxSize,
			maxScale,
			noMinMax,
			noSum,
			b.AggregationLimit,
			b.resFunc(),
		)
		return b.filter(measure), collect
	}
}

// reset ensures s has capacity and sets it length. If the capacity of s too
// small, a new slice is returned with the specified capacity and length.
func reset[T any](s []T, length, capacity int) []T {
	if cap(s) < capacity {
		return make([]T, length, capacity)
	}
	return s[:length]
}
