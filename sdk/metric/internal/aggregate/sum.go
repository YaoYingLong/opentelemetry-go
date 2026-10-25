// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package aggregate

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/internal/x"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

type sumValue[N int64 | float64] struct {
	n             atomicCounter[N]
	res           FilteredExemplarReservoir[N]
	attrs         attribute.Set
	startTime     time.Time
	dropExemplars bool
}

// newDeltaSum returns an aggregator that summarizes a set of measurements as
// their arithmetic sum. Each sum is scoped by attributes and the aggregation
// cycle the measurements were made in.
func newDeltaSum[N int64 | float64](
	monotonic bool,
	limit int,
	r func(attribute.Set) FilteredExemplarReservoir[N],
) *deltaSum[N] {
	s := &deltaSum[N]{
		monotonic: monotonic,
		start:     now(),
		newRes:    r,
	}
	s.vals.init(limit)
	return s
}

// deltaSum is the storage for sums which resets every collection interval.
type deltaSum[N int64 | float64] struct {
	monotonic bool
	start     time.Time

	vals   hotColdMap[*sumValue[N]]
	newRes func(attribute.Set) FilteredExemplarReservoir[N]
}

func (s *deltaSum[N]) measure(ctx context.Context, value N, lazy lazyFilteredAttributes) {
	// 中文导读：Delta Sum 用 hot/cold 两份 map 分离当前写入与当前采集。
	// start 把本次写入登记到某一份，defer done 保证数值和 exemplar 处理完成后才释放登记。
	// 采集切换 hot map 后会等待旧写入结束，避免某次测量被清空却未进入任何输出周期。
	// 相同过滤后属性共用 sumValue；超限的新属性会由 limitedSyncMap 汇入 overflow。
	hotIdx := s.vals.start()
	defer s.vals.done(hotIdx)

	sv := s.vals.hot(hotIdx).LoadOrStoreAttr(lazy, func(attr attribute.Set) *sumValue[N] {
		r := s.newRes(attr)
		_, isDrop := r.(*dropRes[N])
		return &sumValue[N]{
			res:           r,
			attrs:         attr,
			startTime:     now(),
			dropExemplars: isDrop,
		}
	})
	sv.n.add(value)
	if !sv.dropExemplars {
		sv.res.Offer(ctx, value, lazy)
	}
}

func (s *deltaSum[N]) collect(
	dest *metricdata.Aggregation, //nolint:gocritic // The pointer is needed for the ComputeAggregation interface
) int {
	// 这里完成的是本 Reader 的一个增量周期：切换写入目标、读取旧 map、清空旧 map。
	// 收集完把 start 推到本次时间，下一次输出的时间区间从这里开始。
	// 清空发生在 Export 之前，导出失败不会使这些值自动留到下一轮；这不是可重试消息队列。
	t := now()

	// If *dest is not a metricdata.Sum, memory reuse is missed. In that case,
	// use the zero-value sData and hope for better alignment next cycle.
	sData, _ := (*dest).(metricdata.Sum[N])
	sData.Temporality = metricdata.DeltaTemporality
	sData.IsMonotonic = s.monotonic

	// delta always clears values on collection
	readIdx := s.vals.swapHotAndWait()
	// The len will not change while we iterate over values, since we waited
	// for all writes to finish to the cold values and len.
	n := s.vals.Len(readIdx)
	dPts := reset(sData.DataPoints, n, n)

	var i int
	s.vals.Range(readIdx, func(_, value any) bool {
		val := value.(*sumValue[N])
		collectExemplars(&dPts[i].Exemplars, val.res.Collect)
		dPts[i].Attributes = val.attrs
		dPts[i].StartTime = s.start
		dPts[i].Time = t
		dPts[i].Value = val.n.load()
		i++
		return true
	})
	// Unused attribute sets do not report.
	s.vals.Clear(readIdx)

	// The delta collection cycle resets.
	s.start = t

	sData.DataPoints = dPts
	*dest = sData

	return i
}

// newCumulativeSum returns an aggregator that summarizes a set of measurements
// as their arithmetic sum. Each sum is scoped by attributes and the
// aggregation cycle the measurements were made in.
func newCumulativeSum[N int64 | float64](
	monotonic bool,
	limit int,
	r func(attribute.Set) FilteredExemplarReservoir[N],
) *cumulativeSum[N] {
	return &cumulativeSum[N]{
		monotonic: monotonic,
		start:     now(),
		values:    limitedSyncMap[*sumValue[N]]{aggLimit: limit},
		newRes:    r,
	}
}

// cumulativeSum is the storage for sums which never reset.
//
// 中文导读：同步 Cumulative Sum 保留每组属性的累计值，采集后仍继续在同一值上累加。
// 它没有 Delta 的清空步骤，因此本轮没再 Add 的旧序列也可再次出现在输出中。
// 保留状态也意味着属性集合会持续占用空间，基数限制对于长期运行的进程尤其重要。
// 这里没有按“最近是否活跃”自动淘汰旧序列，不能把采集当成通用的内存回收操作。
type cumulativeSum[N int64 | float64] struct {
	monotonic bool
	start     time.Time

	values limitedSyncMap[*sumValue[N]]
	newRes func(attribute.Set) FilteredExemplarReservoir[N]
}

func (s *cumulativeSum[N]) measure(ctx context.Context, value N, lazy lazyFilteredAttributes) {
	sv := s.values.LoadOrStoreAttr(lazy, func(attr attribute.Set) *sumValue[N] {
		r := s.newRes(attr)
		_, isDrop := r.(*dropRes[N])
		return &sumValue[N]{
			res:           r,
			attrs:         attr,
			startTime:     now(),
			dropExemplars: isDrop,
		}
	})
	sv.n.add(value)
	// It is possible for collection to race with measurement and observe the
	// exemplar in the batch of metrics after the add() for cumulative sums.
	// This is an accepted tradeoff to avoid locking during measurement.
	if !sv.dropExemplars {
		sv.res.Offer(ctx, value, lazy)
	}
}

func (s *cumulativeSum[N]) collect(
	dest *metricdata.Aggregation, //nolint:gocritic // The pointer is needed for the ComputeAggregation interface
) int {
	// Cumulative 遍历时仍允许同步测量并发写入，所以预估 Len 仅用于容量，不作为固定结果长度。
	// 各属性序列的读取不是整个 SDK 的同一时刻快照，不能据此要求跨 instrument 的原子一致性。
	// 默认 StartTime 来自聚合器创建时间；启用逐序列起始时间实验特性时才使用各序列首次建值时间。
	t := now()

	// If *dest is not a metricdata.Sum, memory reuse is missed. In that case,
	// use the zero-value sData and hope for better alignment next cycle.
	sData, _ := (*dest).(metricdata.Sum[N])
	sData.Temporality = metricdata.CumulativeTemporality
	sData.IsMonotonic = s.monotonic

	// Values are being concurrently written while we iterate, so only use the
	// current length for capacity.
	dPts := reset(sData.DataPoints, 0, s.values.Len())

	perSeriesStartTimeEnabled := x.PerSeriesStartTimestamps.Enabled()

	var i int
	s.values.Range(func(_, value any) bool {
		val := value.(*sumValue[N])

		startTime := s.start
		if perSeriesStartTimeEnabled {
			startTime = val.startTime
		}
		newPt := metricdata.DataPoint[N]{
			Attributes: val.attrs,
			StartTime:  startTime,
			Time:       t,
			Value:      val.n.load(),
		}
		collectExemplars(&newPt.Exemplars, val.res.Collect)
		dPts = append(dPts, newPt)
		// TODO (#3006): This will use an unbounded amount of memory if there
		// are unbounded number of attribute sets being aggregated. Attribute
		// sets that become "stale" need to be forgotten so this will not
		// overload the system.
		i++
		return true
	})

	sData.DataPoints = dPts
	*dest = sData

	return i
}

// newPrecomputedSum returns an aggregator that summarizes a set of
// observations as their arithmetic sum. Each sum is scoped by attributes and
// the aggregation cycle the measurements were made in.
func newPrecomputedSum[N int64 | float64](
	monotonic bool,
	limit int,
	r func(attribute.Set) FilteredExemplarReservoir[N],
) *precomputedSum[N] {
	return &precomputedSum[N]{
		deltaSum: newDeltaSum[N](monotonic, limit, r),
	}
}

// precomputedSum summarizes a set of observations as their arithmetic sum.
//
// 中文导读：异步 Sum 每次采集只使用本轮回调观察到的值，不沿用上一轮的观察缓冲。
// 同一聚合属性键的观测值会相加，这也覆盖 View 去掉维度后多个原始序列合并的情况。
// reported 只用于 Delta 换算：若连续两轮读到 100 和 130，第二轮输出 30。
// 每轮会用 newReported 替换旧表，缺席的序列不永久保留基线；之后重新出现时从零基线计算。
// 这里直接做差，没有把下降读数自动识别为进程重启并修正，因此回调应遵守 instrument 的语义。
type precomputedSum[N int64 | float64] struct {
	*deltaSum[N]

	reported map[any]N
}

func (s *precomputedSum[N]) delta(
	dest *metricdata.Aggregation, //nolint:gocritic // The pointer is needed for the ComputeAggregation interface
) int {
	t := now()
	newReported := make(map[any]N)

	// If *dest is not a metricdata.Sum, memory reuse is missed. In that case,
	// use the zero-value sData and hope for better alignment next cycle.
	sData, _ := (*dest).(metricdata.Sum[N])
	sData.Temporality = metricdata.DeltaTemporality
	sData.IsMonotonic = s.monotonic

	// delta always clears values on collection
	readIdx := s.vals.swapHotAndWait()
	// The len will not change while we iterate over values, since we waited
	// for all writes to finish to the cold values and len.
	n := s.vals.Len(readIdx)
	dPts := reset(sData.DataPoints, n, n)

	var i int
	s.vals.Range(readIdx, func(key, value any) bool {
		val := value.(*sumValue[N])
		n := val.n.load()

		delta := n - s.reported[key]
		collectExemplars(&dPts[i].Exemplars, val.res.Collect)
		dPts[i].Attributes = val.attrs
		dPts[i].StartTime = s.start
		dPts[i].Time = t
		dPts[i].Value = delta
		newReported[key] = n
		i++
		return true
	})
	// Unused attribute sets do not report.
	s.vals.Clear(readIdx)
	s.reported = newReported
	// The delta collection cycle resets.
	s.start = t

	sData.DataPoints = dPts
	*dest = sData

	return i
}

func (s *precomputedSum[N]) cumulative(
	dest *metricdata.Aggregation, //nolint:gocritic // The pointer is needed for the ComputeAggregation interface
) int {
	// 虽然输出 temporality 是 Cumulative，这里的缓冲仍然必须清空。
	// “累计”描述回调提交的数值语义，不表示 SDK 要把 100、130 两轮读数加成 230。
	// 这与同步 cumulativeSum 保留 Add 增量的状态模型不同，也是异步序列能自然消失的原因。
	t := now()

	// If *dest is not a metricdata.Sum, memory reuse is missed. In that case,
	// use the zero-value sData and hope for better alignment next cycle.
	sData, _ := (*dest).(metricdata.Sum[N])
	sData.Temporality = metricdata.CumulativeTemporality
	sData.IsMonotonic = s.monotonic

	// cumulative precomputed always clears values on collection
	readIdx := s.vals.swapHotAndWait()
	// The len will not change while we iterate over values, since we waited
	// for all writes to finish to the cold values and len.
	n := s.vals.Len(readIdx)
	dPts := reset(sData.DataPoints, n, n)

	var i int
	s.vals.Range(readIdx, func(_, value any) bool {
		val := value.(*sumValue[N])
		collectExemplars(&dPts[i].Exemplars, val.res.Collect)
		dPts[i].Attributes = val.attrs
		dPts[i].StartTime = s.start
		dPts[i].Time = t
		dPts[i].Value = val.n.load()
		i++
		return true
	})
	// Unused attribute sets do not report.
	s.vals.Clear(readIdx)

	sData.DataPoints = dPts
	*dest = sData

	return i
}
