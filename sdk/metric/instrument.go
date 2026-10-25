// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:generate stringer -type=InstrumentKind -trimprefix=InstrumentKind

package metric

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/embedded"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	"go.opentelemetry.io/otel/sdk/metric/internal/aggregate"
	"go.opentelemetry.io/otel/sdk/metric/internal/attrnorm"
)

var zeroScope instrumentation.Scope

// InstrumentKind is the identifier of a group of instruments that all
// performing the same function.
type InstrumentKind uint8

const (
	// instrumentKindUndefined is an undefined instrument kind, it should not
	// be used by any initialized type.
	instrumentKindUndefined InstrumentKind = 0 // nolint:unused
	// InstrumentKindCounter identifies a group of instruments that record
	// increasing values synchronously with the code path they are measuring.
	InstrumentKindCounter InstrumentKind = 1
	// InstrumentKindUpDownCounter identifies a group of instruments that
	// record increasing and decreasing values synchronously with the code path
	// they are measuring.
	InstrumentKindUpDownCounter InstrumentKind = 2
	// InstrumentKindHistogram identifies a group of instruments that record a
	// distribution of values synchronously with the code path they are
	// measuring.
	InstrumentKindHistogram InstrumentKind = 3
	// InstrumentKindObservableCounter identifies a group of instruments that
	// record increasing values in an asynchronous callback.
	InstrumentKindObservableCounter InstrumentKind = 4
	// InstrumentKindObservableUpDownCounter identifies a group of instruments
	// that record increasing and decreasing values in an asynchronous
	// callback.
	InstrumentKindObservableUpDownCounter InstrumentKind = 5
	// InstrumentKindObservableGauge identifies a group of instruments that
	// record current values in an asynchronous callback.
	InstrumentKindObservableGauge InstrumentKind = 6
	// InstrumentKindGauge identifies a group of instruments that record
	// instantaneous values synchronously with the code path they are
	// measuring.
	InstrumentKindGauge InstrumentKind = 7
)

type nonComparable [0]func() // nolint: unused  // This is indeed used.

// Instrument describes properties an instrument is created with.
type Instrument struct {
	// Name is the human-readable identifier of the instrument.
	Name string
	// Description describes the purpose of the instrument.
	Description string
	// Kind defines the functional group of the instrument.
	Kind InstrumentKind
	// Unit is the unit of measurement recorded by the instrument.
	Unit string
	// Scope identifies the instrumentation that created the instrument.
	Scope instrumentation.Scope

	// Ensure forward compatibility if non-comparable fields need to be added.
	nonComparable // nolint: unused
}

// IsEmpty reports whether all Instrument fields are their zero-value.
func (i Instrument) IsEmpty() bool {
	return i.Name == "" &&
		i.Description == "" &&
		i.Kind == instrumentKindUndefined &&
		i.Unit == "" &&
		i.Scope == zeroScope
}

// matches returns whether all the non-zero-value fields of i match the
// corresponding fields of other. If i is empty it will match all other, and
// true will always be returned.
func (i Instrument) matches(other Instrument) bool {
	return i.matchesName(other) &&
		i.matchesDescription(other) &&
		i.matchesKind(other) &&
		i.matchesUnit(other) &&
		i.matchesScope(other)
}

// matchesName returns true if the Name of i is "" or it equals the Name of
// other, otherwise false.
func (i Instrument) matchesName(other Instrument) bool {
	return i.Name == "" || i.Name == other.Name
}

// matchesDescription returns true if the Description of i is "" or it equals
// the Description of other, otherwise false.
func (i Instrument) matchesDescription(other Instrument) bool {
	return i.Description == "" || i.Description == other.Description
}

// matchesKind returns true if the Kind of i is its zero-value or it equals the
// Kind of other, otherwise false.
func (i Instrument) matchesKind(other Instrument) bool {
	return i.Kind == instrumentKindUndefined || i.Kind == other.Kind
}

// matchesUnit returns true if the Unit of i is its zero-value or it equals the
// Unit of other, otherwise false.
func (i Instrument) matchesUnit(other Instrument) bool {
	return i.Unit == "" || i.Unit == other.Unit
}

// matchesScope returns true if the Scope of i is its zero-value or it equals
// the Scope of other, otherwise false.
func (i Instrument) matchesScope(other Instrument) bool {
	return (i.Scope.Name == "" || i.Scope.Name == other.Scope.Name) &&
		(i.Scope.Version == "" || i.Scope.Version == other.Scope.Version) &&
		(i.Scope.SchemaURL == "" || i.Scope.SchemaURL == other.Scope.SchemaURL)
}

// Stream describes the stream of data an instrument produces.
type Stream struct {
	// Name is the human-readable identifier of the stream.
	Name string
	// Description describes the purpose of the data.
	Description string
	// Unit is the unit of measurement recorded.
	Unit string
	// Aggregation the stream uses for an instrument.
	Aggregation Aggregation
	// AttributeFilter is an attribute Filter applied to the attributes
	// recorded for an instrument's measurement. If the filter returns false
	// the attribute will not be recorded, otherwise, if it returns true, it
	// will record the attribute.
	//
	// If nil, the instrument's advisory attribute keys are used. If the
	// instrument has no advisory attribute keys, all attributes are recorded.
	//
	// Note that attributes filtered out by a View may still appear on Exemplars,
	// because Exemplars are recorded with the dropped measurement attributes
	// when View attribute filtering is applied.
	//
	// Use NewAllowKeysFilter from "go.opentelemetry.io/otel/attribute" to
	// provide an allow-list of attribute keys here.
	AttributeFilter attribute.Filter
	// ExemplarReservoirProvider selects the
	// [go.opentelemetry.io/otel/sdk/metric/exemplar.ReservoirProvider] based
	// on the [Aggregation].
	//
	// If unspecified, [DefaultExemplarReservoirProviderSelector] is used.
	ExemplarReservoirProviderSelector ExemplarReservoirProviderSelector
}

// instID are the identifying properties of a instrument.
type instID struct {
	// Name is the name of the stream.
	Name string
	// Description is the description of the stream.
	Description string
	// Kind defines the functional group of the instrument.
	Kind InstrumentKind
	// Unit is the unit of the stream.
	Unit string
	// Number is the number type of the stream.
	Number string
}

// Returns a normalized copy of the instID i.
//
// Instrument names are considered case-insensitive. Standardize the instrument
// name to always be lowercase for the returned instID so it can be compared
// without the name casing affecting the comparison.
func (i instID) normalize() instID {
	i.Name = strings.ToLower(i.Name)
	return i
}

type rawAttributesOption interface {
	RawAttributes() []attribute.KeyValue
	Experimental()
}

func extractRawKVs[T any](opts []T) []attribute.KeyValue {
	// Go 的切片赋值只复制描述符，仍可能共用底层数组；第二次追加原始属性前必须先复制。
	// 否则第一个选项的切片若还有多余容量，append 可能悄悄覆盖调用方仍持有的数据。
	var rawKVs []attribute.KeyValue
	var count int
	for _, opt := range opts {
		if r, ok := any(opt).(rawAttributesOption); ok {
			count++
			if count == 1 {
				rawKVs = r.RawAttributes()
			} else {
				if count == 2 {
					// Create a new slice to avoid modifying the original slice from the first option.
					rawKVs = append([]attribute.KeyValue(nil), rawKVs...)
				}
				rawKVs = append(rawKVs, r.RawAttributes()...)
			}
		}
	}
	return rawKVs
}

func resolveAttributes(configAttrs attribute.Set, rawKVs []attribute.KeyValue) attribute.Set {
	// 这里先把测量选项统一成 attribute.Set；同名键以追加在后的 rawKVs 为准。
	// View 过滤仍在各个 Measure 内执行，不能在这里按某个 Reader 的规则提前丢掉维度。
	configAttrs, _ = attrnorm.SetDedup(configAttrs)
	if len(rawKVs) == 0 {
		return configAttrs
	}
	rawKVs, _ = attrnorm.KeyValuesDedup(rawKVs)
	merged := make([]attribute.KeyValue, 0, configAttrs.Len()+len(rawKVs))
	merged = append(merged, configAttrs.ToSlice()...)
	// rawKVs are appended after configAttrs, meaning they will override any duplicate keys in configAttrs.
	// This behavior is documented in WithUnsafeAttributes.
	merged = append(merged, rawKVs...)
	// TODO(#7743): Defer computing the full attribute.NewSet.
	return attribute.NewSet(merged...)
}

// 中文导读：这个实现同时满足多种 int64 同步 instrument 接口，实际语义由创建时绑定的聚合器决定。
// Counter 与 Histogram 不需要在每次调用时再按 kind 分支：它们保存的 Measure 已经不同。
// measures 在构造完成后供记录路径读取；聚合状态由函数引用的聚合器持有，不存放在此结构体中。
type int64Inst struct {
	measures []aggregate.Measure[int64]

	embedded.Int64Counter
	embedded.Int64UpDownCounter
	embedded.Int64Histogram
	embedded.Int64Gauge
}

var (
	_ metric.Int64Counter       = (*int64Inst)(nil)
	_ metric.Int64UpDownCounter = (*int64Inst)(nil)
	_ metric.Int64Histogram     = (*int64Inst)(nil)
	_ metric.Int64Gauge         = (*int64Inst)(nil)
)

func (i *int64Inst) Add(ctx context.Context, val int64, opts ...metric.AddOption) {
	// 中文导读：这是应用调用 Counter.Add 后进入 SDK 的实际记录入口。
	// 当前 ctx 被继续传给聚合器，供 exemplar 获取关联信息；记录路径不会因 ctx 已取消而丢弃数值。
	// 记录上下文与 Reader 的采集/导出上下文职责不同，取消业务请求不是关闭遥测管线的方式。
	c := metric.NewAddConfig(opts)
	rawKVs := extractRawKVs(opts)
	i.aggregate(ctx, val, resolveAttributes(c.Attributes(), rawKVs))
}

func (i *int64Inst) Record(ctx context.Context, val int64, opts ...metric.RecordOption) {
	c := metric.NewRecordConfig(opts)
	rawKVs := extractRawKVs(opts)
	i.aggregate(ctx, val, resolveAttributes(c.Attributes(), rawKVs))
}

func (i *int64Inst) Enabled(context.Context) bool {
	return len(i.measures) != 0
}

func (i *int64Inst) aggregate(
	ctx context.Context,
	val int64,
	s attribute.Set,
) { // nolint:revive  // okay to shadow pkg with method.
	// 每个函数对应已解析的 Reader/流聚合入口；同一次测量依次更新这些独立状态。
	// 这一步在调用 Add/Record 的 goroutine 内完成，没有启动任务，也没有调用 Exporter。
	// 空列表可能来自未配置 Reader 或 Drop 聚合；遍历为空自然不产生任何聚合更新。
	for _, in := range i.measures {
		in(ctx, val, s)
	}
}

type float64Inst struct {
	measures []aggregate.Measure[float64]

	embedded.Float64Counter
	embedded.Float64UpDownCounter
	embedded.Float64Histogram
	embedded.Float64Gauge
}

var (
	_ metric.Float64Counter       = (*float64Inst)(nil)
	_ metric.Float64UpDownCounter = (*float64Inst)(nil)
	_ metric.Float64Histogram     = (*float64Inst)(nil)
	_ metric.Float64Gauge         = (*float64Inst)(nil)
)

func (i *float64Inst) Add(ctx context.Context, val float64, opts ...metric.AddOption) {
	c := metric.NewAddConfig(opts)
	rawKVs := extractRawKVs(opts)
	i.aggregate(ctx, val, resolveAttributes(c.Attributes(), rawKVs))
}

func (i *float64Inst) Record(ctx context.Context, val float64, opts ...metric.RecordOption) {
	// float64 的记录桥接与 int64 相同；Histogram 的分桶、Gauge 的末值保留发生在各自的 Measure 内。
	// Record 仅负责传递本次数值和属性，不能在这里把所有 instrument 都当作加法计数器处理。
	c := metric.NewRecordConfig(opts)
	rawKVs := extractRawKVs(opts)
	i.aggregate(ctx, val, resolveAttributes(c.Attributes(), rawKVs))
}

func (i *float64Inst) Enabled(context.Context) bool {
	return len(i.measures) != 0
}

func (i *float64Inst) aggregate(ctx context.Context, val float64, s attribute.Set) {
	for _, in := range i.measures {
		in(ctx, val, s)
	}
}

// observableID is a comparable unique identifier of an observable.
type observableID[N int64 | float64] struct {
	name        string
	description string
	kind        InstrumentKind
	unit        string
	scope       instrumentation.Scope
}

type float64Observable struct {
	metric.Float64Observable
	*observable[float64]

	embedded.Float64ObservableCounter
	embedded.Float64ObservableUpDownCounter
	embedded.Float64ObservableGauge
}

var (
	_ metric.Float64ObservableCounter       = float64Observable{}
	_ metric.Float64ObservableUpDownCounter = float64Observable{}
	_ metric.Float64ObservableGauge         = float64Observable{}
)

func newFloat64Observable(m *meter, kind InstrumentKind, name, desc, u string) float64Observable {
	return float64Observable{
		observable: newObservable[float64](m, kind, name, desc, u),
	}
}

type int64Observable struct {
	metric.Int64Observable
	*observable[int64]

	embedded.Int64ObservableCounter
	embedded.Int64ObservableUpDownCounter
	embedded.Int64ObservableGauge
}

var (
	_ metric.Int64ObservableCounter       = int64Observable{}
	_ metric.Int64ObservableUpDownCounter = int64Observable{}
	_ metric.Int64ObservableGauge         = int64Observable{}
)

func newInt64Observable(m *meter, kind InstrumentKind, name, desc, u string) int64Observable {
	return int64Observable{
		observable: newObservable[int64](m, kind, name, desc, u),
	}
}

type observable[N int64 | float64] struct {
	metric.Observable
	observableID[N]

	meter           *meter
	measures        measures[N]
	dropAggregation bool
}

func newObservable[N int64 | float64](m *meter, kind InstrumentKind, name, desc, u string) *observable[N] {
	return &observable[N]{
		observableID: observableID[N]{
			name:        name,
			description: desc,
			kind:        kind,
			unit:        u,
			scope:       m.scope,
		},
		meter: m,
	}
}

// observe records the val for the set of attrs.
func (o *observable[N]) observe(val N, s attribute.Set) {
	o.measures.observe(val, s)
}

func (o *observable[N]) appendMeasures(meas []aggregate.Measure[N]) {
	o.measures = append(o.measures, meas...)
}

type measures[N int64 | float64] []aggregate.Measure[N]

// observe records the val for the set of attrs.
func (m measures[N]) observe(val N, s attribute.Set) {
	// 单 instrument 回调使用的 observer 持有当前 pipeline 的这份函数列表。
	// Observe 没有显式 ctx 参数，因此这里使用 Background；采集取消由外围回调处理。
	for _, in := range m {
		in(context.Background(), val, s)
	}
}

var errEmptyAgg = errors.New("no aggregators for observable instrument")

// registerable returns an error if the observable o should not be registered,
// and nil if it should. An errEmptyAgg error is returned if o is effectively a
// no-op because it does not have any aggregators. Also, an error is returned
// if scope defines a Meter other than the one o was created by.
func (o *observable[N]) registerable(m *meter) error {
	if len(o.measures) == 0 {
		return errEmptyAgg
	}
	if m != o.meter {
		return fmt.Errorf(
			"invalid registration: observable %q from Meter %q, registered with Meter %q",
			o.name,
			o.scope.Name,
			m.scope.Name,
		)
	}
	return nil
}
