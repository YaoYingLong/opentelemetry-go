// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package resource

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/internal/attrnorm"
	"go.opentelemetry.io/otel/sdk/internal/x"
)

// Resource describes an entity about which identifying information
// and metadata is exposed.  Resource is an immutable object,
// equivalent to a map from key to unique value.
//
// Resources should be passed and stored as pointers
// (`*resource.Resource`).  The `nil` value is equivalent to an empty
// Resource.
//
// Note that the Go == operator compares not just the resource attributes but
// also all other internals of the Resource type. Therefore, Resource values
// should not be used as map or database keys. In general, the [Resource.Equal]
// method should be used instead of direct comparison with ==, since that
// method ensures the correct comparison of resource attributes, and the
// [attribute.Distinct] returned from [Resource.Equivalent] should be used for
// map and database keys instead.
type Resource struct {
	// 中文导读：Resource 回答“数据来自哪个服务/进程/主机”；instrumentation.Scope
	// 回答“由哪个埋点库产生”；span/log/metric 上的属性描述某次操作或某组观测。
	// Resource 在配置阶段构造后共享复用，不应为每次请求重新探测主机或进程信息。
	attrs     attribute.Set
	schemaURL string
}

// Compile-time check that the Resource remains comparable.
var _ map[Resource]struct{} = nil

var (
	defaultResource     *Resource
	defaultResourceOnce sync.Once
)

// ErrSchemaURLConflict is an error returned when two Resources are merged
// together that contain different, non-empty, schema URLs.
var ErrSchemaURLConflict = errors.New("conflicting Schema URL")

// New returns a [Resource] built using opts.
// Duplicate top-level attribute keys and duplicate keys inside map
// values are resolved using last-value-wins semantics.
//
// This may return a partial Resource along with an error containing
// [ErrPartialResource] if options that provide a [Detector] are used and that
// error is returned from one or more of the Detectors. It may also return a
// merge-conflict Resource along with an error containing
// [ErrSchemaURLConflict] if merging Resources from the opts results in a
// schema URL conflict (see [Resource.Merge] for more information). It is up to
// the caller to determine if this returned Resource should be used or not
// based on these errors.
func New(ctx context.Context, opts ...Option) (*Resource, error) {
	// Detector 适合扩展云环境、容器等资源信息。配置决定探测顺序，detect 按顺序合并，
	// 后面的属性可覆盖前面的同名属性。Go 允许同时返回“部分可用的 Resource”和 error，
	// 调用者需要用 errors.Is 区分部分探测失败、schema 冲突等情况，而非认为有 err 就无结果。
	cfg := config{}
	for _, opt := range opts {
		cfg = opt.apply(cfg)
	}

	r := &Resource{schemaURL: cfg.schemaURL}
	return r, detect(ctx, r, cfg.detectors)
}

// NewWithAttributes creates a resource from attrs and associates the resource
// with a schema URL. If attrs contains duplicate top-level attribute keys or
// duplicate keys inside map values, the last value will be used. If attrs
// contains any invalid items those items will be dropped. The attrs are assumed
// to be in a schema identified by schemaURL.
func NewWithAttributes(schemaURL string, attrs ...attribute.KeyValue) *Resource {
	resource := NewSchemaless(attrs...)
	resource.schemaURL = schemaURL
	return resource
}

// NewSchemaless creates a resource from attrs. If attrs contains duplicate
// top-level attribute keys or duplicate keys inside map values, the last
// value will be used. If attrs contains any invalid items those items will be
// dropped. The resource will not be associated with a schema URL. If the schema
// of the attrs is known use NewWithAttributes instead.
func NewSchemaless(attrs ...attribute.KeyValue) *Resource {
	if len(attrs) == 0 {
		return &Resource{}
	}

	attrs, _ = attrnorm.KeyValuesDedup(attrs)

	// Ensure attributes comply with the specification:
	// https://github.com/open-telemetry/opentelemetry-specification/blob/v1.20.0/specification/common/README.md#attribute
	s, _ := attribute.NewSetWithFiltered(attrs, func(kv attribute.KeyValue) bool {
		return kv.Valid()
	})

	// If attrs only contains invalid entries do not allocate a new resource.
	if s.Len() == 0 {
		return &Resource{}
	}

	return &Resource{attrs: s} //nolint
}

// String implements the Stringer interface and provides a
// human-readable form of the resource.
//
// Avoid using this representation as the key in a map of resources,
// use Equivalent() as the key instead.
func (r *Resource) String() string {
	if r == nil {
		return ""
	}
	return r.attrs.Encoded(attribute.DefaultEncoder())
}

// MarshalLog is the marshaling function used by the logging system to represent this Resource.
func (r *Resource) MarshalLog() any {
	if r == nil {
		r = Empty()
	}
	return struct {
		Attributes attribute.Set
		SchemaURL  string
	}{
		Attributes: r.attrs,
		SchemaURL:  r.schemaURL,
	}
}

// Attributes returns a copy of attributes from the resource in a sorted order.
// To avoid allocating a new slice, use an iterator.
func (r *Resource) Attributes() []attribute.KeyValue {
	if r == nil {
		r = Empty()
	}
	return r.attrs.ToSlice()
}

// SchemaURL returns the schema URL associated with Resource r.
func (r *Resource) SchemaURL() string {
	if r == nil {
		return ""
	}
	return r.schemaURL
}

// Iter returns an iterator of the Resource attributes.
// This is ideal to use if you do not want a copy of the attributes.
func (r *Resource) Iter() attribute.Iterator {
	if r == nil {
		r = Empty()
	}
	return r.attrs.Iter()
}

// Equal reports whether r and o represent the same resource. Two resources can
// be equal even if they have different schema URLs.
//
// See the documentation on the [Resource] type for the pitfalls of using ==
// with Resource values; most code should use Equal instead.
func (r *Resource) Equal(o *Resource) bool {
	if r == nil {
		r = Empty()
	}
	if o == nil {
		o = Empty()
	}
	return r.Equivalent() == o.Equivalent()
}

// Merge creates a new [Resource] by merging a and b.
//
// If there are common keys between a and b, then the value from b will
// overwrite the value from a, even if b's value is empty.
//
// The SchemaURL of the resources will be merged according to the
// [OpenTelemetry specification rules]:
//
//   - If a's schema URL is empty then the returned Resource's schema URL will
//     be set to the schema URL of b,
//   - Else if b's schema URL is empty then the returned Resource's schema URL
//     will be set to the schema URL of a,
//   - Else if the schema URLs of a and b are the same then that will be the
//     schema URL of the returned Resource,
//   - Else this is a merging error. If the resources have different,
//     non-empty, schema URLs an error containing [ErrSchemaURLConflict] will
//     be returned with the merged Resource. The merged Resource will have an
//     empty schema URL. It may be the case that some unintended attributes
//     have been overwritten or old semantic conventions persisted in the
//     returned Resource. It is up to the caller to determine if this returned
//     Resource should be used or not.
//
// [OpenTelemetry specification rules]: https://github.com/open-telemetry/opentelemetry-specification/blob/v1.20.0/specification/resource/sdk.md#merge
func Merge(a, b *Resource) (*Resource, error) {
	// 合并不修改已有 Resource；nil 按空资源处理。若一边为空，可以直接复用另一边指针，
	// 这依赖 Resource 的不可变契约。b 的同名属性优先，但 schema 不同不能简单“后者覆盖”。
	// schema URL 标识属性采用的语义约定版本，不能把两个版本静默宣布成同一个版本。
	if a == nil && b == nil {
		return Empty(), nil
	}
	if a == nil {
		return b, nil
	}
	if b == nil {
		return a, nil
	}

	// Note: 'b' attributes will overwrite 'a' with last-value-wins in attribute.Key()
	// Meaning this is equivalent to: append(a.Attributes(), b.Attributes()...)
	mi := attribute.NewMergeIterator(b.Set(), a.Set())
	combine := make([]attribute.KeyValue, 0, a.Len()+b.Len())
	for mi.Next() {
		combine = append(combine, mi.Attribute())
	}

	switch {
	case a.schemaURL == "":
		return NewWithAttributes(b.schemaURL, combine...), nil
	case b.schemaURL == "":
		return NewWithAttributes(a.schemaURL, combine...), nil
	case a.schemaURL == b.schemaURL:
		return NewWithAttributes(a.schemaURL, combine...), nil
	}
	// Return the merged resource with an appropriate error. It is up to
	// the user to decide if the returned resource can be used or not.
	return NewSchemaless(combine...), fmt.Errorf(
		"%w: %s and %s",
		ErrSchemaURLConflict,
		a.schemaURL,
		b.schemaURL,
	)
}

// Empty returns an instance of Resource with no attributes. It is
// equivalent to a `nil` Resource.
func Empty() *Resource {
	return &Resource{}
}

// Default returns an instance of Resource with a default
// "service.name" and OpenTelemetrySDK attributes.
func Default() *Resource {
	return DefaultWithContext(context.Background())
}

// DefaultWithContext returns an instance of Resource with a default
// "service.name" and OpenTelemetrySDK attributes.
//
// If the default resource has already been initialized, the provided ctx
// is ignored and the cached resource is returned.
func DefaultWithContext(ctx context.Context) *Resource {
	// sync.Once 不仅防止重复工作，也让并发调用安全地共享初始化结果。
	// 默认资源只探测一次：之后改变环境变量或传入另一个 context，并不会触发重新探测。
	// 需要独立配置或可重复测试时，应显式构造 Resource，而不是依赖全局缓存重置。
	defaultResourceOnce.Do(func() {
		var err error
		defaultDetectors := []Detector{
			defaultServiceNameDetector{},
			fromEnv{},
			telemetrySDK{},
		}
		if x.Resource.Enabled() {
			defaultDetectors = append([]Detector{defaultServiceInstanceIDDetector{}}, defaultDetectors...)
		}
		defaultResource, err = Detect(
			ctx,
			defaultDetectors...,
		)
		if err != nil {
			otel.Handle(err)
		}
		// If Detect did not return a valid resource, fall back to emptyResource.
		if defaultResource == nil {
			defaultResource = &Resource{}
		}
	})
	return defaultResource
}

// Environment returns an instance of Resource with attributes
// extracted from the OTEL_RESOURCE_ATTRIBUTES and OTEL_SERVICE_NAME
// environment variables.
func Environment() *Resource {
	return EnvironmentWithContext(context.Background())
}

// EnvironmentWithContext returns an instance of Resource with attributes
// extracted from the OTEL_RESOURCE_ATTRIBUTES and OTEL_SERVICE_NAME
// environment variables.
func EnvironmentWithContext(ctx context.Context) *Resource {
	detector := &fromEnv{}
	resource, err := detector.Detect(ctx)
	if err != nil {
		otel.Handle(err)
	}
	return resource
}

// Equivalent returns an object that can be compared for equality
// between two resources. This value is suitable for use as a key in
// a map.
func (r *Resource) Equivalent() attribute.Distinct {
	return r.Set().Equivalent()
}

// Set returns the equivalent *attribute.Set of this resource's attributes.
func (r *Resource) Set() *attribute.Set {
	if r == nil {
		r = Empty()
	}
	return &r.attrs
}

// MarshalJSON encodes the resource attributes as a JSON list of { "Key":
// "...", "Value": ... } pairs in order sorted by key.
func (r *Resource) MarshalJSON() ([]byte, error) {
	if r == nil {
		r = Empty()
	}
	return r.attrs.MarshalJSON()
}

// Len returns the number of unique key-values in this Resource.
func (r *Resource) Len() int {
	if r == nil {
		return 0
	}
	return r.attrs.Len()
}

// Encoded returns an encoded representation of the resource.
func (r *Resource) Encoded(enc attribute.Encoder) string {
	if r == nil {
		return ""
	}
	return r.attrs.Encoded(enc)
}
