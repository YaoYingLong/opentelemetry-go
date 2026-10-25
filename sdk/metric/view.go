// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package metric

import (
	"errors"
	"regexp"
	"strings"

	"go.opentelemetry.io/otel/internal/global"
)

var (
	errMultiInst = errors.New("name replacement for multiple instruments")
	errEmptyView = errors.New("no criteria provided for view")

	emptyView = func(Instrument) (Stream, bool) { return Stream{}, false }
)

// View is an override to the default behavior of the SDK. It defines how data
// should be collected for certain instruments. It returns true and the exact
// Stream to use for matching Instruments. Otherwise, if the view does not
// match, false is returned.
//
// Note that attributes filtered out by a View may still appear on Exemplars,
// because Exemplars are recorded with the dropped measurement attributes
// when View attribute filtering is applied.
//
// 中文导读：View 是“instrument 元数据 -> 输出流配置”的函数，不接收每次测量的数值。
// 这让应用可以在不修改埋点库的前提下重命名指标、换聚合或保留部分属性维度。
// 它不是一个运行时导出过滤器；匹配结果交给 pipeline 创建聚合状态，后续测量写入该状态。
// 属性过滤主要改变指标分组维度，被过滤的属性仍可能进入 exemplar，不能当成通用脱敏规则。
type View func(Instrument) (Stream, bool)

// NewView returns a View that applies the Stream mask for all instruments that
// match criteria. The returned View will only apply mask if all non-zero-value
// fields of criteria match the corresponding Instrument passed to the view. If
// no criteria are provided, all field of criteria are their zero-values, a
// view that matches no instruments is returned. If you need to match a
// zero-value field, create a View directly.
//
// The Name field of criteria supports wildcard pattern matching. The "*"
// wildcard is recognized as matching zero or more characters, and "?" is
// recognized as matching exactly one character. For example, a pattern of "*"
// matches all instrument names.
//
// The Stream mask only applies updates for non-zero-value fields. By default,
// the Instrument the View matches against will be use for the Name,
// Description, and Unit of the returned Stream and no Aggregation or
// AttributeFilter are set. All non-zero-value fields of mask are used instead
// of the default. If you need to zero out a Stream field returned from a
// View, create a View directly.
func NewView(criteria Instrument, mask Stream) View {
	// NewView 是构造 View 的便利函数：criteria 的非零字段共同决定匹配条件，
	// mask 的非零字段覆盖输出配置。全零 criteria 不代表“匹配全部”，需要名称通配符 "*"。
	// 若要匹配零值或把输出字段清空，可直接实现 View 函数，绕过这里的非零值约定。
	if criteria.IsEmpty() {
		global.Error(
			errEmptyView, "dropping view",
			"mask", mask,
		)
		return emptyView
	}

	var matchFunc func(Instrument) bool
	if strings.ContainsAny(criteria.Name, "*?") {
		if mask.Name != "" {
			global.Error(
				errMultiInst, "dropping view",
				"criteria", criteria,
				"mask", mask,
			)
			return emptyView
		}

		// Handle branching here in NewView instead of criteria.matches so
		// criteria.matches remains inlinable for the simple case.
		pattern := regexp.QuoteMeta(criteria.Name)
		pattern = "^" + pattern + "$"
		pattern = strings.ReplaceAll(pattern, `\?`, ".")
		pattern = strings.ReplaceAll(pattern, `\*`, ".*")
		re := regexp.MustCompile(pattern)
		matchFunc = func(i Instrument) bool {
			return re.MatchString(i.Name) &&
				criteria.matchesDescription(i) &&
				criteria.matchesKind(i) &&
				criteria.matchesUnit(i) &&
				criteria.matchesScope(i)
		}
	} else {
		matchFunc = criteria.matches
	}

	var agg Aggregation
	if mask.Aggregation != nil {
		agg = mask.Aggregation.copy()
		if err := agg.err(); err != nil {
			global.Error(
				err, "not using aggregation with view",
				"criteria", criteria,
				"mask", mask,
			)
			agg = nil
		}
	}

	// 返回的闭包保留 matchFunc、mask 和已复制的聚合配置。
	// 通配符的正则编译与聚合参数校验在构造时完成，不必在每次 instrument 匹配时重做。
	// 通配匹配不允许统一替换名称，避免多个不同 instrument 被意外改成同一流名。
	return func(i Instrument) (Stream, bool) {
		if matchFunc(i) {
			return Stream{
				Name:                              nonZero(mask.Name, i.Name),
				Description:                       nonZero(mask.Description, i.Description),
				Unit:                              nonZero(mask.Unit, i.Unit),
				Aggregation:                       agg,
				AttributeFilter:                   mask.AttributeFilter,
				ExemplarReservoirProviderSelector: mask.ExemplarReservoirProviderSelector,
			}, true
		}
		return Stream{}, false
	}
}

// nonZero returns v if it is non-zero-valued, otherwise alt.
func nonZero[T comparable](v, alt T) T {
	var zero T
	if v != zero {
		return v
	}
	return alt
}
