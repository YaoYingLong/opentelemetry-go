// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package baggage

import "context"

type baggageContextKeyType int

// 声明常量baggageKey，baggageContextKeyType是常量类型.
const baggageKey baggageContextKeyType = iota

// SetHookFunc is a callback called when storing baggage in the context.
// 将baggage存储到Context时的回调.
type SetHookFunc func(context.Context, List) context.Context

// GetHookFunc is a callback called when getting baggage from the context.
// 从Context获取baggage时的回调.
type GetHookFunc func(context.Context, List) List

type baggageState struct {
	// 是internal/baggage/baggage.go中定义的类型为map[string]Item
	list List

	setHook SetHookFunc
	getHook GetHookFunc
}

// ContextWithSetHook returns a copy of parent with hook configured to be
// invoked every time ContextWithBaggage is called.
//
// Passing nil SetHookFunc creates a context with no set hook to call.
func ContextWithSetHook(parent context.Context, hook SetHookFunc) context.Context {
	var s baggageState
	if v, ok := parent.Value(baggageKey).(baggageState); ok {
		// 如果parent已经存在key为parent的baggageState对象直接使用
		s = v
	}

	s.setHook = hook
	// 生成一个新的Context具体类型为valueCtx，key为baggageKey，值为s
	return context.WithValue(parent, baggageKey, s)
}

// ContextWithGetHook returns a copy of parent with hook configured to be
// invoked every time FromContext is called.
//
// Passing nil GetHookFunc creates a context with no get hook to call.
func ContextWithGetHook(parent context.Context, hook GetHookFunc) context.Context {
	var s baggageState
	if v, ok := parent.Value(baggageKey).(baggageState); ok {
		s = v
	}

	s.getHook = hook
	return context.WithValue(parent, baggageKey, s)
}

// ContextWithList returns a copy of parent with baggage. Passing nil list
// returns a context without any baggage.
func ContextWithList(parent context.Context, list List) context.Context {
	var s baggageState
	if v, ok := parent.Value(baggageKey).(baggageState); ok {
		s = v
	}

	s.list = list
	ctx := context.WithValue(parent, baggageKey, s)
	if s.setHook != nil {
		// 执行setHook方法
		ctx = s.setHook(ctx, list)
	}

	return ctx
}

// ListFromContext returns the baggage contained in ctx.
func ListFromContext(ctx context.Context) List {
	// 获取Context中存储的baggageKey的值得类型
	switch v := ctx.Value(baggageKey).(type) {
	case baggageState:
		if v.getHook != nil {
			// 如果有getHook方法直接调用返回
			return v.getHook(ctx, v.list)
		}
		return v.list
	default:
		return nil
	}
}
