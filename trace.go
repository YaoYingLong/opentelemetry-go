// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package otel

import (
	"go.opentelemetry.io/otel/internal/global"
	"go.opentelemetry.io/otel/trace"
)

// Tracer creates a named tracer that implements Tracer interface.
// If the name is an empty string then provider uses default name.
//
// This is short for GetTracerProvider().Tracer(name, opts...)
func Tracer(name string, opts ...trace.TracerOption) trace.Tracer {
	// 中文导读：这里是埋点库访问 Trace API 的入口，真正的创建逻辑由 Provider 决定。
	// name 标识“哪个埋点库产生数据”（instrumentation scope），通常使用包的导入路径；
	// 它不是 service.name，也不是某一次请求的 span 名称。服务身份配置在 SDK Resource 中。
	// API 返回接口，因而埋点库可以只依赖 API，由宿主应用决定是否安装 SDK、如何采样和导出。
	return GetTracerProvider().Tracer(name, opts...)
}

// GetTracerProvider returns the registered global trace provider.
// If none is registered then an instance of NoopTracerProvider is returned.
//
// Use the trace provider to create a named tracer. E.g.
//
//	tracer := otel.GetTracerProvider().Tracer("example.com/foo")
//
// or
//
//	tracer := otel.Tracer("example.com/foo")
func GetTracerProvider() trace.TracerProvider {
	return global.TracerProvider()
}

// SetTracerProvider registers `tp` as the global trace provider.
func SetTracerProvider(tp trace.TracerProvider) {
	// 应用初始化时把 SDK Provider 接到全局入口。可复用的组件通常接受外部传入的 Provider，
	// 不应在组件内部覆盖全局配置；创建 Provider 的应用也负责最终的 Shutdown。
	// 默认占位 Tracer 如何接到第一次安装的 SDK，继续阅读 internal/global/state.go 和 trace.go。
	global.SetTracerProvider(tp)
}
