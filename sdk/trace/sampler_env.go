// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package trace

import (
	"errors"
	"os"
	"strconv"
	"strings"
)

const (
	tracesSamplerKey    = "OTEL_TRACES_SAMPLER"
	tracesSamplerArgKey = "OTEL_TRACES_SAMPLER_ARG"

	samplerAlwaysOn                = "always_on"
	samplerAlwaysOff               = "always_off"
	samplerTraceIDRatio            = "traceidratio"
	samplerParentBasedAlwaysOn     = "parentbased_always_on"
	samplerParsedBasedAlwaysOff    = "parentbased_always_off"
	samplerParentBasedTraceIDRatio = "parentbased_traceidratio"
)

type errUnsupportedSampler string

func (e errUnsupportedSampler) Error() string {
	return "unsupported sampler: " + string(e)
}

var (
	errNegativeTraceIDRatio       = errors.New("invalid trace ID ratio: less than 0.0")
	errGreaterThanOneTraceIDRatio = errors.New("invalid trace ID ratio: greater than 1.0")
)

type samplerArgParseError struct {
	parseErr error
}

func (e samplerArgParseError) Error() string {
	return "parsing sampler argument: " + e.parseErr.Error()
}

func (e samplerArgParseError) Unwrap() error {
	return e.parseErr
}

func samplerFromEnv() (Sampler, error) {
	// 获取OTEL_TRACES_SAMPLER环境变量配置的内容，即采样类型
	sampler := strings.ToLower(strings.TrimSpace(os.Getenv(tracesSamplerKey)))
	if sampler == "" {
		return nil, nil
	}
	// 获取OTEL_TRACES_SAMPLER_ARG环境变量配置的内容，采样比例
	samplerArg := strings.TrimSpace(os.Getenv(tracesSamplerArgKey))
	hasSamplerArg := samplerArg != ""

	switch sampler {
	case samplerAlwaysOn:
		return AlwaysSample(), nil
	case samplerAlwaysOff:
		return NeverSample(), nil
	case samplerTraceIDRatio:
		if !hasSamplerArg {
			return TraceIDRatioBased(1.0), nil
		}
		return parseTraceIDRatio(samplerArg)
	case samplerParentBasedAlwaysOn:
		return ParentBased(AlwaysSample()), nil
	case samplerParsedBasedAlwaysOff:
		return ParentBased(NeverSample()), nil
	case samplerParentBasedTraceIDRatio:
		if !hasSamplerArg {
			return ParentBased(TraceIDRatioBased(1.0)), nil
		}
		ratio, err := parseTraceIDRatio(samplerArg)
		return ParentBased(ratio), err
	default:
		return nil, errUnsupportedSampler(sampler)
	}
}

func parseTraceIDRatio(arg string) (Sampler, error) {
	v, err := strconv.ParseFloat(arg, 64)
	if err != nil {
		return TraceIDRatioBased(1.0), samplerArgParseError{err}
	}
	if v < 0.0 {
		return TraceIDRatioBased(1.0), errNegativeTraceIDRatio
	}
	if v > 1.0 {
		return TraceIDRatioBased(1.0), errGreaterThanOneTraceIDRatio
	}

	return TraceIDRatioBased(v), nil
}
