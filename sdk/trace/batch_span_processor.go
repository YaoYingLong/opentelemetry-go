// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package trace

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/internal/global"
	"go.opentelemetry.io/otel/sdk/trace/internal/env"
	"go.opentelemetry.io/otel/sdk/trace/internal/observ"
	"go.opentelemetry.io/otel/trace"
)

// Defaults for BatchSpanProcessorOptions.
const (
	DefaultMaxQueueSize = 2048
	// DefaultScheduleDelay is the delay interval between two consecutive exports, in milliseconds.
	DefaultScheduleDelay = 5000
	// DefaultExportTimeout is the duration after which an export is cancelled, in milliseconds.
	DefaultExportTimeout      = 30000
	DefaultMaxExportBatchSize = 512
)

// BatchSpanProcessorOption configures a BatchSpanProcessor.
type BatchSpanProcessorOption func(o *BatchSpanProcessorOptions)

// BatchSpanProcessorOptions is configuration settings for a
// BatchSpanProcessor.
type BatchSpanProcessorOptions struct {
	// MaxQueueSize is the maximum queue size to buffer spans for delayed processing. If the
	// queue gets full it drops the spans. Use BlockOnQueueFull to change this behavior.
	// The default value of MaxQueueSize is 2048.
	MaxQueueSize int

	// BatchTimeout is the maximum duration for constructing a batch. Processor
	// forcefully sends available spans when timeout is reached.
	// The default value of BatchTimeout is 5000 msec.
	BatchTimeout time.Duration

	// ExportTimeout specifies the maximum duration for exporting spans. If the timeout
	// is reached, the export will be cancelled.
	// The default value of ExportTimeout is 30000 msec.
	ExportTimeout time.Duration

	// MaxExportBatchSize is the maximum number of spans to process in a single batch.
	// If there are more than one batch worth of spans then it processes multiple batches
	// of spans one batch after the other without any delay.
	// The default value of MaxExportBatchSize is 512.
	MaxExportBatchSize int

	// BlockOnQueueFull blocks the OnEnd method if the queue is full
	// AND if BlockOnQueueFull is set to true.
	// Blocking option should be used carefully as it can severely affect the performance of an
	// application.
	BlockOnQueueFull bool
}

// batchSpanProcessor is a SpanProcessor that batches asynchronously-received
// spans and sends them to a trace.Exporter when complete.
type batchSpanProcessor struct {
	// 中文导读：业务 goroutine 在 OnEnd 入队，后台 goroutine 聚合并导出。
	// channel 负责交接已结束的 Span 快照；batch 是另一个缓冲区，两者共同占用内存。
	// batchMutex 还协调 ForceFlush 与后台导出，保证同一处理器不会并发调用 ExportSpans。
	e SpanExporter
	o BatchSpanProcessorOptions

	// 有界 channel 限制等待处理的数量，容量取配置值，默认 2048；它不是持久化队列。
	queue   chan ReadOnlySpan
	dropped atomic.Uint32

	inst *observ.BSP

	batch      []ReadOnlySpan
	batchMutex sync.Mutex
	timer      *time.Timer
	stopWait   sync.WaitGroup
	stopOnce   sync.Once
	stopCh     chan struct{}
	stopped    atomic.Bool
}

var _ SpanProcessor = (*batchSpanProcessor)(nil)

// NewBatchSpanProcessor creates a new SpanProcessor that will send completed
// span batches to the exporter with the supplied options.
//
// If the exporter is nil, the span processor will perform no action.
func NewBatchSpanProcessor(exporter SpanExporter, options ...BatchSpanProcessorOption) SpanProcessor {
	// 中文导读：先读取环境变量，再应用显式选项；批量大小、排队容量和超时各自控制不同阶段。
	// BatchTimeout 控制凑批等待，ExportTimeout 控制单次导出的 Context 期限。
	// 内存队列大小默认 2048，可由环境变量和选项覆盖。
	maxQueueSize := env.BatchSpanProcessorMaxQueueSize(DefaultMaxQueueSize)
	// 单批数量默认 512，可由环境变量和选项覆盖。
	maxExportBatchSize := env.BatchSpanProcessorMaxExportBatchSize(DefaultMaxExportBatchSize)

	if maxExportBatchSize > maxQueueSize {
		maxExportBatchSize = min(DefaultMaxExportBatchSize, maxQueueSize)
	}

	o := BatchSpanProcessorOptions{
		// 默认凑批时间为 5s。
		BatchTimeout: time.Duration(env.BatchSpanProcessorScheduleDelay(DefaultScheduleDelay)) * time.Millisecond,
		// 默认导出超时为 30s。
		ExportTimeout:      time.Duration(env.BatchSpanProcessorExportTimeout(DefaultExportTimeout)) * time.Millisecond,
		MaxQueueSize:       maxQueueSize,
		MaxExportBatchSize: maxExportBatchSize,
	}
	// 这里的 Option 是函数类型，通过配置指针直接修改字段，与 Provider 的 apply 模式略有不同。
	for _, opt := range options {
		opt(&o)
	}
	bsp := &batchSpanProcessor{
		e:     exporter,
		o:     o,
		batch: make([]ReadOnlySpan, 0, o.MaxExportBatchSize),
		timer: time.NewTimer(o.BatchTimeout),
		// make(chan T, n) 创建容量为 n 的 channel，发送方和接收方可以短暂解耦。
		queue:  make(chan ReadOnlySpan, o.MaxQueueSize),
		stopCh: make(chan struct{}),
	}

	var err error
	bsp.inst, err = observ.NewBSP(
		nextProcessorID(),
		func() int64 { return int64(len(bsp.queue)) },
		int64(bsp.o.MaxQueueSize),
	)
	if err != nil {
		otel.Handle(err)
	}

	// WaitGroup.Go 启动并登记后台任务，Shutdown 通过 Wait 等待它完成。
	// 正常循环收到停止信号后，继续执行 drainQueue 尝试导出剩余数据，再结束此任务。
	bsp.stopWait.Go(func() {
		bsp.processQueue()
		bsp.drainQueue()
	})

	return bsp
}

var processorIDCounter atomic.Int64

// nextProcessorID returns an identifier for this batch span processor,
// starting with 0 and incrementing by 1 each time it is called.
func nextProcessorID() int64 {
	return processorIDCounter.Add(1) - 1
}

// OnStart method does nothing.
func (*batchSpanProcessor) OnStart(context.Context, ReadWriteSpan) {}

// OnEnd method enqueues a ReadOnlySpan for later processing.
func (bsp *batchSpanProcessor) OnEnd(s ReadOnlySpan) {
	// OnStart 在本实现中是空操作，只有 OnEnd 会排队；WithBlocking 影响的是结束 Span 的调用。
	// Do not enqueue spans after Shutdown.
	if bsp.stopped.Load() {
		return
	}

	// Do not enqueue spans if we are just going to drop them.
	if bsp.e == nil {
		return
	}
	bsp.enqueue(s)
}

// Shutdown flushes the queue and waits until all spans are processed.
// It only executes once. Subsequent call does nothing.
func (bsp *batchSpanProcessor) Shutdown(ctx context.Context) error {
	// 中文导读：先标记停止接收，再唤醒后台任务、等待清空队列，最后关闭 Exporter。
	// stopCh 的关闭广播停止信号，queue 本身不关闭，避免并发发送触发 send on closed channel。
	// ctx 取消会让本次等待返回，但不会强制杀死后台任务；drainQueue 使用自己的导出 Context。
	var err error
	bsp.stopOnce.Do(func() {
		bsp.stopped.Store(true)
		wait := make(chan struct{})
		// exportErr is written by the goroutine before closing wait.
		// It is only read in the <-wait case, so there is no race.
		var exportErr error
		// 写 exportErr 后关闭 wait；接收方观察到 wait 关闭后才能读取该值，形成同步关系。
		go func() {
			close(bsp.stopCh)
			bsp.stopWait.Wait()
			if bsp.e != nil {
				exportErr = bsp.e.Shutdown(ctx)
			}
			close(wait)
		}()
		// Wait until the channel is ready or the context is canceled.
		select {
		case <-wait:
			err = exportErr
		case <-ctx.Done():
			err = ctx.Err()
		}
		if bsp.inst != nil {
			err = errors.Join(err, bsp.inst.Shutdown())
		}
	})
	return err
}

type forceFlushSpan struct {
	// 中文导读：这是借用 Span 队列传递的控制消息，不是真正要导出的 Span。
	// 嵌入接口可提供方法集合，后台必须先识别这个类型，不能当普通 Span 读取其余字段。
	ReadOnlySpan
	flushed chan struct{}
}

func (forceFlushSpan) SpanContext() trace.SpanContext {
	return trace.NewSpanContext(trace.SpanContextConfig{TraceFlags: trace.FlagsSampled})
}

// ForceFlush exports all ended spans that have not yet been exported.
func (bsp *batchSpanProcessor) ForceFlush(ctx context.Context) error {
	// 中文导读：先把标记排到队尾并等待它被消费，再导出尚未满批的缓冲。
	// 只直接调用 exportSpans 会漏掉仍滞留在 channel 中的 Span，所以需要这个队列屏障。
	// 标记被消费只表示其前面的消息已处理到批缓冲/导出阶段，不表示最后一批已发送成功。
	// Interrupt if context is already canceled.
	if err := ctx.Err(); err != nil {
		return err
	}

	// Do nothing after Shutdown.
	if bsp.stopped.Load() {
		return nil
	}

	var err error
	if bsp.e != nil {
		flushCh := make(chan struct{})
		if bsp.enqueueBlockOnQueueFull(ctx, forceFlushSpan{flushed: flushCh}) {
			select {
			case <-bsp.stopCh:
				// The batchSpanProcessor is Shutdown.
				return nil
			case <-flushCh:
				// Processed any items in queue prior to ForceFlush being called
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		wait := make(chan error, 1)
		// 容量 1 允许调用方因取消离开后，后台仍能写入结果并退出，避免卡在发送结果上。
		go func() {
			wait <- bsp.exportSpans(ctx)
		}()
		// Wait until the export is finished or the context is cancelled/timed out
		select {
		case err = <-wait:
		case <-ctx.Done():
			err = ctx.Err()
		}
	}
	return err
}

// WithMaxQueueSize returns a BatchSpanProcessorOption that configures the
// maximum queue size allowed for a BatchSpanProcessor.
func WithMaxQueueSize(size int) BatchSpanProcessorOption {
	return func(o *BatchSpanProcessorOptions) {
		o.MaxQueueSize = size
	}
}

// WithMaxExportBatchSize returns a BatchSpanProcessorOption that configures
// the maximum export batch size allowed for a BatchSpanProcessor.
func WithMaxExportBatchSize(size int) BatchSpanProcessorOption {
	return func(o *BatchSpanProcessorOptions) {
		o.MaxExportBatchSize = size
	}
}

// WithBatchTimeout returns a BatchSpanProcessorOption that configures the
// maximum delay allowed for a BatchSpanProcessor before it will export any
// held span (whether the queue is full or not).
func WithBatchTimeout(delay time.Duration) BatchSpanProcessorOption {
	return func(o *BatchSpanProcessorOptions) {
		o.BatchTimeout = delay
	}
}

// WithExportTimeout returns a BatchSpanProcessorOption that configures the
// amount of time a BatchSpanProcessor waits for an exporter to export before
// abandoning the export.
func WithExportTimeout(timeout time.Duration) BatchSpanProcessorOption {
	return func(o *BatchSpanProcessorOptions) {
		o.ExportTimeout = timeout
	}
}

// WithBlocking returns a BatchSpanProcessorOption that configures a
// BatchSpanProcessor to wait for enqueue operations to succeed instead of
// dropping data when the queue is full.
func WithBlocking() BatchSpanProcessorOption {
	return func(o *BatchSpanProcessorOptions) {
		o.BlockOnQueueFull = true
	}
}

// exportSpans is a subroutine of processing and draining the queue.
func (bsp *batchSpanProcessor) exportSpans(ctx context.Context) error {
	// 中文导读：所有导出路径汇合到这里；持有 batchMutex 直到 ExportSpans 返回。
	// 超时 Context 在获得锁后创建，因此它约束导出过程，并不包含等待 batchMutex 的时间。
	bsp.timer.Reset(bsp.o.BatchTimeout)

	bsp.batchMutex.Lock()
	defer bsp.batchMutex.Unlock()

	if bsp.o.ExportTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeoutCause(ctx, bsp.o.ExportTimeout, errors.New("processor export timeout"))
		defer cancel()
	}

	if l := len(bsp.batch); l > 0 {
		global.Debug("exporting spans", "count", len(bsp.batch), "total_dropped", bsp.dropped.Load())
		if bsp.inst != nil {
			// 统计进入导出处理的 Span 数量，不等同于接收端已成功收到的数量。
			bsp.inst.Processed(ctx, int64(l))
		}
		// 调用配置的 Exporter；传输协议由实现决定，不一定使用 RPC 或发送到 Collector。
		err := bsp.e.ExportSpans(ctx, bsp.batch)

		// A new batch is always created after exporting, even if the batch failed to be exported.
		//
		// It is up to the exporter to implement any type of retry logic if a batch is failing
		// to be exported, since it is specific to the protocol and backend being sent to.
		clear(bsp.batch) // Erase elements to let GC collect objects
		// clear 已解除数组元素对 Span 的引用；[:0] 再重置长度并保留容量，供下一批复用。
		// 失败批次也在这里释放，处理器不会重试；需要重试时由 Exporter 在返回前完成。
		bsp.batch = bsp.batch[:0]

		if err != nil {
			return err
		}
	}
	return nil
}

// processQueue removes spans from the `queue` channel until processor
// is shut down. It calls the exporter in batches of up to MaxExportBatchSize
// waiting up to BatchTimeout to form a batch.
func (bsp *batchSpanProcessor) processQueue() {
	// 中文导读：select 同时等待停止、计时器和入队数据；没有就绪分支时让出执行机会。
	// 异步发生在这个后台任务与业务之间，任务内部调用 ExportSpans 仍然是同步等待。
	defer bsp.timer.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// 循环由 stopCh 结束，正常情况下依靠 select 等待事件，不是持续占用 CPU 的忙轮询。
	for {
		select {
		case <-bsp.stopCh:
			return
		case <-bsp.timer.C:
			// 按配置的凑批时间触发；满批导出也会重置计时器，因此不是固定墙钟周期。
			if err := bsp.exportSpans(ctx); err != nil {
				otel.Handle(err)
			}
		case sd := <-bsp.queue:
			// 类型断言区分控制消息和真实数据，控制消息不进入 batch，也不消耗导出批次。
			if ffs, ok := sd.(forceFlushSpan); ok {
				close(ffs.flushed)
				continue
			}
			bsp.batchMutex.Lock()
			bsp.batch = append(bsp.batch, sd)
			shouldExport := len(bsp.batch) >= bsp.o.MaxExportBatchSize
			bsp.batchMutex.Unlock()
			if shouldExport {
				if !bsp.timer.Stop() {
					// Handle both GODEBUG=asynctimerchan=[0|1] properly.
					select {
					case <-bsp.timer.C:
					default:
					}
				}
				if err := bsp.exportSpans(ctx); err != nil {
					otel.Handle(err)
				}
			}
		}
	}
}

// drainQueue awaits the any caller that had added to bsp.stopWait
// to finish the enqueue, then exports the final batch.
func (bsp *batchSpanProcessor) drainQueue() {
	// 正常处理循环已经退出，这里消费当前剩余队列；default 表示此刻没有可读消息。
	// 关闭前应先停止业务生产，避免把排空队列理解为对仍在并发创建的 Span 提供持久化保证。
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for {
		select {
		case sd := <-bsp.queue:
			if _, ok := sd.(forceFlushSpan); ok {
				// Ignore flush requests as they are not valid spans.
				continue
			}

			bsp.batchMutex.Lock()
			bsp.batch = append(bsp.batch, sd)
			shouldExport := len(bsp.batch) == bsp.o.MaxExportBatchSize
			bsp.batchMutex.Unlock()

			if shouldExport {
				if err := bsp.exportSpans(ctx); err != nil {
					otel.Handle(err)
				}
			}
		default:
			// There are no more enqueued spans. Make final export.
			if err := bsp.exportSpans(ctx); err != nil {
				otel.Handle(err)
			}
			return
		}
	}
}

func (bsp *batchSpanProcessor) enqueue(sd ReadOnlySpan) {
	// 中文导读：默认队列满即丢弃，用有界内存和有限调用耗时保护宿主应用。
	// WithBlocking 把背压传回 Span.End；这里使用 TODO，没有业务请求的取消或超时。
	// 若消费者长期无法推进，阻塞模式会拖住结束 Span 的 goroutine，不能把它当作无损开关。
	ctx := context.TODO()
	if bsp.o.BlockOnQueueFull {
		bsp.enqueueBlockOnQueueFull(ctx, sd)
	} else {
		// 非阻塞模式通过 select 的 default 立即报告丢弃；阻塞模式会等待发送或 Context 取消。
		bsp.enqueueDrop(ctx, sd)
	}
}

func (bsp *batchSpanProcessor) enqueueBlockOnQueueFull(ctx context.Context, sd ReadOnlySpan) bool {
	if !sd.SpanContext().IsSampled() {
		// RecordOnly 也会走到处理器，但内置导出管线只接受 Sampled 为真的 Span。
		return false
	}

	select {
	// 发送成功把快照交给后台；channel 满时此分支等待接收方腾出空间。
	case bsp.queue <- sd:
		return true
	case <-ctx.Done():
		// 此分支由 Context 取消/超时触发，队列满本身不会让它就绪。
		// ForceFlush 传入可取消的 Context；普通 OnEnd 使用 TODO，其 Done channel 为 nil。
		if bsp.inst != nil {
			// 记录本次未能完成排队的处理指标。
			bsp.inst.ProcessedQueueFull(ctx, 1)
		}
		return false
	}
}

func (bsp *batchSpanProcessor) enqueueDrop(ctx context.Context, sd ReadOnlySpan) bool {
	if !sd.SpanContext().IsSampled() {
		// 与阻塞模式保持同一采样过滤条件。
		return false
	}

	select {
	// default 使当前发送无法立即完成时直接返回，排队容量不会自动扩张。
	case bsp.queue <- sd:
		return true
	default:
		// 多个业务 goroutine 可能同时丢弃，用原子计数累积总量。
		bsp.dropped.Add(1)
		if bsp.inst != nil {
			// 可选的 SDK 自观测指标另外记录这次丢弃。
			bsp.inst.ProcessedQueueFull(ctx, 1)
		}
	}
	return false
}

// MarshalLog is the marshaling function used by the logging system to represent this Span Processor.
func (bsp *batchSpanProcessor) MarshalLog() any {
	return struct {
		Type         string
		SpanExporter string
		Config       BatchSpanProcessorOptions
	}{
		Type:         "BatchSpanProcessor",
		SpanExporter: fmt.Sprintf("%T", bsp.e),
		Config:       bsp.o,
	}
}
