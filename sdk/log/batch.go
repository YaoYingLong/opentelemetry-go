// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package log

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/internal/global"
	"go.opentelemetry.io/otel/sdk/log/internal/counter"
	"go.opentelemetry.io/otel/sdk/log/internal/observ"
)

const (
	dfltMaxQSize        = 2048
	dfltExpInterval     = time.Second
	dfltExpTimeout      = 30 * time.Second
	dfltExpMaxBatchSize = 512

	envarMaxQSize        = "OTEL_BLRP_MAX_QUEUE_SIZE"
	envarExpInterval     = "OTEL_BLRP_SCHEDULE_DELAY"
	envarExpTimeout      = "OTEL_BLRP_EXPORT_TIMEOUT"
	envarExpMaxBatchSize = "OTEL_BLRP_MAX_EXPORT_BATCH_SIZE"
)

// This is a compile-time check that BatchProcessor implements Processor.
var _ Processor = (*BatchProcessor)(nil)

// BatchProcessor is a processor that exports batches of log records.
//
// Use [NewBatchProcessor] to create a BatchProcessor. An empty BatchProcessor
// is shut down by default, so no records will be batched or exported.
//
// 中文导读：调用方 Clone 并入队，单一后台 goroutine 取出记录并调用 Exporter。
// 队列限制的是待取出记录数，不是总字节数，也不包含已经取出的导出批次。
// 导出跟不上持续输入时会积压并在队满后丢弃记录；本处理器的 OnEmit 无需等待网络。
type BatchProcessor struct {
	// A single goroutine owns dequeueing and all exporter calls. OnEmit only
	// writes to the bounded queue and signals that goroutine. Consequently,
	// exporter backpressure blocks the exporter goroutine instead of causing
	// another goroutine to retry without making progress.
	exporter Exporter

	// q is the active queue of records that have not yet been exported.
	q *queue
	// batchSize is the maximum number of records in a scheduled export.
	batchSize int

	// exportTrigger is a coalesced signal that records are ready to export.
	exportTrigger chan struct{}
	// flush serializes ForceFlush requests through the worker.
	flush chan batchProcessorRequest
	// shutdown accepts the single Shutdown request. It is separate from flush
	// so shutdown cannot be blocked behind concurrent ForceFlush callers.
	shutdown chan batchProcessorRequest
	// done is closed by the exporter goroutine after exporter shutdown.
	done chan struct{}

	// stopped holds the stopped state of the BatchProcessor.
	stopped atomic.Bool

	// inst is the instrumentation for observability (nil when disabled).
	inst *observ.BLP

	noCmp [0]func() //nolint: unused  // This is indeed used.
}

type batchProcessorRequest struct {
	ctx  context.Context
	resp chan<- error
}

func (r batchProcessorRequest) respond(err error) {
	r.resp <- err
}

// NewBatchProcessor decorates the provided exporter
// so that the log records are batched before they are exported.
//
// Calls to the exporter's Export, ForceFlush, and Shutdown methods are
// synchronized and never invoked concurrently.
func NewBatchProcessor(exporter Exporter, opts ...BatchProcessorOption) *BatchProcessor {
	cfg := newBatchConfig(opts)
	if exporter == nil {
		// Do not panic on nil export.
		exporter = defaultNoopExporter
	}

	b := &BatchProcessor{
		q:             newQueue(cfg.maxQSize.Value),
		batchSize:     cfg.expMaxBatchSize.Value,
		exportTrigger: make(chan struct{}, 1),
		flush:         make(chan batchProcessorRequest),
		shutdown:      make(chan batchProcessorRequest, 1),
		done:          make(chan struct{}),
	}

	var err error
	b.inst, err = observ.NewBLP(
		counter.NextExporterID(),
		func() int64 { return int64(b.q.Len()) },
		int64(cfg.maxQSize.Value),
	)
	if err != nil {
		otel.Handle(err)
	}

	// Wrap exporter with metrics recording if observability is enabled.
	// This must be the innermost wrapper (closest to user exporter) to record
	// metrics just before calling the actual exporter.
	if b.inst != nil {
		exporter = newMetricsExporter(exporter, b.inst)
	}

	// Order is important here. Wrap the timeoutExporter with the chunkExporter
	// to ensure each export completes in timeout (instead of all chunked
	// exports).
	// 包装顺序从外到内是分块、超时、可选指标、实际导出器。每块单独取得
	// 导出超时；ForceFlush/Shutdown 传入的父 context 还会约束整个调用。
	exporter = newTimeoutExporter(exporter, cfg.expTimeout.Value)
	// Use a chunkExporter to ensure ForceFlush and Shutdown calls are batched
	// appropriately on export.
	exporter = newChunkExporter(exporter, cfg.expMaxBatchSize.Value)

	b.exporter = exporter
	b.process(cfg.expInterval.Value)
	return b
}

// process starts the goroutine that owns dequeueing and all exporter calls.
func (b *BatchProcessor) process(interval time.Duration) {
	// 中文导读：工作 goroutine 同时拥有出队、Export、ForceFlush、Shutdown 的
	// 执行权，所以这些导出器方法不会相互重叠。Go 的 select 对多个就绪分支
	// 不保证源码顺序优先；下面先做非阻塞探测，给已等待的生命周期请求优先级。
	go func() {
		timer := time.NewTimer(interval)
		defer timer.Stop()
		// The worker owns and reuses buf. Exporters must not retain the slice
		// passed to them, so it is safe to refill after Export returns.
		buf := make([]Record, b.batchSize)

		for {
			// Probe shutdown by itself first. This makes an already queued terminal
			// request win over every other ready case. Closing done before replying
			// also means a successful Shutdown response observes a stopped worker.
			select {
			case req := <-b.shutdown:
				err := b.shutdownExporter(req.ctx)
				close(b.done)
				req.respond(err)
				return
			default:
			}

			// With no queued shutdown, service a waiting ForceFlush before ordinary
			// export wakes. The default keeps this priority check non-blocking.
			// Shutdown remains selectable in case it arrived after the first probe.
			select {
			case req := <-b.shutdown:
				err := b.shutdownExporter(req.ctx)
				close(b.done)
				req.respond(err)
				return
			case req := <-b.flush:
				err := b.flushExporter(req.ctx)
				req.respond(err)
				continue
			default:
			}

			// No lifecycle request was waiting, so block on the complete event set.
			// Both timer and size-triggered exports start a new interval window.
			select {
			case req := <-b.shutdown:
				err := b.shutdownExporter(req.ctx)
				close(b.done)
				req.respond(err)
				return
			case req := <-b.flush:
				err := b.flushExporter(req.ctx)
				req.respond(err)
			case <-timer.C:
				resetTimer(timer, interval)
				b.exportBatch(buf)
			case <-b.exportTrigger:
				resetTimer(timer, interval)
				b.exportBatch(buf)
			}
		}
	}()
}

func resetTimer(timer *time.Timer, interval time.Duration) {
	// Handle both GODEBUG=asynctimerchan=[0|1] properly.
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(interval)
}

func (b *BatchProcessor) exportBatch(buf []Record) {
	// 定时导出使用独立的后台 context：原请求可能早已结束，而且一批记录
	// 可能来自多个请求。关联 ID 已在 Emit 时写入 Record，不依赖此处 context。
	// 出队后失败不会自动放回队列；重试属于 Exporter 的责任。清空复用缓冲区
	// 可释放对已处理日志的引用，但 Exporter 若忽略取消仍会阻塞这个 worker。
	b.logDroppedRecords()
	n, remaining := b.q.Dequeue(buf)
	if n == 0 {
		return
	}

	err := b.exporter.Export(context.Background(), buf[:n])
	clear(buf[:n])
	if err != nil {
		otel.Handle(err)
	}
	if remaining >= b.batchSize {
		b.triggerExport()
	}
}

func (b *BatchProcessor) flushExporter(ctx context.Context) error {
	// 先检查取消再取走队列，避免已取消的刷新请求消耗待导出的数据。
	// Flush 取得当时队列的快照；允许并发 Emit 的情况下，后续新日志仍可能入队，
	// 因此 ForceFlush 不是暂停生产者或保证队列一直为空的操作。
	if err := ctx.Err(); err != nil {
		return err
	}
	b.logDroppedRecords()
	records := b.q.Flush()
	err := b.exporter.Export(ctx, records)
	clear(records)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return errors.Join(err, ctxErr)
	}
	return errors.Join(err, b.exporter.ForceFlush(ctx))
}

func (b *BatchProcessor) shutdownExporter(ctx context.Context) error {
	b.logDroppedRecords()
	records := b.q.Flush()
	err := b.exporter.Export(ctx, records)
	clear(records)
	if ctxErr := ctx.Err(); ctxErr != nil {
		err = errors.Join(err, ctxErr)
	} else {
		err = errors.Join(err, b.exporter.ForceFlush(ctx))
	}
	err = errors.Join(err, b.exporter.Shutdown(ctx))
	if b.inst != nil {
		err = errors.Join(err, b.inst.Shutdown())
	}
	return err
}

func (b *BatchProcessor) logDroppedRecords() {
	if d := b.q.Dropped(); d > 0 {
		if b.inst != nil {
			b.inst.ProcessedQueueFull(context.Background(), int64(min(math.MaxInt64, d))) // nolint:gosec
		}
		global.Warn("dropped log records", "dropped", d)
	}
}

func (b *BatchProcessor) triggerExport() {
	// 容量为 1 的通知通道只表达“有工作可做”，不承载记录，也不计数批次。
	// 已有通知时走 default 合并唤醒；实际数据仍在队列里，worker 会按剩余数量再触发。
	select {
	case b.exportTrigger <- struct{}{}:
	default:
	}
}

// Enabled returns true, indicating this Processor will process all records.
func (*BatchProcessor) Enabled(context.Context, EnabledParameters) bool {
	return true
}

// OnEmit batches the provided log record.
func (b *BatchProcessor) OnEmit(_ context.Context, r *Record) error {
	if b.stopped.Load() || b.q == nil {
		return nil
	}
	// The record is cloned so that changes done by subsequent processors
	// are not going to lead to a data race.
	// Clone 固定本处理器看到的内容；后注册的处理器再脱敏也不会改到这个副本，
	// 所以需要影响导出数据的处理器应排在 BatchProcessor 前面。
	if n, accepted := b.q.Enqueue(r.Clone()); accepted && n >= b.batchSize {
		b.triggerExport()
	}
	return nil
}

// Shutdown flushes queued log records and the decorated exporter before
// shutting it down.
func (b *BatchProcessor) Shutdown(ctx context.Context) error {
	// 中文导读：Swap 只让一个调用者提交关闭请求，q.Close 再与入队操作互斥，
	// 封住已经通过 stopped 检查的并发 OnEmit。关闭队列后才能取得最后一批数据。
	// 独立且带缓冲的 shutdown 通道让请求可在 worker 忙于导出时提交；resp 也带
	// 一个缓冲，调用方因超时退出后，worker 回答时不会因无人接收而永久阻塞。
	// 返回超时只代表调用方停止等待，实际清理仍需 worker 和导出器继续配合。
	if b.stopped.Swap(true) || b.q == nil {
		return nil
	}

	b.q.Close()
	resp := make(chan error, 1)
	b.shutdown <- batchProcessorRequest{ctx: ctx, resp: resp}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case err := <-resp:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ForceFlush flushes queued log records and flushes the decorated exporter.
func (b *BatchProcessor) ForceFlush(ctx context.Context) error {
	// 刷新请求也走同一 worker，避免调用方直接碰导出器破坏串行性。
	// 等待提交时同时监听 done 和 ctx.Done，避免在关闭后或取消后一直等待接收者。
	if b.stopped.Load() || b.q == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	resp := make(chan error, 1)
	req := batchProcessorRequest{ctx: ctx, resp: resp}
	select {
	case b.flush <- req:
	case <-b.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case err := <-resp:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// queue holds a queue of logging records.
//
// When the queue becomes full, the oldest records in the queue are
// overwritten.
//
// 中文导读：有界环形队列保留最新记录；队满时覆盖最旧记录并增加 dropped。
// 锁只保护环形游标、长度和关闭状态，不覆盖网络导出，因此慢导出不会持有队列锁。
type queue struct {
	sync.Mutex

	dropped     atomic.Uint64
	cap, len    int
	read, write *ring
	closed      bool
}

func newQueue(size int) *queue {
	r := newRing(size)
	return &queue{
		cap:   size,
		read:  r,
		write: r,
	}
}

func (q *queue) Len() int {
	q.Lock()
	defer q.Unlock()

	return q.len
}

// Dropped returns the number of Records dropped during enqueueing since the
// last time Dropped was called.
func (q *queue) Dropped() uint64 {
	// 原子交换把“取出累计值”和“清零”合为一步，并发入队增加的计数不会被覆盖掉。
	return q.dropped.Swap(0)
}

// Enqueue adds r to the queue. The queue size, including the addition of r, is
// returned.
//
// If enqueueing r would exceed the capacity of q, the oldest Record held in q
// will be dropped and r will be retained.
func (q *queue) Enqueue(r Record) (int, bool) {
	q.Lock()
	defer q.Unlock()

	if q.closed {
		return q.len, false
	}

	q.write.Value = r
	q.write = q.write.Next()

	q.len++
	if q.len > q.cap {
		// Overflow. Advance read to be the new "oldest".
		q.len = q.cap
		q.read = q.read.Next()
		q.dropped.Add(1)
	}
	return q.len, true
}

// Dequeue removes up to len(buf) records from the queue and copies them into
// buf. The number copied and the number remaining are returned.
func (q *queue) Dequeue(buf []Record) (int, int) {
	q.Lock()
	defer q.Unlock()

	n := min(len(buf), q.len)
	for i := range n {
		buf[i] = q.read.Value // nolint:gosec // n is bounded by len(buf)
		q.read.Value = Record{}
		q.read = q.read.Next()
	}
	q.len -= n
	return n, q.len
}

// Flush returns all the Records held in the queue and resets it to empty.
func (q *queue) Flush() []Record {
	q.Lock()
	defer q.Unlock()

	return q.flush()
}

// Close stops the queue from accepting records.
func (q *queue) Close() {
	q.Lock()
	defer q.Unlock()

	q.closed = true
}

func (q *queue) flush() []Record {
	out := make([]Record, q.len)
	for i := range out {
		out[i] = q.read.Value
		q.read.Value = Record{}
		q.read = q.read.Next()
	}
	q.len = 0

	return out
}

type batchConfig struct {
	maxQSize        setting[int]
	expInterval     setting[time.Duration]
	expTimeout      setting[time.Duration]
	expMaxBatchSize setting[int]
}

func newBatchConfig(options []BatchProcessorOption) batchConfig {
	var c batchConfig
	for _, o := range options {
		c = o.apply(c)
	}

	c.maxQSize = c.maxQSize.Resolve(
		clearLessThanOne[int](),
		getenv[int](envarMaxQSize),
		clearLessThanOne[int](), // nolint:gocritic // the function argument is duplicated on purpose
		fallback[int](dfltMaxQSize),
	)
	c.expInterval = c.expInterval.Resolve(
		clearLessThanOne[time.Duration](),
		getenv[time.Duration](envarExpInterval),
		clearLessThanOne[time.Duration](), // nolint:gocritic // the function argument is duplicated on purpose
		fallback[time.Duration](dfltExpInterval),
	)
	c.expTimeout = c.expTimeout.Resolve(
		clearLessThanOne[time.Duration](),
		getenv[time.Duration](envarExpTimeout),
		clearLessThanOne[time.Duration](), // nolint:gocritic // the function argument is duplicated on purpose
		fallback[time.Duration](dfltExpTimeout),
	)
	c.expMaxBatchSize = c.expMaxBatchSize.Resolve(
		clearLessThanOne[int](),
		getenv[int](envarExpMaxBatchSize),
		clearLessThanOne[int](), // nolint:gocritic // the function argument is duplicated on purpose
		fallback[int](dfltExpMaxBatchSize),
		clampMax[int](c.maxQSize.Value),
	)
	return c
}

// BatchProcessorOption applies a configuration to a [BatchProcessor].
type BatchProcessorOption interface {
	apply(batchConfig) batchConfig
}

type batchOptionFunc func(batchConfig) batchConfig

func (fn batchOptionFunc) apply(c batchConfig) batchConfig {
	return fn(c)
}

// WithMaxQueueSize sets the maximum queue size used by the Batcher.
// After the size is reached, log records are dropped.
//
// If the OTEL_BLRP_MAX_QUEUE_SIZE environment variable is set and this option
// is not passed, the value of that variable will be used.
//
// By default, if the environment variable is not set and this option is not
// passed, 2048 will be used.
// The default value is also used when the provided value is less than one.
func WithMaxQueueSize(size int) BatchProcessorOption {
	return batchOptionFunc(func(cfg batchConfig) batchConfig {
		cfg.maxQSize = newSetting(size)
		return cfg
	})
}

// WithExportInterval sets the maximum duration between batched exports.
//
// If the OTEL_BLRP_SCHEDULE_DELAY environment variable is set and this option
// is not passed, the value of that variable will be used.
//
// By default, if the environment variable is not set and this option is not
// passed, 1s will be used.
// The default value is also used when the provided value is less than one.
func WithExportInterval(d time.Duration) BatchProcessorOption {
	return batchOptionFunc(func(cfg batchConfig) batchConfig {
		cfg.expInterval = newSetting(d)
		return cfg
	})
}

// WithExportTimeout sets the duration after which a batched export is canceled.
//
// If the OTEL_BLRP_EXPORT_TIMEOUT environment variable is set and this option
// is not passed, the value of that variable will be used.
//
// By default, if the environment variable is not set and this option is not
// passed, 30s will be used.
// The default value is also used when the provided value is less than one.
func WithExportTimeout(d time.Duration) BatchProcessorOption {
	return batchOptionFunc(func(cfg batchConfig) batchConfig {
		cfg.expTimeout = newSetting(d)
		return cfg
	})
}

// WithExportMaxBatchSize sets the maximum batch size of every export.
// A batch will be split into multiple exports so that none exceed this size.
//
// If the OTEL_BLRP_MAX_EXPORT_BATCH_SIZE environment variable is set and this
// option is not passed, the value of that variable will be used.
//
// By default, if the environment variable is not set and this option is not
// passed, 512 or the maximum queue size, if smaller, will be used.
// The default value is also used when the provided value is less than one.
// The effective batch size will not exceed the configured maximum queue size.
func WithExportMaxBatchSize(size int) BatchProcessorOption {
	return batchOptionFunc(func(cfg batchConfig) batchConfig {
		cfg.expMaxBatchSize = newSetting(size)
		return cfg
	})
}

// WithExportBufferSize is retained for source compatibility and has no effect.
// The processor no longer maintains a separately configurable export-request
// buffer. [WithMaxQueueSize] bounds the pending-record queue.
//
// Deprecated: This option is no longer used.
func WithExportBufferSize(_ int) BatchProcessorOption {
	return batchOptionFunc(func(cfg batchConfig) batchConfig {
		return cfg
	})
}
