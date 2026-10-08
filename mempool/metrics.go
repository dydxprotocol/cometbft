package mempool

import (
	"github.com/go-kit/kit/metrics"
	stdprometheus "github.com/prometheus/client_golang/prometheus"
)

const (
	// MetricsSubsystem is a subsystem shared by all metrics exposed by this
	// package.
	MetricsSubsystem = "mempool"
)

//go:generate go run ../scripts/metricsgen -struct=Metrics

// Metrics contains metrics exposed by this package.
// see MetricsProvider for descriptions.
type Metrics struct {
	// Number of uncommitted transactions in the mempool.
	Size metrics.Gauge

	// Total size of the mempool in bytes.
	SizeBytes metrics.Gauge

	// Histogram of transaction sizes in bytes.
	TxSizeBytes metrics.Histogram `metrics_buckettype:"exp" metrics_bucketsizes:"1,3,7"`

	// Number of failed transactions.
	FailedTxs metrics.Counter

	// RejectedTxs defines the number of rejected transactions. These are
	// transactions that passed CheckTx but failed to make it into the mempool
	// due to resource limits, e.g. mempool is full and no lower priority
	// transactions exist in the mempool.
	//metrics:Number of rejected transactions.
	RejectedTxs metrics.Counter

	// EvictedTxs defines the number of evicted transactions. These are valid
	// transactions that passed CheckTx and existed in the mempool but were later
	// evicted to make room for higher priority valid transactions that passed
	// CheckTx.
	//metrics:Number of evicted transactions.
	EvictedTxs metrics.Counter

	// PurgedDurationTxs defines the number of purged transactions due to
	// mempool transaction TTL settings based on time duration.
	//metrics:Number of purged transactions due to TTLDuration.
	PurgedDurationTxs metrics.Counter

	// PurgedNumBlocksTxs defines the number of purged transactions due to
	// mempool transaction TTL settings based on number of blocks.
	//metrics:Number of purged transactions due to TTLNumBlocks.
	PurgedNumBlocksTxs metrics.Counter

	// Number of times transactions are rechecked in the mempool.
	RecheckTimes metrics.Counter

	// Number of connections being actively used for gossiping transactions
	// (experimental feature).
	ActiveOutboundConnections metrics.Gauge

	// Number of transactions received from peers that were dropped because the
	// recv queue was full.
	RecvQueueDroppedTxs metrics.Counter

	// Number of transactions received from peers waiting for a recv worker.
	RecvQueueLen metrics.Gauge

	// Seconds a transaction received from a peer waited for a recv worker.
	RecvQueueWaitSeconds metrics.Histogram `metrics_buckettype:"exp" metrics_bucketsizes:"0.001,2,16"`

	// The vector behind RecvQueueWaitSeconds and the global label values, so
	// the reactor resolves its series once and observes without allocating.
	// Every go-kit Observe builds a label map per call. nil for NopMetrics.
	recvQueueWaitVec  *stdprometheus.HistogramVec
	globalLabelValues []string
}

// recvQueueWaitObserver resolves the wait histogram's series. nil when metrics
// are absent or discarded.
func (m *Metrics) recvQueueWaitObserver() stdprometheus.Observer {
	if m == nil || m.recvQueueWaitVec == nil {
		return nil
	}
	return m.recvQueueWaitVec.WithLabelValues(m.globalLabelValues...)
}
