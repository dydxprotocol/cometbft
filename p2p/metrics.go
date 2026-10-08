package p2p

import (
	"fmt"
	"reflect"
	"regexp"
	"sync"

	"github.com/go-kit/kit/metrics"
	stdprometheus "github.com/prometheus/client_golang/prometheus"
)

const (
	// MetricsSubsystem is a subsystem shared by all metrics exposed by this
	// package.
	MetricsSubsystem = "p2p"
)

var (
	// valueToLabelRegexp is used to find the golang package name and type name
	// so that the name can be turned into a prometheus label where the characters
	// in the label do not include prometheus special characters such as '*' and '.'.
	valueToLabelRegexp = regexp.MustCompile(`\*?(\w+)\.(.*)`)
)

//go:generate go run ../scripts/metricsgen -struct=Metrics

// Metrics contains metrics exposed by this package.
type Metrics struct {
	// Number of peers.
	Peers metrics.Gauge
	// Number of bytes received from a given peer.
	PeerReceiveBytesTotal metrics.Counter `metrics_labels:"peer_id,chID"`
	// Number of bytes sent to a given peer.
	PeerSendBytesTotal metrics.Counter `metrics_labels:"peer_id,chID"`
	// Pending bytes to be sent to a given peer.
	PeerPendingSendBytes metrics.Gauge `metrics_labels:"peer_id"`
	// Number of transactions submitted by each peer.
	NumTxs metrics.Gauge `metrics_labels:"peer_id"`
	// Number of bytes of each message type received.
	MessageReceiveBytesTotal metrics.Counter `metrics_labels:"message_type"`
	// Number of bytes of each message type sent.
	MessageSendBytesTotal metrics.Counter `metrics_labels:"message_type"`

	// The vectors behind the four per-packet counters and the global label
	// values, so a peer resolves each series once and calls Add without
	// allocating. Every go-kit With/Add builds a label map per call. nil for
	// NopMetrics.
	peerReceiveBytesVec    *stdprometheus.CounterVec
	peerSendBytesVec       *stdprometheus.CounterVec
	messageReceiveBytesVec *stdprometheus.CounterVec
	messageSendBytesVec    *stdprometheus.CounterVec
	globalLabelValues      []string

	// reflect.Type -> *messageTypeCounters
	messageCounters sync.Map
}

type messageTypeCounters struct {
	receive stdprometheus.Counter
	send    stdprometheus.Counter
}

// peerChannelCounters resolves the receive and send byte counters for one peer
// and channel. Both are nil when metrics are absent or discarded.
func (m *Metrics) peerChannelCounters(peerID ID, chID byte) (receive, send stdprometheus.Counter) {
	if m == nil || m.peerReceiveBytesVec == nil {
		return nil, nil
	}
	lvs := make([]string, 0, len(m.globalLabelValues)+2)
	lvs = append(lvs, m.globalLabelValues...)
	lvs = append(lvs, string(peerID), fmt.Sprintf("%#x", chID))
	return m.peerReceiveBytesVec.WithLabelValues(lvs...), m.peerSendBytesVec.WithLabelValues(lvs...)
}

// messageTypeCounters resolves the receive and send byte counters for the type
// of msg, caching them per type. nil when metrics are absent or discarded.
func (m *Metrics) messageTypeCounters(msg interface{}, mlc *metricsLabelCache) *messageTypeCounters {
	if m == nil || m.messageReceiveBytesVec == nil {
		return nil
	}
	t := reflect.TypeOf(msg)
	if c, ok := m.messageCounters.Load(t); ok {
		return c.(*messageTypeCounters)
	}
	lvs := make([]string, 0, len(m.globalLabelValues)+1)
	lvs = append(lvs, m.globalLabelValues...)
	lvs = append(lvs, mlc.ValueToMetricLabel(msg))
	c, _ := m.messageCounters.LoadOrStore(t, &messageTypeCounters{
		receive: m.messageReceiveBytesVec.WithLabelValues(lvs...),
		send:    m.messageSendBytesVec.WithLabelValues(lvs...),
	})
	return c.(*messageTypeCounters)
}

type metricsLabelCache struct {
	mtx               *sync.RWMutex
	messageLabelNames map[reflect.Type]string
}

// ValueToMetricLabel is a method that is used to produce a prometheus label value of the golang
// type that is passed in.
// This method uses a map on the Metrics struct so that each label name only needs
// to be produced once to prevent expensive string operations.
func (m *metricsLabelCache) ValueToMetricLabel(i interface{}) string {
	t := reflect.TypeOf(i)
	m.mtx.RLock()

	if s, ok := m.messageLabelNames[t]; ok {
		m.mtx.RUnlock()
		return s
	}
	m.mtx.RUnlock()

	s := t.String()
	ss := valueToLabelRegexp.FindStringSubmatch(s)
	l := fmt.Sprintf("%s_%s", ss[1], ss[2])
	m.mtx.Lock()
	defer m.mtx.Unlock()
	m.messageLabelNames[t] = l
	return l
}

func newMetricsLabelCache() *metricsLabelCache {
	return &metricsLabelCache{
		mtx:               &sync.RWMutex{},
		messageLabelNames: map[reflect.Type]string{},
	}
}
