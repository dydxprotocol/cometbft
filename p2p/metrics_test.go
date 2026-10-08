package p2p

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	p2pproto "github.com/cometbft/cometbft/proto/tendermint/p2p"
)

// The counters a peer caches must be the same series the exported go-kit
// counters write to, and NopMetrics must hand out nothing to cache.
func TestCachedCountersShareSeriesWithGoKitCounters(t *testing.T) {
	m := PrometheusMetrics("cached_counters_test", "chain_id", "test")
	mlc := newMetricsLabelCache()

	recv, send := m.peerChannelCounters("peer1", 0x1)
	recv.Add(3)
	send.Add(5)
	m.PeerReceiveBytesTotal.With("peer_id", "peer1", "chID", "0x1").Add(1)
	m.PeerSendBytesTotal.With("peer_id", "peer1", "chID", "0x1").Add(1)
	require.Equal(t, 4.0, testutil.ToFloat64(m.peerReceiveBytesVec.WithLabelValues("test", "peer1", "0x1")))
	require.Equal(t, 6.0, testutil.ToFloat64(m.peerSendBytesVec.WithLabelValues("test", "peer1", "0x1")))

	msg := &p2pproto.Message{}
	c := m.messageTypeCounters(msg, mlc)
	require.Same(t, c, m.messageTypeCounters(msg, mlc))
	c.receive.Add(2)
	c.send.Add(4)
	m.MessageReceiveBytesTotal.With("message_type", "p2p_Message").Add(1)
	m.MessageSendBytesTotal.With("message_type", "p2p_Message").Add(1)
	require.Equal(t, 3.0, testutil.ToFloat64(m.messageReceiveBytesVec.WithLabelValues("test", "p2p_Message")))
	require.Equal(t, 5.0, testutil.ToFloat64(m.messageSendBytesVec.WithLabelValues("test", "p2p_Message")))

	nop := NopMetrics()
	nr, ns := nop.peerChannelCounters("peer1", 0x1)
	require.Nil(t, nr)
	require.Nil(t, ns)
	require.Nil(t, nop.messageTypeCounters(msg, mlc))
}

// A real send through a peer lands on the cached per-peer and per-type series.
func TestPeerSendUsesCachedCounters(t *testing.T) {
	m := PrometheusMetrics("cached_counters_send_test")
	sw1, sw2 := MakeSwitchPair(func(i int, sw *Switch) *Switch {
		if i == 0 {
			WithMetrics(m)(sw)
		}
		return initSwitchFunc(i, sw)
	})
	t.Cleanup(func() {
		require.NoError(t, sw1.Stop())
		require.NoError(t, sw2.Stop())
	})
	require.Len(t, sw1.Peers().List(), 1)
	p := sw1.Peers().List()[0]

	msg := &p2pproto.Message{Sum: &p2pproto.Message_PexRequest{PexRequest: &p2pproto.PexRequest{}}}
	require.True(t, p.Send(Envelope{ChannelID: 0x1, Message: msg}))

	sent := testutil.ToFloat64(m.peerSendBytesVec.WithLabelValues(string(p.ID()), "0x1"))
	require.Greater(t, sent, 0.0)
	require.Equal(t, sent, testutil.ToFloat64(m.messageSendBytesVec.WithLabelValues("p2p_Message")))
}
