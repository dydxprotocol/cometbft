package mempool

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	codectypes "github.com/cosmos/cosmos-sdk/codec/types"
	cosmostx "github.com/cosmos/cosmos-sdk/types/tx"
	"github.com/fortytw2/leaktest"
	"github.com/go-kit/kit/metrics"
	"github.com/go-kit/log/term"
	stdprometheus "github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	abciclient "github.com/cometbft/cometbft/abci/client"
	"github.com/cometbft/cometbft/abci/example/kvstore"
	abci "github.com/cometbft/cometbft/abci/types"
	cfg "github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/p2p/mock"
	"github.com/cometbft/cometbft/proto/dydxcometbft/clob"
	"github.com/cometbft/cometbft/proto/dydxcometbft/subaccounts"
	memproto "github.com/cometbft/cometbft/proto/tendermint/mempool"
	"github.com/cometbft/cometbft/proxy"
	"github.com/cometbft/cometbft/types"
)

const (
	numTxs  = 1000
	timeout = 120 * time.Second // ridiculously high because CircleCI is slow
)

type peerState struct {
	height int64
}

func (ps peerState) GetHeight() int64 {
	return ps.height
}

// Send a bunch of txs to the first reactor's mempool and wait for them all to
// be received in the others.
func TestReactorBroadcastTxsMessage(t *testing.T) {
	config := cfg.TestConfig()
	// if there were more than two reactors, the order of transactions could not be
	// asserted in waitForTxsOnReactors (due to transactions gossiping). If we
	// replace Connect2Switches (full mesh) with a func, which connects first
	// reactor to others and nothing else, this test should also pass with >2 reactors.
	const N = 2
	reactors, _ := makeAndConnectReactors(config, N)
	defer func() {
		for _, r := range reactors {
			if err := r.Stop(); err != nil {
				assert.NoError(t, err)
			}
		}
	}()
	for _, r := range reactors {
		for _, peer := range r.Switch.Peers().List() {
			peer.Set(types.PeerStateKey, peerState{1})
		}
	}

	txs := checkTxs(t, reactors[0].mempool, numTxs, UnknownPeerID)
	waitForTxsOnReactors(t, txs, reactors)
}

// regression test for https://github.com/cometbft/cometbft/issues/5408
func TestReactorConcurrency(t *testing.T) {
	config := cfg.TestConfig()
	const N = 2
	reactors, _ := makeAndConnectReactors(config, N)
	defer func() {
		for _, r := range reactors {
			if err := r.Stop(); err != nil {
				assert.NoError(t, err)
			}
		}
	}()
	for _, r := range reactors {
		for _, peer := range r.Switch.Peers().List() {
			peer.Set(types.PeerStateKey, peerState{1})
		}
	}
	var wg sync.WaitGroup

	const numTxs = 5

	for i := 0; i < 1000; i++ {
		wg.Add(2)

		// 1. submit a bunch of txs
		// 2. update the whole mempool
		txs := checkTxs(t, reactors[0].mempool, numTxs, UnknownPeerID)
		go func() {
			defer wg.Done()

			reactors[0].mempool.Lock()
			defer reactors[0].mempool.Unlock()

			txResponses := make([]*abci.ExecTxResult, len(txs))
			for i := range txs {
				txResponses[i] = &abci.ExecTxResult{Code: 0}
			}
			err := reactors[0].mempool.Update(1, time.UnixMilli(1), txs, txResponses, nil, nil)
			assert.NoError(t, err)
		}()

		// 1. submit a bunch of txs
		// 2. update none
		_ = checkTxs(t, reactors[1].mempool, numTxs, UnknownPeerID)
		go func() {
			defer wg.Done()

			reactors[1].mempool.Lock()
			defer reactors[1].mempool.Unlock()
			err := reactors[1].mempool.Update(1, time.UnixMilli(1), []types.Tx{}, make([]*abci.ExecTxResult, 0), nil, nil)
			assert.NoError(t, err)
		}()

		// 1. flush the mempool
		reactors[1].mempool.Flush()
	}

	wg.Wait()
}

// Send a bunch of txs to the first reactor's mempool, claiming it came from peer
// ensure peer gets no txs.
func TestReactorNoBroadcastToSender(t *testing.T) {
	config := cfg.TestConfig()
	const N = 2
	reactors, _ := makeAndConnectReactors(config, N)
	defer func() {
		for _, r := range reactors {
			if err := r.Stop(); err != nil {
				assert.NoError(t, err)
			}
		}
	}()
	for _, r := range reactors {
		for _, peer := range r.Switch.Peers().List() {
			peer.Set(types.PeerStateKey, peerState{1})
		}
	}

	const peerID = 1
	checkTxs(t, reactors[0].mempool, numTxs, peerID)
	ensureNoTxs(t, reactors[peerID], 100*time.Millisecond)
}

func TestReactor_MaxTxBytes(t *testing.T) {
	config := cfg.TestConfig()

	const N = 2
	reactors, _ := makeAndConnectReactors(config, N)
	defer func() {
		for _, r := range reactors {
			if err := r.Stop(); err != nil {
				assert.NoError(t, err)
			}
		}
	}()
	for _, r := range reactors {
		for _, peer := range r.Switch.Peers().List() {
			peer.Set(types.PeerStateKey, peerState{1})
		}
	}

	// Broadcast a tx, which has the max size
	// => ensure it's received by the second reactor.
	tx1 := kvstore.NewRandomTx(config.Mempool.MaxTxBytes)
	err := reactors[0].mempool.CheckTx(tx1, func(resp *abci.ResponseCheckTx) {
		require.False(t, resp.IsErr())
	}, TxInfo{SenderID: UnknownPeerID})
	require.NoError(t, err)
	waitForTxsOnReactors(t, []types.Tx{tx1}, reactors)

	reactors[0].mempool.Flush()
	reactors[1].mempool.Flush()

	// Broadcast a tx, which is beyond the max size
	// => ensure it's not sent
	tx2 := kvstore.NewRandomTx(config.Mempool.MaxTxBytes + 1)
	err = reactors[0].mempool.CheckTx(tx2, func(resp *abci.ResponseCheckTx) {
		require.False(t, resp.IsErr())
	}, TxInfo{SenderID: UnknownPeerID})
	require.Error(t, err)
}

func TestBroadcastTxForPeerStopsWhenPeerStops(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	config := cfg.TestConfig()
	const N = 2
	reactors, _ := makeAndConnectReactors(config, N)
	defer func() {
		for _, r := range reactors {
			if err := r.Stop(); err != nil {
				assert.NoError(t, err)
			}
		}
	}()

	// stop peer
	sw := reactors[1].Switch
	sw.StopPeerForError(sw.Peers().List()[0], errors.New("some reason"))

	// check that we are not leaking any go-routines
	// i.e. broadcastTxRoutine finishes when peer is stopped
	leaktest.CheckTimeout(t, 10*time.Second)()
}

func TestBroadcastTxForPeerStopsWhenReactorStops(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	config := cfg.TestConfig()
	const N = 2
	_, switches := makeAndConnectReactors(config, N)

	// stop reactors
	for _, s := range switches {
		assert.NoError(t, s.Stop())
	}

	// check that we are not leaking any go-routines
	// i.e. broadcastTxRoutine finishes when reactor is stopped
	leaktest.CheckTimeout(t, 10*time.Second)()
}

// TODO: This test tests that we don't panic and are able to generate new
// PeerIDs for each peer we add. It seems as though we should be able to test
// this in a much more direct way.
// https://github.com/cometbft/cometbft/issues/9639
func TestDontExhaustMaxActiveIDs(t *testing.T) {
	config := cfg.TestConfig()
	const N = 1
	reactors, _ := makeAndConnectReactors(config, N)
	defer func() {
		for _, r := range reactors {
			if err := r.Stop(); err != nil {
				assert.NoError(t, err)
			}
		}
	}()
	reactor := reactors[0]

	for i := 0; i < MaxActiveIDs+1; i++ {
		peer := mock.NewPeer(nil)
		reactor.Receive(p2p.Envelope{
			ChannelID: MempoolChannel,
			Src:       peer,
			Message:   &memproto.Message{}, // This uses the wrong message type on purpose to stop the peer as in an error state in the reactor.
		},
		)
		reactor.AddPeer(peer)
	}
}

// Test the experimental feature that limits the number of outgoing connections for gossiping
// transactions (only non-persistent peers).
// Note: in this test we know which gossip connections are active or not because of how the p2p
// functions are currently implemented, which affects the order in which peers are added to the
// mempool reactor.
func TestMempoolReactorMaxActiveOutboundConnections(t *testing.T) {
	config := cfg.TestConfig()
	config.Mempool.ExperimentalMaxGossipConnectionsToNonPersistentPeers = 1
	reactors, _ := makeAndConnectReactors(config, 4)
	defer func() {
		for _, r := range reactors {
			if err := r.Stop(); err != nil {
				assert.NoError(t, err)
			}
		}
	}()
	for _, r := range reactors {
		for _, peer := range r.Switch.Peers().List() {
			peer.Set(types.PeerStateKey, peerState{1})
		}
	}

	// Add a bunch transactions to the first reactor.
	txs := newUniqueTxs(100)
	callCheckTx(t, reactors[0].mempool, txs)

	// Wait for all txs to be in the mempool of the second reactor; the other reactors should not
	// receive any tx. (The second reactor only sends transactions to the first reactor.)
	checkTxsInMempool(t, txs, reactors[1], 0)
	for _, r := range reactors[2:] {
		require.Zero(t, r.mempool.Size())
	}

	// Disconnect the second reactor from the first reactor.
	firstPeer := reactors[0].Switch.Peers().List()[0]
	reactors[0].Switch.StopPeerGracefully(firstPeer)

	// Now the third reactor should start receiving transactions from the first reactor; the fourth
	// reactor's mempool should still be empty.
	checkTxsInMempool(t, txs, reactors[2], 0)
	for _, r := range reactors[3:] {
		require.Zero(t, r.mempool.Size())
	}
}

// mempoolLogger is a TestingLogger which uses a different
// color for each validator ("validator" key must exist).
func mempoolLogger() log.Logger {
	return log.TestingLoggerWithColorFn(func(keyvals ...interface{}) term.FgBgColor {
		for i := 0; i < len(keyvals)-1; i += 2 {
			if keyvals[i] == "validator" {
				return term.FgBgColor{Fg: term.Color(uint8(keyvals[i+1].(int) + 1))}
			}
		}
		return term.FgBgColor{}
	})
}

// connect N mempool reactors through N switches
func makeAndConnectReactors(config *cfg.Config, n int) ([]*Reactor, []*p2p.Switch) {
	reactors := make([]*Reactor, n)
	logger := mempoolLogger()
	for i := 0; i < n; i++ {
		app := kvstore.NewInMemoryApplication()
		cc := proxy.NewLocalClientCreator(app)
		mempool, cleanup := newMempoolWithApp(cc)
		defer cleanup()

		reactors[i] = NewReactor(config.Mempool, mempool) // so we dont start the consensus states
		reactors[i].SetLogger(logger.With("validator", i))
	}

	switches := p2p.MakeConnectedSwitches(config.P2P, n, func(i int, s *p2p.Switch) *p2p.Switch {
		s.AddReactor("MEMPOOL", reactors[i])
		return s

	}, p2p.Connect2Switches)
	return reactors, switches
}

func newUniqueTxs(n int) types.Txs {
	txs := make(types.Txs, n)
	for i := 0; i < n; i++ {
		txs[i] = kvstore.NewTxFromID(i)
	}
	return txs
}

func waitForTxsOnReactors(t *testing.T, txs types.Txs, reactors []*Reactor) {
	// wait for the txs in all mempools
	wg := new(sync.WaitGroup)
	for i, reactor := range reactors {
		wg.Add(1)
		go func(r *Reactor, reactorIndex int) {
			defer wg.Done()
			checkTxsInOrder(t, txs, r, reactorIndex)
		}(reactor, i)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	timer := time.After(timeout)
	select {
	case <-timer:
		t.Fatal("Timed out waiting for txs")
	case <-done:
	}
}

// Wait until the mempool has a certain number of transactions.
func waitForNumTxsInMempool(numTxs int, mempool Mempool) {
	for mempool.Size() < numTxs {
		time.Sleep(time.Millisecond * 100)
	}
}

// Wait until all txs are in the mempool and check that the number of txs in the
// mempool is as expected.
func checkTxsInMempool(t *testing.T, txs types.Txs, reactor *Reactor, _ int) {
	waitForNumTxsInMempool(len(txs), reactor.mempool)

	reapedTxs := reactor.mempool.ReapMaxTxs(len(txs))
	require.Equal(t, len(txs), len(reapedTxs))
	require.Equal(t, len(txs), reactor.mempool.Size())
}

// Wait until all txs are in the mempool and check that they are in the same
// order as given.
func checkTxsInOrder(t *testing.T, txs types.Txs, reactor *Reactor, reactorIndex int) {
	waitForNumTxsInMempool(len(txs), reactor.mempool)

	// Check that all transactions in the mempool are in the same order as txs.
	reapedTxs := reactor.mempool.ReapMaxTxs(len(txs))
	for i, tx := range txs {
		assert.Equalf(t, tx, reapedTxs[i],
			"txs at index %d on reactor %d don't match: %v vs %v", i, reactorIndex, tx, reapedTxs[i])
	}
}

// ensure no txs on reactor after some timeout
func ensureNoTxs(t *testing.T, reactor *Reactor, timeout time.Duration) {
	time.Sleep(timeout) // wait for the txs in all mempools
	assert.Zero(t, reactor.mempool.Size())
}

func TestMempoolVectors(t *testing.T) {
	testCases := []struct {
		testName string
		tx       []byte
		expBytes string
	}{
		{"tx 1", []byte{123}, "0a030a017b"},
		{"tx 2", []byte("proto encoding in mempool"), "0a1b0a1970726f746f20656e636f64696e6720696e206d656d706f6f6c"},
	}

	for _, tc := range testCases {
		tc := tc

		msg := memproto.Message{
			Sum: &memproto.Message_Txs{
				Txs: &memproto.Txs{Txs: [][]byte{tc.tx}},
			},
		}
		bz, err := msg.Marshal()
		require.NoError(t, err, tc.testName)

		require.Equal(t, tc.expBytes, hex.EncodeToString(bz), tc.testName)
	}
}

// recordingCounter counts Add calls so tests can read a metric NopMetrics would discard.
type recordingCounter struct{ n atomic.Int64 }

func (c *recordingCounter) With(...string) metrics.Counter { return c }
func (c *recordingCounter) Add(delta float64)              { c.n.Add(int64(delta)) }

func newRecvTestReactor(t *testing.T, workers, queueSize int) (*Reactor, *recordingCounter) {
	t.Helper()
	client, err := proxy.NewLocalClientCreator(kvstore.NewInMemoryApplication()).NewABCIClient()
	require.NoError(t, err)
	return newRecvTestReactorWithClient(t, workers, queueSize, client)
}

func newRecvTestReactorWithClient(
	t *testing.T, workers, queueSize int, client abciclient.Client,
) (*Reactor, *recordingCounter) {
	t.Helper()
	config := cfg.TestConfig()
	config.Mempool.RecvWorkers = workers
	config.Mempool.RecvQueueSize = queueSize
	mempool, cleanup := newMempoolWithAppAndConfigMock(config, client)
	t.Cleanup(cleanup)
	dropped := &recordingCounter{}
	mempool.metrics.RecvQueueDroppedTxs = dropped
	reactor := NewReactor(config.Mempool, mempool)
	reactor.SetLogger(mempoolLogger())
	return reactor, dropped
}

func txsEnvelope(src p2p.Peer, txs ...types.Tx) p2p.Envelope {
	raw := make([][]byte, len(txs))
	for i, tx := range txs {
		raw[i] = tx
	}
	return p2p.Envelope{ChannelID: MempoolChannel, Src: src, Message: &memproto.Txs{Txs: raw}}
}

// With RecvWorkers set, Receive only enqueues: nothing is checked until a
// worker runs, and a full queue drops and counts instead of blocking.
func TestReactorRecvQueueDropsWhenFull(t *testing.T) {
	reactor, dropped := newRecvTestReactor(t, 1, 1)
	peer := mock.NewPeer(nil)
	reactor.InitPeer(peer)
	txs := newUniqueTxs(3)

	reactor.Receive(txsEnvelope(peer, txs...))

	require.Zero(t, reactor.mempool.Size(), "nothing is checked on the receive goroutine")
	require.EqualValues(t, 2, dropped.n.Load())
	require.EqualValues(t, 1, reactor.recvQueued.Load())

	require.NoError(t, reactor.Start())
	t.Cleanup(func() { require.NoError(t, reactor.Stop()) })
	waitForNumTxsInMempool(1, reactor.mempool)
	require.Equal(t, txs[0], reactor.mempool.ReapMaxTxs(1)[0])
}

// With RecvWorkers 0, Receive checks txs inline as before.
func TestReactorRecvWorkersZeroChecksInline(t *testing.T) {
	reactor, dropped := newRecvTestReactor(t, 0, 0)
	peer := mock.NewPeer(nil)
	reactor.InitPeer(peer)
	txs := newUniqueTxs(5)

	reactor.Receive(txsEnvelope(peer, txs...))

	require.Nil(t, reactor.recvReady)
	require.Equal(t, len(txs), reactor.mempool.Size())
	require.Zero(t, dropped.n.Load())
}

// A tx already in the cache is not re-queued, but the new sender is still
// recorded so the tx is not gossiped back to it.
func TestReactorRecvCachedTxRecordsSender(t *testing.T) {
	reactor, _ := newRecvTestReactor(t, 1, 10)
	require.NoError(t, reactor.Start())
	t.Cleanup(func() { require.NoError(t, reactor.Stop()) })
	first, second := mock.NewPeer(nil), mock.NewPeer(nil)
	reactor.InitPeer(first)
	reactor.InitPeer(second)
	tx := newUniqueTxs(1)[0]

	reactor.Receive(txsEnvelope(first, tx))
	waitForNumTxsInMempool(1, reactor.mempool)
	reactor.Receive(txsEnvelope(second, tx))

	memTx := reactor.mempool.getMemTx(tx.Key())
	require.NotNil(t, memTx)
	require.True(t, memTx.isSender(reactor.ids.GetForPeer(first)))
	require.True(t, memTx.isSender(reactor.ids.GetForPeer(second)))
	require.Zero(t, reactor.recvQueued.Load())
}

// A copy of a tx from a second peer that arrives while the first copy is still
// queued is not queued again: the claim is taken at receive time, and the
// queued copy still checks successfully.
func TestReactorRecvQueueClaimsAtReceive(t *testing.T) {
	reactor, dropped := newRecvTestReactor(t, 1, 10) // not started: nothing drains the queue
	first, second := mock.NewPeer(nil), mock.NewPeer(nil)
	reactor.InitPeer(first)
	reactor.InitPeer(second)
	tx := newUniqueTxs(1)[0]

	reactor.Receive(txsEnvelope(first, tx))
	reactor.Receive(txsEnvelope(second, tx))

	require.EqualValues(t, 1, reactor.recvQueued.Load())
	require.Zero(t, dropped.n.Load())
	require.True(t, reactor.mempool.cache.Has(tx))

	q := reactor.recvQueues[reactor.ids.GetForPeer(first)]
	require.Len(t, q.ordered, 1)
	require.Nil(t, reactor.recvQueues[reactor.ids.GetForPeer(second)])
	item := q.ordered[0]
	require.NoError(t, reactor.mempool.CheckTxClaimed(item.tx, nil, item.info))
	require.Equal(t, 1, reactor.mempool.Size())
}

// A tx dropped from a full queue gives up its claim so a later copy is checked.
func TestReactorRecvQueueDropReleasesClaim(t *testing.T) {
	reactor, dropped := newRecvTestReactor(t, 1, 1)
	peer := mock.NewPeer(nil)
	reactor.InitPeer(peer)
	txs := newUniqueTxs(2)

	reactor.Receive(txsEnvelope(peer, txs...))

	require.EqualValues(t, 1, dropped.n.Load())
	require.True(t, reactor.mempool.cache.Has(txs[0]))
	require.False(t, reactor.mempool.cache.Has(txs[1]))
	require.NoError(t, reactor.mempool.CheckTx(txs[1], nil, TxInfo{}))
}

// A single worker applies one peer's txs in the order they were sent: the FIFO
// is drained front to back.
func TestReactorRecvOneWorkerKeepsPeerOrder(t *testing.T) {
	reactor, dropped := newRecvTestReactor(t, 1, 1000)
	require.NoError(t, reactor.Start())
	t.Cleanup(func() { require.NoError(t, reactor.Stop()) })
	peer := mock.NewPeer(nil)
	reactor.InitPeer(peer)
	txs := newUniqueTxs(200)

	for i := 0; i < len(txs); i += 10 {
		reactor.Receive(txsEnvelope(peer, txs[i:i+10]...))
	}

	waitForNumTxsInMempool(len(txs), reactor.mempool)
	require.Equal(t, txs, reactor.mempool.ReapMaxTxs(-1))
	require.Zero(t, dropped.n.Load())
	require.Zero(t, reactor.recvQueued.Load())
}

// gatedApp accepts every tx but holds each CheckTx until opened, and counts the
// calls that have entered, so a test can see how many run at once.
type gatedApp struct {
	*kvstore.Application
	entered atomic.Int64
	release chan struct{}
	once    sync.Once
}

func newGatedApp() *gatedApp {
	return &gatedApp{Application: kvstore.NewInMemoryApplication(), release: make(chan struct{})}
}

func (a *gatedApp) CheckTx(context.Context, *abci.RequestCheckTx) (*abci.ResponseCheckTx, error) {
	a.entered.Add(1)
	<-a.release
	return &abci.ResponseCheckTx{Code: abci.CodeTypeOK}, nil
}

func (a *gatedApp) open() { a.once.Do(func() { close(a.release) }) }

// newGatedReactor drives app through the unsynchronized local client, the one
// a node uses, so the workers' CheckTx calls really run concurrently.
func newGatedReactor(t *testing.T, workers int) (*Reactor, *gatedApp, p2p.Peer) {
	t.Helper()
	app := newGatedApp()
	reactor, _ := newRecvTestReactorWithClient(t, workers, 1000, abciclient.NewUnsyncLocalClient(app))
	require.NoError(t, reactor.Start())
	t.Cleanup(func() { require.NoError(t, reactor.Stop()) })
	t.Cleanup(app.open) // runs before Stop, so blocked workers can exit
	peer := mock.NewPeer(nil)
	reactor.InitPeer(peer)
	return reactor, app, peer
}

// shortTermOrderTx builds a cosmos tx carrying one short-term MsgPlaceOrder,
// which the reactor routes to the sender's fast lane.
func shortTermOrderTx(t *testing.T, clientID uint32) types.Tx {
	t.Helper()
	msg, err := (&clob.MsgPlaceOrder{Order: clob.Order{OrderId: clob.OrderId{
		SubaccountId: subaccounts.SubaccountId{Owner: "dydx1test"},
		ClientId:     clientID,
		OrderFlags:   clob.OrderIdFlags_ShortTerm,
	}}}).Marshal()
	require.NoError(t, err)
	raw, err := (&cosmostx.Tx{Body: &cosmostx.TxBody{Messages: []*codectypes.Any{{
		TypeUrl: "/dydxprotocol.clob.MsgPlaceOrder", Value: msg,
	}}}}).Marshal()
	require.NoError(t, err)
	return raw
}

func shortTermOrderTxs(t *testing.T, n int) types.Txs {
	t.Helper()
	txs := make(types.Txs, n)
	for i := range txs {
		txs[i] = shortTermOrderTx(t, uint32(i))
	}
	return txs
}

// One peer's short-term order txs are checked by every worker at once: the
// queue goes back on recvReady before each check, not after it.
func TestReactorRecvFastLaneUsesAllWorkers(t *testing.T) {
	reactor, app, peer := newGatedReactor(t, 4)
	txs := shortTermOrderTxs(t, 8)

	reactor.Receive(txsEnvelope(peer, txs...))

	require.Eventually(t, func() bool { return app.entered.Load() == 4 }, time.Second, time.Millisecond)
	app.open()
	waitForNumTxsInMempool(len(txs), reactor.mempool)
	require.Zero(t, reactor.recvQueued.Load())
}

// Every other tx from one peer is checked one at a time and in arrival order,
// whatever the worker count, so sequence numbers survive the gossip hop.
func TestReactorRecvOrderedLaneOneAtATime(t *testing.T) {
	reactor, app, peer := newGatedReactor(t, 4)
	txs := newUniqueTxs(8)

	reactor.Receive(txsEnvelope(peer, txs...))

	require.Eventually(t, func() bool { return app.entered.Load() == 1 }, time.Second, time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	require.EqualValues(t, 1, app.entered.Load())
	app.open()
	waitForNumTxsInMempool(len(txs), reactor.mempool)
	require.Equal(t, txs, reactor.mempool.ReapMaxTxs(-1))
	require.Zero(t, reactor.recvQueued.Load())
}

// An ordered-lane tx being checked does not hold up the same peer's fast lane.
func TestReactorRecvOrderedLaneDoesNotBlockFastLane(t *testing.T) {
	reactor, app, peer := newGatedReactor(t, 2)
	txs := append(newUniqueTxs(1), shortTermOrderTxs(t, 2)...)

	reactor.Receive(txsEnvelope(peer, txs...))

	require.Eventually(t, func() bool { return app.entered.Load() == 2 }, time.Second, time.Millisecond)
	app.open()
	waitForNumTxsInMempool(len(txs), reactor.mempool)
	require.Zero(t, reactor.recvQueued.Load())
}

// Two peers with waiting txs are both scheduled, so the pool drains them side
// by side rather than one peer's backlog idling the other workers.
func TestReactorRecvPeersScheduledIndependently(t *testing.T) {
	reactor, _ := newRecvTestReactor(t, 2, 1000) // not started: inspect the schedule
	first, second := mock.NewPeer(nil), mock.NewPeer(nil)
	reactor.InitPeer(first)
	reactor.InitPeer(second)
	txs := newUniqueTxs(6)

	reactor.Receive(txsEnvelope(first, txs[:5]...))
	reactor.Receive(txsEnvelope(second, txs[5]))

	require.Len(t, reactor.recvReady, 2)
	require.EqualValues(t, 6, reactor.recvQueued.Load())
	for _, peer := range []p2p.Peer{first, second} {
		require.True(t, reactor.recvQueues[reactor.ids.GetForPeer(peer)].scheduled)
	}

	require.NoError(t, reactor.Start())
	t.Cleanup(func() { require.NoError(t, reactor.Stop()) })
	waitForNumTxsInMempool(len(txs), reactor.mempool)
	require.Zero(t, reactor.recvQueued.Load())
	require.Empty(t, reactor.recvReady)
}

// The total bound drops and counts once RecvQueueSize txs are waiting across
// all peers, even when no single peer is at its own cap.
func TestReactorRecvQueueTotalBoundDrops(t *testing.T) {
	reactor, dropped := newRecvTestReactor(t, 1, 8) // per-peer cap 2
	txs := newUniqueTxs(9)
	for i := 0; i < 4; i++ {
		peer := mock.NewPeer(nil)
		reactor.InitPeer(peer)
		reactor.Receive(txsEnvelope(peer, txs[2*i:2*i+2]...))
	}
	require.EqualValues(t, 8, reactor.recvQueued.Load())
	require.Zero(t, dropped.n.Load())

	fifth := mock.NewPeer(nil)
	reactor.InitPeer(fifth)
	reactor.Receive(txsEnvelope(fifth, txs[8]))

	require.EqualValues(t, 8, reactor.recvQueued.Load())
	require.EqualValues(t, 1, dropped.n.Load())
	require.False(t, reactor.mempool.cache.Has(txs[8]))
}

// One peer cannot take the whole budget: past its cap its txs drop while
// another peer's still get through.
func TestReactorRecvQueuePerPeerCapDrops(t *testing.T) {
	reactor, dropped := newRecvTestReactor(t, 1, 8) // per-peer cap 2
	busy, quiet := mock.NewPeer(nil), mock.NewPeer(nil)
	reactor.InitPeer(busy)
	reactor.InitPeer(quiet)
	txs := newUniqueTxs(4)

	reactor.Receive(txsEnvelope(busy, txs[:3]...))
	reactor.Receive(txsEnvelope(quiet, txs[3]))

	require.EqualValues(t, 1, dropped.n.Load())
	require.EqualValues(t, 3, reactor.recvQueued.Load())
	require.Len(t, reactor.recvQueues[reactor.ids.GetForPeer(busy)].ordered, 2)
	require.Len(t, reactor.recvQueues[reactor.ids.GetForPeer(quiet)].ordered, 1)
	require.False(t, reactor.mempool.cache.Has(txs[2]))
	require.True(t, reactor.mempool.cache.Has(txs[3]))
}

// Removing a peer releases the claims of its waiting txs so copies from other
// peers can be checked, and its FIFO no longer counts toward the total.
func TestReactorRecvRemovePeerReleasesClaims(t *testing.T) {
	reactor, _ := newRecvTestReactor(t, 1, 100)
	peer := mock.NewPeer(nil)
	reactor.InitPeer(peer)
	txs := newUniqueTxs(3)
	reactor.Receive(txsEnvelope(peer, txs...))
	require.EqualValues(t, 3, reactor.recvQueued.Load())

	reactor.RemovePeer(peer, nil)

	require.Zero(t, reactor.recvQueued.Load())
	require.Empty(t, reactor.recvQueues)
	for _, tx := range txs {
		require.False(t, reactor.mempool.cache.Has(tx))
		require.NoError(t, reactor.mempool.CheckTx(tx, nil, TxInfo{}))
	}
}

// Every tx a worker checks records one wait observation on the same series the
// exported histogram writes to, and NopMetrics hands the reactor no observer.
func TestReactorRecvQueueWaitObserved(t *testing.T) {
	config := cfg.TestConfig()
	config.Mempool.RecvWorkers = 2
	config.Mempool.RecvQueueSize = 1000
	app := kvstore.NewInMemoryApplication()
	cc := proxy.NewLocalClientCreator(app)
	mempool, cleanup := newMempoolWithAppAndConfig(cc, config)
	t.Cleanup(cleanup)
	// Prometheus collectors register globally, so each run needs its own namespace.
	namespace := fmt.Sprintf("recv_wait_test_%d", time.Now().UnixNano())
	m := PrometheusMetrics(namespace, "chain_id", "test")
	mempool.metrics = m
	reactor := NewReactor(config.Mempool, mempool)
	reactor.SetLogger(mempoolLogger())
	require.NotNil(t, reactor.recvWait)
	require.NoError(t, reactor.Start())
	t.Cleanup(func() { require.NoError(t, reactor.Stop()) })
	peer := mock.NewPeer(nil)
	reactor.InitPeer(peer)
	txs := newUniqueTxs(25)

	reactor.Receive(txsEnvelope(peer, txs...))
	waitForNumTxsInMempool(len(txs), mempool)
	m.RecvQueueWaitSeconds.Observe(1)

	families, err := stdprometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	var count uint64
	for _, family := range families {
		if family.GetName() == namespace+"_mempool_recv_queue_wait_seconds" {
			require.Len(t, family.GetMetric(), 1)
			count = family.GetMetric()[0].GetHistogram().GetSampleCount()
		}
	}
	require.EqualValues(t, len(txs)+1, count)

	nop, _ := newRecvTestReactor(t, 1, 10)
	require.Nil(t, nop.recvWait)
}
