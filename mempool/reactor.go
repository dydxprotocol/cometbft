package mempool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	cfg "github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/libs/clist"
	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/p2p"
	protomem "github.com/cometbft/cometbft/proto/tendermint/mempool"
	"github.com/cometbft/cometbft/types"
	stdprometheus "github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/semaphore"
)

// Reactor handles mempool tx broadcasting amongst peers.
// It maintains a map from peer ID to counter, to prevent gossiping txs to the
// peers you received it from.
type Reactor struct {
	p2p.BaseReactor
	config  *cfg.MempoolConfig
	mempool *CListMempool
	ids     *mempoolIDs

	// Semaphores to keep track of how many connections to peers are active for broadcasting
	// transactions. Each semaphore has a capacity that puts an upper bound on the number of
	// connections for different groups of peers.
	activePersistentPeersSemaphore    *semaphore.Weighted
	activeNonPersistentPeersSemaphore *semaphore.Weighted

	// Transactions received from peers wait in a queue per sender until a recv
	// worker checks them. The workers are shared: a sender's queue is put on
	// recvReady when it has a tx a worker could run and is not already there,
	// and a worker takes one tx from it, puts it back if more could run, then
	// checks the tx, so the pool drains every peer round-robin. recvQueued is
	// the total waiting, bounded by RecvQueueSize; each queue is bounded by
	// recvPeerCap. recvReady is nil when RecvWorkers is 0, in which case
	// Receive checks txs inline.
	recvQueues    map[uint16]*peerRecvQueue
	recvQueuesMtx sync.Mutex
	recvReady     chan *peerRecvQueue
	recvQueued    atomic.Int64
	recvPeerCap   int
	recvWait      stdprometheus.Observer // nil when metrics are discarded
	recvDone      chan struct{}
	recvWg        sync.WaitGroup
}

type recvTx struct {
	tx       types.Tx
	info     TxInfo
	enqueued time.Time
}

// peerRecvQueue holds one sender's claimed txs waiting for a worker, in two
// lanes. Short-term clob order txs carry no sequence number, so any number of
// them are checked at once; every other tx is checked one at a time in arrival
// order, so a sender's sequence numbers still line up after a gossip hop.
// scheduled is set from the moment the queue is put on recvReady until a
// worker takes it and finds nothing runnable, so it is on recvReady at most
// once at a time. orderedBusy is set while an ordered-lane tx is being checked.
type peerRecvQueue struct {
	mtx         sync.Mutex
	fast        []recvTx
	ordered     []recvTx
	scheduled   bool
	orderedBusy bool
}

func (q *peerRecvQueue) size() int { return len(q.fast) + len(q.ordered) }

func (q *peerRecvQueue) runnable() bool {
	return len(q.fast) > 0 || (len(q.ordered) > 0 && !q.orderedBusy)
}

// scheduleLocked marks the queue scheduled when it has runnable work and is
// not scheduled yet; the caller then sends it on recvReady.
func (q *peerRecvQueue) scheduleLocked() bool {
	if q.scheduled || !q.runnable() {
		return false
	}
	q.scheduled = true
	return true
}

func popFront(lane *[]recvTx) recvTx {
	item := (*lane)[0]
	if len(*lane) == 1 {
		*lane = nil
	} else {
		(*lane)[0] = recvTx{}
		*lane = (*lane)[1:]
	}
	return item
}

// lazyTxHash formats a tx hash only when a log line is actually written.
type lazyTxHash types.Tx

func (l lazyTxHash) String() string {
	return fmt.Sprintf("%X", types.Tx(l).Hash())
}

// NewReactor returns a new Reactor with the given config and mempool.
func NewReactor(config *cfg.MempoolConfig, mempool *CListMempool) *Reactor {
	memR := &Reactor{
		config:  config,
		mempool: mempool,
		ids:     newMempoolIDs(),
	}
	memR.BaseReactor = *p2p.NewBaseReactor("Mempool", memR)
	memR.activePersistentPeersSemaphore = semaphore.NewWeighted(int64(memR.config.ExperimentalMaxGossipConnectionsToPersistentPeers))
	memR.activeNonPersistentPeersSemaphore = semaphore.NewWeighted(int64(memR.config.ExperimentalMaxGossipConnectionsToNonPersistentPeers))
	if config.RecvWorkers > 0 {
		memR.recvQueues = make(map[uint16]*peerRecvQueue)
		// Each FIFO is on recvReady at most once, and there are at most
		// MaxActiveIDs senders, so a send on recvReady never blocks.
		memR.recvReady = make(chan *peerRecvQueue, MaxActiveIDs)
		memR.recvPeerCap = config.RecvQueueSize / 4
		if memR.recvPeerCap < 1 {
			memR.recvPeerCap = 1
		}
		memR.recvWait = mempool.metrics.recvQueueWaitObserver()
		memR.recvDone = make(chan struct{})
	}

	return memR
}

// InitPeer implements Reactor by creating a state for the peer.
func (memR *Reactor) InitPeer(peer p2p.Peer) p2p.Peer {
	memR.ids.ReserveForPeer(peer)
	return peer
}

// SetLogger sets the Logger on the reactor and the underlying mempool.
func (memR *Reactor) SetLogger(l log.Logger) {
	memR.Logger = l
	memR.mempool.SetLogger(l)
}

// OnStart implements p2p.BaseReactor.
func (memR *Reactor) OnStart() error {
	if !memR.config.Broadcast {
		memR.Logger.Info("Tx broadcasting is disabled")
	}
	if memR.recvReady != nil {
		for i := 0; i < memR.config.RecvWorkers; i++ {
			memR.recvWg.Add(1)
			go memR.recvWorker()
		}
		memR.recvWg.Add(1)
		go memR.recvQueueReporter()
	}
	return nil
}

// OnStop implements p2p.BaseReactor. It runs before Quit() is closed, so the
// workers stop on recvDone instead.
func (memR *Reactor) OnStop() {
	if memR.recvReady != nil {
		close(memR.recvDone)
		memR.recvWg.Wait()
	}
}

// peerRecvQueue returns the sender's FIFO, creating it on first use.
func (memR *Reactor) peerRecvQueue(senderID uint16) *peerRecvQueue {
	memR.recvQueuesMtx.Lock()
	defer memR.recvQueuesMtx.Unlock()
	q := memR.recvQueues[senderID]
	if q == nil {
		q = &peerRecvQueue{}
		memR.recvQueues[senderID] = q
	}
	return q
}

// enqueueRecv appends a claimed tx to the right lane of its sender's queue and
// schedules the queue if it has become runnable. It reports false, after
// releasing the claim, when the total or the per-peer bound is hit.
func (memR *Reactor) enqueueRecv(tx types.Tx, info TxInfo) bool {
	if memR.recvQueued.Add(1) > int64(memR.config.RecvQueueSize) {
		memR.recvQueued.Add(-1)
		memR.dropRecv(tx, "recv queue is full")
		return false
	}
	fast := IsShortTermClobOrderTransaction(tx, memR.Logger)
	q := memR.peerRecvQueue(info.SenderID)
	q.mtx.Lock()
	if q.size() >= memR.recvPeerCap {
		q.mtx.Unlock()
		memR.recvQueued.Add(-1)
		memR.dropRecv(tx, "peer's recv queue is full")
		return false
	}
	item := recvTx{tx: tx, info: info, enqueued: time.Now()}
	if fast {
		q.fast = append(q.fast, item)
	} else {
		q.ordered = append(q.ordered, item)
	}
	schedule := q.scheduleLocked()
	q.mtx.Unlock()
	if schedule {
		memR.recvReady <- q
	}
	return true
}

func (memR *Reactor) dropRecv(tx types.Tx, reason string) {
	memR.mempool.UnclaimTx(tx)
	memR.mempool.metrics.RecvQueueDroppedTxs.Add(1)
	memR.Logger.Debug("Dropped received tx, "+reason, "tx", lazyTxHash(tx))
}

func (memR *Reactor) recvWorker() {
	defer memR.recvWg.Done()
	for {
		select {
		case q := <-memR.recvReady:
			memR.drainOne(q)
		case <-memR.recvDone:
			return
		}
	}
}

// drainOne takes one runnable tx from a sender's queue, puts the queue back on
// recvReady if more could run, then checks the tx. Taking the tx and deciding
// whether to reschedule both happen under the queue's mutex, so an append that
// races with an empty take is either seen here or schedules the queue itself.
// Rescheduling before the check is what lets the other workers take the same
// sender's remaining fast-lane txs while this one is being checked; the
// ordered lane stays busy until its tx returns and is rescheduled then.
func (memR *Reactor) drainOne(q *peerRecvQueue) {
	q.mtx.Lock()
	var item recvTx
	ordered := len(q.ordered) > 0 && !q.orderedBusy
	switch {
	case ordered:
		item = popFront(&q.ordered)
		q.orderedBusy = true
	case len(q.fast) > 0:
		item = popFront(&q.fast)
	default:
		q.scheduled = false
		q.mtx.Unlock()
		return
	}
	q.scheduled = q.runnable()
	reschedule := q.scheduled
	q.mtx.Unlock()
	if reschedule {
		memR.recvReady <- q
	}
	memR.recvQueued.Add(-1)
	if memR.recvWait != nil {
		memR.recvWait.Observe(time.Since(item.enqueued).Seconds())
	}

	memR.checkTx(item.tx, item.info, true)

	if ordered {
		q.mtx.Lock()
		q.orderedBusy = false
		schedule := q.scheduleLocked()
		q.mtx.Unlock()
		if schedule {
			memR.recvReady <- q
		}
	}
}

// recvQueueReporter publishes the queue length once a second. The gauge is not
// touched per tx because every go-kit metric operation allocates.
func (memR *Reactor) recvQueueReporter() {
	defer memR.recvWg.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			memR.mempool.metrics.RecvQueueLen.Set(float64(memR.recvQueued.Load()))
		case <-memR.recvDone:
			return
		}
	}
}

func (memR *Reactor) checkTx(tx types.Tx, txInfo TxInfo, claimed bool) {
	var err error
	if claimed {
		err = memR.mempool.CheckTxClaimed(tx, nil, txInfo)
	} else {
		err = memR.mempool.CheckTx(tx, nil, txInfo)
	}
	if errors.Is(err, ErrTxInCache) {
		memR.Logger.Debug("Tx already exists in cache", "tx", lazyTxHash(tx))
	} else if err != nil {
		memR.Logger.Info("Could not check tx", "tx", lazyTxHash(tx), "err", err)
	}
}

// GetChannels implements Reactor by returning the list of channels for this
// reactor.
func (memR *Reactor) GetChannels() []*p2p.ChannelDescriptor {
	largestTx := make([]byte, memR.config.MaxTxBytes)
	batchMsg := protomem.Message{
		Sum: &protomem.Message_Txs{
			Txs: &protomem.Txs{Txs: [][]byte{largestTx}},
		},
	}

	return []*p2p.ChannelDescriptor{
		{
			ID:                  MempoolChannel,
			Priority:            5,
			RecvMessageCapacity: batchMsg.Size(),
			MessageType:         &protomem.Message{},
		},
	}
}

// AddPeer implements Reactor.
// It starts a broadcast routine ensuring all txs are forwarded to the given peer.
func (memR *Reactor) AddPeer(peer p2p.Peer) {
	if memR.config.Broadcast {
		go func() {
			// Always forward transactions to unconditional peers.
			if !memR.Switch.IsPeerUnconditional(peer.ID()) {
				// Depending on the type of peer, we choose a semaphore to limit the gossiping peers.
				var peerSemaphore *semaphore.Weighted
				if peer.IsPersistent() && memR.config.ExperimentalMaxGossipConnectionsToPersistentPeers > 0 {
					peerSemaphore = memR.activePersistentPeersSemaphore
				} else if !peer.IsPersistent() && memR.config.ExperimentalMaxGossipConnectionsToNonPersistentPeers > 0 {
					peerSemaphore = memR.activeNonPersistentPeersSemaphore
				}

				if peerSemaphore != nil {
					for peer.IsRunning() {
						// Block on the semaphore until a slot is available to start gossiping with this peer.
						// Do not block indefinitely, in case the peer is disconnected before gossiping starts.
						ctxTimeout, cancel := context.WithTimeout(context.TODO(), 30*time.Second)
						// Block sending transactions to peer until one of the connections become
						// available in the semaphore.
						err := peerSemaphore.Acquire(ctxTimeout, 1)
						cancel()

						if err != nil {
							continue
						}

						// Release semaphore to allow other peer to start sending transactions.
						defer peerSemaphore.Release(1)
						break
					}
				}
			}

			memR.mempool.metrics.ActiveOutboundConnections.Add(1)
			defer memR.mempool.metrics.ActiveOutboundConnections.Add(-1)
			memR.broadcastTxRoutine(peer)
		}()
	}
}

// RemovePeer implements Reactor. Txs still waiting in the peer's FIFO give up
// their claims so copies from other peers can be checked.
func (memR *Reactor) RemovePeer(peer p2p.Peer, _ interface{}) {
	if memR.recvReady != nil {
		senderID := memR.ids.GetForPeer(peer)
		memR.recvQueuesMtx.Lock()
		q := memR.recvQueues[senderID]
		delete(memR.recvQueues, senderID)
		memR.recvQueuesMtx.Unlock()
		if q != nil {
			q.mtx.Lock()
			waiting := append(q.fast, q.ordered...)
			q.fast, q.ordered = nil, nil
			q.mtx.Unlock()
			for _, item := range waiting {
				memR.mempool.UnclaimTx(item.tx)
			}
			memR.recvQueued.Add(-int64(len(waiting)))
		}
	}
	memR.ids.Reclaim(peer)
	// broadcast routine checks if peer is gone and returns
}

// Receive implements Reactor.
// It adds any received transactions to the mempool.
func (memR *Reactor) Receive(e p2p.Envelope) {
	memR.Logger.Debug("Receive", "src", e.Src, "chId", e.ChannelID, "msg", e.Message)
	switch msg := e.Message.(type) {
	case *protomem.Txs:
		protoTxs := msg.GetTxs()
		if len(protoTxs) == 0 {
			memR.Logger.Error("received empty txs from peer", "src", e.Src)
			return
		}
		txInfo := TxInfo{SenderID: memR.ids.GetForPeer(e.Src)}
		if e.Src != nil {
			txInfo.SenderP2PID = e.Src.ID()
		}

		for _, tx := range protoTxs {
			ntx := types.Tx(tx)
			if memR.recvReady == nil {
				memR.checkTx(ntx, txInfo, false)
				continue
			}
			// Only cheap work runs on the peer's receive goroutine: consensus
			// messages from the same peer queue behind whatever happens here.
			// The claim goes in before the tx is queued so copies arriving from
			// other peers while it waits are turned away here.
			if !memR.mempool.ClaimTx(ntx, txInfo.SenderID) {
				memR.Logger.Debug("Tx already exists in cache", "tx", lazyTxHash(ntx))
				continue
			}
			memR.enqueueRecv(ntx, txInfo)
		}
	default:
		memR.Logger.Error("unknown message type", "src", e.Src, "chId", e.ChannelID, "msg", e.Message)
		memR.Switch.StopPeerForError(e.Src, fmt.Errorf("mempool cannot handle message of type: %T", e.Message))
		return
	}

	// broadcasting happens from go routines per peer
}

// PeerState describes the state of a peer.
type PeerState interface {
	GetHeight() int64
}

// Send new mempool txs to peer.
func (memR *Reactor) broadcastTxRoutine(peer p2p.Peer) {
	peerID := memR.ids.GetForPeer(peer)
	var next *clist.CElement

	for {
		// In case of both next.NextWaitChan() and peer.Quit() are variable at the same time
		if !memR.IsRunning() || !peer.IsRunning() {
			return
		}

		// This happens because the CElement we were looking at got garbage
		// collected (removed). That is, .NextWait() returned nil. Go ahead and
		// start from the beginning.
		if next == nil {
			select {
			case <-memR.mempool.TxsWaitChan(): // Wait until a tx is available
				if next = memR.mempool.TxsFront(); next == nil {
					continue
				}
			case <-peer.Quit():
				return
			case <-memR.Quit():
				return
			}
		}

		// Make sure the peer is up to date.
		peerState, ok := peer.Get(types.PeerStateKey).(PeerState)
		if !ok {
			// Peer does not have a state yet. We set it in the consensus reactor, but
			// when we add peer in Switch, the order we call reactors#AddPeer is
			// different every time due to us using a map. Sometimes other reactors
			// will be initialized before the consensus reactor. We should wait a few
			// milliseconds and retry.
			time.Sleep(PeerCatchupSleepIntervalMS * time.Millisecond)
			continue
		}

		// Allow for a lag of 1 block.
		memTx := next.Value.(*mempoolTx)
		if peerState.GetHeight() < memTx.Height()-1 {
			time.Sleep(PeerCatchupSleepIntervalMS * time.Millisecond)
			continue
		}

		// NOTE: Transaction batching was disabled due to
		// https://github.com/tendermint/tendermint/issues/5796

		if !memTx.isSender(peerID) {
			success := peer.Send(p2p.Envelope{
				ChannelID: MempoolChannel,
				Message:   &protomem.Txs{Txs: [][]byte{memTx.tx}},
			})
			if !success {
				time.Sleep(PeerCatchupSleepIntervalMS * time.Millisecond)
				continue
			}
		}

		select {
		case <-next.NextWaitChan():
			// see the start of the for loop for nil check
			next = next.Next()
		case <-peer.Quit():
			return
		case <-memR.Quit():
			return
		}
	}
}

// TxsMessage is a Message containing transactions.
type TxsMessage struct {
	Txs []types.Tx
}

// String returns a string representation of the TxsMessage.
func (m *TxsMessage) String() string {
	return fmt.Sprintf("[TxsMessage %v]", m.Txs)
}
