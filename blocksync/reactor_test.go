package blocksync

import (
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	bcproto "github.com/cometbft/cometbft/proto/tendermint/blocksync"

	"github.com/cosmos/gogoproto/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	dbm "github.com/cometbft/cometbft-db"

	abci "github.com/cometbft/cometbft/abci/types"
	cfg "github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/internal/test"
	"github.com/cometbft/cometbft/libs/log"
	mpmocks "github.com/cometbft/cometbft/mempool/mocks"
	"github.com/cometbft/cometbft/p2p"
	p2pmocks "github.com/cometbft/cometbft/p2p/mocks"
	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
	"github.com/cometbft/cometbft/proxy"
	sm "github.com/cometbft/cometbft/state"
	"github.com/cometbft/cometbft/store"
	"github.com/cometbft/cometbft/types"
	cmttime "github.com/cometbft/cometbft/types/time"
)

var config *cfg.Config

func randGenesisDoc(numValidators int, randPower bool, minPower int64) (*types.GenesisDoc, []types.PrivValidator) {
	validators := make([]types.GenesisValidator, numValidators)
	privValidators := make([]types.PrivValidator, numValidators)
	for i := 0; i < numValidators; i++ {
		val, privVal := types.RandValidator(randPower, minPower)
		validators[i] = types.GenesisValidator{
			PubKey: val.PubKey,
			Power:  val.VotingPower,
		}
		privValidators[i] = privVal
	}
	sort.Sort(types.PrivValidatorsByAddress(privValidators))

	consPar := types.DefaultConsensusParams()
	consPar.ABCI.VoteExtensionsEnableHeight = 1
	return &types.GenesisDoc{
		GenesisTime:     cmttime.Now(),
		ChainID:         test.DefaultTestChainID,
		Validators:      validators,
		ConsensusParams: consPar,
	}, privValidators
}

type ReactorPair struct {
	reactor *Reactor
	app     proxy.AppConns
}

func newReactor(
	t *testing.T,
	logger log.Logger,
	genDoc *types.GenesisDoc,
	privVals []types.PrivValidator,
	maxBlockHeight int64,
) ReactorPair {
	if len(privVals) != 1 {
		panic("only support one validator")
	}

	app := abci.NewBaseApplication()
	cc := proxy.NewLocalClientCreator(app)
	proxyApp := proxy.NewAppConns(cc, proxy.NopMetrics())
	err := proxyApp.Start()
	if err != nil {
		panic(fmt.Errorf("error start app: %w", err))
	}

	blockDB := dbm.NewMemDB()
	stateDB := dbm.NewMemDB()
	stateStore := sm.NewStore(stateDB, sm.StoreOptions{
		DiscardABCIResponses: false,
	})
	blockStore := store.NewBlockStore(blockDB)

	state, err := stateStore.LoadFromDBOrGenesisDoc(genDoc)
	if err != nil {
		panic(fmt.Errorf("error constructing state from genesis file: %w", err))
	}

	mp := &mpmocks.Mempool{}
	mp.On("Lock").Return()
	mp.On("Unlock").Return()
	mp.On("FlushAppConn", mock.Anything).Return(nil)
	mp.On("Update",
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything).Return(nil)

	// Make the Reactor itself.
	// NOTE we have to create and commit the blocks first because
	// pool.height is determined from the store.
	fastSync := true
	db := dbm.NewMemDB()
	stateStore = sm.NewStore(db, sm.StoreOptions{
		DiscardABCIResponses: false,
	})
	blockExec := sm.NewBlockExecutor(stateStore, log.TestingLogger(), proxyApp.Consensus(),
		mp, sm.EmptyEvidencePool{}, blockStore)
	if err = stateStore.Save(state); err != nil {
		panic(err)
	}

	// The commit we are building for the current height.
	seenExtCommit := &types.ExtendedCommit{}

	// let's add some blocks in
	for blockHeight := int64(1); blockHeight <= maxBlockHeight; blockHeight++ {
		lastExtCommit := seenExtCommit.Clone()

		thisBlock, err := state.MakeBlock(blockHeight, nil, lastExtCommit.ToCommit(), nil, state.Validators.Proposer.Address)
		require.NoError(t, err)

		thisParts, err := thisBlock.MakePartSet(types.BlockPartSizeBytes)
		require.NoError(t, err)
		blockID := types.BlockID{Hash: thisBlock.Hash(), PartSetHeader: thisParts.Header()}

		// Simulate a commit for the current height
		pubKey, err := privVals[0].GetPubKey()
		if err != nil {
			panic(err)
		}
		addr := pubKey.Address()
		idx, _ := state.Validators.GetByAddress(addr)
		vote, err := types.MakeVote(
			privVals[0],
			thisBlock.Header.ChainID,
			idx,
			thisBlock.Header.Height,
			0,
			cmtproto.PrecommitType,
			blockID,
			time.Now(),
		)
		if err != nil {
			panic(err)
		}
		seenExtCommit = &types.ExtendedCommit{
			Height:             vote.Height,
			Round:              vote.Round,
			BlockID:            blockID,
			ExtendedSignatures: []types.ExtendedCommitSig{vote.ExtendedCommitSig()},
		}

		state, err = blockExec.ApplyBlock(state, blockID, thisBlock)
		if err != nil {
			panic(fmt.Errorf("error apply block: %w", err))
		}

		blockStore.SaveBlockWithExtendedCommit(thisBlock, thisParts, seenExtCommit)
	}

	bcReactor := NewReactor(state.Copy(), blockExec, blockStore, fastSync, NopMetrics(), 0)
	bcReactor.SetLogger(logger.With("module", "blocksync"))

	return ReactorPair{bcReactor, proxyApp}
}

func TestNoBlockResponse(t *testing.T) {
	config = test.ResetTestRoot("blocksync_reactor_test")
	defer os.RemoveAll(config.RootDir)
	genDoc, privVals := randGenesisDoc(1, false, 30)

	maxBlockHeight := int64(65)

	reactorPairs := make([]ReactorPair, 2)

	reactorPairs[0] = newReactor(t, log.TestingLogger(), genDoc, privVals, maxBlockHeight)
	reactorPairs[1] = newReactor(t, log.TestingLogger(), genDoc, privVals, 0)

	p2p.MakeConnectedSwitches(config.P2P, 2, func(i int, s *p2p.Switch) *p2p.Switch {
		s.AddReactor("BLOCKSYNC", reactorPairs[i].reactor)
		return s
	}, p2p.Connect2Switches)

	defer func() {
		for _, r := range reactorPairs {
			err := r.reactor.Stop()
			require.NoError(t, err)
			err = r.app.Stop()
			require.NoError(t, err)
		}
	}()

	tests := []struct {
		height   int64
		existent bool
	}{
		{maxBlockHeight + 2, false},
		{10, true},
		{1, true},
		{100, false},
	}

	for {
		if reactorPairs[1].reactor.pool.IsCaughtUp() {
			break
		}

		time.Sleep(10 * time.Millisecond)
	}

	assert.Equal(t, maxBlockHeight, reactorPairs[0].reactor.store.Height())

	for _, tt := range tests {
		block := reactorPairs[1].reactor.store.LoadBlock(tt.height)
		if tt.existent {
			assert.True(t, block != nil)
		} else {
			assert.True(t, block == nil)
		}
	}
}

// NOTE: This is too hard to test without
// an easy way to add test peer to switch
// or without significant refactoring of the module.
// Alternatively we could actually dial a TCP conn but
// that seems extreme.
func TestBadBlockStopsPeer(t *testing.T) {
	config = test.ResetTestRoot("blocksync_reactor_test")
	defer os.RemoveAll(config.RootDir)
	genDoc, privVals := randGenesisDoc(1, false, 30)

	maxBlockHeight := int64(148)

	// Other chain needs a different validator set
	otherGenDoc, otherPrivVals := randGenesisDoc(1, false, 30)
	otherChain := newReactor(t, log.TestingLogger(), otherGenDoc, otherPrivVals, maxBlockHeight)

	defer func() {
		err := otherChain.reactor.Stop()
		require.Error(t, err)
		err = otherChain.app.Stop()
		require.NoError(t, err)
	}()

	reactorPairs := make([]ReactorPair, 4)

	reactorPairs[0] = newReactor(t, log.TestingLogger(), genDoc, privVals, maxBlockHeight)
	reactorPairs[1] = newReactor(t, log.TestingLogger(), genDoc, privVals, 0)
	reactorPairs[2] = newReactor(t, log.TestingLogger(), genDoc, privVals, 0)
	reactorPairs[3] = newReactor(t, log.TestingLogger(), genDoc, privVals, 0)

	switches := p2p.MakeConnectedSwitches(config.P2P, 4, func(i int, s *p2p.Switch) *p2p.Switch {
		s.AddReactor("BLOCKSYNC", reactorPairs[i].reactor)
		return s
	}, p2p.Connect2Switches)

	defer func() {
		for _, r := range reactorPairs {
			err := r.reactor.Stop()
			require.NoError(t, err)

			err = r.app.Stop()
			require.NoError(t, err)
		}
	}()

	for {
		time.Sleep(1 * time.Second)
		caughtUp := true
		for _, r := range reactorPairs {
			if !r.reactor.pool.IsCaughtUp() {
				caughtUp = false
			}
		}
		if caughtUp {
			break
		}
	}

	// at this time, reactors[0-3] is the newest
	assert.Equal(t, 3, reactorPairs[1].reactor.Switch.Peers().Size())

	// Mark reactorPairs[3] as an invalid peer. Fiddling with .store without a mutex is a data
	// race, but can't be easily avoided.
	reactorPairs[3].reactor.store = otherChain.reactor.store

	lastReactorPair := newReactor(t, log.TestingLogger(), genDoc, privVals, 0)
	reactorPairs = append(reactorPairs, lastReactorPair)

	switches = append(switches, p2p.MakeConnectedSwitches(config.P2P, 1, func(i int, s *p2p.Switch) *p2p.Switch {
		s.AddReactor("BLOCKSYNC", reactorPairs[len(reactorPairs)-1].reactor)
		return s
	}, p2p.Connect2Switches)...)

	for i := 0; i < len(reactorPairs)-1; i++ {
		p2p.Connect2Switches(switches, i, len(reactorPairs)-1)
	}

	for {
		if lastReactorPair.reactor.pool.IsCaughtUp() || lastReactorPair.reactor.Switch.Peers().Size() == 0 {
			break
		}

		time.Sleep(1 * time.Second)
	}

	assert.True(t, lastReactorPair.reactor.Switch.Peers().Size() < len(reactorPairs)-1)
}

func TestCheckSwitchToConsensusLastHeightZero(t *testing.T) {
	const maxBlockHeight = int64(45)

	config = test.ResetTestRoot("blocksync_reactor_test")
	defer os.RemoveAll(config.RootDir)
	genDoc, privVals := randGenesisDoc(1, false, 30)

	reactorPairs := make([]ReactorPair, 1, 2)
	reactorPairs[0] = newReactor(t, log.TestingLogger(), genDoc, privVals, 0)
	reactorPairs[0].reactor.switchToConsensusMs = 50
	defer func() {
		for _, r := range reactorPairs {
			err := r.reactor.Stop()
			require.NoError(t, err)
			err = r.app.Stop()
			require.NoError(t, err)
		}
	}()

	reactorPairs = append(reactorPairs, newReactor(t, log.TestingLogger(), genDoc, privVals, maxBlockHeight))

	var switches []*p2p.Switch
	for _, r := range reactorPairs {
		switches = append(switches, p2p.MakeConnectedSwitches(config.P2P, 1, func(i int, s *p2p.Switch) *p2p.Switch {
			s.AddReactor("BLOCKSYNC", r.reactor)
			return s
		}, p2p.Connect2Switches)...)
	}

	time.Sleep(60 * time.Millisecond)

	// Connect both switches
	p2p.Connect2Switches(switches, 0, 1)

	startTime := time.Now()
	for {
		time.Sleep(20 * time.Millisecond)
		caughtUp := true
		for _, r := range reactorPairs {
			if !r.reactor.pool.IsCaughtUp() {
				caughtUp = false
				break
			}
		}
		if caughtUp {
			break
		}
		if time.Since(startTime) > 90*time.Second {
			msg := "timeout: reactors didn't catch up;"
			for i, r := range reactorPairs {
				h, p, lr := r.reactor.pool.GetStatus()
				c := r.reactor.pool.IsCaughtUp()
				msg += fmt.Sprintf(" reactor#%d (h %d, p %d, lr %d, c %t);", i, h, p, lr, c)
			}
			require.Fail(t, msg)
		}
	}

	// -1 because of "-1" in IsCaughtUp
	// -1 pool.height points to the _next_ height
	// -1 because we measure height of block store
	const maxDiff = 3
	for _, r := range reactorPairs {
		assert.GreaterOrEqual(t, r.reactor.store.Height(), maxBlockHeight-maxDiff)
	}
}

// ExtendedCommitNetworkHelper is intentionally omitted; it depends on reactorOption / genesisDocWithValsPowers
// infrastructure not yet ported to this fork. TestCheckExtendedCommit is likewise omitted.
// The DoS guard is covered by TestFilterMsgBytes and TestStubUnmarshalAllocs below.

// newFilterReactor builds a minimal Reactor wired to a started BlockPool,
// suitable for exercising FilterMsgBytes without spinning up the full p2p
// stack.
func newFilterReactor(t *testing.T) *Reactor {
	t.Helper()

	requestsCh := make(chan BlockRequest, 1000)
	errorsCh := make(chan peerError, 1000)
	pool := NewBlockPool(1, requestsCh, errorsCh)
	require.NoError(t, pool.Start())
	t.Cleanup(func() { _ = pool.Stop() })

	return &Reactor{pool: pool}
}

// seedRequester inserts a bpRequester targeting peerID at the given height,
// bypassing makeRequestersRoutine so the test can drive pool state directly.
func seedRequester(r *Reactor, height int64, peerID p2p.ID) {
	req := newBPRequester(r.pool, height)
	req.peerID = peerID
	r.pool.mtx.Lock()
	r.pool.requesters[height] = req
	r.pool.mtx.Unlock()
}

func mockPeer(id p2p.ID) *p2pmocks.Peer {
	p := &p2pmocks.Peer{}
	p.On("ID").Return(id).Maybe()
	return p
}

func TestFilterMsgBytes(t *testing.T) {
	wireBytesFor := func(t *testing.T, m *bcproto.Message) []byte {
		t.Helper()
		b, err := proto.Marshal(m)
		require.NoError(t, err)
		require.NotEmpty(t, b)
		return b
	}

	blockResponseBytes := func(t *testing.T) []byte {
		return wireBytesFor(t, &bcproto.Message{
			Sum: &bcproto.Message_BlockResponse{
				BlockResponse: &bcproto.BlockResponse{Block: &cmtproto.Block{}},
			},
		})
	}

	blockRequestBytes := func(t *testing.T) []byte {
		return wireBytesFor(t, &bcproto.Message{
			Sum: &bcproto.Message_BlockRequest{
				BlockRequest: &bcproto.BlockRequest{Height: 1},
			},
		})
	}

	const expected p2p.ID = "expected"
	const unexpected p2p.ID = "unexpected"

	tests := []struct {
		name      string
		setup     func(t *testing.T) *Reactor // returns a configured reactor
		chID      byte
		peer      p2p.ID
		bytesFn   func(t *testing.T) []byte
		expectErr string // substring; "" means no error
	}{
		{
			name:      "rejects unsolicited BlockResponse with no requesters",
			setup:     newFilterReactor,
			chID:      BlocksyncChannel,
			peer:      unexpected,
			bytesFn:   blockResponseBytes,
			expectErr: "unsolicited BlockResponse from peer unexpected",
		},
		{
			name: "rejects BlockResponse from peer we did not request from",
			setup: func(t *testing.T) *Reactor {
				r := newFilterReactor(t)
				seedRequester(r, 1, expected)
				return r
			},
			chID:      BlocksyncChannel,
			peer:      unexpected,
			bytesFn:   blockResponseBytes,
			expectErr: "unsolicited BlockResponse from peer unexpected",
		},
		{
			name: "allows BlockResponse from solicited peer",
			setup: func(t *testing.T) *Reactor {
				r := newFilterReactor(t)
				seedRequester(r, 1, expected)
				return r
			},
			chID:    BlocksyncChannel,
			peer:    expected,
			bytesFn: blockResponseBytes,
		},
		{
			name:    "allows non-BlockResponse messages even when disabled",
			setup:   newFilterReactor,
			chID:    BlocksyncChannel,
			peer:    "any",
			bytesFn: blockRequestBytes,
		},
		{
			name:    "ignores other channels",
			setup:   newFilterReactor,
			chID:    byte(0x20),
			peer:    "any",
			bytesFn: blockResponseBytes,
		},
		{
			name:    "ignores empty bytes",
			setup:   newFilterReactor,
			chID:    BlocksyncChannel,
			peer:    "any",
			bytesFn: func(*testing.T) []byte { return nil },
		},
		{
			name: "rejects BlockResponse after pool stopped",
			setup: func(t *testing.T) *Reactor {
				r := newFilterReactor(t)
				seedRequester(r, 1, expected)
				require.NoError(t, r.pool.Stop())
				return r
			},
			chID:      BlocksyncChannel,
			peer:      expected,
			bytesFn:   blockResponseBytes,
			expectErr: "blocksync not active",
		},
		{
			name: "allows BlockResponse at MaxVotesCount commit signatures",
			setup: func(t *testing.T) *Reactor {
				r := newFilterReactor(t)
				seedRequester(r, 1, expected)
				return r
			},
			chID:    BlocksyncChannel,
			peer:    expected,
			bytesFn: func(t *testing.T) []byte { return blockResponseBytesWithSigs(t, types.MaxVotesCount, 0) },
		},
		{
			name: "rejects BlockResponse exceeding commit signature cap",
			setup: func(t *testing.T) *Reactor {
				r := newFilterReactor(t)
				seedRequester(r, 1, expected)
				return r
			},
			chID:      BlocksyncChannel,
			peer:      expected,
			bytesFn:   func(t *testing.T) []byte { return blockResponseBytesWithSigs(t, types.MaxVotesCount+1, 0) },
			expectErr: "too many commit signatures",
		},
		{
			name: "rejects BlockResponse exceeding extended commit signature cap",
			setup: func(t *testing.T) *Reactor {
				r := newFilterReactor(t)
				seedRequester(r, 1, expected)
				return r
			},
			chID:      BlocksyncChannel,
			peer:      expected,
			bytesFn:   func(t *testing.T) []byte { return blockResponseBytesWithSigs(t, 0, types.MaxVotesCount+1) },
			expectErr: "too many extended commit signatures",
		},
		{
			name: "rejects BlockResponse splitting signatures across duplicate Block fields",
			setup: func(t *testing.T) *Reactor {
				r := newFilterReactor(t)
				seedRequester(r, 1, expected)
				return r
			},
			chID: BlocksyncChannel,
			peer: expected,
			bytesFn: func(t *testing.T) []byte {
				half := types.MaxVotesCount/2 + 1 // 2*half > MaxVotesCount
				a := blockResponseBytesWithSigs(t, half, 0)
				b := blockResponseBytesWithSigs(t, half, 0)
				return append(append([]byte{}, a...), b...)
			},
			expectErr: "too many commit signatures",
		},
		{
			name: "rejects BlockResponse when first byte is not BlockResponse proto tag",
			setup: func(t *testing.T) *Reactor {
				r := newFilterReactor(t)
				seedRequester(r, 1, expected)
				return r
			},
			chID: BlocksyncChannel,
			peer: expected,
			bytesFn: func(t *testing.T) []byte {
				// Prepend an empty BlockRequest field (tag 0x0a, len 0)
				// so msgBytes[0] != BlockResponse oneof tag, then append
				// a real BlockResponse payload that exceeds the cap.
				oversized := blockResponseBytesWithSigs(t, types.MaxVotesCount+1, 0)
				return append([]byte{0x0a, 0x00}, oversized...)
			},
			expectErr: "too many commit signatures",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := tc.setup(t)
			err := r.FilterMsgBytes(tc.chID, mockPeer(tc.peer), tc.bytesFn(t))
			if tc.expectErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.expectErr)
		})
	}
}

func TestStubUnmarshalAllocs(t *testing.T) {
	tests := []struct {
		name          string
		numCommits    int
		numExtCommits int
	}{
		{"10k commit sigs", 10_000, 0},
		{"100k commit sigs", 100_000, 0},
		{"1m commit sigs", 1_000_000, 0},
		{"10k ext commit sigs", 0, 10_000},
		{"100k ext commit sigs", 0, 100_000},
		{"1m ext commit sigs", 0, 1_000_000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload := blockResponseBytesWithSigs(t, tt.numCommits, tt.numExtCommits)
			allocs := testing.AllocsPerRun(20, func() {
				var stub bcproto.SigCountMessage
				require.NoError(t, stub.Unmarshal(payload))
				require.Len(t, stub.BlockResponse.Block.LastCommit.Signatures, tt.numCommits)
				require.Len(t, stub.BlockResponse.ExtCommit.ExtendedSignatures, tt.numExtCommits)
			})
			const maxAllocs = 50
			require.LessOrEqualf(t, int(allocs), maxAllocs, "unmarshal allocated %d times, more than max allowed", int(allocs), maxAllocs)
		})
	}
}

func blockResponseBytesWithSigs(t *testing.T, commitSigs, extSigs int) []byte {
	t.Helper()
	commit := &cmtproto.Commit{Signatures: make([]cmtproto.CommitSig, commitSigs)}
	for i := range commit.Signatures {
		commit.Signatures[i] = cmtproto.CommitSig{BlockIdFlag: cmtproto.BlockIDFlagAbsent}
	}

	ext := &cmtproto.ExtendedCommit{ExtendedSignatures: make([]cmtproto.ExtendedCommitSig, extSigs)}
	for i := range ext.ExtendedSignatures {
		ext.ExtendedSignatures[i] = cmtproto.ExtendedCommitSig{BlockIdFlag: cmtproto.BlockIDFlagAbsent}
	}

	msg := &bcproto.Message{
		Sum: &bcproto.Message_BlockResponse{
			BlockResponse: &bcproto.BlockResponse{
				Block:     &cmtproto.Block{LastCommit: commit},
				ExtCommit: ext,
			},
		},
	}

	payload, err := proto.Marshal(msg)
	require.NoError(t, err)
	return payload
}

// TestCheckExtendedCommit and ByzantineReactor are intentionally omitted; see ExtendedCommitNetworkHelper note above.
