package stategen

import (
	"testing"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/core/blocks"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/db/kv"
	testDB "github.com/OffchainLabs/prysm/v7/beacon-chain/db/testing"
	doublylinkedtree "github.com/OffchainLabs/prysm/v7/beacon-chain/forkchoice/doubly-linked-tree"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/state"
	"github.com/OffchainLabs/prysm/v7/cmd/beacon-chain/flags"
	"github.com/OffchainLabs/prysm/v7/config/features"
	"github.com/OffchainLabs/prysm/v7/config/params"
	consensusblocks "github.com/OffchainLabs/prysm/v7/consensus-types/blocks"
	"github.com/OffchainLabs/prysm/v7/consensus-types/primitives"
	"github.com/OffchainLabs/prysm/v7/crypto/bls"
	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	"github.com/OffchainLabs/prysm/v7/testing/assert"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	"github.com/OffchainLabs/prysm/v7/testing/util"
	logTest "github.com/sirupsen/logrus/hooks/test"
)

func TestMigrateToCold_CanSaveFinalizedInfo(t *testing.T) {
	ctx := t.Context()
	beaconDB := testDB.SetupDB(t)
	service := New(beaconDB, doublylinkedtree.New())
	beaconState, _ := util.DeterministicGenesisState(t, 32)
	b := util.NewBeaconBlock()
	b.Block.Slot = 1
	br, err := b.Block.HashTreeRoot()
	require.NoError(t, err)
	util.SaveBlock(t, ctx, service.beaconDB, b)
	require.NoError(t, service.epochBoundaryStateCache.put(br, beaconState))
	require.NoError(t, service.MigrateToCold(ctx, br))

	wanted := &finalizedInfo{state: beaconState, root: br}
	assert.DeepEqual(t, wanted.root, service.finalizedInfo.root)
	assert.Equal(t, primitives.Slot(1), service.migratedSlot)
	expectedHTR, err := wanted.state.HashTreeRoot(ctx)
	require.NoError(t, err)
	actualHTR, err := service.finalizedInfo.state.HashTreeRoot(ctx)
	require.NoError(t, err)
	assert.DeepEqual(t, expectedHTR, actualHTR)
}

func TestMigrateToCold_HappyPath(t *testing.T) {
	hook := logTest.NewGlobal()
	ctx := t.Context()
	beaconDB := testDB.SetupDB(t)

	service := New(beaconDB, doublylinkedtree.New())
	service.slotsPerArchivedPoint = 1
	beaconState, _ := util.DeterministicGenesisState(t, 32)
	stateSlot := primitives.Slot(1)
	require.NoError(t, beaconState.SetSlot(stateSlot))
	b := util.NewBeaconBlock()
	b.Block.Slot = 2
	fRoot, err := b.Block.HashTreeRoot()
	require.NoError(t, err)
	util.SaveBlock(t, ctx, service.beaconDB, b)
	require.NoError(t, service.epochBoundaryStateCache.put(fRoot, beaconState))
	require.NoError(t, service.MigrateToCold(ctx, fRoot))

	gotState, err := service.beaconDB.State(ctx, fRoot)
	require.NoError(t, err)
	assert.DeepSSZEqual(t, beaconState.ToProtoUnsafe(), gotState.ToProtoUnsafe(), "Did not save state")
	gotRoot := service.beaconDB.ArchivedPointRoot(ctx, stateSlot/service.slotsPerArchivedPoint)
	assert.Equal(t, fRoot, gotRoot, "Did not save archived root")
	lastIndex, err := service.beaconDB.LastArchivedSlot(ctx)
	require.NoError(t, err)
	assert.Equal(t, primitives.Slot(1), lastIndex, "Did not save last archived index")

	require.LogsContain(t, hook, "Saved state in DB")
}

func TestMigrateToCold_RegeneratePath_IgnoresOrphan(t *testing.T) {
	hook := logTest.NewGlobal()
	ctx := t.Context()
	beaconDB := testDB.SetupDB(t)

	service := New(beaconDB, doublylinkedtree.New())
	service.slotsPerArchivedPoint = 1
	beaconState, pks := util.DeterministicGenesisState(t, 32)
	genesisStateRoot, err := beaconState.HashTreeRoot(ctx)
	require.NoError(t, err)
	genesis := blocks.NewGenesisBlock(genesisStateRoot[:])
	util.SaveBlock(t, ctx, beaconDB, genesis)
	gRoot, err := genesis.Block.HashTreeRoot()
	require.NoError(t, err)
	assert.NoError(t, beaconDB.SaveState(ctx, beaconState, gRoot))
	assert.NoError(t, beaconDB.SaveGenesisBlockRoot(ctx, gRoot))

	// Add an orphaned block at slot 1.
	orphanConfig := util.DefaultBlockGenConfig()
	orphanConfig.NumAttestations = 0
	orphan, err := util.GenerateFullBlock(beaconState, pks, orphanConfig, 1)
	require.NoError(t, err)
	orphanRoot, err := orphan.Block.HashTreeRoot()
	require.NoError(t, err)
	util.SaveBlock(t, ctx, service.beaconDB, orphan)
	require.NoError(t, service.beaconDB.SaveStateSummary(ctx, &ethpb.StateSummary{Slot: 1, Root: orphanRoot[:]}))

	b1, err := util.GenerateFullBlock(beaconState, pks, util.DefaultBlockGenConfig(), 1)
	require.NoError(t, err)
	wB1, err := consensusblocks.NewSignedBeaconBlock(b1)
	require.NoError(t, err)
	state1, err := executeStateTransitionStateGen(ctx, beaconState.Copy(), wB1)
	require.NoError(t, err)
	r1, err := b1.Block.HashTreeRoot()
	require.NoError(t, err)
	util.SaveBlock(t, ctx, service.beaconDB, b1)
	require.NoError(t, service.beaconDB.SaveStateSummary(ctx, &ethpb.StateSummary{Slot: 1, Root: r1[:]}))

	b64, err := util.GenerateFullBlock(state1, pks, util.DefaultBlockGenConfig(), 64)
	require.NoError(t, err)
	r64, err := b64.Block.HashTreeRoot()
	require.NoError(t, err)
	util.SaveBlock(t, ctx, service.beaconDB, b64)
	require.NoError(t, service.beaconDB.SaveStateSummary(ctx, &ethpb.StateSummary{Slot: 64, Root: r64[:]}))
	require.NoError(t, service.beaconDB.SaveFinalizedCheckpoint(ctx, &ethpb.Checkpoint{Epoch: 2, Root: r64[:]}))
	service.finalizedInfo = &finalizedInfo{
		root:  gRoot,
		state: beaconState,
	}

	require.NoError(t, service.MigrateToCold(ctx, r64))

	s1, err := service.beaconDB.State(ctx, r1)
	require.NoError(t, err)
	assert.Equal(t, s1.Slot(), primitives.Slot(1), "Did not save state")
	gotRoot := service.beaconDB.ArchivedPointRoot(ctx, 1/service.slotsPerArchivedPoint)
	assert.Equal(t, r1, gotRoot, "Did not save archived root")
	lastIndex, err := service.beaconDB.LastArchivedSlot(ctx)
	require.NoError(t, err)
	assert.Equal(t, primitives.Slot(1), lastIndex, "Did not save last archived index")
	assert.Equal(t, false, service.beaconDB.HasState(ctx, orphanRoot), "Saved state for orphaned block")

	require.LogsContain(t, hook, "Saved state in DB")
}

func TestMigrateToCold_StateExistsInDB(t *testing.T) {
	hook := logTest.NewGlobal()
	ctx := t.Context()
	beaconDB := testDB.SetupDB(t)

	service := New(beaconDB, doublylinkedtree.New())
	service.slotsPerArchivedPoint = 1
	beaconState, _ := util.DeterministicGenesisState(t, 32)
	stateSlot := primitives.Slot(1)
	require.NoError(t, beaconState.SetSlot(stateSlot))
	b := util.NewBeaconBlock()
	b.Block.Slot = 2
	fRoot, err := b.Block.HashTreeRoot()
	require.NoError(t, err)
	util.SaveBlock(t, ctx, service.beaconDB, b)
	require.NoError(t, service.epochBoundaryStateCache.put(fRoot, beaconState))
	require.NoError(t, service.beaconDB.SaveState(ctx, beaconState, fRoot))

	service.saveHotStateDB.blockRootsOfSavedStates = [][32]byte{{1}, {2}, {3}, {4}, fRoot}
	require.NoError(t, service.MigrateToCold(ctx, fRoot))
	assert.DeepEqual(t, [][32]byte{{1}, {2}, {3}, {4}}, service.saveHotStateDB.blockRootsOfSavedStates)
	assert.LogsDoNotContain(t, hook, "Saved state in DB")
}

func TestMigrateToCold_ParallelCalls(t *testing.T) {
	hook := logTest.NewGlobal()
	ctx := t.Context()
	beaconDB := testDB.SetupDB(t)

	service := New(beaconDB, doublylinkedtree.New())
	service.slotsPerArchivedPoint = 1
	beaconState, pks := util.DeterministicGenesisState(t, 32)
	genState := beaconState.Copy()
	genesisStateRoot, err := beaconState.HashTreeRoot(ctx)
	require.NoError(t, err)
	genesis := blocks.NewGenesisBlock(genesisStateRoot[:])
	util.SaveBlock(t, ctx, beaconDB, genesis)
	gRoot, err := genesis.Block.HashTreeRoot()
	require.NoError(t, err)
	assert.NoError(t, beaconDB.SaveState(ctx, beaconState, gRoot))
	assert.NoError(t, beaconDB.SaveGenesisBlockRoot(ctx, gRoot))

	b1, err := util.GenerateFullBlock(beaconState, pks, util.DefaultBlockGenConfig(), 1)
	require.NoError(t, err)
	wB1, err := consensusblocks.NewSignedBeaconBlock(b1)
	require.NoError(t, err)
	beaconState, err = executeStateTransitionStateGen(ctx, beaconState, wB1)
	assert.NoError(t, err)
	r1, err := b1.Block.HashTreeRoot()
	require.NoError(t, err)
	util.SaveBlock(t, ctx, service.beaconDB, b1)
	require.NoError(t, service.beaconDB.SaveStateSummary(ctx, &ethpb.StateSummary{Slot: 1, Root: r1[:]}))

	b4, err := util.GenerateFullBlock(beaconState, pks, util.DefaultBlockGenConfig(), 4)
	require.NoError(t, err)
	wB4, err := consensusblocks.NewSignedBeaconBlock(b4)
	require.NoError(t, err)
	beaconState, err = executeStateTransitionStateGen(ctx, beaconState, wB4)
	assert.NoError(t, err)
	r4, err := b4.Block.HashTreeRoot()
	require.NoError(t, err)
	util.SaveBlock(t, ctx, service.beaconDB, b4)
	require.NoError(t, service.beaconDB.SaveStateSummary(ctx, &ethpb.StateSummary{Slot: 4, Root: r4[:]}))

	b7, err := util.GenerateFullBlock(beaconState, pks, util.DefaultBlockGenConfig(), 7)
	require.NoError(t, err)
	wB7, err := consensusblocks.NewSignedBeaconBlock(b7)
	require.NoError(t, err)
	_, err = executeStateTransitionStateGen(ctx, beaconState, wB7)
	assert.NoError(t, err)
	r7, err := b7.Block.HashTreeRoot()
	require.NoError(t, err)
	util.SaveBlock(t, ctx, service.beaconDB, b7)
	require.NoError(t, service.beaconDB.SaveStateSummary(ctx, &ethpb.StateSummary{Slot: 7, Root: r7[:]}))
	require.NoError(t, service.beaconDB.SaveFinalizedCheckpoint(ctx, &ethpb.Checkpoint{Root: r7[:]}))

	service.finalizedInfo = &finalizedInfo{
		root:  genesisStateRoot,
		state: genState,
	}
	service.saveHotStateDB.blockRootsOfSavedStates = [][32]byte{r1, r4, r7}

	// Run the migration routines concurrently for 2 different finalized roots.
	go func() {
		require.NoError(t, service.MigrateToCold(ctx, r4))
	}()

	require.NoError(t, service.MigrateToCold(ctx, r7))

	s1, err := service.beaconDB.State(ctx, r1)
	require.NoError(t, err)
	assert.Equal(t, s1.Slot(), primitives.Slot(1), "Did not save state")
	s4, err := service.beaconDB.State(ctx, r4)
	require.NoError(t, err)
	assert.Equal(t, s4.Slot(), primitives.Slot(4), "Did not save state")

	gotRoot := service.beaconDB.ArchivedPointRoot(ctx, 1/service.slotsPerArchivedPoint)
	assert.Equal(t, r1, gotRoot, "Did not save archived root")
	gotRoot = service.beaconDB.ArchivedPointRoot(ctx, 4)
	assert.Equal(t, r4, gotRoot, "Did not save archived root")
	lastIndex, err := service.beaconDB.LastArchivedSlot(ctx)
	require.NoError(t, err)
	assert.Equal(t, primitives.Slot(4), lastIndex, "Did not save last archived index")
	assert.DeepEqual(t, [][32]byte{r7}, service.saveHotStateDB.blockRootsOfSavedStates, "Did not remove all saved hot state roots")
	require.LogsContain(t, hook, "Saved state in DB")
}

// =========================================================================
// Tests for migrateToColdHdiff (state diff migration)
// =========================================================================

// setStateDiffExponents sets state diff exponents for testing.
// Uses exponents [6, 5] which means:
// - Level 0: Every 2^6 = 64 slots (full snapshot)
// - Level 1: Every 2^5 = 32 slots (diff)
func setStateDiffExponents() {
	globalFlags := flags.GlobalFlags{
		StateDiffExponents: []int{6, 5},
	}
	flags.Init(&globalFlags)
}

// TestMigrateToColdHdiff_CanUpdateFinalizedInfo verifies that the migration
// correctly updates finalized info when migrating to slots not in the diff tree.
func TestMigrateToColdHdiff_CanUpdateFinalizedInfo(t *testing.T) {
	ctx := t.Context()
	// Set exponents and create DB first (without EnableStateDiff flag).
	setStateDiffExponents()
	beaconDB := testDB.SetupDB(t)
	// Initialize the state diff cache via the method on *kv.Store (not in interface).
	require.NoError(t, beaconDB.(*kv.Store).InitStateDiffCacheForTesting(t, 0))
	// Now enable the feature flag.
	resetCfg := features.InitWithReset(&features.Flags{EnableStateDiff: true})
	defer resetCfg()
	service := New(beaconDB, doublylinkedtree.New())

	beaconState, _ := util.DeterministicGenesisState(t, 32)
	genesisStateRoot, err := beaconState.HashTreeRoot(ctx)
	require.NoError(t, err)
	genesis := blocks.NewGenesisBlock(genesisStateRoot[:])
	util.SaveBlock(t, ctx, beaconDB, genesis)
	gRoot, err := genesis.Block.HashTreeRoot()
	require.NoError(t, err)
	require.NoError(t, beaconDB.SaveGenesisBlockRoot(ctx, gRoot))

	// Put genesis state in epoch boundary cache so migrateToColdHdiff doesn't need to retrieve from DB.
	require.NoError(t, service.epochBoundaryStateCache.put(gRoot, beaconState))

	// Set initial finalized info at genesis.
	service.finalizedInfo = &finalizedInfo{
		root:  gRoot,
		state: beaconState,
	}

	// Create finalized block at slot 10 (not in diff tree, so no intermediate states saved).
	finalizedState := beaconState.Copy()
	require.NoError(t, finalizedState.SetSlot(10))
	b := util.NewBeaconBlock()
	b.Block.Slot = 10
	fRoot, err := b.Block.HashTreeRoot()
	require.NoError(t, err)
	util.SaveBlock(t, ctx, beaconDB, b)
	require.NoError(t, service.epochBoundaryStateCache.put(fRoot, finalizedState))

	require.NoError(t, service.MigrateToCold(ctx, fRoot))

	// Verify finalized info is updated.
	assert.Equal(t, primitives.Slot(10), service.migratedSlot)
	assert.DeepEqual(t, fRoot, service.finalizedInfo.root)
	expectedHTR, err := finalizedState.HashTreeRoot(ctx)
	require.NoError(t, err)
	actualHTR, err := service.finalizedInfo.state.HashTreeRoot(ctx)
	require.NoError(t, err)
	assert.DeepEqual(t, expectedHTR, actualHTR)
}

// TestMigrateToColdHdiff_SkipsSlotsNotInDiffTree verifies that the migration
// skips slots that are not in the diff tree.
func TestMigrateToColdHdiff_SkipsSlotsNotInDiffTree(t *testing.T) {
	hook := logTest.NewGlobal()
	ctx := t.Context()
	// Set exponents and create DB first (without EnableStateDiff flag).
	setStateDiffExponents()
	beaconDB := testDB.SetupDB(t)
	// Initialize the state diff cache via the method on *kv.Store (not in interface).
	require.NoError(t, beaconDB.(*kv.Store).InitStateDiffCacheForTesting(t, 0))
	// Now enable the feature flag.
	resetCfg := features.InitWithReset(&features.Flags{EnableStateDiff: true})
	defer resetCfg()
	service := New(beaconDB, doublylinkedtree.New())

	beaconState, pks := util.DeterministicGenesisState(t, 32)
	genesisStateRoot, err := beaconState.HashTreeRoot(ctx)
	require.NoError(t, err)
	genesis := blocks.NewGenesisBlock(genesisStateRoot[:])
	util.SaveBlock(t, ctx, beaconDB, genesis)
	gRoot, err := genesis.Block.HashTreeRoot()
	require.NoError(t, err)
	require.NoError(t, beaconDB.SaveGenesisBlockRoot(ctx, gRoot))

	// Start from slot 1 to avoid slot 0 which is in the diff tree.
	service.finalizedInfo = &finalizedInfo{
		root:  gRoot,
		state: beaconState,
	}
	service.migratedSlot = 1

	// Reset the log hook to ignore setup logs.
	hook.Reset()

	// Create a block at slot 20 (NOT in diff tree with exponents [6,5]).
	b20, err := util.GenerateFullBlock(beaconState, pks, util.DefaultBlockGenConfig(), 20)
	require.NoError(t, err)
	r20, err := b20.Block.HashTreeRoot()
	require.NoError(t, err)
	util.SaveBlock(t, ctx, beaconDB, b20)
	require.NoError(t, beaconDB.SaveStateSummary(ctx, &ethpb.StateSummary{Slot: 20, Root: r20[:]}))

	// Put finalized state in cache.
	finalizedState := beaconState.Copy()
	require.NoError(t, finalizedState.SetSlot(20))
	require.NoError(t, service.epochBoundaryStateCache.put(r20, finalizedState))

	require.NoError(t, service.MigrateToCold(ctx, r20))

	// Verify NO states were saved during migration (slots 1-19 are not in diff tree).
	assert.LogsDoNotContain(t, hook, "Saved state in DB")
}

// TestMigrateToColdHdiff_MissedNonBoundarySlots verifies migration
// when all non-boundary slots are missed.
func TestMigrateToColdHdiff_MissedNonBoundarySlots(t *testing.T) {
	ctx := t.Context()
	chain := setupHdiffMigrationTestChain(t, 32, 64, 96, 128)
	service := New(chain.db, doublylinkedtree.New())
	service.finalizedInfo = &finalizedInfo{root: chain.roots[0], state: chain.states[0]}
	for _, slot := range []primitives.Slot{32, 64, 96, 128} {
		require.NoError(t, service.epochBoundaryStateCache.put(chain.roots[slot], chain.states[slot]))
	}
	chain.finalize(t, 128)

	require.NoError(t, service.MigrateToCold(ctx, chain.roots[128]))

	for _, slot := range []primitives.Slot{32, 64, 96} {
		got, err := chain.db.State(ctx, chain.roots[slot])
		require.NoError(t, err)
		wantRoot, err := chain.states[slot].HashTreeRoot(ctx)
		require.NoError(t, err)
		gotRoot, err := got.HashTreeRoot(ctx)
		require.NoError(t, err)
		assert.Equal(t, wantRoot, gotRoot, "state root mismatch")
	}
}

// TestMigrateToColdHdiff_MissedNonBoundarySlots_BoundaryCacheMissed simulates a
// restart after the boundary cache has been lost.
func TestMigrateToColdHdiff_MissedNonBoundarySlots_BoundaryCacheMissed(t *testing.T) {
	ctx := t.Context()
	chain := setupHdiffMigrationTestChain(t, 32, 64, 96, 128)
	service := New(chain.db, doublylinkedtree.New())
	service.finalizedInfo = &finalizedInfo{root: chain.roots[0], state: chain.states[0]}
	chain.finalize(t, 128)

	require.NoError(t, service.MigrateToCold(ctx, chain.roots[128]))

	for _, slot := range []primitives.Slot{32, 64, 96} {
		got, err := chain.db.State(ctx, chain.roots[slot])
		require.NoError(t, err)
		wantRoot, err := chain.states[slot].HashTreeRoot(ctx)
		require.NoError(t, err)
		gotRoot, err := got.HashTreeRoot(ctx)
		require.NoError(t, err)
		assert.Equal(t, wantRoot, gotRoot, "state root mismatch")
	}
	assert.Equal(t, primitives.Slot(128), service.migratedSlot)
	assert.DeepEqual(t, chain.roots[0], service.finalizedInfo.root)
	assert.Equal(t, primitives.Slot(0), service.finalizedInfo.state.Slot())
}

// TestMigrateToColdHdiff_BoundaryCacheMiss_SelectsCanonicalRoot verifies that
// the finalized block index selects the canonical root when the slot index also has an orphan.
func TestMigrateToColdHdiff_BoundaryCacheMiss_SelectsCanonicalRoot(t *testing.T) {
	ctx := t.Context()
	chain := setupHdiffMigrationTestChain(t, 32, 64, 96, 128)

	// Add an orphaned block at slot 96.
	orphanConfig := util.DefaultBlockGenConfig()
	orphanConfig.NumAttestations = 0
	orphanRoot, _ := chain.addBlock(t, chain.states[64], orphanConfig, 96)
	service := New(chain.db, doublylinkedtree.New())
	service.finalizedInfo = &finalizedInfo{root: chain.roots[0], state: chain.states[0]}

	// Simulate cache eviction for slot 96 only: keep 32/64 and finalized 128.
	for _, slot := range []primitives.Slot{32, 64, 128} {
		require.NoError(t, service.epochBoundaryStateCache.put(chain.roots[slot], chain.states[slot]))
	}

	chain.finalize(t, 128)
	assert.Equal(t, true, chain.db.IsFinalizedBlock(ctx, chain.roots[96]), "Canonical root was not finalized")
	assert.Equal(t, false, chain.db.IsFinalizedBlock(ctx, orphanRoot), "Orphan root was finalized")

	require.NoError(t, service.MigrateToCold(ctx, chain.roots[128]))

	// State by the slot-96 root should remain reconstructible after migration.
	got96, err := chain.db.State(ctx, chain.roots[96])
	require.NoError(t, err)
	wantRoot, err := chain.states[96].HashTreeRoot(ctx)
	require.NoError(t, err)
	gotRoot, err := got96.HashTreeRoot(ctx)
	require.NoError(t, err)
	assert.Equal(t, wantRoot, gotRoot, "slot 96 state root mismatch")
	_, err = chain.db.State(ctx, orphanRoot)
	require.ErrorContains(t, "state root mismatch", err)
}

func TestMigrateToColdHdiff_OrphanOnlyBoundaryUsesFinalizedAncestry(t *testing.T) {
	ctx := t.Context()
	chain := setupHdiffMigrationTestChain(t, 32, 64, 128)
	orphanConfig := util.DefaultBlockGenConfig()
	orphanConfig.NumAttestations = 0
	orphanRoot, _ := chain.addBlock(t, chain.states[64], orphanConfig, 96)
	service := New(chain.db, doublylinkedtree.New())
	service.finalizedInfo = &finalizedInfo{root: chain.roots[0], state: chain.states[0]}
	chain.finalize(t, 128)

	require.NoError(t, service.MigrateToCold(ctx, chain.roots[128]))

	// There is no canonical root at slot 96 in this fixture, so the root-keyed
	// read can only exclude the orphan. The preceding test positively reads its
	// canonical slot-96 root.
	_, err := chain.db.State(ctx, orphanRoot)
	require.ErrorContains(t, "state root mismatch", err)
	assert.Equal(t, primitives.Slot(128), service.migratedSlot)
	assert.DeepEqual(t, chain.roots[0], service.finalizedInfo.root)
	assert.Equal(t, primitives.Slot(0), service.finalizedInfo.state.Slot())
}

// TestMigrateToColdHdiff_NoOpWhenFinalizedSlotNotAdvanced verifies that
// migration is a no-op when the finalized slot has not advanced.
func TestMigrateToColdHdiff_NoOpWhenFinalizedSlotNotAdvanced(t *testing.T) {
	ctx := t.Context()
	// Set exponents and create DB first (without EnableStateDiff flag).
	setStateDiffExponents()
	beaconDB := testDB.SetupDB(t)
	// Initialize the state diff cache via the method on *kv.Store (not in interface).
	require.NoError(t, beaconDB.(*kv.Store).InitStateDiffCacheForTesting(t, 0))
	// Now enable the feature flag.
	resetCfg := features.InitWithReset(&features.Flags{EnableStateDiff: true})
	defer resetCfg()
	service := New(beaconDB, doublylinkedtree.New())

	beaconState, _ := util.DeterministicGenesisState(t, 32)
	genesisStateRoot, err := beaconState.HashTreeRoot(ctx)
	require.NoError(t, err)
	genesis := blocks.NewGenesisBlock(genesisStateRoot[:])
	util.SaveBlock(t, ctx, beaconDB, genesis)
	gRoot, err := genesis.Block.HashTreeRoot()
	require.NoError(t, err)
	require.NoError(t, beaconDB.SaveGenesisBlockRoot(ctx, gRoot))

	// Set finalized info already at slot 50.
	finalizedState := beaconState.Copy()
	require.NoError(t, finalizedState.SetSlot(50))
	service.finalizedInfo = &finalizedInfo{
		root:  gRoot,
		state: finalizedState,
	}
	service.migratedSlot = 50

	// Create block at same slot 50.
	b := util.NewBeaconBlock()
	b.Block.Slot = 50
	fRoot, err := b.Block.HashTreeRoot()
	require.NoError(t, err)
	util.SaveBlock(t, ctx, beaconDB, b)
	require.NoError(t, service.epochBoundaryStateCache.put(fRoot, finalizedState))

	// Migration should be a no-op (finalized slot not advancing).
	require.NoError(t, service.MigrateToCold(ctx, fRoot))
}

// hdiffMigrationTestChain is a helper struct for setting up a test chain
// with finalized blocks and states for testing state diff migration.
type hdiffMigrationTestChain struct {
	db     *kv.Store
	keys   []bls.SecretKey
	roots  map[primitives.Slot][32]byte
	states map[primitives.Slot]state.BeaconState
}

func setupHdiffMigrationTestChain(t *testing.T, slots ...primitives.Slot) *hdiffMigrationTestChain {
	t.Helper()
	ctx := t.Context()
	setStateDiffExponents()
	db := testDB.SetupDB(t).(*kv.Store)
	require.NoError(t, db.InitStateDiffCacheForTesting(t, 0))
	t.Cleanup(features.InitWithReset(&features.Flags{EnableStateDiff: true}))

	genesisState, keys := util.DeterministicGenesisState(t, 32)
	genesisStateRoot, err := genesisState.HashTreeRoot(ctx)
	require.NoError(t, err)
	genesis := blocks.NewGenesisBlock(genesisStateRoot[:])
	util.SaveBlock(t, ctx, db, genesis)
	genesisRoot, err := genesis.Block.HashTreeRoot()
	require.NoError(t, err)
	require.NoError(t, db.SaveGenesisBlockRoot(ctx, genesisRoot))
	require.NoError(t, db.SaveState(ctx, genesisState, genesisRoot))
	require.NoError(t, db.SaveStateSummary(ctx, &ethpb.StateSummary{Slot: 0, Root: genesisRoot[:]}))

	chain := &hdiffMigrationTestChain{
		db:     db,
		keys:   keys,
		roots:  map[primitives.Slot][32]byte{0: genesisRoot},
		states: map[primitives.Slot]state.BeaconState{0: genesisState},
	}
	current := genesisState
	for _, slot := range slots {
		root, nextState := chain.addBlock(t, current, util.DefaultBlockGenConfig(), slot)
		chain.roots[slot] = root
		chain.states[slot] = nextState
		current = nextState
	}
	return chain
}

func (chain *hdiffMigrationTestChain) finalize(t *testing.T, slot primitives.Slot) {
	t.Helper()
	root := chain.roots[slot]
	require.NoError(t, chain.db.SaveFinalizedCheckpoint(t.Context(), &ethpb.Checkpoint{
		Epoch: primitives.Epoch(slot / params.BeaconConfig().SlotsPerEpoch),
		Root:  root[:],
	}))
}

func (chain *hdiffMigrationTestChain) addBlock(
	t *testing.T,
	preState state.BeaconState,
	config *util.BlockGenConfig,
	slot primitives.Slot,
) ([32]byte, state.BeaconState) {
	t.Helper()
	block, err := util.GenerateFullBlock(preState, chain.keys, config, slot)
	require.NoError(t, err)
	signed, err := consensusblocks.NewSignedBeaconBlock(block)
	require.NoError(t, err)
	postState, err := executeStateTransitionStateGen(t.Context(), preState.Copy(), signed)
	require.NoError(t, err)
	root, err := block.Block.HashTreeRoot()
	require.NoError(t, err)
	util.SaveBlock(t, t.Context(), chain.db, block)
	require.NoError(t, chain.db.SaveStateSummary(t.Context(), &ethpb.StateSummary{Slot: slot, Root: root[:]}))
	return root, postState.Copy()
}
