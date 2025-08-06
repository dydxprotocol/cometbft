package types

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReducedProposerSet(t *testing.T) {
	vals := []*Validator{
		newValidator([]byte("val1"), 100, false),
		newValidator([]byte("val2"), 200, true),
		newValidator([]byte("val3"), 300, false),
		newValidator([]byte("val4"), 400, true),
	}
	vset := NewValidatorSet(vals)

	// Initial proposer should have ProposeDisabled=false
	assert.False(t, vset.GetProposer().ProposeDisabled)

	// Track proposers over 100 rounds
	proposers := make(map[string]int)
	for i := 0; i < 100; i++ {
		vset.IncrementProposerPriority(1)
		p := vset.GetProposer()
		assert.False(t, p.ProposeDisabled)
		proposers[string(p.Address)]++
	}

	// Verify that only val1 and val3 are proposers and val3 is selected more
	// often due to having higher voting power
	assert.Equal(t, 2, len(proposers))
	assert.Greater(t, proposers[string([]byte("val3"))], proposers[string([]byte("val1"))])
}

func TestSingleProposer(t *testing.T) {
	vals := []*Validator{
		newValidator([]byte("val1"), 100, true),
		newValidator([]byte("val2"), 200, false), // Only proposer
		newValidator([]byte("val3"), 300, true),
	}
	vset := NewValidatorSet(vals)

	// val2 should always be the proposer
	for i := 0; i < 10; i++ {
		vset.IncrementProposerPriority(1)
		assert.Equal(t, string([]byte("val2")), string(vset.GetProposer().Address))
	}
}

func TestNoProposers(t *testing.T) {
	vals := []*Validator{
		newValidator([]byte("val1"), 100, true),
		newValidator([]byte("val2"), 200, true),
	}

	// Panics when no validators can propose
	assert.Panics(t, func() {
		NewValidatorSet(vals)
	})
}

func TestProposerPriorityIncrementForAll(t *testing.T) {
	vals := []*Validator{
		newValidator([]byte("val1"), 100, false),
		newValidator([]byte("val2"), 100, true),
		newValidator([]byte("val3"), 100, false),
	}
	vset := NewValidatorSet(vals)

	// Store initial priorities
	initial := make([]int64, len(vals))
	for i, v := range vset.Validators {
		initial[i] = v.ProposerPriority
	}

	// Increment 10 times
	for i := 0; i < 10; i++ {
		vset.IncrementProposerPriority(1)
	}

	// Priorities should change for all validators (not just proposers)
	for i, v := range vset.Validators {
		assert.NotEqual(t, initial[i], v.ProposerPriority)
	}
}

func TestCopyProposeDisabled(t *testing.T) {
	vals := []*Validator{
		newValidator([]byte("val1"), 100, true),
		newValidator([]byte("val2"), 200, false),
		newValidator([]byte("val3"), 300, true),
	}
	vset := NewValidatorSet(vals)

	// Make a copy
	vsetCopy := vset.Copy()

	// Verify all validators maintain their ProposeDisabled status
	for i, v := range vset.Validators {
		assert.Equal(t, v.ProposeDisabled, vsetCopy.Validators[i].ProposeDisabled)
	}
}

// As all validators accumulate proposer priority but only proposers get their priority deducted,
// those who aren't part of the proposer set will have accumulated priority that's much higher than
// those who are in the proposer set and can impact proposer selection once they join the proposer set.
//
// Below tests demonstrate that the system quickly resolves this imbalance and behaves as normal.
// Note: can be run with verbose logging to see how priorities change each round.
//
// Mathematically, the max diff of priorities in a proposer set is bounded to 2 * TotalVotingPower
// and a proposer's priority is reduced by TotalVotingPower each time it's selected. Thus, a newly joined
// proposer can be selected at most 2 times in a row before its priority falls below those who have been
// part of the proposer set. TestAddOneToProposerSet and TestAddManyToProposerSet programmatically verify
// such behavior.
func TestAddOneToProposerSet(t *testing.T) {
	// Set up
	// - proposer set: val1
	// - additional validator: val2
	vals := []*Validator{
		newValidator([]byte("val1"), 100, false),
		newValidator([]byte("val2"), 100, true),
	}
	vset := NewValidatorSet(vals)

	// Let proposer selection run for some rounds
	incrementAndLogProposerPriority(vset, 50, "Before adding val2 to proposer set")

	// Add val2 to proposer set
	findValidator(vset, []byte("val2")).ProposeDisabled = false

	// Future proposer selection should be [val2, val2, val1, val2, val1, ...]
	proposerSequence := incrementAndLogProposerPriority(vset, 100, "\nAfter adding val2 to proposer set")

	assert.Equal(t, "val2", proposerSequence[0])
	assert.Equal(t, "val2", proposerSequence[1])

	for i := 2; i < len(proposerSequence); i++ {
		current := proposerSequence[i]
		prev := proposerSequence[i-1]
		assert.NotEqual(t, current, prev)
	}
}

// Can be run with verbose logging to see how priorities change for different validators.
func TestAddManyToProposerSet(t *testing.T) {
	// Set up
	// - proposer set: val1
	// - additional validators: val2, val3, val4
	vals := []*Validator{
		newValidator([]byte("val1"), 100, false),
		newValidator([]byte("val2"), 100, true),
		newValidator([]byte("val3"), 100, true),
		newValidator([]byte("val4"), 100, true),
	}
	vset := NewValidatorSet(vals)

	// Let proposer selection run for some rounds
	incrementAndLogProposerPriority(vset, 50, "Before adding val2, val3, val4 to proposer set")

	// Add val2, val3, val4 to proposer set
	findValidator(vset, []byte("val2")).ProposeDisabled = false
	findValidator(vset, []byte("val3")).ProposeDisabled = false
	findValidator(vset, []byte("val4")).ProposeDisabled = false

	// Afterwards,
	// 1. First 6 selections should be round robin among val2, val3, val4
	// 2. Then system is back to normal and rotates among all 4 proposers
	// i.e. [val2, val3, val4, val2, val3, val4, val1, val2, val3, val4, ...]
	proposerSequence := incrementAndLogProposerPriority(vset, 100, "\nAfter adding val2, val3, val4 to proposer set")

	// Verify first 6 proposers
	firstSix := make(map[string]int)
	for i := 0; i < 6; i++ {
		firstSix[proposerSequence[i]]++
	}

	for _, val := range []string{"val2", "val3", "val4"} {
		assert.Equal(t, 2, firstSix[val], "%s should appear exactly twice in first 6", val)
	}

	// Verify subsequent proposers
	for i := 6; i < len(proposerSequence); i++ {
		switch i % 4 {
		case 2:
			assert.Equal(t, "val1", proposerSequence[i])
		case 3:
			assert.Equal(t, "val2", proposerSequence[i])
		case 0:
			assert.Equal(t, "val3", proposerSequence[i])
		case 1:
			assert.Equal(t, "val4", proposerSequence[i])
		}
	}
}

// Note that this test has no assertions and is mainly for manual inspection of how priorities
// change for a proposer set where voting powers are vastly different.
func TestAddManyToProposerSetDifferentPowers(t *testing.T) {
	// Set up
	// - proposer set: val2, val3
	// - additional validators: val1, val4
	vals := []*Validator{
		newValidator([]byte("val1"), 50, true),
		newValidator([]byte("val2"), 500, false),
		newValidator([]byte("val3"), 5000, false),
		newValidator([]byte("val4"), 50000, true),
	}
	vset := NewValidatorSet(vals)

	// Let proposer selection run for some rounds
	incrementAndLogProposerPriority(vset, 50, "Before adding val1, val4 to proposer set")

	// Add val1, val4 to proposer set
	findValidator(vset, []byte("val1")).ProposeDisabled = false
	findValidator(vset, []byte("val4")).ProposeDisabled = false

	// Run for some rounds
	incrementAndLogProposerPriority(vset, 50, "\nAfter adding val1, val4 to proposer set")
}

// findValidator finds a validator by address given a validator set
func findValidator(vset *ValidatorSet, address []byte) *Validator {
	for _, v := range vset.Validators {
		if string(v.Address) == string(address) {
			return v
		}
	}
	return nil
}

// incrementAndLogProposerPriority is a helper function that increments proposer priority and logs validator set state.
// Returns an array of selected proposers by their address strings.
func incrementAndLogProposerPriority(vset *ValidatorSet, rounds int, phase string) []string {
	// Build header
	header := "Round | Selected"
	divider := "------|----------"
	for _, v := range vset.Validators {
		header += fmt.Sprintf(" | %s(%d,%s)", string(v.Address), v.VotingPower,
			map[bool]string{true: "T", false: "F"}[v.ProposeDisabled])
		divider += "|" + strings.Repeat("-", 13)
	}

	fmt.Printf("%s:\n", phase)
	fmt.Printf("%s\n", header)
	fmt.Printf("%s\n", divider)

	proposerSequence := []string{}
	for round := 1; round <= rounds; round++ {
		vset.IncrementProposerPriority(1)
		proposer := vset.GetProposer()
		proposerSequence = append(proposerSequence, string(proposer.Address))

		row := fmt.Sprintf("%5d | %8s", round, string(proposer.Address))
		for _, v := range vset.Validators {
			row += fmt.Sprintf(" | %*d", 11, v.ProposerPriority)
		}
		fmt.Printf("%s\n", row)
	}

	return proposerSequence
}
