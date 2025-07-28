package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReducedProposerSet(t *testing.T) {
	vals := []*Validator{
		newValidator([]byte("val1"), 100, true),
		newValidator([]byte("val2"), 200, false),
		newValidator([]byte("val3"), 300, true),
		newValidator([]byte("val4"), 400, false),
	}
	vset := NewValidatorSet(vals)

	// Initial proposer should have CanPropose=true
	assert.True(t, vset.GetProposer().CanPropose)

	// Track proposers over 100 rounds
	proposers := make(map[string]int)
	for i := 0; i < 100; i++ {
		vset.IncrementProposerPriority(1)
		p := vset.GetProposer()
		assert.True(t, p.CanPropose)
		proposers[string(p.Address)]++
	}

	// Verify that only val1 and val3 are proposers and val3 is selected more
	// often due to having higher voting power
	assert.Equal(t, 2, len(proposers))
	assert.Greater(t, proposers[string([]byte("val3"))], proposers[string([]byte("val1"))])
}

func TestSingleProposer(t *testing.T) {
	vals := []*Validator{
		newValidator([]byte("val1"), 100, false),
		newValidator([]byte("val2"), 200, true), // Only proposer
		newValidator([]byte("val3"), 300, false),
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
		newValidator([]byte("val1"), 100, false),
		newValidator([]byte("val2"), 200, false),
	}

	// Panics when no validators can propose
	assert.Panics(t, func() {
		NewValidatorSet(vals)
	})
}

func TestProposerPriorityIncrementForAll(t *testing.T) {
	vals := []*Validator{
		newValidator([]byte("val1"), 100, true),
		newValidator([]byte("val2"), 100, false),
		newValidator([]byte("val3"), 100, true),
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

func TestCopyCanPropose(t *testing.T) {
	vals := []*Validator{
		newValidator([]byte("val1"), 100, true),
		newValidator([]byte("val2"), 200, false),
		newValidator([]byte("val3"), 300, true),
	}
	vset := NewValidatorSet(vals)

	// Make a copy
	vsetCopy := vset.Copy()

	// Verify all validators maintain their CanPropose status
	for i, v := range vset.Validators {
		assert.Equal(t, v.CanPropose, vsetCopy.Validators[i].CanPropose)
	}
}
