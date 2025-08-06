package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	abci "github.com/cometbft/cometbft/abci/types"
	"github.com/cometbft/cometbft/crypto"
	"github.com/cometbft/cometbft/crypto/ed25519"
	cryptoenc "github.com/cometbft/cometbft/crypto/encoding"
)

func TestABCIPubKey(t *testing.T) {
	pkEd := ed25519.GenPrivKey().PubKey()
	err := testABCIPubKey(t, pkEd)
	assert.NoError(t, err)
}

func testABCIPubKey(t *testing.T, pk crypto.PubKey) error {
	abciPubKey, err := cryptoenc.PubKeyToProto(pk)
	require.NoError(t, err)
	pk2, err := cryptoenc.PubKeyFromProto(abciPubKey)
	require.NoError(t, err)
	require.Equal(t, pk, pk2)
	return nil
}

func TestABCIValidators(t *testing.T) {
	pkEd := ed25519.GenPrivKey().PubKey()

	// correct validator
	cmtValExpected := NewValidator(pkEd, 10, false)

	cmtVal := NewValidator(pkEd, 10, false)

	abciVal := TM2PB.ValidatorUpdate(cmtVal)
	cmtVals, err := PB2TM.ValidatorUpdates([]abci.ValidatorUpdate{abciVal})
	assert.Nil(t, err)
	assert.Equal(t, cmtValExpected, cmtVals[0])

	abciVals := TM2PB.ValidatorUpdates(NewValidatorSet(cmtVals))
	assert.Equal(t, []abci.ValidatorUpdate{abciVal}, abciVals)

	// val with address
	cmtVal.Address = pkEd.Address()

	abciVal = TM2PB.ValidatorUpdate(cmtVal)
	cmtVals, err = PB2TM.ValidatorUpdates([]abci.ValidatorUpdate{abciVal})
	assert.Nil(t, err)
	assert.Equal(t, cmtValExpected, cmtVals[0])
}

type pubKeyEddie struct{}

func (pubKeyEddie) Address() Address                    { return []byte{} }
func (pubKeyEddie) Bytes() []byte                       { return []byte{} }
func (pubKeyEddie) VerifySignature([]byte, []byte) bool { return false }
func (pubKeyEddie) Equals(crypto.PubKey) bool           { return false }
func (pubKeyEddie) String() string                      { return "" }
func (pubKeyEddie) Type() string                        { return "pubKeyEddie" }

func TestABCIValidatorFromPubKeyAndPower(t *testing.T) {
	pubkey := ed25519.GenPrivKey().PubKey()

	abciVal := TM2PB.NewValidatorUpdate(pubkey, 10, true)
	assert.Equal(t, int64(10), abciVal.Power)
	assert.Equal(t, true, abciVal.ProposeDisabled)

	assert.Panics(t, func() { TM2PB.NewValidatorUpdate(nil, 10, true) })
	assert.Panics(t, func() { TM2PB.NewValidatorUpdate(pubKeyEddie{}, 10, true) })
}

func TestABCIValidatorWithoutPubKey(t *testing.T) {
	pkEd := ed25519.GenPrivKey().PubKey()

	abciVal := TM2PB.Validator(NewValidator(pkEd, 10, false))

	// pubkey must be nil
	cmtValExpected := abci.Validator{
		Address:         pkEd.Address(),
		Power:           10,
		ProposeDisabled: false,
	}

	assert.Equal(t, cmtValExpected, abciVal)
}
