package surf

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLabelVecSearch(t *testing.T) {
	labels := [][]byte{
		{1},
		{2, 3},
		{4, 5, 6},
		{labelTerminator, 7, 8, 9},
	}
	v := new(labelVector)
	v.init(labels, 0, uint32(len(labels)))
	labelShouldExist(t, v, 1, 0, 1, 0)
	labelShouldExist(t, v, 3, 0, 5, 2)
	labelShouldExist(t, v, 5, 3, 7, 4)
	labelShouldExist(t, v, 7, 6, 8, 7)
}

func labelShouldExist(t *testing.T, v *labelVector, k byte, start, size, pos uint32) {
	r, ok := v.Search(k, start, size)
	require.True(t, ok)
	require.Equal(t, pos, r)
}
