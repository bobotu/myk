package surf

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSelect64Search(t *testing.T) {
	require.Equal(t, -1, select64(0, 2))
	require.Equal(t, -1, select64(1, 2))
	require.Equal(t, -1, select64(1<<32, 2))

	require.Equal(t, 62, select64(3, 1))
	require.Equal(t, 31, select64(3<<32, 2))
}
