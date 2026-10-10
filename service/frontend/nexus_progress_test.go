package frontend

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseNexusProgress(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		body    string
		want    nexusProgress
		wantErr string
	}{
		{
			name: "every member, counter as a number",
			body: `{"position": "cursor-7", "counter": 3, "metadata": {"topic": "tokens"}}`,
			want: nexusProgress{Position: "cursor-7", Counter: 3, Metadata: map[string]string{"topic": "tokens"}},
		},
		{
			name: "counter as a decimal string, as proto JSON writes a 64-bit integer",
			body: `{"counter": "9223372036854775807"}`,
			want: nexusProgress{Counter: 9223372036854775807},
		},
		{
			name: "unknown members are ignored",
			body: `{"counter": 1, "later": true}`,
			want: nexusProgress{Counter: 1},
		},
		{name: "not JSON", body: `nope`, wantErr: "not an OperationProgress object"},
		{name: "counter missing", body: `{"position": "p"}`, wantErr: "counter is required"},
		{name: "counter null", body: `{"counter": null}`, wantErr: "counter is required"},
		{name: "counter zero", body: `{"counter": 0}`, wantErr: "positive integer"},
		{name: "counter negative", body: `{"counter": -1}`, wantErr: "positive integer"},
		{name: "counter fractional", body: `{"counter": 1.5}`, wantErr: "positive integer"},
		{name: "counter not numeric", body: `{"counter": "x"}`, wantErr: "positive integer"},
		{name: "counter past int64", body: `{"counter": "9223372036854775808"}`, wantErr: "positive integer"},
		{name: "metadata not strings", body: `{"counter": 1, "metadata": {"k": 1}}`, wantErr: "not an OperationProgress object"},
		{
			name:    "metadata over the limit",
			body:    `{"counter": 1, "metadata": {"k": "` + strings.Repeat("x", maxNexusProgressMetadataBytes) + `"}}`,
			wantErr: "more than the 2048 allowed",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseNexusProgress([]byte(tc.body))
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}

	t.Run("metadata exactly at the limit", func(t *testing.T) {
		t.Parallel()
		value := strings.Repeat("x", maxNexusProgressMetadataBytes-1)
		got, err := parseNexusProgress([]byte(`{"counter": 1, "metadata": {"k": "` + value + `"}}`))
		require.NoError(t, err)
		require.Equal(t, value, got.Metadata["k"])
	})
}

func TestNexusProgressProto(t *testing.T) {
	t.Parallel()
	progress := nexusProgressProto(nexusProgress{
		Position: "cursor-7",
		Counter:  3,
		Metadata: map[string]string{"topic": "tokens"},
	})
	require.Equal(t, "cursor-7", progress.GetPosition())
	require.Equal(t, int64(3), progress.GetCounter())
	require.Equal(t, map[string]string{"topic": "tokens"}, progress.GetMetadata())
	require.Nil(t, progress.GetOperation(), "the caller's server names the operation, not the handler")
}
