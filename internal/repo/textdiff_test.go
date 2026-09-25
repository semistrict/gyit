package repo

import (
	"context"
	"math/rand/v2"
	"testing"
)

func TestLineDiffProducesMinimalApplicableEdits(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	for trial := 0; trial < 2000; trial++ {
		a, b := make([]string, rng.IntN(25)), make([]string, rng.IntN(25))
		for i := range a {
			a[i] = string(rune('a' + rng.IntN(4)))
		}
		for i := range b {
			b[i] = string(rune('a' + rng.IntN(4)))
		}
		edits, err := lineDiff(context.Background(), a, b)
		if err != nil {
			t.Fatal(err)
		}
		ai, bi, cost := 0, 0, 0
		for _, e := range edits {
			if e.a != ai || e.b != bi {
				t.Fatalf("invalid coordinates at %v: %d,%d", e, ai, bi)
			}
			switch e.kind {
			case ' ':
				if a[ai] != b[bi] {
					t.Fatal("matched unequal lines")
				}
				ai++
				bi++
			case '-':
				ai++
				cost++
			case '+':
				bi++
				cost++
			}
		}
		if ai != len(a) || bi != len(b) {
			t.Fatal("incomplete edit script")
		}
		// Independent dynamic programming reference for shortest insert/delete cost.
		dp := make([][]int, len(a)+1)
		for i := range dp {
			dp[i] = make([]int, len(b)+1)
			dp[i][0] = i
		}
		for j := range dp[0] {
			dp[0][j] = j
		}
		for i := 1; i <= len(a); i++ {
			for j := 1; j <= len(b); j++ {
				if a[i-1] == b[j-1] {
					dp[i][j] = dp[i-1][j-1]
				} else {
					dp[i][j] = 1 + min(dp[i-1][j], dp[i][j-1])
				}
			}
		}
		if cost != dp[len(a)][len(b)] {
			t.Fatal("nonminimal diff", a, b, cost, dp[len(a)][len(b)])
		}
	}
}
