package mesh

// weightedRotor performs weighted round-robin selection across coins so that,
// over many ticks, each coin receives a share of jobs proportional to its
// percent. It uses the classic "credit accumulation" method: each tick, every
// coin accrues credit equal to its weight; the highest-credit coin is chosen and
// has the total weight subtracted. This yields a smooth interleaving (e.g. 50/50
// alternates A,B,A,B rather than AAABBB) which minimizes clean-job switches while
// honoring proportions.
type weightedRotor struct {
	credit map[string]float64
}

func newWeightedRotor() *weightedRotor {
	return &weightedRotor{credit: make(map[string]float64)}
}

// next returns the next coin symbol to serve, given the current available
// weights. Coins not in the provided set have their credit forgotten.
func (r *weightedRotor) next(weights []CoinWeight) string {
	if len(weights) == 0 {
		return ""
	}
	if len(weights) == 1 {
		return weights[0].Coin
	}

	var total float64
	present := make(map[string]bool, len(weights))
	for _, w := range weights {
		total += w.Percent
		present[w.Coin] = true
		r.credit[w.Coin] += w.Percent
	}
	// Forget coins no longer present.
	for c := range r.credit {
		if !present[c] {
			delete(r.credit, c)
		}
	}
	if total <= 0 {
		return weights[0].Coin
	}

	// Pick highest-credit coin.
	best := ""
	var bestCredit float64
	for _, w := range weights {
		c := r.credit[w.Coin]
		if best == "" || c > bestCredit {
			best = w.Coin
			bestCredit = c
		}
	}
	// Subtract total from the winner.
	r.credit[best] -= total
	return best
}
