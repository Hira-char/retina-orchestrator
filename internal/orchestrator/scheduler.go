// Copyright (c) 2025 Sorbonne Université
// SPDX-License-Identifier: MIT

package orchestrator

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"os"
	"sync"

	"time"

	"github.com/dioptra-io/retina-commons/api/v2"
)

// pdState holds the scheduling state for a single ProbingDirective, including
// the last observed near and far addresses and the current issuance probability.
type pdState struct {
	lastHitNearAddress net.IP
	lastHitFarAddress  net.IP
	issuanceProb       float64
	directive          *api.ProbingDirective
}

// impactRecord stores the current impact state for a single address.
type impactRecord struct {
	// pds is the set of ProbingDirective IDs currently impacting this address.
	pds map[uint64]*pdState
}

// Scheduler implements the responsible probing algorithm. It schedules
// ProbingDirectives for issuance and updates their issuance probabilities
// based on incoming ForwardingInfoElements.
type Scheduler struct {
	logger  *slog.Logger
	mutex   sync.Mutex
	metrics *Metrics
	// pdMap maps each ProbingDirective ID to its scheduling state, which holds
	// the directive itself, its issuance probability, and last hit addresses.
	pdMap map[uint64]*pdState
	// impactRecords maps each address to the set of directives impacting it.
	impactRecords map[string]*impactRecord
	// lastIssuance is the time of the last issued directive, used for rate limiting.
	lastIssuance   time.Time
	lastCycleBegin time.Time
	// issuancePeriod is the minimum time between two directive issuances,
	// derived from issuanceRate as time.Second / issuanceRate.
	issuancePeriod time.Duration
	randomizer     *randomizer
	// random is used for the Bernoulli experiment in NextPD.
	random *rand.Rand
}

// NewScheduler creates a new Scheduler from the given seed, issuance rate, and
// path to the probing directives file.
// Returns an error if the file cannot be read, issuanceRate is <= 0, or the
// file contains no directives.
func NewScheduler(seed uint64, issuanceRate float64, pdFile string, logger *slog.Logger, metrics *Metrics) (*Scheduler, error) {
	if issuanceRate <= 0.0 {
		return nil, fmt.Errorf("invalid arguments: issuance rate cannot be zero or negative")
	}
	if logger == nil {
		logger = slog.Default()
	}
	if metrics == nil {
		return nil, fmt.Errorf("metrics cannot be nil")
	}

	pds, err := readPDs(pdFile)
	if err != nil {
		return nil, fmt.Errorf("cannot read from file: %w", err)
	}
	if len(pds) == 0 {
		return nil, fmt.Errorf("invalid arguments: pds length cannot be zero")
	}

	logger.Info("Scheduler loaded directives",
		slog.Int("count", len(pds)),
		slog.String("file", pdFile))

	pdMap := make(map[uint64]*pdState, len(pds))
	indices := make([]uint64, 0, len(pds))
	for _, pd := range pds {
		pdMap[pd.ProbingDirectiveId] = &pdState{
			directive:    pd,
			issuanceProb: 1.0,
		}
		indices = append(indices, pd.ProbingDirectiveId)
	}

	randomizer, err := newRandomizer(seed, indices)
	if err != nil {
		return nil, fmt.Errorf("cannot create randomizer: %w", err)
	}

	metrics.PDsTotal.Set(float64(len(pds)))

	return &Scheduler{
		logger:         logger,
		metrics:        metrics,
		pdMap:          pdMap,
		impactRecords:  make(map[string]*impactRecord),
		issuancePeriod: time.Duration(float64(time.Second) / issuanceRate),
		randomizer:     randomizer,
		random:         rand.New(rand.NewPCG(seed, 0)), // #nosec G404
	}, nil
}

// NextPD returns the next ProbingDirective candidate. It blocks until the
// rate limit allows the next issuance, then runs a Bernoulli experiment to
// decide whether to return or skip the directive. Returns nil if skipped.
func (s *Scheduler) NextPD() *api.ProbingDirective {
	s.mutex.Lock()
	oldCycle := s.randomizer.Cycle()
	pd := s.pdMap[s.randomizer.Next()]
	newCycle := s.randomizer.Cycle()
	nextTime := s.lastIssuance.Add(s.issuancePeriod)
	issuanceProb := pd.issuanceProb
	s.mutex.Unlock()

	if s.issuancePeriod >= 10*time.Millisecond {
		time.Sleep(time.Until(nextTime))
	} else {
		for time.Now().Before(nextTime) { // busylock
		}
	}

	s.mutex.Lock()
	s.lastIssuance = time.Now()
	s.mutex.Unlock()

	if oldCycle != newCycle {
		if !s.lastCycleBegin.IsZero() {
			cycleDuration := time.Since(s.lastCycleBegin)
			s.metrics.CycleDurationSeconds.Observe(float64(cycleDuration.Seconds()))
		}
		s.lastCycleBegin = time.Now()
		s.metrics.CyclesTotal.Inc()
	}

	if s.random.Float64() < issuanceProb {
		return pd.directive
	}
	s.metrics.PDsSkippedTotal.Inc()
	s.logger.Debug("PD skipped",
		slog.Uint64("pd_id", pd.directive.ProbingDirectiveId),
		slog.Float64("issuance_prob", issuanceProb))
	return nil
}

// UpdateFromFIE adjusts the issuance probability of a directive based on an
// incoming ForwardingInfoElement. It records the near and far addresses
// observed in the FIE and recalculates the probability according to the number
// of directives currently impacting those addresses.
// Returns an error if the directive ID is not recognized.
func (s *Scheduler) UpdateFromFIE(fie *api.ForwardingInfoElement) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	pd, ok := s.pdMap[fie.ProbingDirectiveId]
	if !ok {
		return fmt.Errorf("probing directive ID %d is not recognized", fie.ProbingDirectiveId)
	}

	oldNearAddress, oldFarAddress := pd.lastHitNearAddress, pd.lastHitFarAddress

	// Last hit addresses can be nil (e.g. on probe timeout).
	pd.lastHitNearAddress = nil
	if fie.NearInfo != nil {
		pd.lastHitNearAddress = net.ParseIP(fie.NearInfo.ReplyAddress)
	}
	pd.lastHitFarAddress = nil
	if fie.FarInfo != nil {
		pd.lastHitFarAddress = net.ParseIP(fie.FarInfo.ReplyAddress)
	}

	if ipKey(oldNearAddress) != ipKey(pd.lastHitNearAddress) {
		s.removeImpact(oldNearAddress, pd)
		s.recordImpact(pd.lastHitNearAddress, pd)
	}
	if ipKey(oldFarAddress) != ipKey(pd.lastHitFarAddress) {
		s.removeImpact(oldFarAddress, pd)
		s.recordImpact(pd.lastHitFarAddress, pd)
	}

	numNearImpacts, numFarImpacts := 0, 0
	if pd.lastHitNearAddress != nil {
		if rec, ok := s.impactRecords[ipKey(pd.lastHitNearAddress)]; ok {
			numNearImpacts = len(rec.pds)
		}
	}
	if pd.lastHitFarAddress != nil {
		if rec, ok := s.impactRecords[ipKey(pd.lastHitFarAddress)]; ok {
			numFarImpacts = len(rec.pds)
		}
	}

	maxImpacts := max(numNearImpacts, numFarImpacts)
	if maxImpacts == 0 {
		pd.issuanceProb = 1.0
	} else {
		pd.issuanceProb = 1.0 / float64(maxImpacts)
	}

	return nil
}

// recordImpact records that the given PD is impacting the specified address.
// Creates a new impact record for the address if none exists yet.
func (s *Scheduler) recordImpact(address net.IP, pd *pdState) {
	if address == nil {
		return
	}
	key := ipKey(address)
	record, ok := s.impactRecords[key]
	if !ok {
		record = &impactRecord{
			pds: make(map[uint64]*pdState),
		}
		s.impactRecords[key] = record
	}
	record.pds[pd.directive.ProbingDirectiveId] = pd
}

// removeImpact removes the given PD from the impact record of the specified
// address. Deletes the impact record entirely if no other PDs are impacting it.
func (s *Scheduler) removeImpact(address net.IP, pd *pdState) {
	if address == nil {
		return
	}
	key := ipKey(address)
	record, ok := s.impactRecords[key]
	if ok {
		delete(record.pds, pd.directive.ProbingDirectiveId)
		if len(record.pds) == 0 {
			delete(s.impactRecords, key)
		}
	}
}

// ipKey returns a normalized string key for a net.IP address, suitable for
// use as a map key. Returns an empty string for nil addresses.
func ipKey(ip net.IP) string {
	if ip == nil {
		return ""
	}
	return ip.To16().String()
}

func readPDs(filepath string) ([]*api.ProbingDirective, error) {
	f, err := os.Open(filepath) //nolint:gosec
	if err != nil {
		return nil, fmt.Errorf("cannot open file: %w", err)
	}
	defer func() {
		_ = f.Close()
	}()

	var results []*api.ProbingDirective
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var obj api.ProbingDirective
		if err := json.Unmarshal(scanner.Bytes(), &obj); err != nil {
			return nil, fmt.Errorf("cannot unmarshal line: %w", err)
		}
		results = append(results, &obj)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scanner error: %w", err)
	}

	return results, nil
}
