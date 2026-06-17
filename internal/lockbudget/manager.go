package lockbudget

import (
	"fmt"
	"log"
	"rtm-107/internal/model"
	"rtm-107/internal/storage"
	"sync"
	"time"
)

type callerRuntimeState struct {
	config        *model.LockBudgetConfig
	periodStartAt time.Time
	periodEndAt   time.Time
	consumedUnits int
	peakConcurrent int
	lockCount     int
	exhaustCount  int
	holdings      map[string]*holdingState
}

type holdingState struct {
	lockName      string
	acquiredAt    time.Time
	expiresAt     time.Time
	lastMeteredAt time.Time
	unitsAccrued  int
}

type Manager struct {
	storage   *storage.Storage
	mu        sync.Mutex
	callers   map[string]*callerRuntimeState
	stopCh    chan struct{}
	ticker    *time.Ticker
	dirty     bool
}

func NewManager(s *storage.Storage) *Manager {
	return &Manager{
		storage: s,
		callers: make(map[string]*callerRuntimeState),
		stopCh:  make(chan struct{}),
	}
}

func (m *Manager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.loadConfigsLocked(); err != nil {
		return fmt.Errorf("load budget configs: %w", err)
	}
	if err := m.loadHoldingsLocked(); err != nil {
		return fmt.Errorf("load budget holdings: %w", err)
	}

	m.ticker = time.NewTicker(1 * time.Second)
	go m.meterLoop()

	log.Println("[lockbudget-manager] started")
	return nil
}

func (m *Manager) Stop() {
	close(m.stopCh)
	if m.ticker != nil {
		m.ticker.Stop()
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	for callerID, rt := range m.callers {
		m.finalizeCurrentPeriodLocked(callerID, rt, now)
	}

	log.Println("[lockbudget-manager] stopped")
}

func (m *Manager) loadConfigsLocked() error {
	configs, err := m.storage.ListLockBudgetConfigs()
	if err != nil {
		return err
	}
	now := time.Now()
	for i := range configs {
		cfg := &configs[i]
		rt := &callerRuntimeState{
			config:        cfg,
			periodStartAt: now,
			periodEndAt:   now.Add(time.Duration(cfg.PeriodSec) * time.Second),
			holdings:      make(map[string]*holdingState),
		}
		m.callers[cfg.CallerID] = rt
	}
	return nil
}

func (m *Manager) loadHoldingsLocked() error {
	records, err := m.storage.ListAllBudgetHoldings()
	if err != nil {
		return err
	}
	now := time.Now()
	for _, r := range records {
		rt, ok := m.callers[r.CallerID]
		if !ok {
			continue
		}
		if r.ExpiresAt.Before(now) {
			continue
		}
		h := &holdingState{
			lockName:      r.LockName,
			acquiredAt:    r.AcquiredAt,
			expiresAt:     r.ExpiresAt,
			lastMeteredAt: r.LastMeteredAt,
			unitsAccrued:  r.UnitsAccrued,
		}
		rt.holdings[r.LockName] = h
		if h.lastMeteredAt.After(rt.periodStartAt) || h.lastMeteredAt.Equal(rt.periodStartAt) {
			rt.consumedUnits += h.unitsAccrued
		}
		rt.lockCount++
	}
	return nil
}

func (m *Manager) meterLoop() {
	for {
		select {
		case <-m.stopCh:
			return
		case <-m.ticker.C:
			m.meterTick()
		}
	}
}

func (m *Manager) meterTick() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	m.dirty = false

	for callerID, rt := range m.callers {
		m.checkPeriodResetLocked(callerID, rt, now)
		m.meterCallerLocked(callerID, rt, now)
	}

	if m.dirty {
		m.persistDirtyLocked(now)
	}
}

func (m *Manager) checkPeriodResetLocked(callerID string, rt *callerRuntimeState, now time.Time) {
	if now.Before(rt.periodEndAt) {
		return
	}

	m.finalizeCurrentPeriodLocked(callerID, rt, rt.periodEndAt)

	elapsedSec := int(now.Sub(rt.periodEndAt).Seconds())
	periodsToAdvance := 1
	if elapsedSec > rt.config.PeriodSec {
		periodsToAdvance = elapsedSec / rt.config.PeriodSec
		if elapsedSec%rt.config.PeriodSec > 0 {
			periodsToAdvance++
		}
	}

	rt.periodStartAt = rt.periodEndAt
	for i := 1; i < periodsToAdvance; i++ {
		rt.periodStartAt = rt.periodStartAt.Add(time.Duration(rt.config.PeriodSec) * time.Second)
		summary := &model.BudgetPeriodSummary{
			CallerID:      callerID,
			PeriodStartAt: rt.periodStartAt,
			PeriodEndAt:   rt.periodStartAt.Add(time.Duration(rt.config.PeriodSec) * time.Second),
			BudgetLimit:   rt.config.BudgetLimit,
			TotalConsumed: 0,
			PeakConcurrent: 0,
			LockCount:     0,
			ExhaustEvents: 0,
		}
		_ = m.storage.UpsertBudgetPeriodSummary(summary)
	}
	rt.periodEndAt = rt.periodStartAt.Add(time.Duration(rt.config.PeriodSec) * time.Second)
	rt.consumedUnits = 0
	rt.peakConcurrent = 0
	rt.lockCount = len(rt.holdings)
	rt.exhaustCount = 0

	for _, h := range rt.holdings {
		h.lastMeteredAt = now
		h.unitsAccrued = 0
	}

	log.Printf("[lockbudget] period reset: caller=%s period_start=%v period_end=%v",
		callerID, rt.periodStartAt.Format(time.RFC3339), rt.periodEndAt.Format(time.RFC3339))
}

func (m *Manager) finalizeCurrentPeriodLocked(callerID string, rt *callerRuntimeState, endTime time.Time) {
	for _, h := range rt.holdings {
		if h.lastMeteredAt.Before(endTime) {
			elapsed := endTime.Sub(h.lastMeteredAt).Seconds()
			units := int(elapsed)
			if units > 0 {
				rt.consumedUnits += units
				h.unitsAccrued += units
				h.lastMeteredAt = endTime
				_ = m.storage.UpdateBudgetHoldingMeter(callerID, h.lockName, h.lastMeteredAt, h.unitsAccrued)
			}
		}
	}

	if rt.lockCount > rt.peakConcurrent {
		rt.peakConcurrent = rt.lockCount
	}

	summary := &model.BudgetPeriodSummary{
		CallerID:       callerID,
		PeriodStartAt:  rt.periodStartAt,
		PeriodEndAt:    endTime,
		BudgetLimit:    rt.config.BudgetLimit,
		TotalConsumed:  rt.consumedUnits,
		PeakConcurrent: rt.peakConcurrent,
		LockCount:      rt.lockCount,
		ExhaustEvents:  rt.exhaustCount,
	}
	_ = m.storage.UpsertBudgetPeriodSummary(summary)
}

func (m *Manager) meterCallerLocked(callerID string, rt *callerRuntimeState, now time.Time) {
	concurrentCount := 0
	var toRemove []string

	for lockName, h := range rt.holdings {
		if !h.expiresAt.After(now) {
			toRemove = append(toRemove, lockName)
			continue
		}
		concurrentCount++

		if h.lastMeteredAt.Before(now) {
			elapsed := now.Sub(h.lastMeteredAt).Seconds()
			units := int(elapsed)
			if units > 0 {
				rt.consumedUnits += units
				h.unitsAccrued += units
				h.lastMeteredAt = now
				_ = m.storage.UpdateBudgetHoldingMeter(callerID, lockName, h.lastMeteredAt, h.unitsAccrued)
				m.dirty = true
			}
		}
	}

	for _, lockName := range toRemove {
		delete(rt.holdings, lockName)
		_ = m.storage.RemoveBudgetHolding(callerID, lockName)
	}

	rt.lockCount = concurrentCount
	if concurrentCount > rt.peakConcurrent {
		rt.peakConcurrent = concurrentCount
	}
}

func (m *Manager) persistDirtyLocked(now time.Time) {
	_ = now
}

func (m *Manager) SetBudget(callerID string, budgetLimit int, periodSec int, warningPct int) (*model.LockBudgetConfig, error) {
	if callerID == "" {
		return nil, fmt.Errorf("caller_id is required")
	}
	if budgetLimit <= 0 {
		return nil, fmt.Errorf("budget_limit must be positive")
	}
	if periodSec <= 0 {
		return nil, fmt.Errorf("period_sec must be positive")
	}
	if warningPct < 0 || warningPct > 100 {
		warningPct = 80
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	cfg := &model.LockBudgetConfig{
		CallerID:    callerID,
		BudgetLimit: budgetLimit,
		PeriodSec:   periodSec,
		WarningPct:  warningPct,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	if rt, ok := m.callers[callerID]; ok {
		cfg.ID = rt.config.ID
		m.finalizeCurrentPeriodLocked(callerID, rt, now)
		rt.config = cfg
		rt.periodStartAt = now
		rt.periodEndAt = now.Add(time.Duration(periodSec) * time.Second)
		rt.consumedUnits = 0
		rt.peakConcurrent = 0
		rt.exhaustCount = 0
		for _, h := range rt.holdings {
			h.lastMeteredAt = now
			h.unitsAccrued = 0
		}
	} else {
		rt = &callerRuntimeState{
			config:        cfg,
			periodStartAt: now,
			periodEndAt:   now.Add(time.Duration(periodSec) * time.Second),
			holdings:      make(map[string]*holdingState),
		}
		m.callers[callerID] = rt
	}

	if err := m.storage.UpsertLockBudgetConfig(cfg); err != nil {
		return nil, err
	}

	log.Printf("[lockbudget] budget set: caller=%s limit=%d period=%ds warning=%d%%",
		callerID, budgetLimit, periodSec, warningPct)
	return cfg, nil
}

func (m *Manager) DeleteBudget(callerID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	rt, ok := m.callers[callerID]
	if !ok {
		return fmt.Errorf("no budget configured for caller: %s", callerID)
	}

	now := time.Now()
	m.finalizeCurrentPeriodLocked(callerID, rt, now)

	for lockName := range rt.holdings {
		_ = m.storage.RemoveBudgetHolding(callerID, lockName)
	}

	delete(m.callers, callerID)
	if err := m.storage.DeleteLockBudgetConfig(callerID); err != nil {
		return err
	}

	log.Printf("[lockbudget] budget deleted: caller=%s", callerID)
	return nil
}

func (m *Manager) CheckAcquire(callerID string, lockName string, leaseSec int) (*model.BudgetAcquireCheckResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	rt, ok := m.callers[callerID]
	if !ok {
		return &model.BudgetAcquireCheckResult{
			Allowed: true,
			Reason:  "no budget configured, unlimited",
		}, nil
	}

	now := time.Now()
	m.checkPeriodResetLocked(callerID, rt, now)
	m.meterCallerLocked(callerID, rt, now)

	remaining := rt.config.BudgetLimit - rt.consumedUnits
	if remaining < 0 {
		remaining = 0
	}

	result := &model.BudgetAcquireCheckResult{
		ConsumedUnits:  rt.consumedUnits,
		RemainingUnits: remaining,
		BudgetLimit:    rt.config.BudgetLimit,
	}

	if rt.consumedUnits >= rt.config.BudgetLimit {
		result.Allowed = false
		result.Reason = fmt.Sprintf("budget exhausted: consumed=%d, limit=%d, remaining=%d",
			rt.consumedUnits, rt.config.BudgetLimit, remaining)

		event := &model.BudgetExhaustEvent{
			CallerID:       callerID,
			ConsumedUnits:  rt.consumedUnits,
			BudgetLimit:    rt.config.BudgetLimit,
			PeriodStartAt:  rt.periodStartAt,
			PeriodEndAt:    rt.periodEndAt,
			AttemptedLock:  lockName,
			UnitsRequested: leaseSec,
			Detail:         result.Reason,
			CreatedAt:      now,
		}
		_ = m.storage.AddBudgetExhaustEvent(event)
		rt.exhaustCount++

		log.Printf("[lockbudget] acquire rejected: caller=%s lock=%s consumed=%d limit=%d",
			callerID, lockName, rt.consumedUnits, rt.config.BudgetLimit)
		return result, nil
	}

	result.Allowed = true

	warningThreshold := rt.config.BudgetLimit * rt.config.WarningPct / 100
	if rt.consumedUnits >= warningThreshold {
		result.Reason = fmt.Sprintf("budget warning: consumed=%d, limit=%d, remaining=%d (warning threshold %d%%)",
			rt.consumedUnits, rt.config.BudgetLimit, remaining, rt.config.WarningPct)
	}

	return result, nil
}

func (m *Manager) StartHolding(callerID string, lockName string, acquiredAt time.Time, expiresAt time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	rt, ok := m.callers[callerID]
	if !ok {
		return nil
	}

	meterStart := acquiredAt
	if meterStart.Before(rt.periodStartAt) {
		meterStart = rt.periodStartAt
	}

	h := &holdingState{
		lockName:      lockName,
		acquiredAt:    acquiredAt,
		expiresAt:     expiresAt,
		lastMeteredAt: meterStart,
		unitsAccrued:  0,
	}

	rt.holdings[lockName] = h
	rt.lockCount = len(rt.holdings)
	if rt.lockCount > rt.peakConcurrent {
		rt.peakConcurrent = rt.lockCount
	}

	return m.storage.UpsertBudgetHolding(callerID, lockName, acquiredAt, expiresAt, meterStart, 0)
}

func (m *Manager) StopHolding(callerID string, lockName string, releasedAt time.Time) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	rt, ok := m.callers[callerID]
	if !ok {
		return 0, nil
	}

	h, ok := rt.holdings[lockName]
	if !ok {
		return 0, nil
	}

	var unitsThisRelease int
	if h.lastMeteredAt.Before(releasedAt) {
		elapsed := releasedAt.Sub(h.lastMeteredAt).Seconds()
		unitsThisRelease = int(elapsed)
		if unitsThisRelease > 0 {
			rt.consumedUnits += unitsThisRelease
			h.unitsAccrued += unitsThisRelease
			h.lastMeteredAt = releasedAt
		}
	}

	totalUnits := h.unitsAccrued
	delete(rt.holdings, lockName)
	rt.lockCount = len(rt.holdings)

	_ = m.storage.RemoveBudgetHolding(callerID, lockName)

	return totalUnits, nil
}

func (m *Manager) RenewHolding(callerID string, lockName string, newExpiresAt time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	rt, ok := m.callers[callerID]
	if !ok {
		return nil
	}

	h, ok := rt.holdings[lockName]
	if !ok {
		return nil
	}

	h.expiresAt = newExpiresAt
	return m.storage.UpdateBudgetHoldingExpiry(callerID, lockName, newExpiresAt)
}

func (m *Manager) GetCallerStatus(callerID string) (*model.CallerBudgetStatusInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	rt, ok := m.callers[callerID]
	if !ok {
		cfg, err := m.storage.GetLockBudgetConfig(callerID)
		if err != nil {
			return nil, err
		}
		if cfg == nil {
			return &model.CallerBudgetStatusInfo{}, nil
		}
		return &model.CallerBudgetStatusInfo{Config: cfg}, nil
	}

	now := time.Now()
	m.checkPeriodResetLocked(callerID, rt, now)
	m.meterCallerLocked(callerID, rt, now)

	remaining := rt.config.BudgetLimit - rt.consumedUnits
	if remaining < 0 {
		remaining = 0
	}
	exhausted := rt.consumedUnits >= rt.config.BudgetLimit
	warningTriggered := false
	if rt.config.WarningPct > 0 {
		threshold := rt.config.BudgetLimit * rt.config.WarningPct / 100
		warningTriggered = rt.consumedUnits >= threshold
	}

	status := &model.LockBudgetStatus{
		CallerID:         callerID,
		BudgetLimit:      rt.config.BudgetLimit,
		PeriodSec:        rt.config.PeriodSec,
		ConsumedUnits:    rt.consumedUnits,
		RemainingUnits:   remaining,
		WarningPct:       rt.config.WarningPct,
		WarningTriggered: warningTriggered,
		Exhausted:        exhausted,
		PeriodStartAt:    rt.periodStartAt,
		PeriodEndAt:      rt.periodEndAt,
		ActiveLocks:      rt.lockCount,
		UpdatedAt:        now,
	}

	heldLocks := make([]model.HeldLockDetail, 0, len(rt.holdings))
	for lockName, h := range rt.holdings {
		heldSec := now.Sub(h.acquiredAt).Seconds()
		projectedSec := h.expiresAt.Sub(h.acquiredAt).Seconds()
		if heldSec < 0 {
			heldSec = 0
		}
		if projectedSec < heldSec {
			projectedSec = heldSec
		}
		heldLocks = append(heldLocks, model.HeldLockDetail{
			LockName:       lockName,
			AcquiredAt:     h.acquiredAt,
			ExpiresAt:      h.expiresAt,
			HeldSec:        heldSec,
			UnitsConsumed:  h.unitsAccrued,
			UnitsProjected: int(projectedSec),
		})
	}

	cfgCopy := *rt.config
	return &model.CallerBudgetStatusInfo{
		Config:    &cfgCopy,
		Status:    status,
		HeldLocks: heldLocks,
	}, nil
}

func (m *Manager) ListAllStatuses() ([]model.LockBudgetStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	var result []model.LockBudgetStatus

	for callerID, rt := range m.callers {
		m.checkPeriodResetLocked(callerID, rt, now)
		m.meterCallerLocked(callerID, rt, now)

		remaining := rt.config.BudgetLimit - rt.consumedUnits
		if remaining < 0 {
			remaining = 0
		}
		exhausted := rt.consumedUnits >= rt.config.BudgetLimit
		warningTriggered := false
		if rt.config.WarningPct > 0 {
			threshold := rt.config.BudgetLimit * rt.config.WarningPct / 100
			warningTriggered = rt.consumedUnits >= threshold
		}

		result = append(result, model.LockBudgetStatus{
			CallerID:         callerID,
			BudgetLimit:      rt.config.BudgetLimit,
			PeriodSec:        rt.config.PeriodSec,
			ConsumedUnits:    rt.consumedUnits,
			RemainingUnits:   remaining,
			WarningPct:       rt.config.WarningPct,
			WarningTriggered: warningTriggered,
			Exhausted:        exhausted,
			PeriodStartAt:    rt.periodStartAt,
			PeriodEndAt:      rt.periodEndAt,
			ActiveLocks:      rt.lockCount,
			UpdatedAt:        now,
		})
	}

	return result, nil
}

func (m *Manager) ListConfigs() ([]model.LockBudgetConfig, error) {
	return m.storage.ListLockBudgetConfigs()
}

func (m *Manager) ListExhaustEvents(callerID string, limit int) ([]model.BudgetExhaustEvent, error) {
	return m.storage.ListBudgetExhaustEvents(callerID, limit)
}

func (m *Manager) ListPeriodSummaries(callerID string, limit int) ([]model.BudgetPeriodSummary, error) {
	return m.storage.ListBudgetPeriodSummaries(callerID, limit)
}

func (m *Manager) GetGlobalStats() (*model.GlobalBudgetStats, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	todayStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	yesterday := now.Add(-24 * time.Hour)

	totalActiveLocks := 0
	callersOverBudget := 0
	callersNearBudget := 0

	for callerID, rt := range m.callers {
		m.checkPeriodResetLocked(callerID, rt, now)
		m.meterCallerLocked(callerID, rt, now)
		totalActiveLocks += rt.lockCount
		if rt.consumedUnits >= rt.config.BudgetLimit {
			callersOverBudget++
		} else if rt.config.WarningPct > 0 {
			threshold := rt.config.BudgetLimit * rt.config.WarningPct / 100
			if rt.consumedUnits >= threshold {
				callersNearBudget++
			}
		}
	}

	totalConsumedToday, _ := m.storage.SumTotalConsumedSince(todayStart)
	exhaustEvents24h, _ := m.storage.CountBudgetExhaustEventsSince("", yesterday)

	return &model.GlobalBudgetStats{
		TotalCallers:       len(m.callers),
		TotalActiveLocks:   totalActiveLocks,
		TotalConsumedToday: totalConsumedToday,
		ExhaustEvents24h:   exhaustEvents24h,
		CallersOverBudget:  callersOverBudget,
		CallersNearBudget:  callersNearBudget,
	}, nil
}

func (m *Manager) HasBudget(callerID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.callers[callerID]
	return ok
}

func (m *Manager) GetConfig(callerID string) (*model.LockBudgetConfig, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rt, ok := m.callers[callerID]
	if ok {
		cfg := *rt.config
		return &cfg, nil
	}
	return m.storage.GetLockBudgetConfig(callerID)
}

func (m *Manager) SetConfig(callerID string, budgetLimit int, periodSec int, warningPct int) (*model.LockBudgetConfig, error) {
	return m.SetBudget(callerID, budgetLimit, periodSec, warningPct)
}

func (m *Manager) DeleteConfig(callerID string) error {
	return m.DeleteBudget(callerID)
}

func (m *Manager) ListStatuses() ([]model.LockBudgetStatus, error) {
	return m.ListAllStatuses()
}

func (m *Manager) GetStatus(callerID string) (*model.LockBudgetStatus, error) {
	info, err := m.GetCallerStatus(callerID)
	if err != nil {
		return nil, err
	}
	return info.Status, nil
}

func (m *Manager) ListHeldLocks(callerID string, now time.Time) ([]model.HeldLockDetail, error) {
	info, err := m.GetCallerStatus(callerID)
	if err != nil {
		return nil, err
	}
	_ = now
	return info.HeldLocks, nil
}
