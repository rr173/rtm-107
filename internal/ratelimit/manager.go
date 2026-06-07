package ratelimit

import (
	"fmt"
	"log"
	"rtm-107/internal/model"
	"rtm-107/internal/storage"
	"sync"
	"time"
)

type Manager struct {
	storage    *storage.Storage
	mu         sync.Mutex
	policies   map[string]*model.RateLimitPolicy
	bindings   map[string]*model.CallerBinding
	stopCh     chan struct{}
	ticker     *time.Ticker
}

func NewManager(s *storage.Storage) *Manager {
	return &Manager{
		storage:  s,
		policies: make(map[string]*model.RateLimitPolicy),
		bindings: make(map[string]*model.CallerBinding),
		stopCh:   make(chan struct{}),
	}
}

func (m *Manager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.loadPoliciesLocked(); err != nil {
		return fmt.Errorf("load policies: %w", err)
	}
	if err := m.loadBindingsLocked(); err != nil {
		return fmt.Errorf("load bindings: %w", err)
	}

	m.ticker = time.NewTicker(500 * time.Millisecond)
	go m.refillLoop()

	log.Println("[ratelimit-manager] started")
	return nil
}

func (m *Manager) Stop() {
	close(m.stopCh)
	if m.ticker != nil {
		m.ticker.Stop()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.persistAllLocked(); err != nil {
		log.Printf("[ratelimit-manager] persist error on stop: %v", err)
	}
	log.Println("[ratelimit-manager] stopped")
}

func (m *Manager) loadPoliciesLocked() error {
	policies, err := m.storage.ListPolicies()
	if err != nil {
		return err
	}
	for i := range policies {
		m.policies[policies[i].Name] = &policies[i]
	}
	return nil
}

func (m *Manager) loadBindingsLocked() error {
	bindings, err := m.storage.ListCallerBindings()
	if err != nil {
		return err
	}
	now := time.Now()
	for i := range bindings {
		b := &bindings[i]
		policy, ok := m.policies[b.PolicyName]
		if ok {
			m.reconcileState(b, policy, now)
		}
		m.bindings[b.CallerID] = b
	}
	return nil
}

func (m *Manager) reconcileState(b *model.CallerBinding, policy *model.RateLimitPolicy, now time.Time) {
	switch policy.Algorithm {
	case model.AlgoFixedWindow:
		if b.WindowStartAt.IsZero() {
			b.WindowStartAt = now
			b.UsedTokens = 0
		} else {
			elapsed := now.Sub(b.WindowStartAt).Seconds()
			if elapsed >= float64(policy.WindowSec) {
				windowsPassed := int(elapsed) / policy.WindowSec
				b.WindowStartAt = b.WindowStartAt.Add(time.Duration(windowsPassed*policy.WindowSec) * time.Second)
				b.UsedTokens = 0
			}
		}
	case model.AlgoTokenBucket:
		if b.LastRefillAt.IsZero() {
			b.LastRefillAt = now
			b.UsedTokens = 0
		} else {
			elapsed := now.Sub(b.LastRefillAt).Seconds()
			refillAmount := int(elapsed * policy.RefillRate)
			if refillAmount > 0 {
				effectiveLimit := b.QuotaLimit + b.BorrowedTokens - b.LentTokens
				if effectiveLimit < 0 {
					effectiveLimit = 0
				}
				currentTokens := effectiveLimit - b.UsedTokens
				newTokens := currentTokens + refillAmount
				if newTokens > effectiveLimit {
					newTokens = effectiveLimit
				}
				b.UsedTokens = effectiveLimit - newTokens
				if b.UsedTokens < 0 {
					b.UsedTokens = 0
				}
				b.LastRefillAt = now
			}
		}
	case model.AlgoSlidingWindow:
		if b.WindowStartAt.IsZero() {
			b.WindowStartAt = now
			b.UsedTokens = 0
		}
	}
}

func (m *Manager) refillLoop() {
	for {
		select {
		case <-m.stopCh:
			return
		case <-m.ticker.C:
			m.refillTick()
		}
	}
}

func (m *Manager) refillTick() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	dirty := false

	for _, b := range m.bindings {
		policy, ok := m.policies[b.PolicyName]
		if !ok {
			continue
		}
		changed := m.applyAlgorithmTick(b, policy, now)
		if changed {
			dirty = true
		}
	}

	if dirty {
		if err := m.persistDirtyLocked(); err != nil {
			log.Printf("[ratelimit-manager] persist error: %v", err)
		}
	}
}

func (m *Manager) applyAlgorithmTick(b *model.CallerBinding, policy *model.RateLimitPolicy, now time.Time) bool {
	switch policy.Algorithm {
	case model.AlgoFixedWindow:
		elapsed := now.Sub(b.WindowStartAt).Seconds()
		if elapsed >= float64(policy.WindowSec) {
			windowsPassed := int(elapsed) / policy.WindowSec
			b.WindowStartAt = b.WindowStartAt.Add(time.Duration(windowsPassed*policy.WindowSec) * time.Second)
			b.UsedTokens = 0
			b.UpdatedAt = now
			return true
		}
	case model.AlgoTokenBucket:
		elapsed := now.Sub(b.LastRefillAt).Seconds()
		if elapsed < 0.001 {
			return false
		}
		refillAmount := elapsed * policy.RefillRate
		if refillAmount < 1 {
			return false
		}
		effectiveLimit := b.QuotaLimit + b.BorrowedTokens - b.LentTokens
		if effectiveLimit < 0 {
			effectiveLimit = 0
		}
		currentTokens := float64(effectiveLimit - b.UsedTokens)
		newTokens := currentTokens + refillAmount
		if newTokens > float64(effectiveLimit) {
			newTokens = float64(effectiveLimit)
		}
		newUsed := int(float64(effectiveLimit) - newTokens)
		if newUsed < 0 {
			newUsed = 0
		}
		if newUsed != b.UsedTokens {
			b.UsedTokens = newUsed
			b.LastRefillAt = now
			b.UpdatedAt = now
			return true
		}
		b.LastRefillAt = now
		return true
	case model.AlgoSlidingWindow:
		elapsed := now.Sub(b.WindowStartAt).Seconds()
		if elapsed >= float64(policy.WindowSec) {
			decayFactor := 1.0 - (float64(policy.WindowSec) / elapsed)
			if decayFactor < 0 {
				decayFactor = 0
			}
			b.UsedTokens = int(float64(b.UsedTokens) * decayFactor)
			if b.UsedTokens < 0 {
				b.UsedTokens = 0
			}
			b.WindowStartAt = now
			b.UpdatedAt = now
			return true
		}
	}
	return false
}

func (m *Manager) persistAllLocked() error {
	for _, b := range m.bindings {
		if err := m.storage.UpdateCallerBinding(b); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) persistDirtyLocked() error {
	return m.persistAllLocked()
}

func (m *Manager) CreatePolicy(name string, algorithm model.AlgorithmType, windowSec int, maxTokens int, refillRate float64, refillUnit string) (*model.RateLimitPolicy, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.policies[name]; ok {
		return nil, fmt.Errorf("policy already exists: %s", name)
	}

	now := time.Now()
	p := &model.RateLimitPolicy{
		Name:       name,
		Algorithm:  algorithm,
		WindowSec:  windowSec,
		MaxTokens:  maxTokens,
		RefillRate: refillRate,
		RefillUnit: refillUnit,
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	if err := m.storage.CreatePolicy(p); err != nil {
		return nil, err
	}

	m.policies[name] = p
	log.Printf("[ratelimit] policy created: name=%s algo=%s", name, algorithm)
	return p, nil
}

func (m *Manager) GetPolicy(name string) (*model.RateLimitPolicy, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	p, ok := m.policies[name]
	if !ok {
		return nil, fmt.Errorf("policy not found: %s", name)
	}
	return p, nil
}

func (m *Manager) ListPolicies() ([]model.RateLimitPolicy, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	policies := make([]model.RateLimitPolicy, 0, len(m.policies))
	for _, p := range m.policies {
		policies = append(policies, *p)
	}
	return policies, nil
}

func (m *Manager) BindCaller(callerID string, policyName string, quotaLimit int) (*model.CallerBinding, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.policies[policyName]; !ok {
		return nil, fmt.Errorf("policy not found: %s", policyName)
	}

	now := time.Now()
	b := &model.CallerBinding{
		CallerID:   callerID,
		PolicyName: policyName,
		QuotaLimit: quotaLimit,
		UsedTokens: 0,
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	policy := m.policies[policyName]
	switch policy.Algorithm {
	case model.AlgoFixedWindow, model.AlgoSlidingWindow:
		b.WindowStartAt = now
	case model.AlgoTokenBucket:
		b.LastRefillAt = now
	}

	if err := m.storage.UpsertCallerBinding(b); err != nil {
		return nil, err
	}

	m.bindings[callerID] = b
	log.Printf("[ratelimit] caller bound: caller=%s policy=%s quota=%d", callerID, policyName, quotaLimit)
	return b, nil
}

func (m *Manager) RequestTokens(callerID string, tokens int) (*model.TokenResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	b, ok := m.bindings[callerID]
	if !ok {
		return nil, fmt.Errorf("caller not found: %s", callerID)
	}

	policy, ok := m.policies[b.PolicyName]
	if !ok {
		return nil, fmt.Errorf("policy not found: %s", b.PolicyName)
	}

	now := time.Now()
	m.applyAlgorithmTick(b, policy, now)

	effectiveLimit := b.QuotaLimit + b.BorrowedTokens - b.LentTokens
	if effectiveLimit < 0 {
		effectiveLimit = 0
	}

	remaining := effectiveLimit - b.UsedTokens
	if remaining < 0 {
		remaining = 0
	}

	result := &model.TokenResult{
		Requested:  tokens,
		QuotaLimit: b.QuotaLimit,
		UsedTokens: b.UsedTokens,
		Remaining:  remaining,
	}

	if remaining >= tokens {
		b.UsedTokens += tokens
		b.UpdatedAt = now
		result.Allowed = true
		result.Granted = tokens
		result.UsedTokens = b.UsedTokens
		result.Remaining = effectiveLimit - b.UsedTokens
	} else {
		result.Allowed = false
		result.Granted = 0
		result.Reason = fmt.Sprintf("insufficient quota: requested=%d, remaining=%d", tokens, remaining)
	}

	event := &model.RateLimitEvent{
		CallerID:   callerID,
		PolicyName: b.PolicyName,
		Requested:  tokens,
		Granted:    result.Granted,
		Allowed:    result.Allowed,
		Reason:     result.Reason,
		CreatedAt:  now,
	}
	_ = m.storage.AddRateLimitEvent(event)

	if err := m.storage.UpdateCallerBinding(b); err != nil {
		return nil, err
	}

	return result, nil
}

func (m *Manager) GetCallerStatus(callerID string) (*model.CallerStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	b, ok := m.bindings[callerID]
	if !ok {
		return nil, fmt.Errorf("caller not found: %s", callerID)
	}

	policy, ok := m.policies[b.PolicyName]
	if !ok {
		return nil, fmt.Errorf("policy not found: %s", b.PolicyName)
	}

	now := time.Now()
	m.applyAlgorithmTick(b, policy, now)

	effectiveLimit := b.QuotaLimit + b.BorrowedTokens - b.LentTokens
	if effectiveLimit < 0 {
		effectiveLimit = 0
	}
	remaining := effectiveLimit - b.UsedTokens
	if remaining < 0 {
		remaining = 0
	}

	rateLimited, _ := m.storage.CountRateLimited(callerID)

	status := &model.CallerStatus{
		CallerID:       b.CallerID,
		PolicyName:     b.PolicyName,
		Algorithm:      string(policy.Algorithm),
		QuotaLimit:     b.QuotaLimit,
		UsedTokens:     b.UsedTokens,
		Remaining:      remaining,
		BorrowedTokens: b.BorrowedTokens,
		LentTokens:     b.LentTokens,
		RateLimited:    rateLimited,
	}

	return status, nil
}

func (m *Manager) ListCallerStatuses() ([]model.CallerStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	var result []model.CallerStatus

	for _, b := range m.bindings {
		policy, ok := m.policies[b.PolicyName]
		if !ok {
			continue
		}
		m.applyAlgorithmTick(b, policy, now)

		effectiveLimit := b.QuotaLimit + b.BorrowedTokens - b.LentTokens
		if effectiveLimit < 0 {
			effectiveLimit = 0
		}
		remaining := effectiveLimit - b.UsedTokens
		if remaining < 0 {
			remaining = 0
		}

		rateLimited, _ := m.storage.CountRateLimited(b.CallerID)

		status := model.CallerStatus{
			CallerID:       b.CallerID,
			PolicyName:     b.PolicyName,
			Algorithm:      string(policy.Algorithm),
			QuotaLimit:     b.QuotaLimit,
			UsedTokens:     b.UsedTokens,
			Remaining:      remaining,
			BorrowedTokens: b.BorrowedTokens,
			LentTokens:     b.LentTokens,
			RateLimited:    rateLimited,
		}
		result = append(result, status)
	}

	return result, nil
}

func (m *Manager) BorrowQuota(fromCaller string, toCaller string, amount int) (*model.BorrowResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	fromB, ok := m.bindings[fromCaller]
	if !ok {
		return &model.BorrowResult{Success: false, Message: "from caller not found"}, nil
	}

	toB, ok := m.bindings[toCaller]
	if !ok {
		return &model.BorrowResult{Success: false, Message: "to caller not found"}, nil
	}

	fromPolicy, ok := m.policies[fromB.PolicyName]
	if ok {
		now := time.Now()
		m.applyAlgorithmTick(fromB, fromPolicy, now)
	}

	toPolicy, ok := m.policies[toB.PolicyName]
	if ok {
		now := time.Now()
		m.applyAlgorithmTick(toB, toPolicy, now)
	}

	fromEffectiveLimit := fromB.QuotaLimit + fromB.BorrowedTokens - fromB.LentTokens
	fromRemaining := fromEffectiveLimit - fromB.UsedTokens

	if fromRemaining < amount {
		return &model.BorrowResult{Success: false, Message: fmt.Sprintf("insufficient free quota: has %d, need %d", fromRemaining, amount)}, nil
	}

	now := time.Now()
	oldFromLent := fromB.LentTokens
	oldToBorrowed := toB.BorrowedTokens
	oldFromUpdated := fromB.UpdatedAt
	oldToUpdated := toB.UpdatedAt

	fromB.LentTokens += amount
	toB.BorrowedTokens += amount
	fromB.UpdatedAt = now
	toB.UpdatedAt = now

	record := &model.QuotaBorrowRecord{
		FromCaller: fromCaller,
		ToCaller:   toCaller,
		Amount:     amount,
		Status:     "active",
		CreatedAt:  now,
	}
	if err := m.storage.CreateBorrowRecord(record); err != nil {
		fromB.LentTokens = oldFromLent
		toB.BorrowedTokens = oldToBorrowed
		fromB.UpdatedAt = oldFromUpdated
		toB.UpdatedAt = oldToUpdated
		return nil, err
	}

	if err := m.storage.UpdateCallerBinding(fromB); err != nil {
		fromB.LentTokens = oldFromLent
		toB.BorrowedTokens = oldToBorrowed
		fromB.UpdatedAt = oldFromUpdated
		toB.UpdatedAt = oldToUpdated
		return nil, err
	}
	if err := m.storage.UpdateCallerBinding(toB); err != nil {
		fromB.LentTokens = oldFromLent
		toB.BorrowedTokens = oldToBorrowed
		fromB.UpdatedAt = oldFromUpdated
		toB.UpdatedAt = oldToUpdated
		return nil, err
	}

	log.Printf("[ratelimit] quota borrowed: from=%s to=%s amount=%d", fromCaller, toCaller, amount)
	return &model.BorrowResult{Success: true, Message: fmt.Sprintf("borrowed %d tokens from %s to %s", amount, fromCaller, toCaller)}, nil
}

func (m *Manager) ReturnQuota(fromCaller string, toCaller string, amount int) (*model.BorrowResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	fromB, ok := m.bindings[fromCaller]
	if !ok {
		return &model.BorrowResult{Success: false, Message: "from caller not found"}, nil
	}

	toB, ok := m.bindings[toCaller]
	if !ok {
		return &model.BorrowResult{Success: false, Message: "to caller not found"}, nil
	}

	if fromB.BorrowedTokens < amount {
		return &model.BorrowResult{Success: false, Message: fmt.Sprintf("borrowed quota less than amount: borrowed=%d, return=%d", fromB.BorrowedTokens, amount)}, nil
	}

	if toB.LentTokens < amount {
		return &model.BorrowResult{Success: false, Message: fmt.Sprintf("lent quota less than amount: lent=%d, return=%d", toB.LentTokens, amount)}, nil
	}

	now := time.Now()
	oldFromBorrowed := fromB.BorrowedTokens
	oldToLent := toB.LentTokens
	oldFromUsed := fromB.UsedTokens
	oldFromUpdated := fromB.UpdatedAt
	oldToUpdated := toB.UpdatedAt

	fromB.BorrowedTokens -= amount
	toB.LentTokens -= amount
	fromB.UpdatedAt = now
	toB.UpdatedAt = now

	if _, ok := m.policies[fromB.PolicyName]; ok {
		effectiveLimit := fromB.QuotaLimit + fromB.BorrowedTokens - fromB.LentTokens
		if effectiveLimit < 0 {
			effectiveLimit = 0
		}
		if fromB.UsedTokens > effectiveLimit {
			fromB.UsedTokens = effectiveLimit
		}
	}

	rollback := func() {
		fromB.BorrowedTokens = oldFromBorrowed
		toB.LentTokens = oldToLent
		fromB.UsedTokens = oldFromUsed
		fromB.UpdatedAt = oldFromUpdated
		toB.UpdatedAt = oldToUpdated
	}

	if err := m.storage.ReturnBorrow(fromCaller, toCaller, amount, now); err != nil {
		rollback()
		return nil, err
	}

	if err := m.storage.UpdateCallerBinding(fromB); err != nil {
		rollback()
		return nil, err
	}
	if err := m.storage.UpdateCallerBinding(toB); err != nil {
		rollback()
		return nil, err
	}

	log.Printf("[ratelimit] quota returned: from=%s to=%s amount=%d", fromCaller, toCaller, amount)
	return &model.BorrowResult{Success: true, Message: fmt.Sprintf("returned %d tokens from %s to %s", amount, fromCaller, toCaller)}, nil
}

func (m *Manager) AdjustQuota(callerID string, newQuotaLimit int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	b, ok := m.bindings[callerID]
	if !ok {
		return fmt.Errorf("caller not found: %s", callerID)
	}

	policy, ok := m.policies[b.PolicyName]
	if ok {
		now := time.Now()
		m.applyAlgorithmTick(b, policy, now)
	}

	b.QuotaLimit = newQuotaLimit
	b.UpdatedAt = time.Now()

	effectiveLimit := b.QuotaLimit + b.BorrowedTokens - b.LentTokens
	if effectiveLimit < 0 {
		effectiveLimit = 0
	}
	if b.UsedTokens > effectiveLimit {
		b.UsedTokens = effectiveLimit
	}

	if err := m.storage.UpdateCallerBinding(b); err != nil {
		return err
	}

	log.Printf("[ratelimit] quota adjusted: caller=%s new_quota=%d", callerID, newQuotaLimit)
	return nil
}

func (m *Manager) GetGlobalStats() (*model.GlobalStats, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	total, allowed, err := m.storage.CountAllEvents()
	if err != nil {
		return nil, err
	}

	borrows, err := m.storage.ListActiveBorrows()
	if err != nil {
		return nil, err
	}

	borrowedAmount := 0
	for _, b := range borrows {
		borrowedAmount += b.Amount
	}

	stats := &model.GlobalStats{
		TotalCallers:     len(m.bindings),
		TotalPolicies:    len(m.policies),
		TotalRequests:    total,
		TotalAllowed:     allowed,
		TotalRateLimited: total - allowed,
		ActiveBorrows:    len(borrows),
		BorrowedAmount:   borrowedAmount,
	}

	return stats, nil
}

func (m *Manager) GetCallerHistory(callerID string, limit int) ([]model.RateLimitEvent, error) {
	return m.storage.ListRateLimitEvents(callerID, limit)
}

func (m *Manager) ListBorrowRecords() ([]model.QuotaBorrowRecord, error) {
	return m.storage.ListActiveBorrows()
}
