package shadow

import (
	"fmt"
	"log"
	"rtm-107/internal/audit"
	"rtm-107/internal/lock"
	"rtm-107/internal/model"
	"rtm-107/internal/ratelimit"
	"rtm-107/internal/storage"
	"strconv"
	"sync"
	"time"
)

type Manager struct {
	storage      *storage.Storage
	lockMgr      *lock.Manager
	rateLimitMgr *ratelimit.Manager
	auditMgr     *audit.Manager

	mu       sync.Mutex
	plans    map[int64]*model.ShadowPlan
	stopCh   chan struct{}
	ticker   *time.Ticker
}

func NewManager(s *storage.Storage, lm *lock.Manager, rlm *ratelimit.Manager, am *audit.Manager) *Manager {
	return &Manager{
		storage:      s,
		lockMgr:      lm,
		rateLimitMgr: rlm,
		auditMgr:     am,
		plans:        make(map[int64]*model.ShadowPlan),
		stopCh:       make(chan struct{}),
	}
}

func (m *Manager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	plans, err := m.storage.ListShadowPlans()
	if err != nil {
		return fmt.Errorf("load shadow plans: %w", err)
	}
	for i := range plans {
		p := &plans[i]
		m.plans[p.ID] = p
		if p.Status == model.ShadowPlanStatusRunning {
			log.Printf("[shadow] recovering running plan: id=%d name=%s", p.ID, p.Name)
		}
	}

	m.ticker = time.NewTicker(1 * time.Second)
	go m.mirrorLoop()

	log.Println("[shadow-manager] started")
	return nil
}

func (m *Manager) Stop() {
	close(m.stopCh)
	if m.ticker != nil {
		m.ticker.Stop()
	}
	log.Println("[shadow-manager] stopped")
}

func (m *Manager) CreatePlan(name, description, mode string, mirrorSec int) (*model.ShadowPlan, error) {
	if mode != "replay" && mode != "mirror" {
		return nil, fmt.Errorf("mode must be 'replay' or 'mirror'")
	}

	now := time.Now()
	plan := &model.ShadowPlan{
		Name:        name,
		Description: description,
		Status:      model.ShadowPlanStatusDraft,
		Mode:        mode,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	if mode == "replay" {
		minID, maxID, err := m.storage.GetAuditLogRange()
		if err != nil {
			return nil, fmt.Errorf("get audit log range: %w", err)
		}
		if maxID == 0 {
			return nil, fmt.Errorf("no audit logs available for replay")
		}
		plan.AuditLogStartID = minID
		plan.AuditLogEndID = maxID
	}

	if err := m.storage.CreateShadowPlan(plan); err != nil {
		return nil, err
	}

	if mode == "mirror" && mirrorSec > 0 {
		plan.MirrorUntil = now.Add(time.Duration(mirrorSec) * time.Second)
		if err := m.storage.UpdateShadowPlanMirror(plan.ID, plan.MirrorUntil, now); err != nil {
			return nil, err
		}
	}

	m.mu.Lock()
	m.plans[plan.ID] = plan
	m.mu.Unlock()

	log.Printf("[shadow] created plan: id=%d name=%s mode=%s", plan.ID, plan.Name, plan.Mode)
	return plan, nil
}

func (m *Manager) GetPlan(id int64) (*model.ShadowPlan, error) {
	return m.storage.GetShadowPlan(id)
}

func (m *Manager) ListPlans() ([]model.ShadowPlan, error) {
	return m.storage.ListShadowPlans()
}

func (m *Manager) AddOverride(planID int64, category model.ShadowRuleCategory, targetKey, field, newValue string) (*model.ShadowConfigOverride, error) {
	plan, err := m.storage.GetShadowPlan(planID)
	if err != nil {
		return nil, err
	}
	if plan == nil {
		return nil, fmt.Errorf("plan not found")
	}
	if plan.Status != model.ShadowPlanStatusDraft {
		return nil, fmt.Errorf("can only add overrides to draft plans")
	}

	origValue := m.getCurrentValue(category, targetKey, field)

	ov := &model.ShadowConfigOverride{
		PlanID:    planID,
		Category:  category,
		TargetKey: targetKey,
		Field:     field,
		OrigValue: origValue,
		NewValue:  newValue,
		CreatedAt: time.Now(),
	}
	if err := m.storage.CreateShadowConfigOverride(ov); err != nil {
		return nil, err
	}
	log.Printf("[shadow] added override: plan=%d category=%s target=%s field=%s %s->%s",
		planID, category, targetKey, field, origValue, newValue)
	return ov, nil
}

func (m *Manager) ListOverrides(planID int64) ([]model.ShadowConfigOverride, error) {
	return m.storage.ListShadowConfigOverrides(planID)
}

func (m *Manager) RemoveOverride(id int64) error {
	return m.storage.DeleteShadowConfigOverride(id)
}

func (m *Manager) StartPlan(planID int64) error {
	plan, err := m.storage.GetShadowPlan(planID)
	if err != nil {
		return err
	}
	if plan == nil {
		return fmt.Errorf("plan not found")
	}
	if plan.Status != model.ShadowPlanStatusDraft {
		return fmt.Errorf("plan must be in draft status to start")
	}

	now := time.Now()
	if err := m.storage.UpdateShadowPlanStatus(planID, model.ShadowPlanStatusRunning, now); err != nil {
		return err
	}
	plan.Status = model.ShadowPlanStatusRunning
	plan.UpdatedAt = now

	m.mu.Lock()
	m.plans[planID] = plan
	m.mu.Unlock()

	if plan.Mode == "replay" {
		go m.runReplay(planID)
	}

	log.Printf("[shadow] started plan: id=%d name=%s mode=%s", planID, plan.Name, plan.Mode)
	return nil
}

func (m *Manager) CancelPlan(planID int64) error {
	plan, err := m.storage.GetShadowPlan(planID)
	if err != nil {
		return err
	}
	if plan == nil {
		return fmt.Errorf("plan not found")
	}
	if plan.Status != model.ShadowPlanStatusRunning && plan.Status != model.ShadowPlanStatusDraft {
		return fmt.Errorf("plan cannot be cancelled in current status: %s", plan.Status)
	}

	now := time.Now()
	if err := m.storage.UpdateShadowPlanStatus(planID, model.ShadowPlanStatusCancelled, now); err != nil {
		return err
	}

	m.mu.Lock()
	if p, ok := m.plans[planID]; ok {
		p.Status = model.ShadowPlanStatusCancelled
		p.UpdatedAt = now
	}
	m.mu.Unlock()

	log.Printf("[shadow] cancelled plan: id=%d", planID)
	return nil
}

func (m *Manager) ApplyPlan(planID int64) error {
	plan, err := m.storage.GetShadowPlan(planID)
	if err != nil {
		return err
	}
	if plan == nil {
		return fmt.Errorf("plan not found")
	}
	if plan.Status != model.ShadowPlanStatusCompleted && plan.Status != model.ShadowPlanStatusRunning {
		return fmt.Errorf("plan must be completed or running to apply")
	}

	overrides, err := m.storage.ListShadowConfigOverrides(planID)
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for _, ov := range overrides {
		if err := m.applyOverride(ov); err != nil {
			return fmt.Errorf("apply override %d: %w", ov.ID, err)
		}
	}

	now := time.Now()
	if err := m.storage.UpdateShadowPlanApplied(planID, now, now); err != nil {
		return err
	}

	if p, ok := m.plans[planID]; ok {
		p.Status = model.ShadowPlanStatusApplied
		p.AppliedAt = now
		p.UpdatedAt = now
	}

	log.Printf("[shadow] applied plan: id=%d name=%s - all overrides are now live", planID, plan.Name)
	return nil
}

func (m *Manager) GetDiffRecords(planID int64, limit int) ([]model.ShadowDiffRecord, error) {
	return m.storage.ListShadowDiffRecords(planID, limit)
}

func (m *Manager) GetDiffStats(planID int64) (*model.ShadowDiffStats, error) {
	return m.storage.GetShadowDiffStats(planID)
}

func (m *Manager) getCurrentValue(category model.ShadowRuleCategory, targetKey, field string) string {
	switch category {
	case model.ShadowRuleCircuitBreaker:
		rule, err := m.storage.GetCircuitBreakerRule(targetKey)
		if err != nil || rule == nil {
			return ""
		}
		switch field {
		case "failure_threshold":
			return strconv.Itoa(rule.FailureThreshold)
		case "window_sec":
			return strconv.Itoa(rule.WindowSec)
		case "cooldown_sec":
			return strconv.Itoa(rule.CooldownSec)
		}
	case model.ShadowRuleRateLimit:
		binding, err := m.storage.GetCallerBinding(targetKey)
		if err != nil || binding == nil {
			return ""
		}
		switch field {
		case "quota_limit":
			return strconv.Itoa(binding.QuotaLimit)
		}
		policy, err := m.storage.GetPolicy(binding.PolicyName)
		if err != nil || policy == nil {
			return ""
		}
		switch field {
		case "max_tokens":
			return strconv.Itoa(policy.MaxTokens)
		}
	case model.ShadowRuleReservation:
		return ""
	case model.ShadowRuleLockDependency:
		return ""
	}
	return ""
}

func (m *Manager) runReplay(planID int64) {
	plan, err := m.storage.GetShadowPlan(planID)
	if err != nil || plan == nil {
		log.Printf("[shadow] replay failed to load plan %d: %v", planID, err)
		return
	}

	overrides, err := m.storage.ListShadowConfigOverrides(planID)
	if err != nil {
		log.Printf("[shadow] replay failed to load overrides for plan %d: %v", planID, err)
		return
	}

	logs, err := m.storage.ListAuditLogsInRange(plan.AuditLogStartID, plan.AuditLogEndID)
	if err != nil {
		log.Printf("[shadow] replay failed to load audit logs for plan %d: %v", planID, err)
		return
	}

	log.Printf("[shadow] starting replay for plan %d: %d audit logs to evaluate", planID, len(logs))

	for _, auditLog := range logs {
		liveDecision := m.classifyLiveDecision(auditLog)
		shadowDecision := m.classifyShadowDecision(auditLog, overrides)

		if liveDecision != shadowDecision {
			cat := m.classifyRuleCategory(auditLog, overrides)
			detail := m.buildDiffDetail(auditLog, liveDecision, shadowDecision, overrides)

			diff := &model.ShadowDiffRecord{
				PlanID:          planID,
				AuditLogID:      auditLog.ID,
				RequestCaller:   auditLog.Caller,
				RequestOp:       string(auditLog.Operation),
				RequestResource: auditLog.Resource,
				LiveDecision:    liveDecision,
				ShadowDecision:  shadowDecision,
				RuleCategory:    cat,
				Detail:          detail,
				CreatedAt:       time.Now(),
			}
			if err := m.storage.CreateShadowDiffRecord(diff); err != nil {
				log.Printf("[shadow] failed to record diff: %v", err)
			}
		}
	}

	now := time.Now()
	if err := m.storage.UpdateShadowPlanStatus(planID, model.ShadowPlanStatusCompleted, now); err != nil {
		log.Printf("[shadow] failed to update plan status: %v", err)
	}

	m.mu.Lock()
	if p, ok := m.plans[planID]; ok {
		p.Status = model.ShadowPlanStatusCompleted
		p.UpdatedAt = now
	}
	m.mu.Unlock()

	cnt, _ := m.storage.CountShadowDiffs(planID)
	log.Printf("[shadow] replay completed for plan %d: %d differences found", planID, cnt)
}

func (m *Manager) classifyLiveDecision(al model.AuditLog) model.ShadowDecision {
	if al.Success {
		return model.ShadowDecisionAdmit
	}
	switch {
	case al.FailReason == "circuit_breaker_open" || al.FailReason == audit.ErrCircuitBreakerOpen.Error():
		return model.ShadowDecisionCircuitBreak
	case al.FailReason == "rate limited" || al.FailReason == "quota_exceeded":
		return model.ShadowDecisionRateLimit
	case al.FailReason == "not acquired":
		return model.ShadowDecisionWait
	case al.FailReason == "deadlock" || al.FailReason == "deadlock_detected":
		return model.ShadowDecisionDeadlockReject
	default:
		return model.ShadowDecisionReject
	}
}

func (m *Manager) classifyShadowDecision(al model.AuditLog, overrides []model.ShadowConfigOverride) model.ShadowDecision {
	liveDecision := m.classifyLiveDecision(al)

	for _, ov := range overrides {
		switch ov.Category {
		case model.ShadowRuleCircuitBreaker:
			if ov.TargetKey == al.Caller {
				if ov.Field == "failure_threshold" {
					newThreshold, _ := strconv.Atoi(ov.NewValue)
					origThreshold, _ := strconv.Atoi(ov.OrigValue)
					if newThreshold < origThreshold && al.Success {
						failures, err := m.storage.CountFailuresInWindow(al.Caller, 10, time.Now())
						if err == nil && failures >= newThreshold {
							return model.ShadowDecisionCircuitBreak
						}
					}
					if newThreshold > origThreshold && liveDecision == model.ShadowDecisionCircuitBreak {
						return model.ShadowDecisionAdmit
					}
				}
			}
		case model.ShadowRuleRateLimit:
			if ov.TargetKey == al.Caller {
				if ov.Field == "quota_limit" || ov.Field == "max_tokens" {
					newLimit, _ := strconv.Atoi(ov.NewValue)
					origLimit, _ := strconv.Atoi(ov.OrigValue)
					if newLimit < origLimit && liveDecision == model.ShadowDecisionAdmit {
						binding, err := m.storage.GetCallerBinding(al.Caller)
						if err == nil && binding != nil {
							usedRatio := float64(binding.UsedTokens) / float64(newLimit)
							if usedRatio >= 1.0 {
								return model.ShadowDecisionRateLimit
							}
						}
					}
					if newLimit > origLimit && liveDecision == model.ShadowDecisionRateLimit {
						return model.ShadowDecisionAdmit
					}
				}
			}
		}
	}

	return liveDecision
}

func (m *Manager) classifyRuleCategory(al model.AuditLog, overrides []model.ShadowConfigOverride) model.ShadowRuleCategory {
	for _, ov := range overrides {
		if ov.TargetKey == al.Caller || ov.TargetKey == al.Resource {
			return ov.Category
		}
	}
	if al.FailReason == "circuit_breaker_open" || al.FailReason == audit.ErrCircuitBreakerOpen.Error() {
		return model.ShadowRuleCircuitBreaker
	}
	if al.FailReason == "rate limited" || al.FailReason == "quota_exceeded" {
		return model.ShadowRuleRateLimit
	}
	return model.ShadowRuleLockDependency
}

func (m *Manager) buildDiffDetail(al model.AuditLog, live, shadow model.ShadowDecision, overrides []model.ShadowConfigOverride) string {
	for _, ov := range overrides {
		if ov.TargetKey == al.Caller || ov.TargetKey == al.Resource {
			return fmt.Sprintf("rule %s/%s.%s changed from %s to %s causes %s->%s",
				ov.Category, ov.TargetKey, ov.Field, ov.OrigValue, ov.NewValue, live, shadow)
		}
	}
	return fmt.Sprintf("live=%s shadow=%s for caller=%s op=%s resource=%s",
		live, shadow, al.Caller, al.Operation, al.Resource)
}

func (m *Manager) applyOverride(ov model.ShadowConfigOverride) error {
	switch ov.Category {
	case model.ShadowRuleCircuitBreaker:
		rule, err := m.storage.GetCircuitBreakerRule(ov.TargetKey)
		if err != nil || rule == nil {
			return fmt.Errorf("circuit breaker rule not found: %s", ov.TargetKey)
		}
		newVal, err := strconv.Atoi(ov.NewValue)
		if err != nil {
			return fmt.Errorf("invalid new value: %s", ov.NewValue)
		}
		switch ov.Field {
		case "failure_threshold":
			rule.FailureThreshold = newVal
		case "window_sec":
			rule.WindowSec = newVal
		case "cooldown_sec":
			rule.CooldownSec = newVal
		}
		_, err = m.auditMgr.SetCircuitBreakerRule(ov.TargetKey, rule.WindowSec, rule.FailureThreshold, rule.CooldownSec)
		return err
	case model.ShadowRuleRateLimit:
		binding, err := m.storage.GetCallerBinding(ov.TargetKey)
		if err != nil || binding == nil {
			return fmt.Errorf("caller binding not found: %s", ov.TargetKey)
		}
		newVal, err := strconv.Atoi(ov.NewValue)
		if err != nil {
			return fmt.Errorf("invalid new value: %s", ov.NewValue)
		}
		switch ov.Field {
		case "quota_limit":
			return m.auditMgr.AdjustQuota(ov.TargetKey, newVal)
		case "max_tokens":
			policy, err := m.storage.GetPolicy(binding.PolicyName)
			if err != nil || policy == nil {
				return fmt.Errorf("policy not found: %s", binding.PolicyName)
			}
			policy.MaxTokens = newVal
			_, err = m.rateLimitMgr.CreatePolicy(policy.Name, policy.Algorithm, policy.WindowSec, policy.MaxTokens, policy.RefillRate, policy.RefillUnit)
			return err
		}
	}
	return nil
}

func (m *Manager) RecordMirrorDiff(planID int64, caller, op, resource string, liveDecision, shadowDecision model.ShadowDecision, category model.ShadowRuleCategory, detail string) error {
	diff := &model.ShadowDiffRecord{
		PlanID:          planID,
		RequestCaller:   caller,
		RequestOp:       op,
		RequestResource: resource,
		LiveDecision:    liveDecision,
		ShadowDecision:  shadowDecision,
		RuleCategory:    category,
		Detail:          detail,
		CreatedAt:       time.Now(),
	}
	return m.storage.CreateShadowDiffRecord(diff)
}

func (m *Manager) GetRunningMirrorPlans() []*model.ShadowPlan {
	m.mu.Lock()
	defer m.mu.Unlock()
	var running []*model.ShadowPlan
	for _, p := range m.plans {
		if p.Status == model.ShadowPlanStatusRunning && p.Mode == "mirror" {
			running = append(running, p)
		}
	}
	return running
}

func (m *Manager) mirrorLoop() {
	for {
		select {
		case <-m.stopCh:
			return
		case <-m.ticker.C:
			m.checkMirrorExpiry()
		}
	}
}

func (m *Manager) checkMirrorExpiry() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	for _, p := range m.plans {
		if p.Status == model.ShadowPlanStatusRunning && p.Mode == "mirror" {
			if !p.MirrorUntil.IsZero() && now.After(p.MirrorUntil) {
				log.Printf("[shadow] mirror plan expired: id=%d name=%s", p.ID, p.Name)
				if err := m.storage.UpdateShadowPlanStatus(p.ID, model.ShadowPlanStatusCompleted, now); err != nil {
					log.Printf("[shadow] failed to update plan status: %v", err)
				}
				p.Status = model.ShadowPlanStatusCompleted
				p.UpdatedAt = now
			}
		}
	}
}

func (m *Manager) EvaluateShadow(caller, op, resource string, liveSuccess bool, liveFailReason string) {
	m.mu.Lock()
	runningMirror := make([]*model.ShadowPlan, 0)
	for _, p := range m.plans {
		if p.Status == model.ShadowPlanStatusRunning && p.Mode == "mirror" {
			runningMirror = append(runningMirror, p)
		}
	}
	m.mu.Unlock()

	if len(runningMirror) == 0 {
		return
	}

	liveDecision := m.classifyLiveDecision(model.AuditLog{
		Caller:     caller,
		Operation:  model.AuditOperationType(op),
		Resource:   resource,
		Success:    liveSuccess,
		FailReason: liveFailReason,
	})

	for _, plan := range runningMirror {
		overrides, err := m.storage.ListShadowConfigOverrides(plan.ID)
		if err != nil {
			continue
		}

		al := model.AuditLog{
			Caller:     caller,
			Operation:  model.AuditOperationType(op),
			Resource:   resource,
			Success:    liveSuccess,
			FailReason: liveFailReason,
		}
		shadowDecision := m.classifyShadowDecision(al, overrides)

		if liveDecision != shadowDecision {
			cat := m.classifyRuleCategory(al, overrides)
			detail := m.buildDiffDetail(al, liveDecision, shadowDecision, overrides)
			if err := m.RecordMirrorDiff(plan.ID, caller, op, resource, liveDecision, shadowDecision, cat, detail); err != nil {
				log.Printf("[shadow] failed to record mirror diff: %v", err)
			}
		}
	}
}
