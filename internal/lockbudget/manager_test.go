package lockbudget

import (
	"os"
	"rtm-107/internal/model"
	"rtm-107/internal/storage"
	"testing"
	"time"
)

func setupTestManager(t *testing.T) (*Manager, func()) {
	t.Helper()

	tmpFile, err := os.CreateTemp("", "lockbudget_test_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	tmpPath := tmpFile.Name()
	tmpFile.Close()

	s, err := storage.New(tmpPath)
	if err != nil {
		os.Remove(tmpPath)
		t.Fatalf("failed to create storage: %v", err)
	}

	m := NewManager(s)
	if err := m.Start(); err != nil {
		s.Close()
		os.Remove(tmpPath)
		t.Fatalf("failed to start manager: %v", err)
	}

	cleanup := func() {
		m.Stop()
		s.Close()
		os.Remove(tmpPath)
	}

	return m, cleanup
}

func TestSetAndGetConfig(t *testing.T) {
	m, cleanup := setupTestManager(t)
	defer cleanup()

	cfg, err := m.SetConfig("caller-a", 500, 60, 80)
	if err != nil {
		t.Fatalf("SetConfig failed: %v", err)
	}
	if cfg.CallerID != "caller-a" {
		t.Errorf("expected caller-a, got %s", cfg.CallerID)
	}
	if cfg.BudgetLimit != 500 {
		t.Errorf("expected limit 500, got %d", cfg.BudgetLimit)
	}
	if cfg.PeriodSec != 60 {
		t.Errorf("expected period 60, got %d", cfg.PeriodSec)
	}
	if cfg.WarningPct != 80 {
		t.Errorf("expected warning 80, got %d", cfg.WarningPct)
	}

	got, err := m.GetConfig("caller-a")
	if err != nil {
		t.Fatalf("GetConfig failed: %v", err)
	}
	if got == nil {
		t.Fatal("expected config, got nil")
	}
	if got.BudgetLimit != 500 {
		t.Errorf("expected limit 500, got %d", got.BudgetLimit)
	}
}

func TestListConfigs(t *testing.T) {
	m, cleanup := setupTestManager(t)
	defer cleanup()

	_, err := m.SetConfig("caller-a", 500, 60, 80)
	if err != nil {
		t.Fatalf("SetConfig caller-a failed: %v", err)
	}
	_, err = m.SetConfig("caller-b", 300, 30, 70)
	if err != nil {
		t.Fatalf("SetConfig caller-b failed: %v", err)
	}

	configs, err := m.ListConfigs()
	if err != nil {
		t.Fatalf("ListConfigs failed: %v", err)
	}
	if len(configs) != 2 {
		t.Errorf("expected 2 configs, got %d", len(configs))
	}
}

func TestDeleteConfig(t *testing.T) {
	m, cleanup := setupTestManager(t)
	defer cleanup()

	_, err := m.SetConfig("caller-a", 500, 60, 80)
	if err != nil {
		t.Fatalf("SetConfig failed: %v", err)
	}

	if err := m.DeleteConfig("caller-a"); err != nil {
		t.Fatalf("DeleteConfig failed: %v", err)
	}

	got, err := m.GetConfig("caller-a")
	if err != nil {
		t.Fatalf("GetConfig after delete failed: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil config after delete, got %+v", got)
	}
}

func TestCheckAcquireNoBudget(t *testing.T) {
	m, cleanup := setupTestManager(t)
	defer cleanup()

	result, err := m.CheckAcquire("no-budget-caller", "lock-1", 60)
	if err != nil {
		t.Fatalf("CheckAcquire failed: %v", err)
	}
	if !result.Allowed {
		t.Errorf("expected allowed for caller without budget, got rejected: %s", result.Reason)
	}
}

func TestCheckAcquireWithinBudget(t *testing.T) {
	m, cleanup := setupTestManager(t)
	defer cleanup()

	_, err := m.SetConfig("caller-a", 500, 60, 80)
	if err != nil {
		t.Fatalf("SetConfig failed: %v", err)
	}

	result, err := m.CheckAcquire("caller-a", "lock-1", 60)
	if err != nil {
		t.Fatalf("CheckAcquire failed: %v", err)
	}
	if !result.Allowed {
		t.Errorf("expected allowed within budget, got rejected: %s", result.Reason)
	}
	if result.BudgetLimit != 500 {
		t.Errorf("expected budget limit 500, got %d", result.BudgetLimit)
	}
	if result.ConsumedUnits != 0 {
		t.Errorf("expected 0 consumed units at start, got %d", result.ConsumedUnits)
	}
}

func TestHoldingAndReleaseMetering(t *testing.T) {
	m, cleanup := setupTestManager(t)
	defer cleanup()

	_, err := m.SetConfig("caller-a", 500, 60, 80)
	if err != nil {
		t.Fatalf("SetConfig failed: %v", err)
	}

	now := time.Now()
	acquiredAt := now
	expiresAt := now.Add(30 * time.Second)

	if err := m.StartHolding("caller-a", "lock-1", acquiredAt, expiresAt); err != nil {
		t.Fatalf("StartHolding failed: %v", err)
	}

	status, err := m.GetStatus("caller-a")
	if err != nil {
		t.Fatalf("GetStatus failed: %v", err)
	}
	if status == nil {
		t.Fatal("expected status, got nil")
	}
	if status.ActiveLocks != 1 {
		t.Errorf("expected 1 active lock, got %d", status.ActiveLocks)
	}

	time.Sleep(2500 * time.Millisecond)

	releasedAt := time.Now()
	totalUnits, err := m.StopHolding("caller-a", "lock-1", releasedAt)
	if err != nil {
		t.Fatalf("StopHolding failed: %v", err)
	}
	if totalUnits < 2 {
		t.Errorf("expected at least 2 units after 2.5s hold, got %d", totalUnits)
	}

	status, err = m.GetStatus("caller-a")
	if err != nil {
		t.Fatalf("GetStatus after release failed: %v", err)
	}
	if status.ConsumedUnits < 2 {
		t.Errorf("expected at least 2 consumed units, got %d", status.ConsumedUnits)
	}
	if status.RemainingUnits > 498 {
		t.Errorf("expected remaining <= 498, got %d", status.RemainingUnits)
	}
}

func TestConcurrentHoldingMetering(t *testing.T) {
	m, cleanup := setupTestManager(t)
	defer cleanup()

	_, err := m.SetConfig("caller-a", 100, 60, 80)
	if err != nil {
		t.Fatalf("SetConfig failed: %v", err)
	}

	now := time.Now()
	expiresAt := now.Add(60 * time.Second)

	if err := m.StartHolding("caller-a", "lock-1", now, expiresAt); err != nil {
		t.Fatalf("StartHolding lock-1 failed: %v", err)
	}
	if err := m.StartHolding("caller-a", "lock-2", now, expiresAt); err != nil {
		t.Fatalf("StartHolding lock-2 failed: %v", err)
	}
	if err := m.StartHolding("caller-a", "lock-3", now, expiresAt); err != nil {
		t.Fatalf("StartHolding lock-3 failed: %v", err)
	}

	status, err := m.GetStatus("caller-a")
	if err != nil {
		t.Fatalf("GetStatus failed: %v", err)
	}
	if status.ActiveLocks != 3 {
		t.Errorf("expected 3 active locks, got %d", status.ActiveLocks)
	}

	time.Sleep(2500 * time.Millisecond)

	status, err = m.GetStatus("caller-a")
	if err != nil {
		t.Fatalf("GetStatus after wait failed: %v", err)
	}

	expectedMinConsumed := 3 * 2
	if status.ConsumedUnits < expectedMinConsumed {
		t.Errorf("expected at least %d consumed units (3 locks x 2+ sec), got %d",
			expectedMinConsumed, status.ConsumedUnits)
	}
}

func TestBudgetExhaustion(t *testing.T) {
	m, cleanup := setupTestManager(t)
	defer cleanup()

	_, err := m.SetConfig("caller-a", 5, 60, 80)
	if err != nil {
		t.Fatalf("SetConfig failed: %v", err)
	}

	now := time.Now()
	expiresAt := now.Add(60 * time.Second)
	if err := m.StartHolding("caller-a", "lock-1", now, expiresAt); err != nil {
		t.Fatalf("StartHolding failed: %v", err)
	}

	time.Sleep(6500 * time.Millisecond)

	result, err := m.CheckAcquire("caller-a", "lock-2", 30)
	if err != nil {
		t.Fatalf("CheckAcquire failed: %v", err)
	}
	if result.Allowed {
		t.Errorf("expected reject after budget exhaustion, got allowed. consumed=%d limit=%d",
			result.ConsumedUnits, result.BudgetLimit)
	}
	if !result.BudgetRejected() {
		t.Error("expected BudgetRejected true")
	}

	events, err := m.ListExhaustEvents("caller-a", 10)
	if err != nil {
		t.Fatalf("ListExhaustEvents failed: %v", err)
	}
	if len(events) < 1 {
		t.Error("expected at least 1 exhaust event, got 0")
	}
}

func TestRenewHolding(t *testing.T) {
	m, cleanup := setupTestManager(t)
	defer cleanup()

	_, err := m.SetConfig("caller-a", 500, 60, 80)
	if err != nil {
		t.Fatalf("SetConfig failed: %v", err)
	}

	now := time.Now()
	expiresAt := now.Add(10 * time.Second)
	if err := m.StartHolding("caller-a", "lock-1", now, expiresAt); err != nil {
		t.Fatalf("StartHolding failed: %v", err)
	}

	newExpiresAt := now.Add(60 * time.Second)
	if err := m.RenewHolding("caller-a", "lock-1", newExpiresAt); err != nil {
		t.Fatalf("RenewHolding failed: %v", err)
	}

	info, err := m.GetCallerStatus("caller-a")
	if err != nil {
		t.Fatalf("GetCallerStatus failed: %v", err)
	}
	found := false
	for _, h := range info.HeldLocks {
		if h.LockName == "lock-1" {
			found = true
			if !h.ExpiresAt.Equal(newExpiresAt) {
				t.Errorf("expected expires at %v, got %v", newExpiresAt, h.ExpiresAt)
			}
			break
		}
	}
	if !found {
		t.Error("lock-1 not found in held locks after renew")
	}
}

func TestListStatuses(t *testing.T) {
	m, cleanup := setupTestManager(t)
	defer cleanup()

	_, err := m.SetConfig("caller-a", 500, 60, 80)
	if err != nil {
		t.Fatalf("SetConfig caller-a failed: %v", err)
	}
	_, err = m.SetConfig("caller-b", 300, 30, 70)
	if err != nil {
		t.Fatalf("SetConfig caller-b failed: %v", err)
	}

	statuses, err := m.ListStatuses()
	if err != nil {
		t.Fatalf("ListStatuses failed: %v", err)
	}
	if len(statuses) != 2 {
		t.Errorf("expected 2 statuses, got %d", len(statuses))
	}
}

func TestListHeldLocks(t *testing.T) {
	m, cleanup := setupTestManager(t)
	defer cleanup()

	_, err := m.SetConfig("caller-a", 500, 60, 80)
	if err != nil {
		t.Fatalf("SetConfig failed: %v", err)
	}

	now := time.Now()
	expiresAt := now.Add(60 * time.Second)
	if err := m.StartHolding("caller-a", "lock-1", now, expiresAt); err != nil {
		t.Fatalf("StartHolding lock-1 failed: %v", err)
	}
	if err := m.StartHolding("caller-a", "lock-2", now, expiresAt); err != nil {
		t.Fatalf("StartHolding lock-2 failed: %v", err)
	}

	holdings, err := m.ListHeldLocks("caller-a", time.Now())
	if err != nil {
		t.Fatalf("ListHeldLocks failed: %v", err)
	}
	if len(holdings) != 2 {
		t.Errorf("expected 2 held locks, got %d", len(holdings))
	}
}

func TestGlobalStats(t *testing.T) {
	m, cleanup := setupTestManager(t)
	defer cleanup()

	_, err := m.SetConfig("caller-a", 500, 60, 80)
	if err != nil {
		t.Fatalf("SetConfig caller-a failed: %v", err)
	}
	_, err = m.SetConfig("caller-b", 300, 30, 70)
	if err != nil {
		t.Fatalf("SetConfig caller-b failed: %v", err)
	}

	now := time.Now()
	expiresAt := now.Add(60 * time.Second)
	if err := m.StartHolding("caller-a", "lock-1", now, expiresAt); err != nil {
		t.Fatalf("StartHolding failed: %v", err)
	}

	stats, err := m.GetGlobalStats()
	if err != nil {
		t.Fatalf("GetGlobalStats failed: %v", err)
	}
	if stats.TotalCallers != 2 {
		t.Errorf("expected 2 total callers, got %d", stats.TotalCallers)
	}
	if stats.TotalActiveLocks != 1 {
		t.Errorf("expected 1 total active locks, got %d", stats.TotalActiveLocks)
	}
}

func TestHasBudget(t *testing.T) {
	m, cleanup := setupTestManager(t)
	defer cleanup()

	if m.HasBudget("caller-unknown") {
		t.Error("expected false for unknown caller")
	}

	_, err := m.SetConfig("caller-a", 500, 60, 80)
	if err != nil {
		t.Fatalf("SetConfig failed: %v", err)
	}

	if !m.HasBudget("caller-a") {
		t.Error("expected true for configured caller")
	}
}

func TestPeriodReset(t *testing.T) {
	m, cleanup := setupTestManager(t)
	defer cleanup()

	_, err := m.SetConfig("caller-a", 100, 2, 80)
	if err != nil {
		t.Fatalf("SetConfig failed: %v", err)
	}

	now := time.Now()
	expiresAt := now.Add(60 * time.Second)
	if err := m.StartHolding("caller-a", "lock-1", now, expiresAt); err != nil {
		t.Fatalf("StartHolding failed: %v", err)
	}

	time.Sleep(1100 * time.Millisecond)
	status1, err := m.GetStatus("caller-a")
	if err != nil {
		t.Fatalf("GetStatus first period failed: %v", err)
	}
	if status1.ConsumedUnits < 1 {
		t.Errorf("expected >= 1 consumed units in first period, got %d", status1.ConsumedUnits)
	}

	time.Sleep(2100 * time.Millisecond)
	status2, err := m.GetStatus("caller-a")
	if err != nil {
		t.Fatalf("GetStatus after reset failed: %v", err)
	}
	if !status2.PeriodStartAt.After(status1.PeriodStartAt) {
		t.Errorf("expected new period start after old, old=%v new=%v",
			status1.PeriodStartAt, status2.PeriodStartAt)
	}
}

func TestBudgetAcquireCheckResultMethods(t *testing.T) {
	result := &model.BudgetAcquireCheckResult{
		Allowed:        true,
		ConsumedUnits:  100,
		RemainingUnits: 400,
		BudgetLimit:    500,
		Reason:         "ok",
	}
	if result.BudgetRejected() {
		t.Error("expected BudgetRejected false for allowed result")
	}

	result2 := &model.BudgetAcquireCheckResult{
		Allowed:        false,
		ConsumedUnits:  500,
		RemainingUnits: 0,
		BudgetLimit:    500,
		Reason:         "exhausted",
	}
	if !result2.BudgetRejected() {
		t.Error("expected BudgetRejected true for rejected result")
	}
}
