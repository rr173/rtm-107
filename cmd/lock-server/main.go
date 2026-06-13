package main

import (
	"fmt"
	"log"
	"os"
	"rtm-107/internal/api"
	"rtm-107/internal/audit"
	"rtm-107/internal/lock"
	"rtm-107/internal/model"
	"rtm-107/internal/orchestration"
	"rtm-107/internal/ratelimit"
	"rtm-107/internal/shadow"
	"rtm-107/internal/storage"
	"rtm-107/internal/topology"
	"time"

	"github.com/gin-gonic/gin"
)

func main() {
	dbPath := os.Getenv("DB_PATH")
	if dbPath == "" {
		dbPath = "./data/locks.db"
	}

	if err := os.MkdirAll("./data", 0755); err != nil {
		log.Fatalf("create data dir: %v", err)
	}

	s, err := storage.New(dbPath)
	if err != nil {
		log.Fatalf("init storage: %v", err)
	}
	defer s.Close()

	mgr := lock.NewManager(s)
	if err := mgr.Start(); err != nil {
		log.Fatalf("start lock manager: %v", err)
	}
	defer mgr.Stop()

	rlMgr := ratelimit.NewManager(s)
	if err := rlMgr.Start(); err != nil {
		log.Fatalf("start rate limit manager: %v", err)
	}
	defer rlMgr.Stop()

	orchMgr := orchestration.NewManager(s, mgr, rlMgr)
	if err := orchMgr.Start(); err != nil {
		log.Fatalf("start orchestration manager: %v", err)
	}
	defer orchMgr.Stop()

	auditMgr := audit.NewManager(s, mgr, rlMgr)
	if err := auditMgr.Start(); err != nil {
		log.Fatalf("start audit manager: %v", err)
	}
	defer auditMgr.Stop()

	topoMgr := topology.NewManager(s, mgr, rlMgr)
	if err := topoMgr.Start(); err != nil {
		log.Fatalf("start topology manager: %v", err)
	}
	defer topoMgr.Stop()

	shadowMgr := shadow.NewManager(s, mgr, rlMgr, auditMgr)
	if err := shadowMgr.Start(); err != nil {
		log.Fatalf("start shadow manager: %v", err)
	}
	defer shadowMgr.Stop()

	if err := seedDemoData(mgr, rlMgr); err != nil {
		log.Printf("seed demo data: %v", err)
	}

	if err := seedOrchDemoData(mgr, rlMgr, orchMgr); err != nil {
		log.Printf("seed orchestration demo data: %v", err)
	}

	if err := seedAuditDemoData(auditMgr, s); err != nil {
		log.Printf("seed audit demo data: %v", err)
	}

	if err := seedTopologyDemoData(topoMgr, rlMgr, mgr); err != nil {
		log.Printf("seed topology demo data: %v", err)
	}

	if err := seedShadowDemoData(shadowMgr, s); err != nil {
		log.Printf("seed shadow demo data: %v", err)
	}

	r := gin.Default()

	r.Use(func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}
		c.Next()
	})

	handler := api.NewHandler(mgr, rlMgr, orchMgr, auditMgr, topoMgr, shadowMgr)
	handler.RegisterRoutes(r)

	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":8080"
	}

	log.Printf("server starting on %s", addr)
	if err := r.Run(addr); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

func seedDemoData(lockMgr *lock.Manager, rlMgr *ratelimit.Manager) error {
	locks, err := lockMgr.ListAllLocks()
	if err != nil {
		return err
	}
	if len(locks) > 0 {
		log.Println("[demo] lock data already exists, skipping seed")
	} else {
		log.Println("[demo] seeding lock demo data...")

		if _, err := lockMgr.AcquireLock("resource-a", "alice", 120, true); err != nil {
			return err
		}
		log.Println("[demo] acquired lock resource-a by alice (reentrant, 120s)")

		if _, err := lockMgr.AcquireLock("resource-b", "bob", 60, false); err != nil {
			return err
		}
		log.Println("[demo] acquired lock resource-b by bob (non-reentrant, 60s)")

		if result, err := lockMgr.AcquireLock("resource-a", "charlie", 30, false); err != nil {
			return err
		} else if result.Queued {
			log.Println("[demo] charlie queued for resource-a")
		}
	}

	policies, err := rlMgr.ListPolicies()
	if err != nil {
		return err
	}
	if len(policies) > 0 {
		log.Println("[demo] rate limit data already exists, skipping seed")
		return nil
	}

	log.Println("[demo] seeding rate limit demo data...")

	if _, err := rlMgr.CreatePolicy("token-bucket-policy", model.AlgoTokenBucket, 0, 100, 10.0, "per_second"); err != nil {
		return err
	}
	log.Println("[demo] created token-bucket-policy: max=100, refill=10/s")

	if _, err := rlMgr.CreatePolicy("sliding-window-policy", model.AlgoSlidingWindow, 60, 50, 0, ""); err != nil {
		return err
	}
	log.Println("[demo] created sliding-window-policy: window=60s, max=50")

	if _, err := rlMgr.CreatePolicy("fixed-window-policy", model.AlgoFixedWindow, 30, 30, 0, ""); err != nil {
		return err
	}
	log.Println("[demo] created fixed-window-policy: window=30s, max=30")

	if _, err := rlMgr.BindCaller("service-alpha", "token-bucket-policy", 100); err != nil {
		return err
	}
	log.Println("[demo] bound service-alpha to token-bucket-policy (quota=100)")

	if _, err := rlMgr.BindCaller("service-beta", "sliding-window-policy", 50); err != nil {
		return err
	}
	log.Println("[demo] bound service-beta to sliding-window-policy (quota=50)")

	if _, err := rlMgr.BindCaller("service-gamma", "fixed-window-policy", 30); err != nil {
		return err
	}
	log.Println("[demo] bound service-gamma to fixed-window-policy (quota=30)")

	if result, err := rlMgr.RequestTokens("service-alpha", 5, false, 0); err == nil && result.Allowed {
		log.Printf("[demo] service-alpha requested 5 tokens, granted, remaining=%d", result.Remaining)
	}

	if result, err := rlMgr.RequestTokens("service-beta", 3, false, 0); err == nil && result.Allowed {
		log.Printf("[demo] service-beta requested 3 tokens, granted, remaining=%d", result.Remaining)
	}

	log.Println("[demo] rate limit demo data seeded successfully")
	log.Println("[demo] tip: watch service-alpha's tokens refilling via GET /api/v1/ratelimit/callers/service-alpha")
	return nil
}

func seedOrchDemoData(lockMgr *lock.Manager, rlMgr *ratelimit.Manager, orchMgr *orchestration.Manager) error {
	existingTxs, err := orchMgr.ListTxs("")
	if err != nil {
		return err
	}
	if len(existingTxs) > 0 {
		log.Println("[demo-orch] orchestration data already exists, skipping seed")
		return nil
	}

	log.Println("[demo-orch] seeding orchestration demo data...")

	_, _ = lockMgr.ReleaseLock("resource-a", "alice")
	_, _ = lockMgr.ReleaseLock("resource-a", "charlie")
	_, _ = lockMgr.ReleaseLock("resource-a", "bob")
	log.Println("[demo-orch] ensured resource-a is free for orchestration demo")

	locks := []model.TxLockSpec{
		{LockName: "resource-a", LeaseSec: 300},
	}
	tokens := []model.TxTokenSpec{
		{CallerID: "service-alpha", Tokens: 10},
	}

	tx, err := orchMgr.CreateTx("demo-orch-holder", 300, locks, tokens)
	if err != nil {
		return err
	}

	if tx.Status == model.TxStatusCommitted {
		log.Printf("[demo-orch] created demo transaction: tx=%s status=%s", tx.ID, tx.Status)
		log.Printf("[demo-orch]   - holds lock: resource-a (lease 300s)")
		log.Printf("[demo-orch]   - consumed tokens: service-alpha x 10")
		log.Printf("[demo-orch]   - timeout: 300s")
		log.Println("[demo-orch] tip: query via GET /api/v1/orchestration/tx/" + tx.ID)
	} else {
		log.Printf("[demo-orch] demo tx not committed: status=%s reason=%s", tx.Status, tx.FailReason)
	}

	return nil
}

func seedAuditDemoData(auditMgr *audit.Manager, s *storage.Storage) error {
	existingRules, err := s.ListCircuitBreakerRules()
	if err != nil {
		return err
	}

	hasGammaRule := false
	for _, r := range existingRules {
		if r.CallerID == "service-gamma" {
			hasGammaRule = true
			break
		}
	}

	if !hasGammaRule {
		log.Println("[demo-audit] seeding audit & circuit breaker demo data...")

		if _, err := auditMgr.SetCircuitBreakerRule("service-gamma", 10, 3, 60); err != nil {
			return err
		}
		log.Println("[demo-audit] created circuit breaker rule for service-gamma: 10s window, 3 failures threshold, 60s cooldown")

		now := time.Now()
		twoHoursAgo := now.Add(-2 * time.Hour)
		oneHourAgo := now.Add(-1 * time.Hour)

		history := &model.CircuitBreakerHistory{
			CallerID:      "service-gamma",
			State:         "open",
			TriggeredAt:   twoHoursAgo,
			RecoveredAt:   oneHourAgo,
			TriggerReason: "failure count 3 reached threshold 3 in 10 seconds",
			RecoverReason: "cooldown_expired",
		}
		if err := s.AddCircuitBreakerHistory(history); err != nil {
			return err
		}
		log.Println("[demo-audit] added historical circuit breaker record for service-gamma (triggered 2h ago, recovered 1h ago)")

		for i := 0; i < 2; i++ {
			logEntry := &model.AuditLog{
				Timestamp:  twoHoursAgo.Add(time.Duration(i) * time.Second),
				Caller:     "service-gamma",
				Operation:  model.AuditOpRequestTokens,
				Resource:   "service-gamma",
				Success:    false,
				FailReason: "rate limited",
			}
			_ = s.AddAuditLog(logEntry)
		}
		log.Println("[demo-audit] added sample audit failure logs for service-gamma")
		log.Println("[demo-audit] tip: check rules via GET /api/v1/audit/circuit-breaker/rules")
		log.Println("[demo-audit] tip: check history via GET /api/v1/audit/circuit-breaker/history/service-gamma")
	} else {
		log.Println("[demo-audit] audit demo data already exists, skipping seed")
	}

	return nil
}

func seedTopologyDemoData(topoMgr *topology.Manager, rlMgr *ratelimit.Manager, lockMgr *lock.Manager) error {
	existingNodes, err := topoMgr.ListNodes()
	if err != nil {
		return err
	}

	hasCluster := false
	for _, n := range existingNodes {
		if n.Name == "cluster" {
			hasCluster = true
			break
		}
	}

	if hasCluster {
		log.Println("[demo-topology] topology data already exists, skipping seed")
		return nil
	}

	log.Println("[demo-topology] seeding topology demo data...")

	_, err = topoMgr.RegisterNode("cluster", "lock-cluster", "token-bucket-policy", 1)
	if err != nil {
		return err
	}
	log.Println("[demo-topology] registered node: cluster (lock=lock-cluster, policy=token-bucket-policy, cost=1)")

	_, err = topoMgr.RegisterNode("namespace", "lock-namespace", "", 0)
	if err != nil {
		return err
	}
	log.Println("[demo-topology] registered node: namespace (lock=lock-namespace, no policy)")

	_, err = topoMgr.RegisterNode("pod", "lock-pod", "", 0)
	if err != nil {
		return err
	}
	log.Println("[demo-topology] registered node: pod (lock=lock-pod, no policy)")

	_, err = topoMgr.DeclareEdge("namespace", "pod")
	if err != nil {
		return err
	}
	log.Println("[demo-topology] declared edge: namespace -> pod")

	_, err = topoMgr.DeclareEdge("cluster", "namespace")
	if err != nil {
		return err
	}
	log.Println("[demo-topology] declared edge: cluster -> namespace")

	log.Println("[demo-topology] dependency chain: cluster -> namespace -> pod")
	log.Println("[demo-topology] acquiring pod to demonstrate cascade acquire...")

	result, err := topoMgr.CascadeAcquire("pod", "demo-holder", 300, false)
	if err != nil {
		log.Printf("[demo-topology] cascade acquire error: %v", err)
	} else if result.Success {
		log.Printf("[demo-topology] cascade acquire success! acquired nodes: %v", result.Acquired)
		log.Printf("[demo-topology]  - automatically acquired cluster (with 1 token consumed)")
		log.Printf("[demo-topology]  - automatically acquired namespace")
		log.Printf("[demo-topology]  - acquired target: pod")
		log.Printf("[demo-topology]  - total steps: %d, duration: %dms", len(result.Steps), result.DurationMs)

		_, _ = topoMgr.CascadeRelease("cluster", "demo-holder", true)
		log.Println("[demo-topology] force released all nodes via cluster (demo cleanup)")
	} else {
		log.Printf("[demo-topology] cascade acquire failed: %s (rolled_back=%v)", result.Message, result.RolledBack)
	}

	log.Println("[demo-topology] tip: view graph via GET /api/v1/topology/graph")
	log.Println("[demo-topology] tip: acquire pod via POST /api/v1/topology/acquire")
	log.Println("[demo-topology]   body: {\"target_node\":\"pod\",\"holder\":\"user1\",\"lease_sec\":60,\"reentrant\":false}")
	log.Println("[demo-topology] tip: check ancestors via GET /api/v1/topology/nodes/pod/ancestors")
	log.Println("[demo-topology] tip: view holder tree via GET /api/v1/topology/holders/user1/tree")

	return nil
}

func seedShadowDemoData(shadowMgr *shadow.Manager, s *storage.Storage) error {
	existingPlans, err := shadowMgr.ListPlans()
	if err != nil {
		return err
	}
	if len(existingPlans) > 0 {
		log.Println("[demo-shadow] shadow data already exists, skipping seed")
		return nil
	}

	log.Println("[demo-shadow] seeding shadow evaluation demo data...")

	plan, err := shadowMgr.CreatePlan(
		"demo-stricter-gamma",
		"Stricter circuit breaker for service-gamma and lower quota for resource-a related callers",
		"replay",
		0,
	)
	if err != nil {
		return fmt.Errorf("create shadow plan: %w", err)
	}
	log.Printf("[demo-shadow] created shadow plan: id=%d name=%s", plan.ID, plan.Name)

	_, err = shadowMgr.AddOverride(plan.ID, model.ShadowRuleCircuitBreaker, "service-gamma", "failure_threshold", "2")
	if err != nil {
		return fmt.Errorf("add cb override: %w", err)
	}
	log.Println("[demo-shadow] override: service-gamma circuit breaker failure_threshold 3->2")

	_, err = shadowMgr.AddOverride(plan.ID, model.ShadowRuleCircuitBreaker, "service-gamma", "window_sec", "5")
	if err != nil {
		return fmt.Errorf("add cb window override: %w", err)
	}
	log.Println("[demo-shadow] override: service-gamma circuit breaker window_sec 10->5")

	_, err = shadowMgr.AddOverride(plan.ID, model.ShadowRuleRateLimit, "service-gamma", "quota_limit", "15")
	if err != nil {
		return fmt.Errorf("add rl override: %w", err)
	}
	log.Println("[demo-shadow] override: service-gamma rate limit quota_limit 30->15")

	now := time.Now()
	for i := 0; i < 4; i++ {
		logEntry := &model.AuditLog{
			Timestamp:  now.Add(-time.Duration(10-i) * time.Second),
			Caller:     "service-gamma",
			Operation:  model.AuditOpRequestTokens,
			Resource:   "service-gamma",
			Success:    false,
			FailReason: "rate limited",
		}
		if err := s.AddAuditLog(logEntry); err != nil {
			return err
		}
	}

	logEntry := &model.AuditLog{
		Timestamp:  now.Add(-3 * time.Second),
		Caller:     "service-gamma",
		Operation:  model.AuditOpRequestTokens,
		Resource:   "service-gamma",
		Success:    true,
		FailReason: "",
	}
	if err := s.AddAuditLog(logEntry); err != nil {
		return err
	}

	logEntry2 := &model.AuditLog{
		Timestamp:  now.Add(-2 * time.Second),
		Caller:     "service-alpha",
		Operation:  model.AuditOpRequestTokens,
		Resource:   "service-alpha",
		Success:    true,
		FailReason: "",
	}
	if err := s.AddAuditLog(logEntry2); err != nil {
		return err
	}

	if err := shadowMgr.StartPlan(plan.ID); err != nil {
		return fmt.Errorf("start shadow plan: %w", err)
	}
	log.Printf("[demo-shadow] started shadow plan: id=%d - replay running", plan.ID)

	log.Println("[demo-shadow] tip: view plans via GET /api/v1/shadow/plans")
	log.Println("[demo-shadow] tip: view diffs via GET /api/v1/shadow/plans/1/diffs")
	log.Println("[demo-shadow] tip: view stats via GET /api/v1/shadow/plans/1/stats")
	log.Println("[demo-shadow] tip: apply to production via POST /api/v1/shadow/plans/1/apply")

	return nil
}
