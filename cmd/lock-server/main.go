package main

import (
	"log"
	"os"
	"rtm-107/internal/api"
	"rtm-107/internal/lock"
	"rtm-107/internal/model"
	"rtm-107/internal/ratelimit"
	"rtm-107/internal/storage"

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

	if err := seedDemoData(mgr, rlMgr); err != nil {
		log.Printf("seed demo data: %v", err)
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

	handler := api.NewHandler(mgr, rlMgr)
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
