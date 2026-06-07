package main

import (
	"log"
	"os"
	"rtm-107/internal/api"
	"rtm-107/internal/lock"
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

	if err := seedDemoData(mgr); err != nil {
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

	handler := api.NewHandler(mgr)
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

func seedDemoData(mgr *lock.Manager) error {
	locks, err := mgr.ListAllLocks()
	if err != nil {
		return err
	}
	if len(locks) > 0 {
		log.Println("[demo] data already exists, skipping seed")
		return nil
	}

	log.Println("[demo] seeding demo data...")

	if _, err := mgr.AcquireLock("resource-a", "alice", 120, true); err != nil {
		return err
	}
	log.Println("[demo] acquired lock resource-a by alice (reentrant, 120s)")

	if _, err := mgr.AcquireLock("resource-b", "bob", 60, false); err != nil {
		return err
	}
	log.Println("[demo] acquired lock resource-b by bob (non-reentrant, 60s)")

	if result, err := mgr.AcquireLock("resource-a", "charlie", 30, false); err != nil {
		return err
	} else if result.Queued {
		log.Println("[demo] charlie queued for resource-a")
	}

	log.Println("[demo] demo data seeded successfully")
	return nil
}
