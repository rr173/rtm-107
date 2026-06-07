package storage

import (
	"database/sql"
	"fmt"
	"log"
	"rtm-107/internal/model"
	"time"

	_ "modernc.org/sqlite"
)

type Storage struct {
	db *sql.DB
}

func New(path string) (*Storage, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}

	s := &Storage{db: db}
	if err := s.initSchema(); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *Storage) Close() error {
	return s.db.Close()
}

func (s *Storage) DB() *sql.DB {
	return s.db
}

func (s *Storage) initSchema() error {
	schema := `
	CREATE TABLE IF NOT EXISTS locks (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL UNIQUE,
		status TEXT NOT NULL DEFAULT 'free',
		holder TEXT DEFAULT '',
		reentrant INTEGER NOT NULL DEFAULT 0,
		count INTEGER NOT NULL DEFAULT 0,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS leases (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		lock_name TEXT NOT NULL,
		holder TEXT NOT NULL,
		lease_sec INTEGER NOT NULL,
		acquired_at DATETIME NOT NULL,
		expires_at DATETIME NOT NULL,
		active INTEGER NOT NULL DEFAULT 1
	);

	CREATE TABLE IF NOT EXISTS wait_queue (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		lock_name TEXT NOT NULL,
		holder TEXT NOT NULL,
		reentrant INTEGER NOT NULL DEFAULT 0,
		lease_sec INTEGER NOT NULL,
		enqueued_at DATETIME NOT NULL,
		timeout_at DATETIME NOT NULL
	);

	CREATE TABLE IF NOT EXISTS op_history (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		lock_name TEXT NOT NULL,
		holder TEXT NOT NULL,
		operation TEXT NOT NULL,
		detail TEXT DEFAULT '',
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	);

	CREATE INDEX IF NOT EXISTS idx_leases_active ON leases(active);
	CREATE INDEX IF NOT EXISTS idx_leases_lock_name ON leases(lock_name);
	CREATE INDEX IF NOT EXISTS idx_wait_queue_lock_name ON wait_queue(lock_name);
	CREATE INDEX IF NOT EXISTS idx_history_lock_name ON op_history(lock_name);
	`

	_, err := s.db.Exec(schema)
	return err
}

func (s *Storage) GetLock(name string) (*model.Lock, error) {
	row := s.db.QueryRow(`
		SELECT id, name, status, holder, reentrant, count, created_at, updated_at
		FROM locks WHERE name = ?
	`, name)

	var l model.Lock
	var reentrantInt int
	err := row.Scan(&l.ID, &l.Name, &l.Status, &l.Holder, &reentrantInt, &l.Count, &l.CreatedAt, &l.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	l.Reentrant = reentrantInt != 0
	return &l, nil
}

func (s *Storage) UpsertLock(l *model.Lock) error {
	reentrantInt := 0
	if l.Reentrant {
		reentrantInt = 1
	}
	_, err := s.db.Exec(`
		INSERT INTO locks (name, status, holder, reentrant, count, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(name) DO UPDATE SET
			status = excluded.status,
			holder = excluded.holder,
			reentrant = excluded.reentrant,
			count = excluded.count,
			updated_at = excluded.updated_at
	`, l.Name, l.Status, l.Holder, reentrantInt, l.Count, l.CreatedAt, l.UpdatedAt)
	return err
}

func (s *Storage) ListLocks() ([]model.Lock, error) {
	rows, err := s.db.Query(`
		SELECT id, name, status, holder, reentrant, count, created_at, updated_at
		FROM locks ORDER BY id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var locks []model.Lock
	for rows.Next() {
		var l model.Lock
		var reentrantInt int
		if err := rows.Scan(&l.ID, &l.Name, &l.Status, &l.Holder, &reentrantInt, &l.Count, &l.CreatedAt, &l.UpdatedAt); err != nil {
			return nil, err
		}
		l.Reentrant = reentrantInt != 0
		locks = append(locks, l)
	}
	return locks, nil
}

func (s *Storage) GetActiveLease(lockName string) (*model.Lease, error) {
	row := s.db.QueryRow(`
		SELECT id, lock_name, holder, lease_sec, acquired_at, expires_at, active
		FROM leases WHERE lock_name = ? AND active = 1 ORDER BY id DESC LIMIT 1
	`, lockName)

	var l model.Lease
	var activeInt int
	err := row.Scan(&l.ID, &l.LockName, &l.Holder, &l.LeaseSec, &l.AcquiredAt, &l.ExpiresAt, &activeInt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	l.Active = activeInt != 0
	return &l, nil
}

func (s *Storage) CreateLease(l *model.Lease) error {
	activeInt := 0
	if l.Active {
		activeInt = 1
	}
	result, err := s.db.Exec(`
		INSERT INTO leases (lock_name, holder, lease_sec, acquired_at, expires_at, active)
		VALUES (?, ?, ?, ?, ?, ?)
	`, l.LockName, l.Holder, l.LeaseSec, l.AcquiredAt, l.ExpiresAt, activeInt)
	if err != nil {
		return err
	}
	l.ID, _ = result.LastInsertId()
	return nil
}

func (s *Storage) DeactivateLease(lockName string) error {
	_, err := s.db.Exec(`UPDATE leases SET active = 0 WHERE lock_name = ? AND active = 1`, lockName)
	return err
}

func (s *Storage) UpdateLeaseExpiry(lockName string, newExpiresAt time.Time) error {
	_, err := s.db.Exec(`
		UPDATE leases SET expires_at = ? WHERE lock_name = ? AND active = 1
	`, newExpiresAt, lockName)
	return err
}

func (s *Storage) ListActiveLeases() ([]model.Lease, error) {
	rows, err := s.db.Query(`
		SELECT id, lock_name, holder, lease_sec, acquired_at, expires_at, active
		FROM leases WHERE active = 1 ORDER BY id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var leases []model.Lease
	for rows.Next() {
		var l model.Lease
		var activeInt int
		if err := rows.Scan(&l.ID, &l.LockName, &l.Holder, &l.LeaseSec, &l.AcquiredAt, &l.ExpiresAt, &activeInt); err != nil {
			return nil, err
		}
		l.Active = activeInt != 0
		leases = append(leases, l)
	}
	return leases, nil
}

func (s *Storage) Enqueue(item *model.WaitQueueItem) error {
	result, err := s.db.Exec(`
		INSERT INTO wait_queue (lock_name, holder, reentrant, lease_sec, enqueued_at, timeout_at)
		VALUES (?, ?, ?, ?, ?, ?)
	`, item.LockName, item.Holder, boolToInt(item.Reentrant), item.LeaseSec, item.EnqueuedAt, item.TimeoutAt)
	if err != nil {
		return err
	}
	item.ID, _ = result.LastInsertId()
	return nil
}

func (s *Storage) Dequeue(lockName string) (*model.WaitQueueItem, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	row := tx.QueryRow(`
		SELECT id, lock_name, holder, reentrant, lease_sec, enqueued_at, timeout_at
		FROM wait_queue WHERE lock_name = ? ORDER BY id LIMIT 1
	`, lockName)

	var item model.WaitQueueItem
	var reentrantInt int
	err = row.Scan(&item.ID, &item.LockName, &item.Holder, &reentrantInt, &item.LeaseSec, &item.EnqueuedAt, &item.TimeoutAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	item.Reentrant = reentrantInt != 0

	_, err = tx.Exec(`DELETE FROM wait_queue WHERE id = ?`, item.ID)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &item, nil
}

func (s *Storage) RemoveFromQueue(lockName, holder string) error {
	_, err := s.db.Exec(`DELETE FROM wait_queue WHERE lock_name = ? AND holder = ?`, lockName, holder)
	return err
}

func (s *Storage) RemoveFromQueueByID(id int64) error {
	_, err := s.db.Exec(`DELETE FROM wait_queue WHERE id = ?`, id)
	return err
}

func (s *Storage) ListWaitQueue(lockName string) ([]model.WaitQueueItem, error) {
	rows, err := s.db.Query(`
		SELECT id, lock_name, holder, reentrant, lease_sec, enqueued_at, timeout_at
		FROM wait_queue WHERE lock_name = ? ORDER BY id
	`, lockName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []model.WaitQueueItem
	for rows.Next() {
		var item model.WaitQueueItem
		var reentrantInt int
		if err := rows.Scan(&item.ID, &item.LockName, &item.Holder, &reentrantInt, &item.LeaseSec, &item.EnqueuedAt, &item.TimeoutAt); err != nil {
			return nil, err
		}
		item.Reentrant = reentrantInt != 0
		items = append(items, item)
	}
	return items, nil
}

func (s *Storage) ListAllWaitQueue() ([]model.WaitQueueItem, error) {
	rows, err := s.db.Query(`
		SELECT id, lock_name, holder, reentrant, lease_sec, enqueued_at, timeout_at
		FROM wait_queue ORDER BY id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []model.WaitQueueItem
	for rows.Next() {
		var item model.WaitQueueItem
		var reentrantInt int
		if err := rows.Scan(&item.ID, &item.LockName, &item.Holder, &reentrantInt, &item.LeaseSec, &item.EnqueuedAt, &item.TimeoutAt); err != nil {
			return nil, err
		}
		item.Reentrant = reentrantInt != 0
		items = append(items, item)
	}
	return items, nil
}

func (s *Storage) WaitQueueLen(lockName string) (int, error) {
	var count int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM wait_queue WHERE lock_name = ?`, lockName).Scan(&count)
	return count, err
}

func (s *Storage) AddHistory(h *model.OperationHistory) error {
	result, err := s.db.Exec(`
		INSERT INTO op_history (lock_name, holder, operation, detail, created_at)
		VALUES (?, ?, ?, ?, ?)
	`, h.LockName, h.Holder, h.Operation, h.Detail, h.CreatedAt)
	if err != nil {
		return err
	}
	h.ID, _ = result.LastInsertId()
	return nil
}

func (s *Storage) ListHistory(lockName string, limit int) ([]model.OperationHistory, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.db.Query(`
		SELECT id, lock_name, holder, operation, detail, created_at
		FROM op_history WHERE lock_name = ? ORDER BY id DESC LIMIT ?
	`, lockName, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var history []model.OperationHistory
	for rows.Next() {
		var h model.OperationHistory
		if err := rows.Scan(&h.ID, &h.LockName, &h.Holder, &h.Operation, &h.Detail, &h.CreatedAt); err != nil {
			return nil, err
		}
		history = append(history, h)
	}
	return history, nil
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func (s *Storage) LogInfo(format string, v ...interface{}) {
	log.Printf("[storage] "+format, v...)
}
