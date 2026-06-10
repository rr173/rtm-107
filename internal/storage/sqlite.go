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
	db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)")
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}

	db.SetMaxOpenConns(1)

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

	CREATE TABLE IF NOT EXISTS rl_policies (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL UNIQUE,
		algorithm TEXT NOT NULL,
		window_sec INTEGER DEFAULT 0,
		max_tokens INTEGER NOT NULL,
		refill_rate REAL DEFAULT 0,
		refill_unit TEXT DEFAULT '',
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS rl_caller_bindings (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		caller_id TEXT NOT NULL UNIQUE,
		policy_name TEXT NOT NULL,
		quota_limit INTEGER NOT NULL,
		used_tokens INTEGER NOT NULL DEFAULT 0,
		borrowed_tokens INTEGER NOT NULL DEFAULT 0,
		lent_tokens INTEGER NOT NULL DEFAULT 0,
		reserved_tokens INTEGER NOT NULL DEFAULT 0,
		last_refill_at DATETIME,
		window_start_at DATETIME,
		prev_window_count INTEGER NOT NULL DEFAULT 0,
		curr_window_count INTEGER NOT NULL DEFAULT 0,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS rl_events (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		caller_id TEXT NOT NULL,
		policy_name TEXT NOT NULL,
		requested INTEGER NOT NULL,
		granted INTEGER NOT NULL,
		allowed INTEGER NOT NULL DEFAULT 1,
		reason TEXT DEFAULT '',
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS rl_borrow_records (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		from_caller TEXT NOT NULL,
		to_caller TEXT NOT NULL,
		amount INTEGER NOT NULL,
		status TEXT NOT NULL DEFAULT 'active',
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		returned_at DATETIME
	);

	CREATE TABLE IF NOT EXISTS rl_wait_queue (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		caller_id TEXT NOT NULL,
		tokens INTEGER NOT NULL,
		enqueued_at DATETIME NOT NULL,
		timeout_at DATETIME NOT NULL
	);

	CREATE TABLE IF NOT EXISTS rl_reservations (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		policy_name TEXT NOT NULL,
		caller_id TEXT NOT NULL,
		tokens INTEGER NOT NULL,
		start_at DATETIME NOT NULL,
		end_at DATETIME NOT NULL,
		status TEXT NOT NULL DEFAULT 'pending',
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	);

	CREATE INDEX IF NOT EXISTS idx_rl_events_caller ON rl_events(caller_id);
	CREATE INDEX IF NOT EXISTS idx_rl_events_policy ON rl_events(policy_name);
	CREATE INDEX IF NOT EXISTS idx_rl_borrow_from ON rl_borrow_records(from_caller);
	CREATE INDEX IF NOT EXISTS idx_rl_borrow_to ON rl_borrow_records(to_caller);
	CREATE INDEX IF NOT EXISTS idx_rl_wait_caller ON rl_wait_queue(caller_id);
	CREATE INDEX IF NOT EXISTS idx_rl_reservations_policy ON rl_reservations(policy_name);
	CREATE INDEX IF NOT EXISTS idx_rl_reservations_caller ON rl_reservations(caller_id);
	CREATE INDEX IF NOT EXISTS idx_rl_reservations_status ON rl_reservations(status);
	CREATE INDEX IF NOT EXISTS idx_rl_reservations_start ON rl_reservations(start_at);
	CREATE INDEX IF NOT EXISTS idx_rl_reservations_end ON rl_reservations(end_at);

	CREATE TABLE IF NOT EXISTS orch_txs (
		id TEXT PRIMARY KEY,
		holder TEXT NOT NULL,
		status TEXT NOT NULL,
		timeout_sec INTEGER NOT NULL,
		fail_reason TEXT DEFAULT '',
		created_at DATETIME NOT NULL,
		updated_at DATETIME NOT NULL,
		expires_at DATETIME NOT NULL
	);

	CREATE TABLE IF NOT EXISTS orch_tx_locks (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		tx_id TEXT NOT NULL,
		lock_name TEXT NOT NULL,
		lease_sec INTEGER NOT NULL,
		holder TEXT NOT NULL,
		created_at DATETIME NOT NULL
	);

	CREATE TABLE IF NOT EXISTS orch_tx_tokens (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		tx_id TEXT NOT NULL,
		caller_id TEXT NOT NULL,
		tokens INTEGER NOT NULL,
		created_at DATETIME NOT NULL
	);

	CREATE TABLE IF NOT EXISTS orch_tx_state_changes (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		tx_id TEXT NOT NULL,
		from_state TEXT NOT NULL,
		to_state TEXT NOT NULL,
		reason TEXT DEFAULT '',
		created_at DATETIME NOT NULL
	);

	CREATE INDEX IF NOT EXISTS idx_orch_txs_status ON orch_txs(status);
	CREATE INDEX IF NOT EXISTS idx_orch_txs_expires ON orch_txs(expires_at);
	CREATE INDEX IF NOT EXISTS idx_orch_tx_locks_tx ON orch_tx_locks(tx_id);
	CREATE INDEX IF NOT EXISTS idx_orch_tx_tokens_tx ON orch_tx_tokens(tx_id);
	CREATE INDEX IF NOT EXISTS idx_orch_tx_state_changes_tx ON orch_tx_state_changes(tx_id);
	`

	_, err := s.db.Exec(schema)
	if err != nil {
		return err
	}

	if err := s.migrateSchema(); err != nil {
		return err
	}

	return nil
}

func (s *Storage) migrateSchema() error {
	columns := []string{"reserved_tokens"}
	for _, col := range columns {
		row := s.db.QueryRow(`
			SELECT COUNT(*) FROM pragma_table_info('rl_caller_bindings') WHERE name = ?
		`, col)
		var count int
		if err := row.Scan(&count); err != nil {
			return err
		}
		if count == 0 {
			log.Printf("[storage-migration] adding column %s to rl_caller_bindings", col)
			_, err := s.db.Exec(fmt.Sprintf(
				"ALTER TABLE rl_caller_bindings ADD COLUMN %s INTEGER NOT NULL DEFAULT 0", col))
			if err != nil {
				return err
			}
		}
	}
	return nil
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

func (s *Storage) CreatePolicy(p *model.RateLimitPolicy) error {
	result, err := s.db.Exec(`
		INSERT INTO rl_policies (name, algorithm, window_sec, max_tokens, refill_rate, refill_unit, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, p.Name, p.Algorithm, p.WindowSec, p.MaxTokens, p.RefillRate, p.RefillUnit, p.CreatedAt, p.UpdatedAt)
	if err != nil {
		return err
	}
	p.ID, _ = result.LastInsertId()
	return nil
}

func (s *Storage) GetPolicy(name string) (*model.RateLimitPolicy, error) {
	row := s.db.QueryRow(`
		SELECT id, name, algorithm, window_sec, max_tokens, refill_rate, refill_unit, created_at, updated_at
		FROM rl_policies WHERE name = ?
	`, name)

	var p model.RateLimitPolicy
	err := row.Scan(&p.ID, &p.Name, &p.Algorithm, &p.WindowSec, &p.MaxTokens, &p.RefillRate, &p.RefillUnit, &p.CreatedAt, &p.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &p, nil
}

func (s *Storage) ListPolicies() ([]model.RateLimitPolicy, error) {
	rows, err := s.db.Query(`
		SELECT id, name, algorithm, window_sec, max_tokens, refill_rate, refill_unit, created_at, updated_at
		FROM rl_policies ORDER BY id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var policies []model.RateLimitPolicy
	for rows.Next() {
		var p model.RateLimitPolicy
		if err := rows.Scan(&p.ID, &p.Name, &p.Algorithm, &p.WindowSec, &p.MaxTokens, &p.RefillRate, &p.RefillUnit, &p.CreatedAt, &p.UpdatedAt); err != nil {
			return nil, err
		}
		policies = append(policies, p)
	}
	return policies, nil
}

func (s *Storage) UpsertCallerBinding(b *model.CallerBinding) error {
	result, err := s.db.Exec(`
		INSERT INTO rl_caller_bindings (caller_id, policy_name, quota_limit, used_tokens, borrowed_tokens, lent_tokens, reserved_tokens, last_refill_at, window_start_at, prev_window_count, curr_window_count, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(caller_id) DO UPDATE SET
			policy_name = excluded.policy_name,
			quota_limit = excluded.quota_limit,
			used_tokens = excluded.used_tokens,
			borrowed_tokens = excluded.borrowed_tokens,
			lent_tokens = excluded.lent_tokens,
			reserved_tokens = excluded.reserved_tokens,
			last_refill_at = excluded.last_refill_at,
			window_start_at = excluded.window_start_at,
			prev_window_count = excluded.prev_window_count,
			curr_window_count = excluded.curr_window_count,
			updated_at = excluded.updated_at
	`, b.CallerID, b.PolicyName, b.QuotaLimit, b.UsedTokens, b.BorrowedTokens, b.LentTokens, b.ReservedTokens,
		nullTime(b.LastRefillAt), nullTime(b.WindowStartAt), b.PrevWindowCount, b.CurrWindowCount, b.CreatedAt, b.UpdatedAt)
	if err != nil {
		return err
	}
	if b.ID == 0 {
		b.ID, _ = result.LastInsertId()
	}
	return nil
}

func (s *Storage) GetCallerBinding(callerID string) (*model.CallerBinding, error) {
	row := s.db.QueryRow(`
		SELECT id, caller_id, policy_name, quota_limit, used_tokens, borrowed_tokens, lent_tokens, reserved_tokens, last_refill_at, window_start_at, prev_window_count, curr_window_count, created_at, updated_at
		FROM rl_caller_bindings WHERE caller_id = ?
	`, callerID)

	var b model.CallerBinding
	var lastRefillAt, windowStartAt sql.NullTime
	err := row.Scan(&b.ID, &b.CallerID, &b.PolicyName, &b.QuotaLimit, &b.UsedTokens, &b.BorrowedTokens, &b.LentTokens, &b.ReservedTokens,
		&lastRefillAt, &windowStartAt, &b.PrevWindowCount, &b.CurrWindowCount, &b.CreatedAt, &b.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if lastRefillAt.Valid {
		b.LastRefillAt = lastRefillAt.Time
	}
	if windowStartAt.Valid {
		b.WindowStartAt = windowStartAt.Time
	}
	return &b, nil
}

func (s *Storage) ListCallerBindings() ([]model.CallerBinding, error) {
	rows, err := s.db.Query(`
		SELECT id, caller_id, policy_name, quota_limit, used_tokens, borrowed_tokens, lent_tokens, reserved_tokens, last_refill_at, window_start_at, prev_window_count, curr_window_count, created_at, updated_at
		FROM rl_caller_bindings ORDER BY id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var bindings []model.CallerBinding
	for rows.Next() {
		var b model.CallerBinding
		var lastRefillAt, windowStartAt sql.NullTime
		if err := rows.Scan(&b.ID, &b.CallerID, &b.PolicyName, &b.QuotaLimit, &b.UsedTokens, &b.BorrowedTokens, &b.LentTokens, &b.ReservedTokens,
			&lastRefillAt, &windowStartAt, &b.PrevWindowCount, &b.CurrWindowCount, &b.CreatedAt, &b.UpdatedAt); err != nil {
			return nil, err
		}
		if lastRefillAt.Valid {
			b.LastRefillAt = lastRefillAt.Time
		}
		if windowStartAt.Valid {
			b.WindowStartAt = windowStartAt.Time
		}
		bindings = append(bindings, b)
	}
	return bindings, nil
}

func (s *Storage) UpdateCallerBinding(b *model.CallerBinding) error {
	_, err := s.db.Exec(`
		UPDATE rl_caller_bindings SET
			policy_name = ?,
			quota_limit = ?,
			used_tokens = ?,
			borrowed_tokens = ?,
			lent_tokens = ?,
			reserved_tokens = ?,
			last_refill_at = ?,
			window_start_at = ?,
			prev_window_count = ?,
			curr_window_count = ?,
			updated_at = ?
		WHERE caller_id = ?
	`, b.PolicyName, b.QuotaLimit, b.UsedTokens, b.BorrowedTokens, b.LentTokens, b.ReservedTokens,
		nullTime(b.LastRefillAt), nullTime(b.WindowStartAt), b.PrevWindowCount, b.CurrWindowCount, b.UpdatedAt, b.CallerID)
	return err
}

func (s *Storage) AddRateLimitEvent(e *model.RateLimitEvent) error {
	allowedInt := 0
	if e.Allowed {
		allowedInt = 1
	}
	result, err := s.db.Exec(`
		INSERT INTO rl_events (caller_id, policy_name, requested, granted, allowed, reason, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, e.CallerID, e.PolicyName, e.Requested, e.Granted, allowedInt, e.Reason, e.CreatedAt)
	if err != nil {
		return err
	}
	e.ID, _ = result.LastInsertId()
	return nil
}

func (s *Storage) ListRateLimitEvents(callerID string, limit int) ([]model.RateLimitEvent, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.db.Query(`
		SELECT id, caller_id, policy_name, requested, granted, allowed, reason, created_at
		FROM rl_events WHERE caller_id = ? ORDER BY id DESC LIMIT ?
	`, callerID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []model.RateLimitEvent
	for rows.Next() {
		var e model.RateLimitEvent
		var allowedInt int
		if err := rows.Scan(&e.ID, &e.CallerID, &e.PolicyName, &e.Requested, &e.Granted, &allowedInt, &e.Reason, &e.CreatedAt); err != nil {
			return nil, err
		}
		e.Allowed = allowedInt != 0
		events = append(events, e)
	}
	return events, nil
}

func (s *Storage) CountRateLimited(callerID string) (int64, error) {
	var count int64
	err := s.db.QueryRow(`SELECT COUNT(*) FROM rl_events WHERE caller_id = ? AND allowed = 0`, callerID).Scan(&count)
	return count, err
}

func (s *Storage) CountAllEvents() (int64, int64, error) {
	var total, allowed int64
	row := s.db.QueryRow(`SELECT COUNT(*), SUM(CASE WHEN allowed = 1 THEN 1 ELSE 0 END) FROM rl_events`)
	err := row.Scan(&total, &allowed)
	return total, allowed, err
}

func (s *Storage) CreateBorrowRecord(r *model.QuotaBorrowRecord) error {
	result, err := s.db.Exec(`
		INSERT INTO rl_borrow_records (from_caller, to_caller, amount, status, created_at)
		VALUES (?, ?, ?, ?, ?)
	`, r.FromCaller, r.ToCaller, r.Amount, r.Status, r.CreatedAt)
	if err != nil {
		return err
	}
	r.ID, _ = result.LastInsertId()
	return nil
}

func (s *Storage) ListActiveBorrows() ([]model.QuotaBorrowRecord, error) {
	rows, err := s.db.Query(`
		SELECT id, from_caller, to_caller, amount, status, created_at, returned_at
		FROM rl_borrow_records WHERE status = 'active' ORDER BY id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []model.QuotaBorrowRecord
	for rows.Next() {
		var r model.QuotaBorrowRecord
		var returnedAt sql.NullTime
		if err := rows.Scan(&r.ID, &r.FromCaller, &r.ToCaller, &r.Amount, &r.Status, &r.CreatedAt, &returnedAt); err != nil {
			return nil, err
		}
		if returnedAt.Valid {
			r.ReturnedAt = returnedAt.Time
		}
		records = append(records, r)
	}
	return records, nil
}

func (s *Storage) ReturnBorrow(fromCaller, toCaller string, amount int, returnedAt time.Time) error {
	row := s.db.QueryRow(`
		SELECT id FROM rl_borrow_records
		WHERE from_caller = ? AND to_caller = ? AND status = 'active' AND amount = ?
		ORDER BY id LIMIT 1
	`, fromCaller, toCaller, amount)

	var id int64
	err := row.Scan(&id)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}

	_, err = s.db.Exec(`
		UPDATE rl_borrow_records SET status = 'returned', returned_at = ?
		WHERE id = ?
	`, returnedAt, id)
	return err
}

func (s *Storage) AddWaitItem(item *model.RateLimitWaitItem) error {
	result, err := s.db.Exec(`
		INSERT INTO rl_wait_queue (caller_id, tokens, enqueued_at, timeout_at)
		VALUES (?, ?, ?, ?)
	`, item.CallerID, item.Tokens, item.EnqueuedAt, item.TimeoutAt)
	if err != nil {
		return err
	}
	item.ID, _ = result.LastInsertId()
	return nil
}

func (s *Storage) ListWaitItemsByCaller(callerID string) ([]model.RateLimitWaitItem, error) {
	rows, err := s.db.Query(`
		SELECT id, caller_id, tokens, enqueued_at, timeout_at
		FROM rl_wait_queue WHERE caller_id = ? ORDER BY id
	`, callerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []model.RateLimitWaitItem
	for rows.Next() {
		var item model.RateLimitWaitItem
		if err := rows.Scan(&item.ID, &item.CallerID, &item.Tokens, &item.EnqueuedAt, &item.TimeoutAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, nil
}

func (s *Storage) ListAllWaitItems() ([]model.RateLimitWaitItem, error) {
	rows, err := s.db.Query(`
		SELECT id, caller_id, tokens, enqueued_at, timeout_at
		FROM rl_wait_queue ORDER BY id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []model.RateLimitWaitItem
	for rows.Next() {
		var item model.RateLimitWaitItem
		if err := rows.Scan(&item.ID, &item.CallerID, &item.Tokens, &item.EnqueuedAt, &item.TimeoutAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, nil
}

func (s *Storage) RemoveWaitItem(id int64) error {
	_, err := s.db.Exec(`DELETE FROM rl_wait_queue WHERE id = ?`, id)
	return err
}

func (s *Storage) RemoveExpiredWaitItems(now time.Time) (int64, error) {
	result, err := s.db.Exec(`DELETE FROM rl_wait_queue WHERE timeout_at <= ?`, now)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (s *Storage) CountWaitItems(callerID string) (int, error) {
	var count int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM rl_wait_queue WHERE caller_id = ?`, callerID).Scan(&count)
	return count, err
}

func (s *Storage) CreateReservation(r *model.QuotaReservation) error {
	result, err := s.db.Exec(`
		INSERT INTO rl_reservations (policy_name, caller_id, tokens, start_at, end_at, status, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, r.PolicyName, r.CallerID, r.Tokens, r.StartAt, r.EndAt, r.Status, r.CreatedAt, r.UpdatedAt)
	if err != nil {
		return err
	}
	r.ID, _ = result.LastInsertId()
	return nil
}

func (s *Storage) GetReservation(id int64) (*model.QuotaReservation, error) {
	row := s.db.QueryRow(`
		SELECT id, policy_name, caller_id, tokens, start_at, end_at, status, created_at, updated_at
		FROM rl_reservations WHERE id = ?
	`, id)

	var r model.QuotaReservation
	err := row.Scan(&r.ID, &r.PolicyName, &r.CallerID, &r.Tokens, &r.StartAt, &r.EndAt, &r.Status, &r.CreatedAt, &r.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &r, nil
}

func (s *Storage) UpdateReservationStatus(id int64, status model.ReservationStatus, updatedAt time.Time) error {
	_, err := s.db.Exec(`
		UPDATE rl_reservations SET status = ?, updated_at = ? WHERE id = ?
	`, status, updatedAt, id)
	return err
}

func (s *Storage) CancelReservation(id int64, updatedAt time.Time) error {
	return s.UpdateReservationStatus(id, model.ReservationStatusCancelled, updatedAt)
}

func (s *Storage) ListReservationsByPolicy(policyName string, status string) ([]model.QuotaReservation, error) {
	var rows *sql.Rows
	var err error

	if status != "" {
		rows, err = s.db.Query(`
			SELECT id, policy_name, caller_id, tokens, start_at, end_at, status, created_at, updated_at
			FROM rl_reservations WHERE policy_name = ? AND status = ? ORDER BY start_at
		`, policyName, status)
	} else {
		rows, err = s.db.Query(`
			SELECT id, policy_name, caller_id, tokens, start_at, end_at, status, created_at, updated_at
			FROM rl_reservations WHERE policy_name = ? ORDER BY start_at
		`, policyName)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var reservations []model.QuotaReservation
	for rows.Next() {
		var r model.QuotaReservation
		if err := rows.Scan(&r.ID, &r.PolicyName, &r.CallerID, &r.Tokens, &r.StartAt, &r.EndAt, &r.Status, &r.CreatedAt, &r.UpdatedAt); err != nil {
			return nil, err
		}
		reservations = append(reservations, r)
	}
	return reservations, nil
}

func (s *Storage) ListReservationsByCaller(callerID string, status string) ([]model.QuotaReservation, error) {
	var rows *sql.Rows
	var err error

	if status != "" {
		rows, err = s.db.Query(`
			SELECT id, policy_name, caller_id, tokens, start_at, end_at, status, created_at, updated_at
			FROM rl_reservations WHERE caller_id = ? AND status = ? ORDER BY start_at
		`, callerID, status)
	} else {
		rows, err = s.db.Query(`
			SELECT id, policy_name, caller_id, tokens, start_at, end_at, status, created_at, updated_at
			FROM rl_reservations WHERE caller_id = ? ORDER BY start_at
		`, callerID)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var reservations []model.QuotaReservation
	for rows.Next() {
		var r model.QuotaReservation
		if err := rows.Scan(&r.ID, &r.PolicyName, &r.CallerID, &r.Tokens, &r.StartAt, &r.EndAt, &r.Status, &r.CreatedAt, &r.UpdatedAt); err != nil {
			return nil, err
		}
		reservations = append(reservations, r)
	}
	return reservations, nil
}

func (s *Storage) ListAllReservations(status string) ([]model.QuotaReservation, error) {
	var rows *sql.Rows
	var err error

	if status != "" {
		rows, err = s.db.Query(`
			SELECT id, policy_name, caller_id, tokens, start_at, end_at, status, created_at, updated_at
			FROM rl_reservations WHERE status = ? ORDER BY start_at
		`, status)
	} else {
		rows, err = s.db.Query(`
			SELECT id, policy_name, caller_id, tokens, start_at, end_at, status, created_at, updated_at
			FROM rl_reservations ORDER BY start_at
		`)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var reservations []model.QuotaReservation
	for rows.Next() {
		var r model.QuotaReservation
		if err := rows.Scan(&r.ID, &r.PolicyName, &r.CallerID, &r.Tokens, &r.StartAt, &r.EndAt, &r.Status, &r.CreatedAt, &r.UpdatedAt); err != nil {
			return nil, err
		}
		reservations = append(reservations, r)
	}
	return reservations, nil
}

func (s *Storage) ListReservationsInTimeRange(policyName string, startAt time.Time, endAt time.Time) ([]model.QuotaReservation, error) {
	rows, err := s.db.Query(`
		SELECT id, policy_name, caller_id, tokens, start_at, end_at, status, created_at, updated_at
		FROM rl_reservations
		WHERE policy_name = ?
		  AND status IN ('pending', 'active')
		  AND start_at < ?
		  AND end_at > ?
		ORDER BY start_at
	`, policyName, endAt, startAt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var reservations []model.QuotaReservation
	for rows.Next() {
		var r model.QuotaReservation
		if err := rows.Scan(&r.ID, &r.PolicyName, &r.CallerID, &r.Tokens, &r.StartAt, &r.EndAt, &r.Status, &r.CreatedAt, &r.UpdatedAt); err != nil {
			return nil, err
		}
		reservations = append(reservations, r)
	}
	return reservations, nil
}

func (s *Storage) ListPendingReservationsToStart(now time.Time) ([]model.QuotaReservation, error) {
	rows, err := s.db.Query(`
		SELECT id, policy_name, caller_id, tokens, start_at, end_at, status, created_at, updated_at
		FROM rl_reservations
		WHERE status = 'pending' AND start_at <= ?
		ORDER BY start_at
	`, now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var reservations []model.QuotaReservation
	for rows.Next() {
		var r model.QuotaReservation
		if err := rows.Scan(&r.ID, &r.PolicyName, &r.CallerID, &r.Tokens, &r.StartAt, &r.EndAt, &r.Status, &r.CreatedAt, &r.UpdatedAt); err != nil {
			return nil, err
		}
		reservations = append(reservations, r)
	}
	return reservations, nil
}

func (s *Storage) ListActiveReservationsToEnd(now time.Time) ([]model.QuotaReservation, error) {
	rows, err := s.db.Query(`
		SELECT id, policy_name, caller_id, tokens, start_at, end_at, status, created_at, updated_at
		FROM rl_reservations
		WHERE status = 'active' AND end_at <= ?
		ORDER BY end_at
	`, now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var reservations []model.QuotaReservation
	for rows.Next() {
		var r model.QuotaReservation
		if err := rows.Scan(&r.ID, &r.PolicyName, &r.CallerID, &r.Tokens, &r.StartAt, &r.EndAt, &r.Status, &r.CreatedAt, &r.UpdatedAt); err != nil {
			return nil, err
		}
		reservations = append(reservations, r)
	}
	return reservations, nil
}

func nullTime(t time.Time) interface{} {
	if t.IsZero() {
		return nil
	}
	return t
}

func (s *Storage) CreateOrchTx(tx *model.OrchestrationTx) error {
	_, err := s.db.Exec(`
		INSERT INTO orch_txs (id, holder, status, timeout_sec, fail_reason, created_at, updated_at, expires_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, tx.ID, tx.Holder, tx.Status, tx.TimeoutSec, tx.FailReason, tx.CreatedAt, tx.UpdatedAt, tx.ExpiresAt)
	return err
}

func (s *Storage) UpdateOrchTxStatus(txID string, status model.TxStatus, failReason string, updatedAt time.Time) error {
	_, err := s.db.Exec(`
		UPDATE orch_txs SET status = ?, fail_reason = ?, updated_at = ? WHERE id = ?
	`, status, failReason, updatedAt, txID)
	return err
}

func (s *Storage) GetOrchTx(txID string) (*model.OrchestrationTx, error) {
	row := s.db.QueryRow(`
		SELECT id, holder, status, timeout_sec, fail_reason, created_at, updated_at, expires_at
		FROM orch_txs WHERE id = ?
	`, txID)

	var tx model.OrchestrationTx
	err := row.Scan(&tx.ID, &tx.Holder, &tx.Status, &tx.TimeoutSec, &tx.FailReason,
		&tx.CreatedAt, &tx.UpdatedAt, &tx.ExpiresAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &tx, nil
}

func (s *Storage) ListOrchTxs(status string) ([]model.OrchestrationTx, error) {
	var rows *sql.Rows
	var err error

	if status != "" {
		rows, err = s.db.Query(`
			SELECT id, holder, status, timeout_sec, fail_reason, created_at, updated_at, expires_at
			FROM orch_txs WHERE status = ? ORDER BY created_at DESC
		`, status)
	} else {
		rows, err = s.db.Query(`
			SELECT id, holder, status, timeout_sec, fail_reason, created_at, updated_at, expires_at
			FROM orch_txs ORDER BY created_at DESC
		`)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var txs []model.OrchestrationTx
	for rows.Next() {
		var tx model.OrchestrationTx
		if err := rows.Scan(&tx.ID, &tx.Holder, &tx.Status, &tx.TimeoutSec, &tx.FailReason,
			&tx.CreatedAt, &tx.UpdatedAt, &tx.ExpiresAt); err != nil {
			return nil, err
		}
		txs = append(txs, tx)
	}
	return txs, nil
}

func (s *Storage) ListActiveOrchTxs(now time.Time) ([]model.OrchestrationTx, error) {
	rows, err := s.db.Query(`
		SELECT id, holder, status, timeout_sec, fail_reason, created_at, updated_at, expires_at
		FROM orch_txs WHERE status = ? ORDER BY created_at DESC
	`, model.TxStatusCommitted)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var txs []model.OrchestrationTx
	for rows.Next() {
		var tx model.OrchestrationTx
		if err := rows.Scan(&tx.ID, &tx.Holder, &tx.Status, &tx.TimeoutSec, &tx.FailReason,
			&tx.CreatedAt, &tx.UpdatedAt, &tx.ExpiresAt); err != nil {
			return nil, err
		}
		txs = append(txs, tx)
	}
	return txs, nil
}

func (s *Storage) AddTxLock(txLock *model.TxLock) error {
	result, err := s.db.Exec(`
		INSERT INTO orch_tx_locks (tx_id, lock_name, lease_sec, holder, created_at)
		VALUES (?, ?, ?, ?, ?)
	`, txLock.TxID, txLock.LockName, txLock.LeaseSec, txLock.Holder, txLock.CreatedAt)
	if err != nil {
		return err
	}
	txLock.ID, _ = result.LastInsertId()
	return nil
}

func (s *Storage) ListTxLocks(txID string) ([]model.TxLock, error) {
	rows, err := s.db.Query(`
		SELECT id, tx_id, lock_name, lease_sec, holder, created_at
		FROM orch_tx_locks WHERE tx_id = ? ORDER BY id
	`, txID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var locks []model.TxLock
	for rows.Next() {
		var l model.TxLock
		if err := rows.Scan(&l.ID, &l.TxID, &l.LockName, &l.LeaseSec, &l.Holder, &l.CreatedAt); err != nil {
			return nil, err
		}
		locks = append(locks, l)
	}
	return locks, nil
}

func (s *Storage) AddTxToken(txToken *model.TxToken) error {
	result, err := s.db.Exec(`
		INSERT INTO orch_tx_tokens (tx_id, caller_id, tokens, created_at)
		VALUES (?, ?, ?, ?)
	`, txToken.TxID, txToken.CallerID, txToken.Tokens, txToken.CreatedAt)
	if err != nil {
		return err
	}
	txToken.ID, _ = result.LastInsertId()
	return nil
}

func (s *Storage) ListTxTokens(txID string) ([]model.TxToken, error) {
	rows, err := s.db.Query(`
		SELECT id, tx_id, caller_id, tokens, created_at
		FROM orch_tx_tokens WHERE tx_id = ? ORDER BY id
	`, txID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tokens []model.TxToken
	for rows.Next() {
		var t model.TxToken
		if err := rows.Scan(&t.ID, &t.TxID, &t.CallerID, &t.Tokens, &t.CreatedAt); err != nil {
			return nil, err
		}
		tokens = append(tokens, t)
	}
	return tokens, nil
}

func (s *Storage) AddTxStateChange(sc *model.TxStateChange) error {
	result, err := s.db.Exec(`
		INSERT INTO orch_tx_state_changes (tx_id, from_state, to_state, reason, created_at)
		VALUES (?, ?, ?, ?, ?)
	`, sc.TxID, sc.FromState, sc.ToState, sc.Reason, sc.CreatedAt)
	if err != nil {
		return err
	}
	sc.ID, _ = result.LastInsertId()
	return nil
}

func (s *Storage) ListTxStateChanges(txID string) ([]model.TxStateChange, error) {
	rows, err := s.db.Query(`
		SELECT id, tx_id, from_state, to_state, reason, created_at
		FROM orch_tx_state_changes WHERE tx_id = ? ORDER BY id
	`, txID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var changes []model.TxStateChange
	for rows.Next() {
		var sc model.TxStateChange
		if err := rows.Scan(&sc.ID, &sc.TxID, &sc.FromState, &sc.ToState, &sc.Reason, &sc.CreatedAt); err != nil {
			return nil, err
		}
		changes = append(changes, sc)
	}
	return changes, nil
}
