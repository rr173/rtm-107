package lock

import (
	"fmt"
	"log"
	"rtm-107/internal/model"
	"rtm-107/internal/storage"
	"sync"
	"time"
)

const (
	WaitTimeoutSec = 30
)

type Manager struct {
	storage *storage.Storage
	mu      sync.Mutex
	timers  map[string]*time.Timer
	stopCh  chan struct{}
}

func NewManager(s *storage.Storage) *Manager {
	return &Manager{
		storage: s,
		timers:  make(map[string]*time.Timer),
		stopCh:  make(chan struct{}),
	}
}

func (m *Manager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.rebuildTimersLocked(); err != nil {
		return fmt.Errorf("rebuild timers: %w", err)
	}
	go m.watchWaitQueue()
	log.Println("[lock-manager] started")
	return nil
}

func (m *Manager) Stop() {
	close(m.stopCh)
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, t := range m.timers {
		t.Stop()
	}
	log.Println("[lock-manager] stopped")
}

func (m *Manager) rebuildTimersLocked() error {
	leases, err := m.storage.ListActiveLeases()
	if err != nil {
		return err
	}

	now := time.Now()
	for _, lease := range leases {
		l := lease
		if l.ExpiresAt.Before(now) {
			log.Printf("[lock-manager] lease expired on startup: lock=%s holder=%s", l.LockName, l.Holder)
			m.expireLockLocked(l.LockName)
		} else {
			duration := time.Until(l.ExpiresAt)
			m.setLeaseTimerLocked(l.LockName, duration)
			log.Printf("[lock-manager] rebuilt lease timer: lock=%s holder=%s remaining=%.1fs", l.LockName, l.Holder, duration.Seconds())
		}
	}
	return nil
}

func (m *Manager) setLeaseTimerLocked(lockName string, duration time.Duration) {
	if t, ok := m.timers[lockName]; ok {
		t.Stop()
	}

	m.timers[lockName] = time.AfterFunc(duration, func() {
		log.Printf("[lock-manager] lease expired: lock=%s", lockName)
		m.expireLock(lockName)
	})
}

func (m *Manager) stopLeaseTimerLocked(lockName string) {
	if t, ok := m.timers[lockName]; ok {
		t.Stop()
		delete(m.timers, lockName)
	}
}

type AcquireResult struct {
	Acquired bool
	Queued   bool
	Lock     *model.Lock
	Lease    *model.Lease
	Position int
}

func (m *Manager) AcquireLock(lockName, holder string, leaseSec int, reentrant bool) (*AcquireResult, error) {
	if leaseSec <= 0 {
		return nil, fmt.Errorf("lease_sec must be positive")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	return m.acquireLockLocked(lockName, holder, leaseSec, reentrant)
}

func (m *Manager) acquireLockLocked(lockName, holder string, leaseSec int, reentrant bool) (*AcquireResult, error) {
	lock, err := m.storage.GetLock(lockName)
	if err != nil {
		return nil, err
	}

	now := time.Now()

	if lock == nil {
		lock = &model.Lock{
			Name:      lockName,
			Status:    model.LockStatusFree,
			Holder:    "",
			Reentrant: reentrant,
			Count:     0,
			CreatedAt: now,
			UpdatedAt: now,
		}
		if err := m.storage.UpsertLock(lock); err != nil {
			return nil, err
		}
	}

	if lock.Status == model.LockStatusHeld {
		if lock.Holder == holder && lock.Reentrant && reentrant {
			lock.Count++
			lock.UpdatedAt = now
			if err := m.storage.UpsertLock(lock); err != nil {
				return nil, err
			}
			m.addHistoryLocked(lockName, holder, model.OpAcquire, fmt.Sprintf("reentrant acquire, count=%d", lock.Count))
			lease, _ := m.storage.GetActiveLease(lockName)
			return &AcquireResult{Acquired: true, Lock: lock, Lease: lease}, nil
		}

		item := &model.WaitQueueItem{
			LockName:   lockName,
			Holder:     holder,
			Reentrant:  reentrant,
			LeaseSec:   leaseSec,
			EnqueuedAt: now,
			TimeoutAt:  now.Add(time.Duration(WaitTimeoutSec) * time.Second),
		}
		if err := m.storage.Enqueue(item); err != nil {
			return nil, err
		}
		m.addHistoryLocked(lockName, holder, model.OpAcquire, "queued")

		queue, err := m.storage.ListWaitQueue(lockName)
		if err != nil {
			return nil, err
		}
		position := len(queue)

		return &AcquireResult{Queued: true, Position: position, Lock: lock}, nil
	}

	lock.Status = model.LockStatusHeld
	lock.Holder = holder
	lock.Reentrant = reentrant
	lock.Count = 1
	lock.UpdatedAt = now
	if err := m.storage.UpsertLock(lock); err != nil {
		return nil, err
	}

	lease := &model.Lease{
		LockName:   lockName,
		Holder:     holder,
		LeaseSec:   leaseSec,
		AcquiredAt: now,
		ExpiresAt:  now.Add(time.Duration(leaseSec) * time.Second),
		Active:     true,
	}
	if err := m.storage.CreateLease(lease); err != nil {
		return nil, err
	}

	m.setLeaseTimerLocked(lockName, time.Duration(leaseSec)*time.Second)
	m.addHistoryLocked(lockName, holder, model.OpAcquire, fmt.Sprintf("acquired, lease=%ds", leaseSec))

	return &AcquireResult{Acquired: true, Lock: lock, Lease: lease}, nil
}

type ReleaseResult struct {
	Released bool
	Count    int
	Granted  *model.Lock
}

func (m *Manager) ReleaseLock(lockName, holder string) (*ReleaseResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.releaseLockLocked(lockName, holder)
}

func (m *Manager) releaseLockLocked(lockName, holder string) (*ReleaseResult, error) {
	lock, err := m.storage.GetLock(lockName)
	if err != nil {
		return nil, err
	}
	if lock == nil {
		return nil, fmt.Errorf("lock not found: %s", lockName)
	}

	if lock.Status != model.LockStatusHeld {
		return &ReleaseResult{Released: false}, nil
	}

	if lock.Holder != holder {
		return nil, fmt.Errorf("not the holder: current=%s", lock.Holder)
	}

	if lock.Reentrant && lock.Count > 1 {
		lock.Count--
		lock.UpdatedAt = time.Now()
		if err := m.storage.UpsertLock(lock); err != nil {
			return nil, err
		}
		m.addHistoryLocked(lockName, holder, model.OpRelease, fmt.Sprintf("reentrant release, count=%d", lock.Count))
		return &ReleaseResult{Released: true, Count: lock.Count}, nil
	}

	m.stopLeaseTimerLocked(lockName)

	if err := m.storage.DeactivateLease(lockName); err != nil {
		return nil, err
	}

	lock.Status = model.LockStatusFree
	lock.Holder = ""
	lock.Count = 0
	lock.UpdatedAt = time.Now()
	if err := m.storage.UpsertLock(lock); err != nil {
		return nil, err
	}

	m.addHistoryLocked(lockName, holder, model.OpRelease, "released")

	grantedLock, err := m.tryGrantNextLocked(lockName)
	if err != nil {
		return nil, err
	}

	return &ReleaseResult{Released: true, Count: 0, Granted: grantedLock}, nil
}

func (m *Manager) tryGrantNextLocked(lockName string) (*model.Lock, error) {
	item, err := m.storage.Dequeue(lockName)
	if err != nil {
		return nil, err
	}
	if item == nil {
		return nil, nil
	}

	now := time.Now()
	if item.TimeoutAt.Before(now) {
		m.addHistoryLocked(lockName, item.Holder, model.OpTimeout, "timed out before grant")
		return m.tryGrantNextLocked(lockName)
	}

	lock, err := m.storage.GetLock(lockName)
	if err != nil {
		return nil, err
	}

	lock.Status = model.LockStatusHeld
	lock.Holder = item.Holder
	lock.Reentrant = item.Reentrant
	lock.Count = 1
	lock.UpdatedAt = now
	if err := m.storage.UpsertLock(lock); err != nil {
		return nil, err
	}

	lease := &model.Lease{
		LockName:   lockName,
		Holder:     item.Holder,
		LeaseSec:   item.LeaseSec,
		AcquiredAt: now,
		ExpiresAt:  now.Add(time.Duration(item.LeaseSec) * time.Second),
		Active:     true,
	}
	if err := m.storage.CreateLease(lease); err != nil {
		return nil, err
	}

	m.setLeaseTimerLocked(lockName, time.Duration(item.LeaseSec)*time.Second)
	m.addHistoryLocked(lockName, item.Holder, model.OpGrantNext, fmt.Sprintf("granted from queue, lease=%ds", item.LeaseSec))

	return lock, nil
}

func (m *Manager) RenewLease(lockName, holder string, addSec int) (*model.Lease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	lease, err := m.storage.GetActiveLease(lockName)
	if err != nil {
		return nil, err
	}
	if lease == nil {
		return nil, fmt.Errorf("no active lease for lock: %s", lockName)
	}

	if lease.Holder != holder {
		return nil, fmt.Errorf("not the lease holder: current=%s", lease.Holder)
	}

	now := time.Now()
	if lease.ExpiresAt.Before(now) {
		return nil, fmt.Errorf("lease already expired")
	}

	newExpiresAt := lease.ExpiresAt.Add(time.Duration(addSec) * time.Second)
	if err := m.storage.UpdateLeaseExpiry(lockName, newExpiresAt); err != nil {
		return nil, err
	}

	remaining := time.Until(newExpiresAt)
	m.setLeaseTimerLocked(lockName, remaining)
	lease.ExpiresAt = newExpiresAt

	m.addHistoryLocked(lockName, holder, model.OpRenew, fmt.Sprintf("renewed +%ds", addSec))

	return lease, nil
}

func (m *Manager) expireLock(lockName string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.expireLockLocked(lockName)
}

func (m *Manager) expireLockLocked(lockName string) {
	lock, err := m.storage.GetLock(lockName)
	if err != nil {
		log.Printf("[lock-manager] expireLock get lock error: %v", err)
		return
	}
	if lock == nil || lock.Status != model.LockStatusHeld {
		return
	}

	holder := lock.Holder

	delete(m.timers, lockName)

	if err := m.storage.DeactivateLease(lockName); err != nil {
		log.Printf("[lock-manager] deactivate lease error: %v", err)
		return
	}

	lock.Status = model.LockStatusExpired
	lock.Holder = ""
	lock.Count = 0
	lock.UpdatedAt = time.Now()
	if err := m.storage.UpsertLock(lock); err != nil {
		log.Printf("[lock-manager] upsert lock error: %v", err)
		return
	}

	m.addHistoryLocked(lockName, holder, model.OpExpire, "lease expired")

	lock.Status = model.LockStatusFree
	lock.UpdatedAt = time.Now()
	if err := m.storage.UpsertLock(lock); err != nil {
		log.Printf("[lock-manager] upsert lock free error: %v", err)
		return
	}

	if _, err := m.tryGrantNextLocked(lockName); err != nil {
		log.Printf("[lock-manager] tryGrantNext error: %v", err)
	}
}

func (m *Manager) watchWaitQueue() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.checkWaitQueueTimeouts()
		}
	}
}

func (m *Manager) checkWaitQueueTimeouts() {
	m.mu.Lock()
	defer m.mu.Unlock()

	items, err := m.storage.ListAllWaitQueue()
	if err != nil {
		log.Printf("[lock-manager] list wait queue error: %v", err)
		return
	}

	now := time.Now()
	for _, item := range items {
		if item.TimeoutAt.Before(now) {
			if err := m.storage.RemoveFromQueueByID(item.ID); err != nil {
				log.Printf("[lock-manager] remove from queue error: %v", err)
				continue
			}
			m.addHistoryLocked(item.LockName, item.Holder, model.OpTimeout, "wait timeout")
			log.Printf("[lock-manager] wait timeout: lock=%s holder=%s", item.LockName, item.Holder)
		}
	}
}

func (m *Manager) ListAllLocks() ([]model.LockStatusInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	locks, err := m.storage.ListLocks()
	if err != nil {
		return nil, err
	}

	var result []model.LockStatusInfo
	for _, lock := range locks {
		info := model.LockStatusInfo{
			Name:      lock.Name,
			Status:    lock.Status,
			Holder:    lock.Holder,
			Reentrant: lock.Reentrant,
			Count:     lock.Count,
		}

		if lock.Status == model.LockStatusHeld {
			lease, err := m.storage.GetActiveLease(lock.Name)
			if err == nil && lease != nil {
				remaining := time.Until(lease.ExpiresAt).Seconds()
				if remaining < 0 {
					remaining = 0
				}
				info.RemainingSec = remaining
			}
		}

		queueLen, err := m.storage.WaitQueueLen(lock.Name)
		if err == nil {
			info.WaitQueueLen = queueLen
		}

		result = append(result, info)
	}
	return result, nil
}

func (m *Manager) GetLockDetail(lockName string, withHistory bool) (*model.LockDetail, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	lock, err := m.storage.GetLock(lockName)
	if err != nil {
		return nil, err
	}
	if lock == nil {
		return nil, fmt.Errorf("lock not found: %s", lockName)
	}

	detail := &model.LockDetail{
		Lock: *lock,
	}

	if lock.Status == model.LockStatusHeld {
		lease, err := m.storage.GetActiveLease(lockName)
		if err == nil && lease != nil {
			detail.Lease = lease
		}
	}

	queue, err := m.storage.ListWaitQueue(lockName)
	if err != nil {
		return nil, err
	}
	detail.WaitQueue = queue

	if withHistory {
		history, err := m.storage.ListHistory(lockName, 50)
		if err != nil {
			return nil, err
		}
		detail.History = history
	}

	return detail, nil
}

func (m *Manager) ListActiveLeases() ([]model.Lease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.storage.ListActiveLeases()
}

func (m *Manager) GetLockHistory(lockName string, limit int) ([]model.OperationHistory, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.storage.ListHistory(lockName, limit)
}

func (m *Manager) addHistoryLocked(lockName, holder string, op model.OperationType, detail string) {
	h := &model.OperationHistory{
		LockName:  lockName,
		Holder:    holder,
		Operation: op,
		Detail:    detail,
		CreatedAt: time.Now(),
	}
	_ = m.storage.AddHistory(h)
}
