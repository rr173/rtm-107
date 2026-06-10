package lock

import (
	"fmt"
	"log"
	"rtm-107/internal/model"
	"rtm-107/internal/storage"
	"sort"
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
	Acquired       bool
	Queued         bool
	Lock           *model.Lock
	Lease          *model.Lease
	Position       int
	Deadlock       bool
	DeadlockCycle  *model.DeadlockCycle
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
		if lock.Holder == holder {
			if lock.Reentrant && reentrant {
				lock.Count++
				lock.UpdatedAt = now
				if err := m.storage.UpsertLock(lock); err != nil {
					return nil, err
				}
				m.addHistoryLocked(lockName, holder, model.OpAcquire, fmt.Sprintf("reentrant acquire, count=%d", lock.Count))
				lease, _ := m.storage.GetActiveLease(lockName)
				fillLeaseRemaining(lease)
				return &AcquireResult{Acquired: true, Lock: lock, Lease: lease}, nil
			}
			return nil, fmt.Errorf("already hold this lock (non-reentrant)")
		}

		deadlockCycle, err := m.checkDeadlockLocked(holder, lockName, lock.Holder)
		if err != nil {
			return nil, err
		}
		if deadlockCycle != nil {
			m.addHistoryLocked(lockName, holder, model.OpAcquire, "rejected: deadlock detected")
			return &AcquireResult{
				Deadlock:      true,
				DeadlockCycle: deadlockCycle,
				Lock:          lock,
			}, nil
		}

		queue, err := m.storage.ListWaitQueue(lockName)
		if err != nil {
			return nil, err
		}
		for _, item := range queue {
			if item.Holder == holder {
				m.addHistoryLocked(lockName, holder, model.OpAcquire, "rejected: already in queue")
				return nil, fmt.Errorf("already in wait queue for this lock")
			}
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

		position := len(queue) + 1

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

	fillLeaseRemaining(lease)
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

	fillLeaseRemaining(lease)
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
			fillLeaseRemaining(lease)
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

	leases, err := m.storage.ListActiveLeases()
	if err != nil {
		return nil, err
	}
	for i := range leases {
		fillLeaseRemaining(&leases[i])
	}
	return leases, nil
}

func (m *Manager) GetLockHistory(lockName string, limit int) ([]model.OperationHistory, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.storage.ListHistory(lockName, limit)
}

func (m *Manager) CancelWaitForHolder(holder string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	items, err := m.storage.ListAllWaitQueue()
	if err != nil {
		return 0, err
	}

	count := 0
	for _, item := range items {
		if item.Holder == holder {
			if err := m.storage.RemoveFromQueueByID(item.ID); err != nil {
				return count, err
			}
			m.addHistoryLocked(item.LockName, holder, model.OpRelease, "cancelled from wait queue by orchestration rollback")
			count++
		}
	}
	return count, nil
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

func fillLeaseRemaining(lease *model.Lease) {
	if lease == nil || !lease.Active {
		return
	}
	remaining := time.Until(lease.ExpiresAt).Seconds()
	if remaining < 0 {
		remaining = 0
	}
	lease.RemainingSec = remaining
}

func (m *Manager) buildWaitGraphLocked() ([]model.WaitGraphEdge, map[string]map[string]string, error) {
	waitItems, err := m.storage.ListAllWaitQueue()
	if err != nil {
		return nil, nil, err
	}

	locks, err := m.storage.ListLocks()
	if err != nil {
		return nil, nil, err
	}

	lockHolderMap := make(map[string]string)
	for _, l := range locks {
		if l.Status == model.LockStatusHeld {
			lockHolderMap[l.Name] = l.Holder
		}
	}

	var edges []model.WaitGraphEdge
	adj := make(map[string]map[string]string)

	for _, item := range waitItems {
		holder, ok := lockHolderMap[item.LockName]
		if !ok || holder == "" {
			continue
		}
		if holder == item.Holder {
			continue
		}
		edge := model.WaitGraphEdge{
			Waiter:   item.Holder,
			LockName: item.LockName,
			Holder:   holder,
		}
		edges = append(edges, edge)
		if adj[item.Holder] == nil {
			adj[item.Holder] = make(map[string]string)
		}
		adj[item.Holder][holder] = item.LockName
	}

	return edges, adj, nil
}

func (m *Manager) detectCycle(adj map[string]map[string]string, start string) ([]string, []string, bool) {
	visited := make(map[string]bool)
	recStack := make(map[string]bool)
	path := make([]string, 0)

	var dfs func(node string) bool
	var cycleNodes []string
	var cycleStartIdx int

	dfs = func(node string) bool {
		visited[node] = true
		recStack[node] = true
		path = append(path, node)

		for next := range adj[node] {
			if !visited[next] {
				if dfs(next) {
					return true
				}
			} else if recStack[next] {
				for i, p := range path {
					if p == next {
						cycleStartIdx = i
						break
					}
				}
				cycleNodes = make([]string, len(path)-cycleStartIdx)
				copy(cycleNodes, path[cycleStartIdx:])
				return true
			}
		}

		path = path[:len(path)-1]
		recStack[node] = false
		return false
	}

	if !dfs(start) {
		return nil, nil, false
	}

	cycleLocks := make([]string, 0, len(cycleNodes))
	for i := 0; i < len(cycleNodes); i++ {
		cur := cycleNodes[i]
		next := cycleNodes[(i+1)%len(cycleNodes)]
		if lockName, ok := adj[cur][next]; ok {
			cycleLocks = append(cycleLocks, lockName)
		}
	}

	return cycleNodes, cycleLocks, true
}

func (m *Manager) checkDeadlockLocked(waiter, lockName, holder string) (*model.DeadlockCycle, error) {
	_, adj, err := m.buildWaitGraphLocked()
	if err != nil {
		return nil, err
	}

	if adj[waiter] == nil {
		adj[waiter] = make(map[string]string)
	}
	adj[waiter][holder] = lockName

	cycleNodes, cycleLocks, hasCycle := m.detectCycle(adj, waiter)
	if !hasCycle {
		return nil, nil
	}

	var cycle []model.WaitGraphEdge
	for i := 0; i < len(cycleNodes)-1; i++ {
		cycle = append(cycle, model.WaitGraphEdge{
			Waiter:   cycleNodes[i],
			LockName: cycleLocks[i],
			Holder:   cycleNodes[i+1],
		})
	}
	if len(cycleNodes) > 0 && len(cycleLocks) > 0 {
		lastIdx := len(cycleLocks) - 1
		cycle = append(cycle, model.WaitGraphEdge{
			Waiter:   cycleNodes[len(cycleNodes)-1],
			LockName: cycleLocks[lastIdx],
			Holder:   cycleNodes[0],
		})
	}

	return &model.DeadlockCycle{Cycle: cycle}, nil
}

func (m *Manager) GetWaitGraph() (*model.WaitGraph, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	edges, _, err := m.buildWaitGraphLocked()
	if err != nil {
		return nil, err
	}

	nodeSet := make(map[string]bool)
	for _, e := range edges {
		nodeSet[e.Waiter] = true
		nodeSet[e.Holder] = true
	}
	nodes := make([]string, 0, len(nodeSet))
	for n := range nodeSet {
		nodes = append(nodes, n)
	}
	sort.Strings(nodes)

	return &model.WaitGraph{
		Edges: edges,
		Nodes: nodes,
	}, nil
}

func (m *Manager) AcquireLocksBatch(lockNames []string, holder string, leaseSec int, reentrant bool) (*model.BatchAcquireResult, error) {
	if leaseSec <= 0 {
		return nil, fmt.Errorf("lease_sec must be positive")
	}
	if len(lockNames) == 0 {
		return nil, fmt.Errorf("lock_names must not be empty")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	uniqueLocks := make(map[string]bool)
	for _, name := range lockNames {
		uniqueLocks[name] = true
	}
	sortedNames := make([]string, 0, len(uniqueLocks))
	for name := range uniqueLocks {
		sortedNames = append(sortedNames, name)
	}
	sort.Strings(sortedNames)

	for _, lockName := range sortedNames {
		lock, err := m.storage.GetLock(lockName)
		if err != nil {
			return nil, err
		}
		if lock != nil && lock.Status == model.LockStatusHeld {
			return &model.BatchAcquireResult{
				Acquired:   false,
				FailedLock: lockName,
				FailedBy:   lock.Holder,
			}, nil
		}
	}

	acquiredLocks := make([]*model.Lock, 0)
	acquiredLeases := make([]*model.Lease, 0)

	for _, lockName := range sortedNames {
		result, err := m.acquireLockNoQueueLocked(lockName, holder, leaseSec, reentrant)
		if err != nil {
			m.rollbackBatchLocked(acquiredLocks, holder)
			return &model.BatchAcquireResult{
				Acquired:   false,
				FailedLock: lockName,
				FailedBy:   err.Error(),
			}, nil
		}
		if result != nil {
			acquiredLocks = append(acquiredLocks, result.Lock)
			acquiredLeases = append(acquiredLeases, result.Lease)
		}
	}

	resultLocks := make([]model.Lock, 0, len(acquiredLocks))
	resultLeases := make([]model.Lease, 0, len(acquiredLeases))
	for _, l := range acquiredLocks {
		resultLocks = append(resultLocks, *l)
	}
	for _, l := range acquiredLeases {
		lease := *l
		fillLeaseRemaining(&lease)
		resultLeases = append(resultLeases, lease)
	}

	for _, lockName := range sortedNames {
		m.addHistoryLocked(lockName, holder, model.OpAcquire, fmt.Sprintf("batch acquire, lease=%ds", leaseSec))
	}

	return &model.BatchAcquireResult{
		Acquired: true,
		Locks:    resultLocks,
		Leases:   resultLeases,
	}, nil
}

func (m *Manager) acquireLockNoQueueLocked(lockName, holder string, leaseSec int, reentrant bool) (*AcquireResult, error) {
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
		if lock.Holder == holder {
			if lock.Reentrant && reentrant {
				lock.Count++
				lock.UpdatedAt = now
				if err := m.storage.UpsertLock(lock); err != nil {
					return nil, err
				}
				m.addHistoryLocked(lockName, holder, model.OpAcquire, fmt.Sprintf("reentrant acquire, count=%d", lock.Count))
				lease, _ := m.storage.GetActiveLease(lockName)
				fillLeaseRemaining(lease)
				return &AcquireResult{Acquired: true, Lock: lock, Lease: lease}, nil
			}
			return nil, fmt.Errorf("already hold this lock (non-reentrant)")
		}
		return nil, fmt.Errorf("lock held by %s", lock.Holder)
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

	fillLeaseRemaining(lease)
	return &AcquireResult{Acquired: true, Lock: lock, Lease: lease}, nil
}

func (m *Manager) rollbackBatchLocked(locks []*model.Lock, holder string) {
	for i := len(locks) - 1; i >= 0; i-- {
		lock := locks[i]
		_, _ = m.releaseLockLocked(lock.Name, holder)
		m.addHistoryLocked(lock.Name, holder, model.OpRelease, "batch rollback")
	}
}
