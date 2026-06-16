package heatmap

import (
	"fmt"
	"log"
	"rtm-107/internal/model"
	"rtm-107/internal/storage"
	"sort"
	"sync"
	"time"
)

type Manager struct {
	storage *storage.Storage
	lockMgr LockManager
	config  model.HeatmapConfig
	mu      sync.RWMutex
	stopCh  chan struct{}

	activeWaits      map[string]map[string]*waitRecord
	recentAlertLocks map[string]time.Time
}

type LockManager interface {
	ListAllLocks() ([]model.LockStatusInfo, error)
	WaitQueueLen(lockName string) (int, error)
	ListAllWaitQueue() ([]model.WaitQueueItem, error)
}

type waitRecord struct {
	LockName    string
	Holder      string
	EnqueuedAt  time.Time
	WaitStartAt time.Time
}

func NewManager(s *storage.Storage, lockMgr LockManager) *Manager {
	return &Manager{
		storage:          s,
		lockMgr:          lockMgr,
		stopCh:           make(chan struct{}),
		activeWaits:      make(map[string]map[string]*waitRecord),
		recentAlertLocks: make(map[string]time.Time),
		config: model.HeatmapConfig{
			WindowMinutes:       5,
			AlertThresholdMs:    5000,
			AlertSuppressMin:    10,
			TopN:                10,
			HistoryRetentionMin: 1440,
		},
	}
}

func (m *Manager) Start() error {
	m.mu.Lock()
	cfg, err := m.storage.GetHeatmapConfig()
	if err != nil {
		m.mu.Unlock()
		return fmt.Errorf("get heatmap config: %w", err)
	}
	m.config = *cfg

	if m.lockMgr != nil {
		items, err := m.lockMgr.ListAllWaitQueue()
		if err != nil {
			log.Printf("[heatmap] restore active waits warning: %v", err)
		} else {
			restored := 0
			for _, it := range items {
				if it.EnqueuedAt.IsZero() {
					continue
				}
				if m.activeWaits[it.LockName] == nil {
					m.activeWaits[it.LockName] = make(map[string]*waitRecord)
				}
				m.activeWaits[it.LockName][it.Holder] = &waitRecord{
					LockName:    it.LockName,
					Holder:      it.Holder,
					EnqueuedAt:  it.EnqueuedAt,
					WaitStartAt: it.EnqueuedAt,
				}
				restored++
			}
			if restored > 0 {
				log.Printf("[heatmap] restored %d active wait records from storage", restored)
			}
		}
	}
	m.mu.Unlock()

	go m.backgroundLoop()
	log.Println("[heatmap] started")
	return nil
}

func (m *Manager) Stop() {
	close(m.stopCh)
	log.Println("[heatmap] stopped")
}

func (m *Manager) backgroundLoop() {
	alertTicker := time.NewTicker(30 * time.Second)
	purgeTicker := time.NewTicker(1 * time.Hour)
	defer alertTicker.Stop()
	defer purgeTicker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-alertTicker.C:
			m.checkHotspotsAndAlert()
		case <-purgeTicker.C:
			m.purgeOldData()
		}
	}
}

func (m *Manager) RecordLockRequest(lockName string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.recordRequestLocked(lockName, 1, 0, 0, 0)
}

func (m *Manager) RecordLockEnqueue(lockName, holder string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	wr := &waitRecord{
		LockName:    lockName,
		Holder:      holder,
		EnqueuedAt:  time.Now(),
		WaitStartAt: time.Now(),
	}
	if m.activeWaits[lockName] == nil {
		m.activeWaits[lockName] = make(map[string]*waitRecord)
	}
	m.activeWaits[lockName][holder] = wr

	m.recordRequestLocked(lockName, 0, 1, 0, 0)
}

func (m *Manager) RecordLockGranted(lockName, holder string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	waitMap, ok := m.activeWaits[lockName]
	if !ok {
		return
	}
	wr, ok := waitMap[holder]
	if !ok {
		return
	}
	waitMs := int64(time.Since(wr.WaitStartAt).Milliseconds())
	delete(waitMap, holder)
	if len(waitMap) == 0 {
		delete(m.activeWaits, lockName)
	}

	m.recordRequestLocked(lockName, 0, 0, waitMs, waitMs)
}

func (m *Manager) RecordLockGrantedWithEnqueue(lockName, holder string, enqueuedAt time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var waitStart time.Time
	waitMap, ok := m.activeWaits[lockName]
	if ok {
		if wr, ok := waitMap[holder]; ok {
			waitStart = wr.WaitStartAt
			delete(waitMap, holder)
			if len(waitMap) == 0 {
				delete(m.activeWaits, lockName)
			}
		}
	}
	if waitStart.IsZero() && !enqueuedAt.IsZero() {
		waitStart = enqueuedAt
	}
	if waitStart.IsZero() {
		return
	}
	waitMs := int64(time.Since(waitStart).Milliseconds())
	if waitMs < 0 {
		waitMs = 0
	}

	m.recordRequestLocked(lockName, 0, 0, waitMs, waitMs)
}

func (m *Manager) RecordLockTimeout(lockName, holder string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	waitMap, ok := m.activeWaits[lockName]
	if !ok {
		return
	}
	wr, ok := waitMap[holder]
	if !ok {
		return
	}
	waitMs := int64(time.Since(wr.WaitStartAt).Milliseconds())
	delete(waitMap, holder)
	if len(waitMap) == 0 {
		delete(m.activeWaits, lockName)
	}

	m.recordRequestLocked(lockName, 0, 0, waitMs, waitMs)
}

func (m *Manager) RecordLockTimeoutWithEnqueue(lockName, holder string, enqueuedAt time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var waitStart time.Time
	waitMap, ok := m.activeWaits[lockName]
	if ok {
		if wr, ok := waitMap[holder]; ok {
			waitStart = wr.WaitStartAt
			delete(waitMap, holder)
			if len(waitMap) == 0 {
				delete(m.activeWaits, lockName)
			}
		}
	}
	if waitStart.IsZero() && !enqueuedAt.IsZero() {
		waitStart = enqueuedAt
	}
	if waitStart.IsZero() {
		return
	}
	waitMs := int64(time.Since(waitStart).Milliseconds())
	if waitMs < 0 {
		waitMs = 0
	}

	m.recordRequestLocked(lockName, 0, 0, waitMs, waitMs)
}

func (m *Manager) RecordLockRequestWithWait(lockName string, waitMs int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if waitMs > 0 {
		m.recordRequestLocked(lockName, 1, 1, waitMs, waitMs)
	} else {
		m.recordRequestLocked(lockName, 1, 0, 0, 0)
	}
}

func (m *Manager) recordRequestLocked(lockName string, reqCnt, waitCnt, totalWaitMs, maxWaitMs int64) {
	bucket := time.Now().Truncate(time.Minute)
	now := time.Now()
	stat := &model.LockContentionMinuteStat{
		LockName:     lockName,
		MinuteBucket: bucket,
		RequestCount: reqCnt,
		WaitCount:    waitCnt,
		TotalWaitMs:  totalWaitMs,
		MaxWaitMs:    maxWaitMs,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	if err := m.storage.UpsertLockContentionStat(stat); err != nil {
		log.Printf("[heatmap] record stat error: %v", err)
	}
}

func (m *Manager) checkHotspotsAndAlert() {
	m.mu.RLock()
	windowMin := m.config.WindowMinutes
	threshold := m.config.AlertThresholdMs
	suppressMin := m.config.AlertSuppressMin
	if suppressMin <= 0 {
		suppressMin = 10
	}
	topN := m.config.TopN
	m.mu.RUnlock()

	endTime := time.Now().Truncate(time.Minute).Add(time.Minute)
	startTime := endTime.Add(-time.Duration(windowMin) * time.Minute)

	heats, err := m.storage.GetAggregatedLockHeatInWindow(startTime, endTime)
	if err != nil {
		log.Printf("[heatmap] check hotspots get heat error: %v", err)
		return
	}

	m.mu.Lock()
	now := time.Now()
	for i, h := range heats {
		if i >= topN {
			break
		}
		if h.AvgWaitMs <= threshold {
			continue
		}

		lastAlertAt, exists := m.recentAlertLocks[h.LockName]
		suppressDur := time.Duration(suppressMin) * time.Minute
		if exists && now.Sub(lastAlertAt) < suppressDur {
			continue
		}

		qLen, _ := m.lockMgr.WaitQueueLen(h.LockName)
		h.CurrentQueueLen = qLen

		alert := &model.HotspotAlertEvent{
			LockName:        h.LockName,
			AvgWaitMs:       h.AvgWaitMs,
			ThresholdMs:     threshold,
			RequestCount:    h.RequestCount,
			WaitCount:       h.WaitCount,
			MaxWaitMs:       h.MaxWaitMs,
			CurrentQueueLen: qLen,
			WindowMinutes:   windowMin,
			AlertType:       "avg_wait_exceeded",
			Detail:          fmt.Sprintf("锁 %s 在最近 %d 分钟内平均等待 %.2fms 超过阈值 %.2fms", h.LockName, windowMin, h.AvgWaitMs, threshold),
			Acknowledged:    false,
			CreatedAt:       now,
		}
		if err := m.storage.CreateHotspotAlert(alert); err != nil {
			log.Printf("[heatmap] create alert error: %v", err)
			continue
		}
		m.recentAlertLocks[h.LockName] = now
		log.Printf("[heatmap] HOTSPOT ALERT: lock=%s avg_wait=%.2fms threshold=%.2fms reqs=%d queue=%d",
			h.LockName, h.AvgWaitMs, threshold, h.RequestCount, qLen)
	}
	m.mu.Unlock()
}

func (m *Manager) purgeOldData() {
	m.mu.RLock()
	retention := m.config.HistoryRetentionMin
	m.mu.RUnlock()

	n, err := m.storage.PurgeOldLockContentionStats(retention)
	if err != nil {
		log.Printf("[heatmap] purge error: %v", err)
		return
	}
	if n > 0 {
		log.Printf("[heatmap] purged %d old contention stats", n)
	}
}

func (m *Manager) GetTopHeatLocks(limit int) ([]model.LockHeatInfo, error) {
	m.mu.RLock()
	windowMin := m.config.WindowMinutes
	if limit <= 0 {
		limit = m.config.TopN
	}
	m.mu.RUnlock()

	endTime := time.Now().Truncate(time.Minute).Add(time.Minute)
	startTime := endTime.Add(-time.Duration(windowMin) * time.Minute)

	heats, err := m.storage.GetAggregatedLockHeatInWindow(startTime, endTime)
	if err != nil {
		return nil, err
	}

	for i := range heats {
		qLen, _ := m.lockMgr.WaitQueueLen(heats[i].LockName)
		heats[i].CurrentQueueLen = qLen
	}

	sort.Slice(heats, func(i, j int) bool {
		return heats[i].HeatScore > heats[j].HeatScore
	})

	if limit < len(heats) {
		heats = heats[:limit]
	}
	return heats, nil
}

func (m *Manager) GetLockTrend(lockName string, minutes int) ([]model.LockTrendPoint, error) {
	if minutes <= 0 {
		minutes = 60
	}
	endTime := time.Now().Truncate(time.Minute).Add(time.Minute)
	startTime := endTime.Add(-time.Duration(minutes) * time.Minute)

	stats, err := m.storage.GetLockContentionStatsInWindow(lockName, startTime, endTime)
	if err != nil {
		return nil, err
	}

	bucketMap := make(map[time.Time]*model.LockContentionMinuteStat)
	for i := range stats {
		bucketMap[stats[i].MinuteBucket] = &stats[i]
	}

	points := make([]model.LockTrendPoint, 0, minutes)
	for t := startTime; t.Before(endTime); t = t.Add(time.Minute) {
		p := model.LockTrendPoint{
			MinuteBucket: t,
		}
		if s, ok := bucketMap[t]; ok {
			p.RequestCount = s.RequestCount
			p.WaitCount = s.WaitCount
			p.MaxWaitMs = s.MaxWaitMs
			if s.WaitCount > 0 {
				p.AvgWaitMs = float64(s.TotalWaitMs) / float64(s.WaitCount)
			}
		}
		points = append(points, p)
	}
	return points, nil
}

func (m *Manager) ListAlerts(lockName string, acknowledged *bool, limit int) ([]model.HotspotAlertEvent, error) {
	if limit <= 0 {
		limit = 100
	}
	return m.storage.ListHotspotAlerts(lockName, acknowledged, limit)
}

func (m *Manager) AcknowledgeAlert(id int64, acknowledgedBy string) error {
	return m.storage.AcknowledgeHotspotAlert(id, acknowledgedBy)
}

func (m *Manager) GetConfig() model.HeatmapConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.config
}

func (m *Manager) UpdateConfig(req model.UpdateHeatmapConfigRequest) (*model.HeatmapConfig, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if req.WindowMinutes != nil && *req.WindowMinutes > 0 {
		m.config.WindowMinutes = *req.WindowMinutes
	}
	if req.AlertThresholdMs != nil && *req.AlertThresholdMs > 0 {
		m.config.AlertThresholdMs = *req.AlertThresholdMs
	}
	if req.AlertSuppressMin != nil && *req.AlertSuppressMin > 0 {
		m.config.AlertSuppressMin = *req.AlertSuppressMin
	}
	if req.TopN != nil && *req.TopN > 0 {
		m.config.TopN = *req.TopN
	}
	if req.HistoryRetentionMin != nil && *req.HistoryRetentionMin > 0 {
		m.config.HistoryRetentionMin = *req.HistoryRetentionMin
	}

	if err := m.storage.UpdateHeatmapConfig(&m.config); err != nil {
		return nil, err
	}

	cfgCopy := m.config
	return &cfgCopy, nil
}

func (m *Manager) GetGlobalStats() (*model.HeatmapGlobalStats, error) {
	m.mu.RLock()
	windowMin := m.config.WindowMinutes
	cfg := m.config
	m.mu.RUnlock()

	endTime := time.Now().Truncate(time.Minute).Add(time.Minute)
	startTime := endTime.Add(-time.Duration(windowMin) * time.Minute)

	heats, err := m.storage.GetAggregatedLockHeatInWindow(startTime, endTime)
	if err != nil {
		return nil, err
	}

	stats := &model.HeatmapGlobalStats{
		Config: cfg,
	}

	totalReqs := int64(0)
	totalWaits := int64(0)
	totalWaitMs := int64(0)
	hotCount := 0

	for _, h := range heats {
		totalReqs += h.RequestCount
		totalWaits += h.WaitCount
		totalWaitMs += int64(h.AvgWaitMs * float64(h.WaitCount))
		if h.AvgWaitMs > cfg.AlertThresholdMs {
			hotCount++
		}
	}
	stats.TotalLocks = len(heats)
	stats.TotalRequests = totalReqs
	stats.TotalWaits = totalWaits
	stats.HotLocks = hotCount
	if totalWaits > 0 {
		stats.OverallAvgWaitMs = float64(totalWaitMs) / float64(totalWaits)
	}

	falseVal := false
	alerts, err := m.storage.ListHotspotAlerts("", &falseVal, 1000)
	if err == nil {
		stats.ActiveAlerts = len(alerts)
	}

	return stats, nil
}
