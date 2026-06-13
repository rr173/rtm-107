package model

import "time"

type LockStatus string

const (
	LockStatusFree     LockStatus = "free"
	LockStatusHeld     LockStatus = "held"
	LockStatusExpired  LockStatus = "expired"
)

type Lock struct {
	ID        int64      `json:"id"`
	Name      string     `json:"name"`
	Status    LockStatus `json:"status"`
	Holder    string     `json:"holder,omitempty"`
	Reentrant bool       `json:"reentrant"`
	Count     int        `json:"count"`
	CreatedAt time.Time  `json:"created_at"`
	UpdatedAt time.Time  `json:"updated_at"`
}

type Lease struct {
	ID            int64     `json:"id"`
	LockName      string    `json:"lock_name"`
	Holder        string    `json:"holder"`
	LeaseSec      int       `json:"lease_sec"`
	AcquiredAt    time.Time `json:"acquired_at"`
	ExpiresAt     time.Time `json:"expires_at"`
	Active        bool      `json:"active"`
	RemainingSec  float64   `json:"remaining_sec,omitempty"`
}

type WaitQueueItem struct {
	ID         int64     `json:"id"`
	LockName   string    `json:"lock_name"`
	Holder     string    `json:"holder"`
	Reentrant  bool      `json:"reentrant"`
	LeaseSec   int       `json:"lease_sec"`
	EnqueuedAt time.Time `json:"enqueued_at"`
	TimeoutAt  time.Time `json:"timeout_at"`
}

type OperationType string

const (
	OpAcquire     OperationType = "acquire"
	OpRelease     OperationType = "release"
	OpRenew       OperationType = "renew"
	OpExpire      OperationType = "expire"
	OpTimeout     OperationType = "timeout"
	OpGrantNext   OperationType = "grant_next"
)

type OperationHistory struct {
	ID        int64         `json:"id"`
	LockName  string        `json:"lock_name"`
	Holder    string        `json:"holder"`
	Operation OperationType `json:"operation"`
	Detail    string        `json:"detail"`
	CreatedAt time.Time     `json:"created_at"`
}

type LockDetail struct {
	Lock       Lock            `json:"lock"`
	Lease      *Lease          `json:"lease,omitempty"`
	WaitQueue  []WaitQueueItem `json:"wait_queue"`
	History    []OperationHistory `json:"history,omitempty"`
}

type LockStatusInfo struct {
	Name           string     `json:"name"`
	Status         LockStatus `json:"status"`
	Holder         string     `json:"holder,omitempty"`
	Reentrant      bool       `json:"reentrant"`
	Count          int        `json:"count"`
	RemainingSec   float64    `json:"remaining_sec,omitempty"`
	WaitQueueLen   int        `json:"wait_queue_len"`
}

type WaitGraphEdge struct {
	Waiter    string `json:"waiter"`
	LockName  string `json:"lock_name"`
	Holder    string `json:"holder"`
}

type DeadlockCycle struct {
	Cycle []WaitGraphEdge `json:"cycle"`
}

type BatchAcquireRequest struct {
	LockNames []string `json:"lock_names" binding:"required,min=1"`
	Holder    string   `json:"holder" binding:"required"`
	LeaseSec  int      `json:"lease_sec" binding:"required,min=1"`
	Reentrant bool     `json:"reentrant"`
}

type BatchAcquireResult struct {
	Acquired    bool     `json:"acquired"`
	FailedLock  string   `json:"failed_lock,omitempty"`
	FailedBy    string   `json:"failed_by,omitempty"`
	Locks       []Lock   `json:"locks,omitempty"`
	Leases      []Lease  `json:"leases,omitempty"`
}

type WaitGraph struct {
	Edges []WaitGraphEdge `json:"edges"`
	Nodes []string        `json:"nodes"`
}

type AlgorithmType string

const (
	AlgoFixedWindow   AlgorithmType = "fixed_window"
	AlgoSlidingWindow AlgorithmType = "sliding_window"
	AlgoTokenBucket   AlgorithmType = "token_bucket"
)

type RateLimitPolicy struct {
	ID           int64         `json:"id"`
	Name         string        `json:"name"`
	Algorithm    AlgorithmType `json:"algorithm"`
	WindowSec    int           `json:"window_sec,omitempty"`
	MaxTokens    int           `json:"max_tokens"`
	RefillRate   float64       `json:"refill_rate,omitempty"`
	RefillUnit   string        `json:"refill_unit,omitempty"`
	CreatedAt    time.Time     `json:"created_at"`
	UpdatedAt    time.Time     `json:"updated_at"`
}

type CallerBinding struct {
	ID                int64     `json:"id"`
	CallerID          string    `json:"caller_id"`
	PolicyName        string    `json:"policy_name"`
	QuotaLimit        int       `json:"quota_limit"`
	UsedTokens        int       `json:"used_tokens"`
	BorrowedTokens    int       `json:"borrowed_tokens"`
	LentTokens        int       `json:"lent_tokens"`
	ReservedTokens    int       `json:"reserved_tokens"`
	LastRefillAt      time.Time `json:"last_refill_at,omitempty"`
	WindowStartAt     time.Time `json:"window_start_at,omitempty"`
	PrevWindowCount   int       `json:"prev_window_count,omitempty"`
	CurrWindowCount   int       `json:"curr_window_count,omitempty"`
	CreatedAt         time.Time `json:"created_at"`
	UpdatedAt         time.Time `json:"updated_at"`
}

type RateLimitEvent struct {
	ID         int64     `json:"id"`
	CallerID   string    `json:"caller_id"`
	PolicyName string    `json:"policy_name"`
	Requested  int       `json:"requested"`
	Granted    int       `json:"granted"`
	Allowed    bool      `json:"allowed"`
	Reason     string    `json:"reason,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
}

type QuotaBorrowRecord struct {
	ID         int64     `json:"id"`
	FromCaller string    `json:"from_caller"`
	ToCaller   string    `json:"to_caller"`
	Amount     int       `json:"amount"`
	Status     string    `json:"status"`
	CreatedAt  time.Time `json:"created_at"`
	ReturnedAt time.Time `json:"returned_at,omitempty"`
}

type TokenRequest struct {
	Tokens   int  `json:"tokens" binding:"required,min=1"`
	Waitable bool `json:"waitable,omitempty"`
	WaitSec  int  `json:"wait_sec,omitempty"`
}

type TokenResult struct {
	Allowed       bool   `json:"allowed"`
	Queued        bool   `json:"queued,omitempty"`
	Position      int    `json:"position,omitempty"`
	Granted       int    `json:"granted"`
	Requested     int    `json:"requested"`
	Remaining     int    `json:"remaining"`
	QuotaLimit    int    `json:"quota_limit"`
	UsedTokens    int    `json:"used_tokens"`
	Reason        string `json:"reason,omitempty"`
}

type RateLimitWaitItem struct {
	ID         int64     `json:"id"`
	CallerID   string    `json:"caller_id"`
	Tokens     int       `json:"tokens"`
	EnqueuedAt time.Time `json:"enqueued_at"`
	TimeoutAt  time.Time `json:"timeout_at"`
}

type CallerStatus struct {
	CallerID       string `json:"caller_id"`
	PolicyName     string `json:"policy_name"`
	Algorithm      string `json:"algorithm"`
	QuotaLimit     int    `json:"quota_limit"`
	PolicyMax      int    `json:"policy_max"`
	UsedTokens     int    `json:"used_tokens"`
	Remaining      int    `json:"remaining"`
	BorrowedTokens int    `json:"borrowed_tokens"`
	LentTokens     int    `json:"lent_tokens"`
	ReservedTokens int    `json:"reserved_tokens"`
	RateLimited    int64  `json:"rate_limited_count"`
	WaitQueueLen   int    `json:"wait_queue_len,omitempty"`
}

type GlobalStats struct {
	TotalCallers      int   `json:"total_callers"`
	TotalPolicies     int   `json:"total_policies"`
	TotalRequests     int64 `json:"total_requests"`
	TotalAllowed      int64 `json:"total_allowed"`
	TotalRateLimited  int64 `json:"total_rate_limited"`
	ActiveBorrows     int   `json:"active_borrows"`
	BorrowedAmount    int   `json:"borrowed_amount"`
}

type BorrowRequest struct {
	FromCaller string `json:"from_caller" binding:"required"`
	ToCaller   string `json:"to_caller" binding:"required"`
	Amount     int    `json:"amount" binding:"required,min=1"`
}

type BorrowResult struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
}

type ReturnRequest struct {
	FromCaller string `json:"from_caller" binding:"required"`
	ToCaller   string `json:"to_caller" binding:"required"`
	Amount     int    `json:"amount" binding:"required,min=1"`
}

type PolicyCreateRequest struct {
	Name       string        `json:"name" binding:"required"`
	Algorithm  AlgorithmType `json:"algorithm" binding:"required"`
	WindowSec  int           `json:"window_sec"`
	MaxTokens  int           `json:"max_tokens" binding:"required,min=1"`
	RefillRate float64       `json:"refill_rate"`
	RefillUnit string        `json:"refill_unit"`
}

type BindCallerRequest struct {
	CallerID   string `json:"caller_id" binding:"required"`
	PolicyName string `json:"policy_name" binding:"required"`
	QuotaLimit int    `json:"quota_limit" binding:"required,min=1"`
}

type AdjustQuotaRequest struct {
	NewQuotaLimit int `json:"new_quota_limit" binding:"required,min=0"`
}

type ReservationStatus string

const (
	ReservationStatusPending   ReservationStatus = "pending"
	ReservationStatusActive    ReservationStatus = "active"
	ReservationStatusCompleted ReservationStatus = "completed"
	ReservationStatusCancelled ReservationStatus = "cancelled"
)

type QuotaReservation struct {
	ID         int64             `json:"id"`
	PolicyName string            `json:"policy_name"`
	CallerID   string            `json:"caller_id"`
	Tokens     int               `json:"tokens"`
	StartAt    time.Time         `json:"start_at"`
	EndAt      time.Time         `json:"end_at"`
	Status     ReservationStatus `json:"status"`
	CreatedAt  time.Time         `json:"created_at"`
	UpdatedAt  time.Time         `json:"updated_at"`
}

type CreateReservationRequest struct {
	PolicyName string    `json:"policy_name" binding:"required"`
	CallerID   string    `json:"caller_id" binding:"required"`
	Tokens     int       `json:"tokens" binding:"required,min=1"`
	StartAt    time.Time `json:"start_at" binding:"required"`
	EndAt      time.Time `json:"end_at" binding:"required"`
}

type ReservationResult struct {
	Success     bool              `json:"success"`
	Message     string            `json:"message,omitempty"`
	Reservation *QuotaReservation `json:"reservation,omitempty"`
}

type TxStatus string

const (
	TxStatusCreated    TxStatus = "created"
	TxStatusCommitted  TxStatus = "committed"
	TxStatusRolledBack TxStatus = "rolled_back"
	TxStatusReleased   TxStatus = "released"
	TxStatusTimedOut   TxStatus = "timed_out"
)

type TxLockSpec struct {
	LockName string `json:"lock_name" binding:"required"`
	LeaseSec int    `json:"lease_sec" binding:"required,min=1"`
}

type TxTokenSpec struct {
	CallerID string `json:"caller_id" binding:"required"`
	Tokens   int    `json:"tokens" binding:"required,min=1"`
}

type TxLock struct {
	ID        int64     `json:"id"`
	TxID      string    `json:"tx_id"`
	LockName  string    `json:"lock_name"`
	LeaseSec  int       `json:"lease_sec"`
	Holder    string    `json:"holder"`
	CreatedAt time.Time `json:"created_at"`
}

type TxToken struct {
	ID        int64     `json:"id"`
	TxID      string    `json:"tx_id"`
	CallerID  string    `json:"caller_id"`
	Tokens    int       `json:"tokens"`
	CreatedAt time.Time `json:"created_at"`
}

type TxStateChange struct {
	ID        int64     `json:"id"`
	TxID      string    `json:"tx_id"`
	FromState TxStatus  `json:"from_state"`
	ToState   TxStatus  `json:"to_state"`
	Reason    string    `json:"reason,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

type OrchestrationTx struct {
	ID            string     `json:"id"`
	Holder        string     `json:"holder"`
	Status        TxStatus   `json:"status"`
	TimeoutSec    int        `json:"timeout_sec"`
	FailReason    string     `json:"fail_reason,omitempty"`
	Locks         []TxLock   `json:"locks,omitempty"`
	Tokens        []TxToken  `json:"tokens,omitempty"`
	StateChanges  []TxStateChange `json:"state_changes,omitempty"`
	CreatedAt     time.Time  `json:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at"`
	ExpiresAt     time.Time  `json:"expires_at"`
}

type CreateTxRequest struct {
	Holder     string        `json:"holder" binding:"required"`
	TimeoutSec int           `json:"timeout_sec" binding:"required,min=1"`
	Locks      []TxLockSpec  `json:"locks" binding:"required,min=1"`
	Tokens     []TxTokenSpec `json:"tokens"`
}

type PreCheckResult struct {
	ConflictingLocks  []ConflictingLockInfo `json:"conflicting_locks,omitempty"`
	InsufficientQuota []InsufficientQuotaInfo `json:"insufficient_quota,omitempty"`
	CanProceed        bool                  `json:"can_proceed"`
}

type ConflictingLockInfo struct {
	LockName string `json:"lock_name"`
	Holder   string `json:"holder"`
}

type InsufficientQuotaInfo struct {
	CallerID  string `json:"caller_id"`
	Requested int    `json:"requested"`
	Remaining int    `json:"remaining"`
}

type ReleaseTxRequest struct {
	Holder string `json:"holder" binding:"required"`
}

type AuditOperationType string

const (
	AuditOpAcquireLock      AuditOperationType = "acquire_lock"
	AuditOpReleaseLock      AuditOperationType = "release_lock"
	AuditOpRenewLock        AuditOperationType = "renew_lock"
	AuditOpAcquireLocksBatch AuditOperationType = "acquire_locks_batch"
	AuditOpRequestTokens    AuditOperationType = "request_tokens"
	AuditOpReturnTokens     AuditOperationType = "return_tokens"
	AuditOpBorrowQuota      AuditOperationType = "borrow_quota"
	AuditOpReturnQuota      AuditOperationType = "return_quota"
	AuditOpAdjustQuota      AuditOperationType = "adjust_quota"
)

type AuditLog struct {
	ID         int64              `json:"id"`
	Timestamp  time.Time          `json:"timestamp"`
	Caller     string             `json:"caller"`
	Operation  AuditOperationType `json:"operation"`
	Resource   string             `json:"resource"`
	Success    bool               `json:"success"`
	FailReason string             `json:"fail_reason,omitempty"`
}

type CircuitBreakerRule struct {
	ID               int64     `json:"id"`
	CallerID         string    `json:"caller_id"`
	WindowSec        int       `json:"window_sec"`
	FailureThreshold int       `json:"failure_threshold"`
	CooldownSec      int       `json:"cooldown_sec"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

type CircuitBreakerState string

const (
	CircuitBreakerClosed   CircuitBreakerState = "closed"
	CircuitBreakerOpen     CircuitBreakerState = "open"
	CircuitBreakerHalfOpen CircuitBreakerState = "half_open"
)

type CircuitBreakerStatus struct {
	ID             int64              `json:"id"`
	CallerID       string             `json:"caller_id"`
	State          CircuitBreakerState `json:"state"`
	TriggeredAt    time.Time          `json:"triggered_at,omitempty"`
	ExpiresAt      time.Time          `json:"expires_at,omitempty"`
	FailuresInWindow int              `json:"failures_in_window"`
	TriggerReason  string             `json:"trigger_reason,omitempty"`
	UpdatedAt      time.Time          `json:"updated_at"`
}

type CircuitBreakerHistory struct {
	ID            int64     `json:"id"`
	CallerID      string    `json:"caller_id"`
	State         string    `json:"state"`
	TriggeredAt   time.Time `json:"triggered_at"`
	RecoveredAt   time.Time `json:"recovered_at,omitempty"`
	TriggerReason string    `json:"trigger_reason,omitempty"`
	RecoverReason string    `json:"recover_reason,omitempty"`
}

type CreateCircuitBreakerRuleRequest struct {
	CallerID         string `json:"caller_id"`
	WindowSec        int    `json:"window_sec" binding:"required,min=1"`
	FailureThreshold int    `json:"failure_threshold" binding:"required,min=1"`
	CooldownSec      int    `json:"cooldown_sec" binding:"required,min=1"`
}

type AuditQueryRequest struct {
	Caller    string `form:"caller"`
	Resource  string `form:"resource"`
	Success   *bool  `form:"success"`
	StartTime string `form:"start_time"`
	EndTime   string `form:"end_time"`
	Page      int    `form:"page,default=1"`
	PageSize  int    `form:"page_size,default=20"`
}

type PaginatedAuditLogs struct {
	Logs     []AuditLog `json:"logs"`
	Total    int64      `json:"total"`
	Page     int        `json:"page"`
	PageSize int        `json:"page_size"`
}

type CallerStats struct {
	CallerID        string  `json:"caller_id"`
	TotalRequests   int64   `json:"total_requests"`
	SuccessCount    int64   `json:"success_count"`
	FailureCount    int64   `json:"failure_count"`
	SuccessRate     float64 `json:"success_rate"`
	FailureRate     float64 `json:"failure_rate"`
	Requests1Min    int64   `json:"requests_1min"`
	Requests5Min    int64   `json:"requests_5min"`
	Requests15Min   int64   `json:"requests_15min"`
}

type GlobalAuditStats struct {
	TotalRequests   int64   `json:"total_requests"`
	SuccessCount    int64   `json:"success_count"`
	FailureCount    int64   `json:"failure_count"`
	SuccessRate     float64 `json:"success_rate"`
	FailureRate     float64 `json:"failure_rate"`
	Requests1Min    int64   `json:"requests_1min"`
	Requests5Min    int64   `json:"requests_5min"`
	Requests15Min   int64   `json:"requests_15min"`
	ActiveBreakers int    `json:"active_breakers"`
}

type TopologyNode struct {
	ID          int64     `json:"id"`
	Name        string    `json:"name"`
	LockName    string    `json:"lock_name"`
	RatePolicy  string    `json:"rate_policy,omitempty"`
	TokenCost   int       `json:"token_cost"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type TopologyEdge struct {
	ID          int64     `json:"id"`
	FromNode    string    `json:"from_node"`
	ToNode      string    `json:"to_node"`
	CreatedAt   time.Time `json:"created_at"`
}

type TopologyGraph struct {
	Nodes []TopologyNode `json:"nodes"`
	Edges []TopologyEdge `json:"edges"`
}

type RegisterNodeRequest struct {
	Name       string `json:"name" binding:"required"`
	LockName   string `json:"lock_name"`
	RatePolicy string `json:"rate_policy,omitempty"`
	TokenCost  int    `json:"token_cost"`
}

type DeclareEdgeRequest struct {
	FromNode string `json:"from_node" binding:"required"`
	ToNode   string `json:"to_node" binding:"required"`
}

type CascadeAcquireRequest struct {
	TargetNode string `json:"target_node" binding:"required"`
	Holder     string `json:"holder" binding:"required"`
	LeaseSec   int    `json:"lease_sec" binding:"required,min=1"`
	Reentrant  bool   `json:"reentrant"`
}

type CascadeAcquireStep struct {
	NodeName string `json:"node_name"`
	LockName string `json:"lock_name"`
	Action   string `json:"action"`
	Success  bool   `json:"success"`
	Message  string `json:"message,omitempty"`
}

type CascadeAcquireResult struct {
	Success  bool                  `json:"success"`
	RolledBack bool                `json:"rolled_back"`
	Steps    []CascadeAcquireStep  `json:"steps"`
	Acquired []string              `json:"acquired,omitempty"`
	Message  string                `json:"message,omitempty"`
	DurationMs int64               `json:"duration_ms"`
}

type CascadeReleaseRequest struct {
	TargetNode string `json:"target_node" binding:"required"`
	Holder     string `json:"holder" binding:"required"`
	Force      bool   `json:"force"`
}

type CascadeReleaseStep struct {
	NodeName string `json:"node_name"`
	LockName string `json:"lock_name"`
	Action   string `json:"action"`
	Success  bool   `json:"success"`
	Message  string `json:"message,omitempty"`
}

type CascadeReleaseResult struct {
	Success  bool                  `json:"success"`
	Steps    []CascadeReleaseStep  `json:"steps"`
	Released []string              `json:"released,omitempty"`
	Message  string                `json:"message,omitempty"`
	DurationMs int64               `json:"duration_ms"`
}

type NodeAncestorsResult struct {
	NodeName  string   `json:"node_name"`
	Ancestors []string `json:"ancestors"`
}

type NodeDescendantsResult struct {
	NodeName    string   `json:"node_name"`
	Descendants []string `json:"descendants"`
}

type HolderResourceTree struct {
	Holder     string              `json:"holder"`
	RootNodes  []string            `json:"root_nodes"`
	HeldNodes  []string            `json:"held_nodes"`
	Tree       map[string][]string `json:"tree"`
}

type TopologyOperationType string

const (
	TopologyOpAcquire TopologyOperationType = "cascade_acquire"
	TopologyOpRelease TopologyOperationType = "cascade_release"
)

type TopologyOperationHistory struct {
	ID           int64                  `json:"id"`
	Operation    TopologyOperationType  `json:"operation"`
	TargetNode   string                 `json:"target_node"`
	Holder       string                 `json:"holder"`
	Success      bool                   `json:"success"`
	RolledBack   bool                   `json:"rolled_back"`
	NodesTouched []string               `json:"nodes_touched"`
	DurationMs   int64                  `json:"duration_ms"`
	Message      string                 `json:"message,omitempty"`
	CreatedAt    time.Time              `json:"created_at"`
}

type TopologyStats struct {
	TotalNodes      int `json:"total_nodes"`
	TotalEdges      int `json:"total_edges"`
	TotalOperations int `json:"total_operations"`
	AcquireOps      int `json:"acquire_operations"`
	ReleaseOps      int `json:"release_operations"`
}

type ShadowPlanStatus string

const (
	ShadowPlanStatusDraft     ShadowPlanStatus = "draft"
	ShadowPlanStatusRunning   ShadowPlanStatus = "running"
	ShadowPlanStatusCompleted ShadowPlanStatus = "completed"
	ShadowPlanStatusApplied   ShadowPlanStatus = "applied"
	ShadowPlanStatusCancelled ShadowPlanStatus = "cancelled"
)

type ShadowDecision string

const (
	ShadowDecisionAdmit        ShadowDecision = "admit"
	ShadowDecisionWait         ShadowDecision = "wait"
	ShadowDecisionDeadlockReject ShadowDecision = "deadlock_reject"
	ShadowDecisionCircuitBreak   ShadowDecision = "circuit_break"
	ShadowDecisionRateLimit      ShadowDecision = "rate_limit"
	ShadowDecisionTxRollback     ShadowDecision = "tx_rollback"
	ShadowDecisionReject         ShadowDecision = "reject"
)

type ShadowRuleCategory string

const (
	ShadowRuleLockDependency ShadowRuleCategory = "lock_dependency"
	ShadowRuleRateLimit      ShadowRuleCategory = "rate_limit"
	ShadowRuleReservation    ShadowRuleCategory = "reservation"
	ShadowRuleCircuitBreaker ShadowRuleCategory = "circuit_breaker"
)

type ShadowPlan struct {
	ID          int64            `json:"id"`
	Name        string           `json:"name"`
	Description string           `json:"description,omitempty"`
	Status      ShadowPlanStatus `json:"status"`
	Mode        string           `json:"mode"`
	AuditLogStartID int64        `json:"audit_log_start_id,omitempty"`
	AuditLogEndID   int64        `json:"audit_log_end_id,omitempty"`
	MirrorUntil    time.Time     `json:"mirror_until,omitempty"`
	AppliedAt      time.Time     `json:"applied_at,omitempty"`
	CreatedAt   time.Time        `json:"created_at"`
	UpdatedAt   time.Time        `json:"updated_at"`
}

type ShadowConfigOverride struct {
	ID         int64            `json:"id"`
	PlanID     int64            `json:"plan_id"`
	Category   ShadowRuleCategory `json:"category"`
	TargetKey  string           `json:"target_key"`
	Field      string           `json:"field"`
	OrigValue  string           `json:"orig_value"`
	NewValue   string           `json:"new_value"`
	CreatedAt  time.Time        `json:"created_at"`
}

type ShadowDiffRecord struct {
	ID             int64          `json:"id"`
	PlanID         int64          `json:"plan_id"`
	AuditLogID     int64          `json:"audit_log_id,omitempty"`
	RequestCaller  string         `json:"request_caller"`
	RequestOp      string         `json:"request_op"`
	RequestResource string        `json:"request_resource"`
	LiveDecision   ShadowDecision `json:"live_decision"`
	ShadowDecision ShadowDecision `json:"shadow_decision"`
	RuleCategory   ShadowRuleCategory `json:"rule_category"`
	Detail         string         `json:"detail,omitempty"`
	CreatedAt      time.Time      `json:"created_at"`
}

type CreateShadowPlanRequest struct {
	Name        string `json:"name" binding:"required"`
	Description string `json:"description"`
	Mode        string `json:"mode" binding:"required"`
	MirrorSec   int    `json:"mirror_sec"`
}

type UpdateShadowOverrideRequest struct {
	Category  ShadowRuleCategory `json:"category" binding:"required"`
	TargetKey string             `json:"target_key" binding:"required"`
	Field     string             `json:"field" binding:"required"`
	NewValue  string             `json:"new_value" binding:"required"`
}

type ShadowDiffStats struct {
	PlanID            int64                       `json:"plan_id"`
	TotalDiffs        int64                       `json:"total_diffs"`
	ByCategory        map[ShadowRuleCategory]int64 `json:"by_category"`
	ByDecisionPair    map[string]int64             `json:"by_decision_pair"`
	TopCallers        []ShadowCallerImpact         `json:"top_callers"`
	TopResources      []ShadowResourceImpact       `json:"top_resources"`
	TopConflictReasons []ShadowConflictReason      `json:"top_conflict_reasons"`
}

type ShadowCallerImpact struct {
	CallerID  string `json:"caller_id"`
	DiffCount int64  `json:"diff_count"`
}

type ShadowResourceImpact struct {
	Resource  string `json:"resource"`
	DiffCount int64  `json:"diff_count"`
}

type ShadowConflictReason struct {
	Reason    string `json:"reason"`
	Category  ShadowRuleCategory `json:"category"`
	Count     int64  `json:"count"`
}
