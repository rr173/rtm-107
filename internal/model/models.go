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
	ID             int64     `json:"id"`
	CallerID       string    `json:"caller_id"`
	PolicyName     string    `json:"policy_name"`
	QuotaLimit     int       `json:"quota_limit"`
	UsedTokens     int       `json:"used_tokens"`
	BorrowedTokens int       `json:"borrowed_tokens"`
	LentTokens     int       `json:"lent_tokens"`
	LastRefillAt   time.Time `json:"last_refill_at,omitempty"`
	WindowStartAt  time.Time `json:"window_start_at,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
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
	Tokens int `json:"tokens" binding:"required,min=1"`
}

type TokenResult struct {
	Allowed       bool   `json:"allowed"`
	Granted       int    `json:"granted"`
	Requested     int    `json:"requested"`
	Remaining     int    `json:"remaining"`
	QuotaLimit    int    `json:"quota_limit"`
	UsedTokens    int    `json:"used_tokens"`
	Reason        string `json:"reason,omitempty"`
}

type CallerStatus struct {
	CallerID       string `json:"caller_id"`
	PolicyName     string `json:"policy_name"`
	Algorithm      string `json:"algorithm"`
	QuotaLimit     int    `json:"quota_limit"`
	UsedTokens     int    `json:"used_tokens"`
	Remaining      int    `json:"remaining"`
	BorrowedTokens int    `json:"borrowed_tokens"`
	LentTokens     int    `json:"lent_tokens"`
	RateLimited    int64  `json:"rate_limited_count"`
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
