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
	ID         int64     `json:"id"`
	LockName   string    `json:"lock_name"`
	Holder     string    `json:"holder"`
	LeaseSec   int       `json:"lease_sec"`
	AcquiredAt time.Time `json:"acquired_at"`
	ExpiresAt  time.Time `json:"expires_at"`
	Active     bool      `json:"active"`
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
