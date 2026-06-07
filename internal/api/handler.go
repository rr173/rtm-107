package api

import (
	"net/http"
	"rtm-107/internal/lock"
	"rtm-107/internal/model"
	"rtm-107/internal/ratelimit"
	"strconv"

	"github.com/gin-gonic/gin"
)

type Handler struct {
	manager      *lock.Manager
	rateLimiter  *ratelimit.Manager
}

func NewHandler(m *lock.Manager, rl *ratelimit.Manager) *Handler {
	return &Handler{manager: m, rateLimiter: rl}
}

func (h *Handler) RegisterRoutes(r *gin.Engine) {
	r.GET("/health", h.Health)

	api := r.Group("/api/v1")
	{
		locks := api.Group("/locks")
		{
			locks.GET("", h.ListLocks)
			locks.GET("/:name", h.GetLock)
			locks.POST("/:name/acquire", h.AcquireLock)
			locks.POST("/:name/release", h.ReleaseLock)
			locks.POST("/:name/renew", h.RenewLock)
			locks.GET("/:name/history", h.GetLockHistory)
			locks.POST("/batch/acquire", h.AcquireLocksBatch)
		}
		api.GET("/leases", h.ListLeases)
		api.GET("/wait-graph", h.GetWaitGraph)

		rateLimit := api.Group("/ratelimit")
		{
			policies := rateLimit.Group("/policies")
			{
				policies.GET("", h.ListPolicies)
				policies.GET("/:name", h.GetPolicy)
				policies.POST("", h.CreatePolicy)
			}
			callers := rateLimit.Group("/callers")
			{
				callers.GET("", h.ListCallers)
				callers.GET("/:id", h.GetCallerStatus)
				callers.POST("/bind", h.BindCaller)
				callers.POST("/:id/request", h.RequestTokens)
				callers.POST("/:id/adjust", h.AdjustQuota)
				callers.GET("/:id/history", h.GetCallerHistory)
			}
			rateLimit.POST("/borrow", h.BorrowQuota)
			rateLimit.POST("/return", h.ReturnQuota)
			rateLimit.GET("/borrows", h.ListBorrows)
			rateLimit.GET("/stats", h.GetGlobalStats)
			rateLimit.GET("/wait-queue", h.ListWaitQueue)
			rateLimit.GET("/callers/:id/wait-queue", h.GetCallerWaitQueue)

			reservations := rateLimit.Group("/reservations")
			{
				reservations.POST("", h.CreateReservation)
				reservations.GET("", h.ListReservations)
				reservations.GET("/:id", h.GetReservation)
				reservations.POST("/:id/cancel", h.CancelReservation)
			}
		}
	}
}

type AcquireRequest struct {
	Holder    string `json:"holder" binding:"required"`
	LeaseSec  int    `json:"lease_sec" binding:"required,min=1"`
	Reentrant bool   `json:"reentrant"`
}

type ReleaseRequest struct {
	Holder string `json:"holder" binding:"required"`
}

type RenewRequest struct {
	Holder string `json:"holder" binding:"required"`
	AddSec int    `json:"add_sec" binding:"required,min=1"`
}

func (h *Handler) Health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func (h *Handler) ListLocks(c *gin.Context) {
	locks, err := h.manager.ListAllLocks()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"locks": locks})
}

func (h *Handler) GetLock(c *gin.Context) {
	name := c.Param("name")
	withHistory := c.Query("history") == "true"

	detail, err := h.manager.GetLockDetail(name, withHistory)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"lock": detail})
}

func (h *Handler) AcquireLock(c *gin.Context) {
	name := c.Param("name")

	var req AcquireRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	result, err := h.manager.AcquireLock(name, req.Holder, req.LeaseSec, req.Reentrant)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if result.Deadlock {
		c.JSON(http.StatusConflict, gin.H{
			"acquired":       false,
			"deadlock":       true,
			"deadlock_cycle": result.DeadlockCycle.Cycle,
			"lock":           result.Lock,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"acquired": result.Acquired,
		"queued":   result.Queued,
		"position": result.Position,
		"lock":     result.Lock,
		"lease":    result.Lease,
	})
}

func (h *Handler) ReleaseLock(c *gin.Context) {
	name := c.Param("name")

	var req ReleaseRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	result, err := h.manager.ReleaseLock(name, req.Holder)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"released": result.Released,
		"count":    result.Count,
		"granted":  result.Granted,
	})
}

func (h *Handler) RenewLock(c *gin.Context) {
	name := c.Param("name")

	var req RenewRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	lease, err := h.manager.RenewLease(name, req.Holder, req.AddSec)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"lease": lease})
}

func (h *Handler) GetLockHistory(c *gin.Context) {
	name := c.Param("name")
	limitStr := c.DefaultQuery("limit", "50")
	limit, _ := strconv.Atoi(limitStr)

	history, err := h.manager.GetLockHistory(name, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"history": history})
}

func (h *Handler) ListLeases(c *gin.Context) {
	leases, err := h.manager.ListActiveLeases()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"leases": leases})
}

type BatchAcquireRequest struct {
	LockNames []string `json:"lock_names" binding:"required,min=1"`
	Holder    string   `json:"holder" binding:"required"`
	LeaseSec  int      `json:"lease_sec" binding:"required,min=1"`
	Reentrant bool     `json:"reentrant"`
}

func (h *Handler) AcquireLocksBatch(c *gin.Context) {
	var req BatchAcquireRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	result, err := h.manager.AcquireLocksBatch(req.LockNames, req.Holder, req.LeaseSec, req.Reentrant)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if !result.Acquired {
		c.JSON(http.StatusConflict, gin.H{
			"acquired":    false,
			"failed_lock": result.FailedLock,
			"failed_by":   result.FailedBy,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"acquired": true,
		"locks":    result.Locks,
		"leases":   result.Leases,
	})
}

func (h *Handler) GetWaitGraph(c *gin.Context) {
	graph, err := h.manager.GetWaitGraph()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"nodes": graph.Nodes,
		"edges": graph.Edges,
	})
}

func (h *Handler) CreatePolicy(c *gin.Context) {
	var req model.PolicyCreateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	policy, err := h.rateLimiter.CreatePolicy(req.Name, req.Algorithm, req.WindowSec, req.MaxTokens, req.RefillRate, req.RefillUnit)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"policy": policy})
}

func (h *Handler) GetPolicy(c *gin.Context) {
	name := c.Param("name")

	policy, err := h.rateLimiter.GetPolicy(name)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"policy": policy})
}

func (h *Handler) ListPolicies(c *gin.Context) {
	policies, err := h.rateLimiter.ListPolicies()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"policies": policies})
}

func (h *Handler) BindCaller(c *gin.Context) {
	var req model.BindCallerRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	binding, err := h.rateLimiter.BindCaller(req.CallerID, req.PolicyName, req.QuotaLimit)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"binding": binding})
}

func (h *Handler) RequestTokens(c *gin.Context) {
	callerID := c.Param("id")

	var req model.TokenRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	result, err := h.rateLimiter.RequestTokens(callerID, req.Tokens, req.Waitable, req.WaitSec)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, result)
}

func (h *Handler) GetCallerStatus(c *gin.Context) {
	callerID := c.Param("id")

	status, err := h.rateLimiter.GetCallerStatus(callerID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"caller": status})
}

func (h *Handler) ListCallers(c *gin.Context) {
	statuses, err := h.rateLimiter.ListCallerStatuses()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"callers": statuses})
}

func (h *Handler) AdjustQuota(c *gin.Context) {
	callerID := c.Param("id")

	var req model.AdjustQuotaRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := h.rateLimiter.AdjustQuota(callerID, req.NewQuotaLimit); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	status, err := h.rateLimiter.GetCallerStatus(callerID)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"success": true})
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true, "caller": status})
}

func (h *Handler) BorrowQuota(c *gin.Context) {
	var req model.BorrowRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	result, err := h.rateLimiter.BorrowQuota(req.FromCaller, req.ToCaller, req.Amount)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if !result.Success {
		c.JSON(http.StatusBadRequest, result)
		return
	}

	c.JSON(http.StatusOK, result)
}

func (h *Handler) ReturnQuota(c *gin.Context) {
	var req model.ReturnRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	result, err := h.rateLimiter.ReturnQuota(req.FromCaller, req.ToCaller, req.Amount)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if !result.Success {
		c.JSON(http.StatusBadRequest, result)
		return
	}

	c.JSON(http.StatusOK, result)
}

func (h *Handler) GetGlobalStats(c *gin.Context) {
	stats, err := h.rateLimiter.GetGlobalStats()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"stats": stats})
}

func (h *Handler) GetCallerHistory(c *gin.Context) {
	callerID := c.Param("id")
	limitStr := c.DefaultQuery("limit", "50")
	limit, _ := strconv.Atoi(limitStr)

	history, err := h.rateLimiter.GetCallerHistory(callerID, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"history": history})
}

func (h *Handler) ListBorrows(c *gin.Context) {
	records, err := h.rateLimiter.ListBorrowRecords()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"borrows": records})
}

func (h *Handler) ListWaitQueue(c *gin.Context) {
	items, err := h.rateLimiter.ListWaitItems("")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"wait_queue": items})
}

func (h *Handler) GetCallerWaitQueue(c *gin.Context) {
	callerID := c.Param("id")

	items, err := h.rateLimiter.ListWaitItems(callerID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"wait_queue": items})
}

func (h *Handler) CreateReservation(c *gin.Context) {
	var req model.CreateReservationRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	result, err := h.rateLimiter.CreateReservation(req.PolicyName, req.CallerID, req.Tokens, req.StartAt, req.EndAt)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if !result.Success {
		c.JSON(http.StatusBadRequest, result)
		return
	}

	c.JSON(http.StatusOK, result)
}

func (h *Handler) GetReservation(c *gin.Context) {
	idStr := c.Param("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid reservation id"})
		return
	}

	reservation, err := h.rateLimiter.GetReservation(id)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"reservation": reservation})
}

func (h *Handler) ListReservations(c *gin.Context) {
	policyName := c.Query("policy")
	callerID := c.Query("caller")
	status := c.Query("status")

	reservations, err := h.rateLimiter.ListReservations(policyName, callerID, status)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"reservations": reservations})
}

func (h *Handler) CancelReservation(c *gin.Context) {
	idStr := c.Param("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid reservation id"})
		return
	}

	result, err := h.rateLimiter.CancelReservation(id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if !result.Success {
		c.JSON(http.StatusBadRequest, result)
		return
	}

	c.JSON(http.StatusOK, result)
}
