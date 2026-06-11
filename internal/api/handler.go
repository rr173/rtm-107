package api

import (
	"errors"
	"net/http"
	"rtm-107/internal/audit"
	"rtm-107/internal/lock"
	"rtm-107/internal/model"
	"rtm-107/internal/orchestration"
	"rtm-107/internal/ratelimit"
	"rtm-107/internal/topology"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

type Handler struct {
	manager      *lock.Manager
	rateLimiter  *ratelimit.Manager
	orchMgr      *orchestration.Manager
	auditMgr     *audit.Manager
	topoMgr      *topology.Manager
}

func NewHandler(m *lock.Manager, rl *ratelimit.Manager, om *orchestration.Manager, am *audit.Manager, tm *topology.Manager) *Handler {
	return &Handler{manager: m, rateLimiter: rl, orchMgr: om, auditMgr: am, topoMgr: tm}
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

		orch := api.Group("/orchestration")
		{
			orch.POST("/precheck", h.PreCheckTx)
			orch.POST("/tx", h.CreateTx)
			orch.GET("/tx", h.ListTxs)
			orch.GET("/tx/:id", h.GetTx)
			orch.POST("/tx/:id/release", h.ReleaseTx)
			orch.GET("/tx/:id/history", h.GetTxHistory)
		}

		auditGroup := api.Group("/audit")
		{
			auditGroup.GET("/logs", h.QueryAuditLogs)

			cb := auditGroup.Group("/circuit-breaker")
			{
				cb.POST("/rules", h.CreateCircuitBreakerRule)
				cb.GET("/rules", h.ListCircuitBreakerRules)
				cb.GET("/rules/:caller", h.GetCircuitBreakerRule)
				cb.DELETE("/rules/:caller", h.DeleteCircuitBreakerRule)

				cb.GET("/status", h.ListAllCircuitBreakerStatuses)
				cb.GET("/status/open", h.ListOpenCircuitBreakers)
				cb.GET("/status/:caller", h.GetCircuitBreakerStatus)
				cb.POST("/status/:caller/reset", h.ResetCircuitBreaker)

				cb.GET("/history", h.ListAllCircuitBreakerHistory)
				cb.GET("/history/:caller", h.GetCircuitBreakerHistory)
			}

			stats := auditGroup.Group("/stats")
			{
				stats.GET("", h.GetAuditGlobalStats)
				stats.GET("/callers", h.GetAllCallerStats)
				stats.GET("/callers/:id", h.GetCallerStats)
			}
		}

		topo := api.Group("/topology")
		{
			nodes := topo.Group("/nodes")
			{
				nodes.GET("", h.ListTopoNodes)
				nodes.GET("/:name", h.GetTopoNode)
				nodes.POST("", h.RegisterTopoNode)
			}

			edges := topo.Group("/edges")
			{
				edges.GET("", h.ListTopoEdges)
				edges.POST("", h.DeclareTopoEdge)
				edges.DELETE("/:from/:to", h.RemoveTopoEdge)
			}

			topo.GET("/graph", h.GetTopoGraph)
			topo.GET("/nodes/:name/ancestors", h.GetNodeAncestors)
			topo.GET("/nodes/:name/descendants", h.GetNodeDescendants)
			topo.GET("/holders/:holder/tree", h.GetHolderResourceTree)

			topo.POST("/acquire", h.CascadeAcquire)
			topo.POST("/release", h.CascadeRelease)

			topo.GET("/history", h.ListTopoHistory)
			topo.GET("/stats", h.GetTopoStats)
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

	result, err := h.auditMgr.AcquireLock(name, req.Holder, req.LeaseSec, req.Reentrant)
	if err != nil {
		if errors.Is(err, audit.ErrCircuitBreakerOpen) {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error":              err.Error(),
				"circuit_breaker_open": true,
			})
			return
		}
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

	result, err := h.auditMgr.ReleaseLock(name, req.Holder)
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

	lease, err := h.auditMgr.RenewLock(name, req.Holder, req.AddSec)
	if err != nil {
		if errors.Is(err, audit.ErrCircuitBreakerOpen) {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error":                err.Error(),
				"circuit_breaker_open": true,
			})
			return
		}
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

	result, err := h.auditMgr.AcquireLocksBatch(req.LockNames, req.Holder, req.LeaseSec, req.Reentrant)
	if err != nil {
		if errors.Is(err, audit.ErrCircuitBreakerOpen) {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error":                err.Error(),
				"circuit_breaker_open": true,
			})
			return
		}
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

	result, err := h.auditMgr.RequestTokens(callerID, req.Tokens, req.Waitable, req.WaitSec)
	if err != nil {
		if errors.Is(err, audit.ErrCircuitBreakerOpen) {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error":                err.Error(),
				"circuit_breaker_open": true,
			})
			return
		}
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

	if err := h.auditMgr.AdjustQuota(callerID, req.NewQuotaLimit); err != nil {
		if errors.Is(err, audit.ErrCircuitBreakerOpen) {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error":                err.Error(),
				"circuit_breaker_open": true,
			})
			return
		}
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

	result, err := h.auditMgr.BorrowQuota(req.FromCaller, req.ToCaller, req.Amount)
	if err != nil {
		if errors.Is(err, audit.ErrCircuitBreakerOpen) {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error":                err.Error(),
				"circuit_breaker_open": true,
			})
			return
		}
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

	result, err := h.auditMgr.ReturnQuota(req.FromCaller, req.ToCaller, req.Amount)
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

func (h *Handler) PreCheckTx(c *gin.Context) {
	var req model.CreateTxRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	result, err := h.orchMgr.PreCheck(req.Locks, req.Tokens)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, result)
}

func (h *Handler) CreateTx(c *gin.Context) {
	var req model.CreateTxRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	tx, err := h.orchMgr.CreateTx(req.Holder, req.TimeoutSec, req.Locks, req.Tokens)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if tx.Status == model.TxStatusRolledBack {
		c.JSON(http.StatusConflict, gin.H{
			"tx":          tx,
			"committed":   false,
			"fail_reason": tx.FailReason,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"tx":        tx,
		"committed": true,
	})
}

func (h *Handler) ListTxs(c *gin.Context) {
	status := c.Query("status")

	txs, err := h.orchMgr.ListTxs(status)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"txs": txs})
}

func (h *Handler) GetTx(c *gin.Context) {
	txID := c.Param("id")

	tx, err := h.orchMgr.GetTx(txID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"tx": tx})
}

func (h *Handler) ReleaseTx(c *gin.Context) {
	txID := c.Param("id")

	var req model.ReleaseTxRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	tx, err := h.orchMgr.ReleaseTx(txID, req.Holder)
	if err != nil {
		errMsg := err.Error()
		if strings.HasPrefix(errMsg, "permission denied") {
			c.JSON(http.StatusForbidden, gin.H{"error": errMsg})
			return
		}
		c.JSON(http.StatusBadRequest, gin.H{"error": errMsg})
		return
	}

	c.JSON(http.StatusOK, gin.H{"tx": tx})
}

func (h *Handler) GetTxHistory(c *gin.Context) {
	txID := c.Param("id")

	history, err := h.orchMgr.GetTxHistory(txID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"history": history})
}

func (h *Handler) QueryAuditLogs(c *gin.Context) {
	caller := c.Query("caller")
	resource := c.Query("resource")

	var successPtr *bool
	successStr := c.Query("success")
	if successStr != "" {
		s := successStr == "true"
		successPtr = &s
	}

	var startTime, endTime time.Time
	startStr := c.Query("start_time")
	if startStr != "" {
		if t, err := time.Parse(time.RFC3339, startStr); err == nil {
			startTime = t
		}
	}
	endStr := c.Query("end_time")
	if endStr != "" {
		if t, err := time.Parse(time.RFC3339, endStr); err == nil {
			endTime = t
		}
	}

	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	pageSize, _ := strconv.Atoi(c.DefaultQuery("page_size", "20"))

	result, err := h.auditMgr.QueryAuditLogs(caller, resource, successPtr, startTime, endTime, page, pageSize)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, result)
}

func (h *Handler) CreateCircuitBreakerRule(c *gin.Context) {
	var req model.CreateCircuitBreakerRuleRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	rule, err := h.auditMgr.SetCircuitBreakerRule(req.CallerID, req.WindowSec, req.FailureThreshold, req.CooldownSec)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"rule": rule})
}

func (h *Handler) ListCircuitBreakerRules(c *gin.Context) {
	rules, err := h.auditMgr.ListCircuitBreakerRules()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"rules": rules})
}

func (h *Handler) GetCircuitBreakerRule(c *gin.Context) {
	callerID := c.Param("caller")
	if callerID == "default" {
		callerID = ""
	}

	rule, err := h.auditMgr.GetCircuitBreakerRule(callerID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	if rule == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "rule not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"rule": rule})
}

func (h *Handler) DeleteCircuitBreakerRule(c *gin.Context) {
	callerID := c.Param("caller")
	if callerID == "default" {
		callerID = ""
	}

	if err := h.auditMgr.DeleteCircuitBreakerRule(callerID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true})
}

func (h *Handler) ListAllCircuitBreakerStatuses(c *gin.Context) {
	statuses, err := h.auditMgr.ListAllCircuitBreakerStatuses()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"statuses": statuses})
}

func (h *Handler) ListOpenCircuitBreakers(c *gin.Context) {
	statuses, err := h.auditMgr.ListOpenCircuitBreakers()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"open_breakers": statuses})
}

func (h *Handler) GetCircuitBreakerStatus(c *gin.Context) {
	callerID := c.Param("caller")
	status, err := h.auditMgr.GetCircuitBreakerStatus(callerID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": status})
}

func (h *Handler) ResetCircuitBreaker(c *gin.Context) {
	callerID := c.Param("caller")
	if err := h.auditMgr.ManuallyCloseCircuitBreaker(callerID); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "message": "circuit breaker reset"})
}

func (h *Handler) ListAllCircuitBreakerHistory(c *gin.Context) {
	limitStr := c.DefaultQuery("limit", "50")
	limit, _ := strconv.Atoi(limitStr)

	history, err := h.auditMgr.GetCircuitBreakerHistory("", limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"history": history})
}

func (h *Handler) GetCircuitBreakerHistory(c *gin.Context) {
	callerID := c.Param("caller")
	limitStr := c.DefaultQuery("limit", "50")
	limit, _ := strconv.Atoi(limitStr)

	history, err := h.auditMgr.GetCircuitBreakerHistory(callerID, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"history": history})
}

func (h *Handler) GetAuditGlobalStats(c *gin.Context) {
	stats, err := h.auditMgr.GetGlobalStats()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"stats": stats})
}

func (h *Handler) GetAllCallerStats(c *gin.Context) {
	stats, err := h.auditMgr.GetAllCallerStats()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"caller_stats": stats})
}

func (h *Handler) GetCallerStats(c *gin.Context) {
	callerID := c.Param("id")
	stats, err := h.auditMgr.GetCallerStats(callerID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"stats": stats})
}

func (h *Handler) RegisterTopoNode(c *gin.Context) {
	var req model.RegisterNodeRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	node, err := h.topoMgr.RegisterNode(req.Name, req.LockName, req.RatePolicy, req.TokenCost)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"node": node})
}

func (h *Handler) ListTopoNodes(c *gin.Context) {
	nodes, err := h.topoMgr.ListNodes()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"nodes": nodes})
}

func (h *Handler) GetTopoNode(c *gin.Context) {
	name := c.Param("name")
	node, err := h.topoMgr.GetNode(name)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"node": node})
}

func (h *Handler) DeclareTopoEdge(c *gin.Context) {
	var req model.DeclareEdgeRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	edge, err := h.topoMgr.DeclareEdge(req.FromNode, req.ToNode)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"edge": edge})
}

func (h *Handler) ListTopoEdges(c *gin.Context) {
	graph, err := h.topoMgr.GetGraph()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"edges": graph.Edges})
}

func (h *Handler) RemoveTopoEdge(c *gin.Context) {
	fromNode := c.Param("from")
	toNode := c.Param("to")
	if err := h.topoMgr.RemoveEdge(fromNode, toNode); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true})
}

func (h *Handler) GetTopoGraph(c *gin.Context) {
	graph, err := h.topoMgr.GetGraph()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"graph": graph})
}

func (h *Handler) GetNodeAncestors(c *gin.Context) {
	name := c.Param("name")
	result, err := h.topoMgr.GetAncestors(name)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"result": result})
}

func (h *Handler) GetNodeDescendants(c *gin.Context) {
	name := c.Param("name")
	result, err := h.topoMgr.GetDescendants(name)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"result": result})
}

func (h *Handler) GetHolderResourceTree(c *gin.Context) {
	holder := c.Param("holder")
	result, err := h.topoMgr.GetHolderResourceTree(holder)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"tree": result})
}

func (h *Handler) CascadeAcquire(c *gin.Context) {
	var req model.CascadeAcquireRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	result, err := h.topoMgr.CascadeAcquire(req.TargetNode, req.Holder, req.LeaseSec, req.Reentrant)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if !result.Success {
		c.JSON(http.StatusConflict, gin.H{"result": result})
		return
	}
	c.JSON(http.StatusOK, gin.H{"result": result})
}

func (h *Handler) CascadeRelease(c *gin.Context) {
	var req model.CascadeReleaseRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	result, err := h.topoMgr.CascadeRelease(req.TargetNode, req.Holder, req.Force)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if !result.Success {
		c.JSON(http.StatusConflict, gin.H{"result": result})
		return
	}
	c.JSON(http.StatusOK, gin.H{"result": result})
}

func (h *Handler) ListTopoHistory(c *gin.Context) {
	holder := c.Query("holder")
	limitStr := c.DefaultQuery("limit", "100")
	limit, _ := strconv.Atoi(limitStr)

	history, err := h.topoMgr.ListHistory(holder, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"history": history})
}

func (h *Handler) GetTopoStats(c *gin.Context) {
	stats, err := h.topoMgr.GetStats()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"stats": stats})
}
