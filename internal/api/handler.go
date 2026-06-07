package api

import (
	"net/http"
	"rtm-107/internal/lock"
	"strconv"

	"github.com/gin-gonic/gin"
)

type Handler struct {
	manager *lock.Manager
}

func NewHandler(m *lock.Manager) *Handler {
	return &Handler{manager: m}
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
		}
		api.GET("/leases", h.ListLeases)
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
