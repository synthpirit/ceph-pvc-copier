package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/ceph/go-ceph/rados"
	"github.com/ceph/go-ceph/rbd"
	"github.com/gin-gonic/gin"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

// ============================================================================
// Data Models
// ============================================================================

type CopyRequest struct {
	SrcCluster string `json:"src_cluster"`
	SrcNS      string `json:"src_ns"`
	SrcPVC     string `json:"src_pvc"`
	DstCluster string `json:"dst_cluster"`
	DstNS      string `json:"dst_ns"`
	DstPVC     string `json:"dst_pvc"`
}

type CopyTask struct {
	ID        string       `json:"task_id"`
	Request   *CopyRequest `json:"request"`
	Status    string       `json:"status"` // pending, running, completed, failed, cancelled
	Progress  int          `json:"progress"`
	Message   string       `json:"message"`
	Error     string       `json:"error,omitempty"`
	StartTime time.Time    `json:"start_time"`
	EndTime   time.Time    `json:"end_time,omitempty"`
	ctx       context.Context
	cancel    context.CancelFunc
	mu        sync.RWMutex
}

// K8sClusterConfig is registered in-memory via API for PVC copy operations.
type K8sClusterConfig struct {
	Name       string `json:"name"`
	Kubeconfig string `json:"kubeconfig"` // file path or raw YAML content
}

type CephConfig struct {
	Monitors string `json:"monitors"`
	UserID   string `json:"user_id"`
	Key      string `json:"key"`
	Pool     string `json:"pool"`
}

// SnapshotInfo represents a single RBD snapshot.
type SnapshotInfo struct {
	ID   uint64 `json:"id"`
	Name string `json:"name"`
	Size uint64 `json:"size"`
}

// CreateSnapshotRequest is the body for POST …/snapshots.
// SnapName is optional; a timestamp-based name is generated when omitted.
type CreateSnapshotRequest struct {
	SnapName string `json:"snap_name"`
}

// ============================================================================
// Ceph Client
// ============================================================================

type CephClient struct {
	config *CephConfig
	conn   *rados.Conn
	ioctx  *rados.IOContext
}

func NewCephClient(config *CephConfig) (*CephClient, error) {
	conn, err := rados.NewConnWithUser(config.UserID)
	if err != nil {
		return nil, fmt.Errorf("failed to create rados connection: %w", err)
	}
	if err := conn.SetConfigOption("mon_host", config.Monitors); err != nil {
		conn.Shutdown()
		return nil, fmt.Errorf("failed to set mon_host: %w", err)
	}
	if err := conn.SetConfigOption("key", config.Key); err != nil {
		conn.Shutdown()
		return nil, fmt.Errorf("failed to set key: %w", err)
	}
	if err := conn.Connect(); err != nil {
		conn.Shutdown()
		return nil, fmt.Errorf("failed to connect to ceph: %w", err)
	}
	ioctx, err := conn.OpenIOContext(config.Pool)
	if err != nil {
		conn.Shutdown()
		return nil, fmt.Errorf("failed to open pool %s: %w", config.Pool, err)
	}
	return &CephClient{config: config, conn: conn, ioctx: ioctx}, nil
}

func (c *CephClient) Close() {
	if c.ioctx != nil {
		c.ioctx.Destroy()
		c.ioctx = nil
	}
	if c.conn != nil {
		c.conn.Shutdown()
		c.conn = nil
	}
}

// CopyImage executes snapshot → clone → flatten entirely within Ceph (no data transfer).
func (c *CephClient) CopyImage(ctx context.Context, srcImage, dstImage string, progressCb func(string, int)) error {
	if progressCb == nil {
		progressCb = func(string, int) {}
	}

	progressCb("Opening source image", 10)
	srcImg, err := rbd.OpenImage(c.ioctx, srcImage, rbd.NoSnapshot)
	if err != nil {
		return fmt.Errorf("failed to open source image %s: %w", srcImage, err)
	}
	defer srcImg.Close()

	snapName := fmt.Sprintf("pvc-copy-%d", time.Now().Unix())
	progressCb(fmt.Sprintf("Creating snapshot: %s", snapName), 20)
	snap, err := srcImg.CreateSnapshot(snapName)
	if err != nil {
		return fmt.Errorf("failed to create snapshot: %w", err)
	}

	snapshotCreated := true
	defer func() {
		if snapshotCreated {
			log.Printf("Cleaning up snapshot: %s@%s", srcImage, snapName)
			_ = snap.Unprotect()
			_ = snap.Remove()
		}
	}()

	progressCb("Protecting snapshot", 30)
	if err := snap.Protect(); err != nil {
		return fmt.Errorf("failed to protect snapshot: %w", err)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	progressCb(fmt.Sprintf("Cloning to %s", dstImage), 50)
	clonedImg, err := srcImg.Clone(snapName, c.ioctx, dstImage, uint64(rbd.FeatureLayering), 0)
	if err != nil {
		return fmt.Errorf("failed to clone image: %w", err)
	}
	clonedImg.Close()

	progressCb("Flattening cloned image (this may take a while)", 60)
	if err := c.flattenImage(ctx, dstImage); err != nil {
		_ = rbd.RemoveImage(c.ioctx, dstImage)
		return fmt.Errorf("failed to flatten image: %w", err)
	}

	progressCb("Cleaning up snapshot", 95)
	if err := snap.Unprotect(); err != nil {
		log.Printf("Warning: failed to unprotect snapshot: %v", err)
	}
	if err := snap.Remove(); err != nil {
		log.Printf("Warning: failed to remove snapshot: %v", err)
	}
	snapshotCreated = false

	progressCb("Copy completed successfully", 100)
	return nil
}

// flattenImage runs Flatten in a goroutine so ctx cancellation is observable.
// librbd Flatten is uninterruptible; on cancel we wait for it to finish before returning.
func (c *CephClient) flattenImage(ctx context.Context, imageName string) error {
	img, err := rbd.OpenImage(c.ioctx, imageName, rbd.NoSnapshot)
	if err != nil {
		return fmt.Errorf("failed to open image: %w", err)
	}
	defer img.Close()

	done := make(chan error, 1)
	go func() { done <- img.Flatten() }()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		log.Println("Flatten cancel requested; waiting for librbd to finish (uninterruptible)")
		<-done
		return ctx.Err()
	}
}

// ============================================================================
// K8s Helpers
// ============================================================================

type RBDImageInfo struct {
	ImageName string
	Pool      string
	ClusterID string
}

func GetRBDImageFromPVC(ctx context.Context, client kubernetes.Interface, namespace, pvcName string) (*RBDImageInfo, *corev1.PersistentVolumeClaim, error) {
	pvc, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName, metav1.GetOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get PVC %s/%s: %w", namespace, pvcName, err)
	}
	if pvc.Status.Phase != corev1.ClaimBound {
		return nil, pvc, fmt.Errorf("PVC %s/%s is not bound (status: %s)", namespace, pvcName, pvc.Status.Phase)
	}
	if pvc.Spec.VolumeName == "" {
		return nil, pvc, fmt.Errorf("PVC %s/%s has no volume name", namespace, pvcName)
	}

	pv, err := client.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
	if err != nil {
		return nil, pvc, fmt.Errorf("failed to get PV %s: %w", pvc.Spec.VolumeName, err)
	}
	if pv.Spec.CSI == nil {
		return nil, pvc, fmt.Errorf("PV %s is not a CSI volume", pv.Name)
	}

	imageName := pv.Spec.CSI.VolumeAttributes["imageName"]
	if imageName == "" {
		return nil, pvc, fmt.Errorf("could not find imageName in PV CSI volumeAttributes")
	}
	return &RBDImageInfo{
		ImageName: imageName,
		Pool:      pv.Spec.CSI.VolumeAttributes["pool"],
		ClusterID: pv.Spec.CSI.VolumeAttributes["clusterID"],
	}, pvc, nil
}

func CreatePVCFromRBDImage(
	ctx context.Context,
	client kubernetes.Interface,
	namespace, pvcName string,
	rbdImage string,
	srcPVC *corev1.PersistentVolumeClaim,
	srcPV *corev1.PersistentVolume,
) error {
	if err := ensureNamespace(ctx, client, namespace); err != nil {
		return err
	}

	pvName := fmt.Sprintf("pv-%s-%s-%d", namespace, pvcName, time.Now().Unix())

	srcAttrs := srcPV.Spec.CSI.VolumeAttributes
	newAttrs := map[string]string{
		"clusterID":     srcAttrs["clusterID"],
		"pool":          srcAttrs["pool"],
		"imageName":     rbdImage,
		"imageFeatures": srcAttrs["imageFeatures"],
		"imageFormat":   "2",
		// staticVolume=true 让 ceph-csi 跳过 VolumeHandle 解析，直接用 imageName 挂载
		"staticVolume": "true",
	}
	if v := srcAttrs["journalPool"]; v != "" {
		newAttrs["journalPool"] = v
	}
	for _, k := range []string{"mounter", "mapOptions", "unmapOptions",
		"encrypted", "encryptionKMSID", "tryOtherMounters"} {
		if v := srcAttrs[k]; v != "" {
			newAttrs[k] = v
		}
	}

	volumeHandle := rbdImage

	newPV := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: pvName,
			Annotations: map[string]string{
				"pv.kubernetes.io/provisioned-by": srcPV.Spec.CSI.Driver,
				"copied-from-image":               srcAttrs["imageName"],
				"copied-from-pvc":                 fmt.Sprintf("%s/%s", srcPVC.Namespace, srcPVC.Name),
				"copied-at":                       time.Now().Format(time.RFC3339),
			},
		},
		Spec: corev1.PersistentVolumeSpec{
			Capacity:                      srcPV.Spec.Capacity,
			AccessModes:                   srcPV.Spec.AccessModes,
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
			StorageClassName:              srcPV.Spec.StorageClassName,
			VolumeMode:                    srcPV.Spec.VolumeMode,
			MountOptions:                  srcPV.Spec.MountOptions,
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver:                     srcPV.Spec.CSI.Driver,
					VolumeHandle:               volumeHandle,
					FSType:                     srcPV.Spec.CSI.FSType,
					VolumeAttributes:           newAttrs,
					NodeStageSecretRef:         srcPV.Spec.CSI.NodeStageSecretRef,
					ControllerPublishSecretRef: srcPV.Spec.CSI.ControllerPublishSecretRef,
					NodePublishSecretRef:       srcPV.Spec.CSI.NodePublishSecretRef,
					ControllerExpandSecretRef:  srcPV.Spec.CSI.ControllerExpandSecretRef,
				},
			},
			ClaimRef: &corev1.ObjectReference{
				APIVersion: "v1",
				Kind:       "PersistentVolumeClaim",
				Namespace:  namespace,
				Name:       pvcName,
			},
		},
	}

	log.Printf("Creating PV %s (volumeHandle=%s, imageName=%s, staticVolume=true)",
		pvName, volumeHandle, rbdImage)
	if _, err := client.CoreV1().PersistentVolumes().Create(ctx, newPV, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("failed to create PV: %w", err)
	}

	newPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: namespace,
			Annotations: map[string]string{
				"copied-from-pvc": fmt.Sprintf("%s/%s", srcPVC.Namespace, srcPVC.Name),
				"copied-at":       time.Now().Format(time.RFC3339),
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      srcPVC.Spec.AccessModes,
			Resources:        srcPVC.Spec.Resources,
			StorageClassName: srcPVC.Spec.StorageClassName,
			VolumeName:       pvName,
			VolumeMode:       srcPVC.Spec.VolumeMode,
		},
	}

	log.Printf("Creating PVC %s/%s bound to PV %s", namespace, pvcName, pvName)
	if _, err := client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, newPVC, metav1.CreateOptions{}); err != nil {
		_ = client.CoreV1().PersistentVolumes().Delete(ctx, pvName, metav1.DeleteOptions{})
		return fmt.Errorf("failed to create PVC: %w", err)
	}
	return nil
}

func ensureNamespace(ctx context.Context, client kubernetes.Interface, namespace string) error {
	_, err := client.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to check namespace %s: %w", namespace, err)
	}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}
	if _, err := client.CoreV1().Namespaces().Create(ctx, ns, metav1.CreateOptions{}); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return nil
		}
		return fmt.Errorf("failed to create namespace %s: %w", namespace, err)
	}
	return nil
}

// ============================================================================
// Server
// ============================================================================

type Server struct {
	k8sClusters  map[string]*K8sClusterConfig
	k8sClusterMu sync.RWMutex

	k8sClients map[string]kubernetes.Interface
	k8sMu      sync.RWMutex

	cephConfig *CephConfig
	cephMu     sync.RWMutex

	tasks  map[string]*CopyTask
	taskMu sync.RWMutex

	router *gin.Engine
	port   string
}

const finishedTaskTTL = 24 * time.Hour

func NewServer(port string) *Server {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Logger(), gin.Recovery())

	s := &Server{
		k8sClusters: make(map[string]*K8sClusterConfig),
		k8sClients:  make(map[string]kubernetes.Interface),
		tasks:       make(map[string]*CopyTask),
		port:        port,
		router:      router,
	}
	s.setupCORS()
	s.registerRoutes()
	go s.reapTasks(finishedTaskTTL)
	return s
}

func (s *Server) setupCORS() {
	s.router.Use(func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Content-Type")
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}
		c.Next()
	})
}

func (s *Server) registerRoutes() {
	s.router.GET("/health", s.handleHealth)
	s.router.GET("/api/v1/docs", apiDoc)

	// K8s cluster management (in-memory registry for PVC copy)
	s.router.GET("/api/v1/clusters", s.handleListClusters)
	s.router.POST("/api/v1/clusters", s.handleAddCluster)
	s.router.DELETE("/api/v1/clusters/:cluster", s.handleDeleteCluster)
	s.router.GET("/api/v1/clusters/:cluster/pvcs", s.handleListPVCs)

	// Ceph config (shared across all clusters)
	s.router.GET("/api/v1/ceph", s.handleGetCeph)
	s.router.POST("/api/v1/ceph", s.handleSetCeph)

	// PVC copy tasks
	s.router.POST("/api/v1/copy", s.handleStartCopy)
	s.router.GET("/api/v1/copy/:task_id", s.handleGetTask)
	s.router.POST("/api/v1/copy/:task_id/cancel", s.handleCancelTask)
	s.router.GET("/api/v1/tasks", s.handleListTasks)

	// RBD snapshot management: /api/v1/clusters/:cluster/namespaces/:ns/pvcs/:pvc/snapshots[/:snap]
	snapBase := "/api/v1/clusters/:cluster/namespaces/:ns/pvcs/:pvc/snapshots"
	s.router.GET(snapBase, s.handleListSnapshots)
	s.router.POST(snapBase, s.handleCreateSnapshot)
	s.router.DELETE(snapBase+"/:snap", s.handleDeleteSnapshot)

	// Terminal features (handlers defined in terminal.go)
	s.router.POST("/api/v1/directory", queryDirectory)
	s.router.POST("/api/v1/file", getFileContent)
	s.router.GET("/api/v1/terminal", wsHandler)
}

func (s *Server) reapTasks(ttl time.Duration) {
	ticker := time.NewTicker(ttl / 12)
	defer ticker.Stop()
	for range ticker.C {
		cutoff := time.Now().Add(-ttl)
		s.taskMu.Lock()
		for id, t := range s.tasks {
			t.mu.RLock()
			finished := t.Status == "completed" || t.Status == "failed" || t.Status == "cancelled"
			stale := !t.EndTime.IsZero() && t.EndTime.Before(cutoff)
			t.mu.RUnlock()
			if finished && stale {
				delete(s.tasks, id)
			}
		}
		s.taskMu.Unlock()
	}
}

func (s *Server) Start() error {
	log.Printf("Starting Ceph PVC Copier & Terminal API on :%s", s.port)
	return s.router.Run(":" + s.port)
}

// ============================================================================
// Handlers – Health & Docs
// ============================================================================

func (s *Server) handleHealth(c *gin.Context) {
	c.JSON(200, gin.H{"status": "ok"})
}

// ============================================================================
// Handlers – K8s Clusters
// ============================================================================

func (s *Server) handleListClusters(c *gin.Context) {
	s.k8sClusterMu.RLock()
	list := make([]*K8sClusterConfig, 0, len(s.k8sClusters))
	for _, cfg := range s.k8sClusters {
		list = append(list, cfg)
	}
	s.k8sClusterMu.RUnlock()
	c.JSON(200, gin.H{"clusters": list, "count": len(list)})
}

func (s *Server) handleAddCluster(c *gin.Context) {
	var cfg K8sClusterConfig
	if err := c.ShouldBindJSON(&cfg); err != nil {
		c.JSON(400, gin.H{"error": "invalid request body"})
		return
	}
	if cfg.Name == "" || cfg.Kubeconfig == "" {
		c.JSON(400, gin.H{"error": "name and kubeconfig are required"})
		return
	}
	if _, err := buildK8sClient(&cfg); err != nil {
		c.JSON(400, gin.H{"error": fmt.Sprintf("failed to connect: %v", err)})
		return
	}
	s.k8sClusterMu.Lock()
	s.k8sClusters[cfg.Name] = &cfg
	s.k8sClusterMu.Unlock()
	c.JSON(201, gin.H{"message": "cluster added"})
}

func (s *Server) handleDeleteCluster(c *gin.Context) {
	name := c.Param("cluster")
	s.k8sClusterMu.Lock()
	delete(s.k8sClusters, name)
	s.k8sClusterMu.Unlock()
	s.k8sMu.Lock()
	delete(s.k8sClients, name)
	s.k8sMu.Unlock()
	c.JSON(200, gin.H{"message": "cluster deleted"})
}

func (s *Server) handleListPVCs(c *gin.Context) {
	name := c.Param("cluster")
	ns := c.DefaultQuery("namespace", "default")

	client, err := s.getK8sClient(name)
	if err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}

	list, err := client.CoreV1().PersistentVolumeClaims(ns).List(c.Request.Context(), metav1.ListOptions{})
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}

	type pvcInfo struct {
		Name         string `json:"name"`
		Namespace    string `json:"namespace"`
		Size         string `json:"size"`
		Status       string `json:"status"`
		StorageClass string `json:"storage_class"`
		VolumeName   string `json:"volume_name"`
	}

	pvcs := make([]pvcInfo, 0, len(list.Items))
	for _, pvc := range list.Items {
		size := ""
		if r, ok := pvc.Spec.Resources.Requests[corev1.ResourceStorage]; ok {
			size = r.String()
		}
		sc := ""
		if pvc.Spec.StorageClassName != nil {
			sc = *pvc.Spec.StorageClassName
		}
		pvcs = append(pvcs, pvcInfo{
			Name: pvc.Name, Namespace: pvc.Namespace, Size: size,
			Status: string(pvc.Status.Phase), StorageClass: sc,
			VolumeName: pvc.Spec.VolumeName,
		})
	}
	c.JSON(200, gin.H{"cluster": name, "namespace": ns, "pvcs": pvcs, "count": len(pvcs)})
}

// ============================================================================
// Handlers – Ceph
// ============================================================================

func (s *Server) handleGetCeph(c *gin.Context) {
	s.cephMu.RLock()
	defer s.cephMu.RUnlock()
	if s.cephConfig == nil {
		c.JSON(404, gin.H{"error": "ceph not configured"})
		return
	}
	safe := *s.cephConfig
	if safe.Key != "" {
		safe.Key = "***"
	}
	c.JSON(200, safe)
}

func (s *Server) handleSetCeph(c *gin.Context) {
	var cfg CephConfig
	if err := c.ShouldBindJSON(&cfg); err != nil {
		c.JSON(400, gin.H{"error": "invalid request"})
		return
	}
	if cfg.Monitors == "" || cfg.Key == "" {
		c.JSON(400, gin.H{"error": "monitors and key are required"})
		return
	}
	if cfg.UserID == "" {
		cfg.UserID = "admin"
	}
	if cfg.Pool == "" {
		cfg.Pool = "rbd"
	}
	client, err := NewCephClient(&cfg)
	if err != nil {
		c.JSON(400, gin.H{"error": fmt.Sprintf("failed to connect to Ceph: %v", err)})
		return
	}
	client.Close()
	s.cephMu.Lock()
	s.cephConfig = &cfg
	s.cephMu.Unlock()
	c.JSON(200, gin.H{"message": "ceph configured"})
}

// ============================================================================
// Handlers – Copy Tasks
// ============================================================================

func (s *Server) handleStartCopy(c *gin.Context) {
	var req CopyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": "invalid request"})
		return
	}
	if err := s.validateCopyRequest(&req); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Hour)
	task := &CopyTask{
		ID:        fmt.Sprintf("task-%d", time.Now().UnixNano()),
		Request:   &req,
		Status:    "pending",
		StartTime: time.Now(),
		ctx:       ctx,
		cancel:    cancel,
	}
	s.taskMu.Lock()
	s.tasks[task.ID] = task
	s.taskMu.Unlock()

	go s.executeCopy(task)
	c.JSON(202, gin.H{
		"task_id":    task.ID,
		"status":     task.Status,
		"message":    "copy started",
		"start_time": task.StartTime,
	})
}

func (s *Server) handleGetTask(c *gin.Context) {
	id := c.Param("task_id")
	s.taskMu.RLock()
	task, ok := s.tasks[id]
	s.taskMu.RUnlock()
	if !ok {
		c.JSON(404, gin.H{"error": "task not found"})
		return
	}
	task.mu.RLock()
	defer task.mu.RUnlock()
	c.JSON(200, gin.H{
		"task_id":    task.ID,
		"status":     task.Status,
		"progress":   task.Progress,
		"message":    task.Message,
		"error":      task.Error,
		"start_time": task.StartTime,
		"end_time":   task.EndTime,
		"request":    task.Request,
	})
}

func (s *Server) handleCancelTask(c *gin.Context) {
	id := c.Param("task_id")
	s.taskMu.RLock()
	task, ok := s.tasks[id]
	s.taskMu.RUnlock()
	if !ok {
		c.JSON(404, gin.H{"error": "task not found"})
		return
	}
	task.mu.Lock()
	if task.Status == "running" || task.Status == "pending" {
		task.cancel()
		task.Status = "cancelled"
		task.Message = "cancelled by user"
	}
	task.mu.Unlock()
	c.JSON(200, gin.H{"message": "cancel submitted"})
}

func (s *Server) handleListTasks(c *gin.Context) {
	s.taskMu.RLock()
	defer s.taskMu.RUnlock()
	list := make([]gin.H, 0, len(s.tasks))
	for _, t := range s.tasks {
		t.mu.RLock()
		list = append(list, gin.H{
			"task_id":    t.ID,
			"status":     t.Status,
			"progress":   t.Progress,
			"message":    t.Message,
			"start_time": t.StartTime,
			"end_time":   t.EndTime,
		})
		t.mu.RUnlock()
	}
	c.JSON(200, gin.H{"tasks": list, "count": len(list)})
}

// ============================================================================
// Core: execute PVC copy
// ============================================================================

func (s *Server) executeCopy(task *CopyTask) {
	defer task.cancel()
	defer func() {
		task.mu.Lock()
		task.EndTime = time.Now()
		task.mu.Unlock()
	}()

	updateProgress := func(msg string, pct int) {
		task.mu.Lock()
		task.Message = msg
		if pct > 0 {
			task.Progress = pct
		}
		task.mu.Unlock()
		log.Printf("[%s] %d%% - %s", task.ID, pct, msg)
	}

	failTask := func(format string, args ...interface{}) {
		errMsg := fmt.Sprintf(format, args...)
		task.mu.Lock()
		task.Status = "failed"
		task.Error = errMsg
		task.Message = "copy failed"
		task.mu.Unlock()
		log.Printf("[%s] FAILED: %s", task.ID, errMsg)
	}

	task.mu.Lock()
	task.Status = "running"
	task.mu.Unlock()

	updateProgress("Connecting to K8s clusters", 2)
	srcK8s, err := s.getK8sClient(task.Request.SrcCluster)
	if err != nil {
		failTask("failed to get source K8s client: %v", err)
		return
	}
	dstK8s, err := s.getK8sClient(task.Request.DstCluster)
	if err != nil {
		failTask("failed to get destination K8s client: %v", err)
		return
	}

	updateProgress("Getting source PVC info", 5)
	srcRBD, srcPVC, err := GetRBDImageFromPVC(task.ctx, srcK8s, task.Request.SrcNS, task.Request.SrcPVC)
	if err != nil {
		failTask("failed to get source RBD info: %v", err)
		return
	}
	srcPV, err := srcK8s.CoreV1().PersistentVolumes().Get(task.ctx, srcPVC.Spec.VolumeName, metav1.GetOptions{})
	if err != nil {
		failTask("failed to get source PV: %v", err)
		return
	}
	log.Printf("[%s] Source RBD image: %s (pool: %s)", task.ID, srcRBD.ImageName, srcRBD.Pool)

	updateProgress("Checking destination PVC", 8)
	_, err = dstK8s.CoreV1().PersistentVolumeClaims(task.Request.DstNS).Get(
		task.ctx, task.Request.DstPVC, metav1.GetOptions{},
	)
	switch {
	case err == nil:
		failTask("destination PVC %s/%s already exists", task.Request.DstNS, task.Request.DstPVC)
		return
	case !apierrors.IsNotFound(err):
		failTask("failed to check destination PVC: %v", err)
		return
	}

	updateProgress("Connecting to Ceph", 10)
	s.cephMu.RLock()
	cephCfg := s.cephConfig
	s.cephMu.RUnlock()
	if cephCfg == nil {
		failTask("ceph is not configured")
		return
	}

	cephCfgCopy := *cephCfg
	if srcRBD.Pool != "" && srcRBD.Pool != cephCfgCopy.Pool {
		cephCfgCopy.Pool = srcRBD.Pool
	}

	cephClient, err := NewCephClient(&cephCfgCopy)
	if err != nil {
		failTask("failed to connect to Ceph: %v", err)
		return
	}
	defer cephClient.Close()

	dstImageName := fmt.Sprintf("csi-vol-%d", time.Now().UnixNano())
	if err := cephClient.CopyImage(task.ctx, srcRBD.ImageName, dstImageName, updateProgress); err != nil {
		failTask("failed to copy image: %v", err)
		return
	}

	updateProgress("Creating destination PVC", 97)
	if err := CreatePVCFromRBDImage(task.ctx, dstK8s, task.Request.DstNS, task.Request.DstPVC,
		dstImageName, srcPVC, srcPV); err != nil {
		log.Printf("[%s] Cleanup: removing cloned image %s", task.ID, dstImageName)
		_ = rbd.RemoveImage(cephClient.ioctx, dstImageName)
		failTask("failed to create destination PVC: %v", err)
		return
	}

	task.mu.Lock()
	task.Status = "completed"
	task.Progress = 100
	task.Message = fmt.Sprintf("copied %s/%s -> %s/%s (rbd image: %s)",
		task.Request.SrcNS, task.Request.SrcPVC,
		task.Request.DstNS, task.Request.DstPVC, dstImageName)
	task.mu.Unlock()
	log.Printf("[%s] Completed successfully", task.ID)
}

// ============================================================================
// Handlers – RBD Snapshots
// ============================================================================

// newCephClientForPool returns a CephClient opened on the given pool.
// Falls back to the globally configured pool when pool is empty.
func (s *Server) newCephClientForPool(pool string) (*CephClient, error) {
	s.cephMu.RLock()
	cfg := s.cephConfig
	s.cephMu.RUnlock()
	if cfg == nil {
		return nil, fmt.Errorf("ceph is not configured (POST /api/v1/ceph first)")
	}
	cfgCopy := *cfg
	if pool != "" && pool != cfgCopy.Pool {
		cfgCopy.Pool = pool
	}
	return NewCephClient(&cfgCopy)
}

// snapshotParams extracts the three common path params and returns the RBD image info.
func (s *Server) snapshotParams(c *gin.Context) (*RBDImageInfo, error) {
	cluster := c.Param("cluster")
	ns := c.Param("ns")
	pvc := c.Param("pvc")

	k8sClient, err := s.getK8sClient(cluster)
	if err != nil {
		return nil, fmt.Errorf("cluster %q: %w", cluster, err)
	}
	rbdInfo, _, err := GetRBDImageFromPVC(c.Request.Context(), k8sClient, ns, pvc)
	if err != nil {
		return nil, fmt.Errorf("PVC %s/%s: %w", ns, pvc, err)
	}
	return rbdInfo, nil
}

func (s *Server) handleListSnapshots(c *gin.Context) {
	rbdInfo, err := s.snapshotParams(c)
	if err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}

	cephClient, err := s.newCephClientForPool(rbdInfo.Pool)
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	defer cephClient.Close()

	img, err := rbd.OpenImage(cephClient.ioctx, rbdInfo.ImageName, rbd.NoSnapshot)
	if err != nil {
		c.JSON(500, gin.H{"error": fmt.Sprintf("failed to open image: %v", err)})
		return
	}
	defer img.Close()

	snaps, err := img.GetSnapshotNames()
	if err != nil {
		c.JSON(500, gin.H{"error": fmt.Sprintf("failed to list snapshots: %v", err)})
		return
	}

	list := make([]SnapshotInfo, 0, len(snaps))
	for _, sn := range snaps {
		list = append(list, SnapshotInfo{ID: sn.Id, Name: sn.Name, Size: sn.Size})
	}
	c.JSON(200, gin.H{
		"image":     rbdInfo.ImageName,
		"pool":      rbdInfo.Pool,
		"snapshots": list,
		"count":     len(list),
	})
}

func (s *Server) handleCreateSnapshot(c *gin.Context) {
	rbdInfo, err := s.snapshotParams(c)
	if err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}

	var req CreateSnapshotRequest
	_ = c.ShouldBindJSON(&req) // body is optional
	if req.SnapName == "" {
		req.SnapName = fmt.Sprintf("snap-%d", time.Now().Unix())
	}

	cephClient, err := s.newCephClientForPool(rbdInfo.Pool)
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	defer cephClient.Close()

	img, err := rbd.OpenImage(cephClient.ioctx, rbdInfo.ImageName, rbd.NoSnapshot)
	if err != nil {
		c.JSON(500, gin.H{"error": fmt.Sprintf("failed to open image: %v", err)})
		return
	}
	defer img.Close()

	if _, err := img.CreateSnapshot(req.SnapName); err != nil {
		c.JSON(500, gin.H{"error": fmt.Sprintf("failed to create snapshot: %v", err)})
		return
	}

	c.JSON(201, gin.H{
		"message":    "snapshot created",
		"snap_name":  req.SnapName,
		"image":      rbdInfo.ImageName,
		"pool":       rbdInfo.Pool,
		"created_at": time.Now().Format(time.RFC3339),
	})
}

func (s *Server) handleDeleteSnapshot(c *gin.Context) {
	rbdInfo, err := s.snapshotParams(c)
	if err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	snapName := c.Param("snap")

	cephClient, err := s.newCephClientForPool(rbdInfo.Pool)
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	defer cephClient.Close()

	img, err := rbd.OpenImage(cephClient.ioctx, rbdInfo.ImageName, rbd.NoSnapshot)
	if err != nil {
		c.JSON(500, gin.H{"error": fmt.Sprintf("failed to open image: %v", err)})
		return
	}
	defer img.Close()

	snap := img.GetSnapshot(snapName)

	// Unprotect before removal if needed (a protected snapshot cannot be deleted).
	if protected, err := snap.IsProtected(); err == nil && protected {
		if err := snap.Unprotect(); err != nil {
			c.JSON(500, gin.H{"error": fmt.Sprintf("failed to unprotect snapshot: %v", err)})
			return
		}
	}

	if err := snap.Remove(); err != nil {
		c.JSON(500, gin.H{"error": fmt.Sprintf("failed to delete snapshot: %v", err)})
		return
	}

	c.JSON(200, gin.H{"message": "snapshot deleted", "snap_name": snapName})
}

// ============================================================================
// Helpers
// ============================================================================

func (s *Server) validateCopyRequest(r *CopyRequest) error {
	if r.SrcCluster == "" || r.SrcNS == "" || r.SrcPVC == "" {
		return fmt.Errorf("src_cluster, src_ns, src_pvc are required")
	}
	if r.DstCluster == "" || r.DstNS == "" || r.DstPVC == "" {
		return fmt.Errorf("dst_cluster, dst_ns, dst_pvc are required")
	}
	if err := s.clusterExists(r.SrcCluster); err != nil {
		return fmt.Errorf("source cluster %q not found: %v", r.SrcCluster, err)
	}
	if err := s.clusterExists(r.DstCluster); err != nil {
		return fmt.Errorf("destination cluster %q not found: %v", r.DstCluster, err)
	}
	s.cephMu.RLock()
	cephOk := s.cephConfig != nil
	s.cephMu.RUnlock()
	if !cephOk {
		return fmt.Errorf("ceph is not configured (POST /api/v1/ceph first)")
	}
	return nil
}

// clusterExists returns nil if the cluster is reachable via in-memory registry or database.
func (s *Server) clusterExists(name string) error {
	s.k8sClusterMu.RLock()
	_, ok := s.k8sClusters[name]
	s.k8sClusterMu.RUnlock()
	if ok {
		return nil
	}
	_, err := GetClusterRestConfig(name)
	return err
}

// getK8sClient returns a cached client, creating one from the in-memory registry or
// falling back to the database cluster store if the name is not registered locally.
func (s *Server) getK8sClient(clusterName string) (kubernetes.Interface, error) {
	s.k8sMu.RLock()
	if c, ok := s.k8sClients[clusterName]; ok {
		s.k8sMu.RUnlock()
		return c, nil
	}
	s.k8sMu.RUnlock()

	var client kubernetes.Interface

	s.k8sClusterMu.RLock()
	cfg, ok := s.k8sClusters[clusterName]
	s.k8sClusterMu.RUnlock()

	if ok {
		var err error
		client, err = buildK8sClient(cfg)
		if err != nil {
			return nil, err
		}
	} else {
		// Fallback: try the database cluster store (terminal.go)
		restConfig, err := GetClusterRestConfig(clusterName)
		if err != nil {
			return nil, fmt.Errorf("cluster %q not found in registry or database", clusterName)
		}
		client, err = kubernetes.NewForConfig(restConfig)
		if err != nil {
			return nil, err
		}
	}

	s.k8sMu.Lock()
	s.k8sClients[clusterName] = client
	s.k8sMu.Unlock()
	return client, nil
}

func buildK8sClient(cfg *K8sClusterConfig) (kubernetes.Interface, error) {
	restConfig, err := clientcmd.BuildConfigFromFlags("", cfg.Kubeconfig)
	if err != nil {
		// Try treating Kubeconfig as raw YAML content
		apiConfig, err2 := clientcmd.Load([]byte(cfg.Kubeconfig))
		if err2 != nil {
			return nil, fmt.Errorf("invalid kubeconfig (tried file path and content): %v / %v", err, err2)
		}
		restConfig, err = clientcmd.NewDefaultClientConfig(*apiConfig, &clientcmd.ConfigOverrides{}).ClientConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to build rest config: %w", err)
		}
	}
	return kubernetes.NewForConfig(restConfig)
}

// ============================================================================
// Main
// ============================================================================

func main() {
	dbDSN := os.Getenv("DB_DSN")
	if dbDSN != "" {
		if err := InitDB(dbDSN); err != nil {
			log.Printf("Warning: database initialization failed: %v", err)
			log.Println("Continuing without database support.")
		}
	} else {
		log.Println("No DB_DSN provided. Database support disabled.")
	}

	port := os.Getenv("SERVER_PORT")
	if port == "" {
		port = "8080"
	}

	srv := NewServer(port)
	if err := srv.Start(); err != nil {
		log.Fatal(err)
	}
}
