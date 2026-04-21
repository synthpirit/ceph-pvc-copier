package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/ceph/go-ceph/rados"
	"github.com/ceph/go-ceph/rbd"
	"github.com/gorilla/mux"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

// ============================================================================
// 场景说明
// ============================================================================
//
// 两个 K8s 集群共用同一个 Ceph 集群：
//
//   K8s Cluster A              K8s Cluster B
//        │                          │
//        │    PVC A                 │    PVC B
//        │     │                    │     │
//        │     ▼                    │     ▼
//        │    PV A ──────┐  ┌──────PV B
//        │               │  │       │
//        └───────────────┼──┼───────┘
//                        │  │
//                        ▼  ▼
//                  ┌──────────────┐
//                  │ Ceph Cluster │
//                  │              │
//                  │  image-A     │
//                  │  image-B     │
//                  └──────────────┘
//
// 复制流程（全部在 Ceph 层完成，无需数据传输）：
//   1. 在源 K8s 获取 PVC 对应的 RBD image 名
//   2. Ceph: 对 image-A 创建 snapshot
//   3. Ceph: 从 snapshot clone 出 image-B-new
//   4. Ceph: flatten image-B-new（解除与 snapshot 的依赖）
//   5. Ceph: 删除 snapshot
//   6. 在目标 K8s 创建 PV 指向 image-B-new，再创建 PVC 绑定

// ============================================================================
// Data Models
// ============================================================================

// CopyRequest Web API 请求体
type CopyRequest struct {
	SrcCluster string `json:"src_cluster"`
	SrcNS      string `json:"src_ns"`
	SrcPVC     string `json:"src_pvc"`

	DstCluster string `json:"dst_cluster"`
	DstNS      string `json:"dst_ns"`
	DstPVC     string `json:"dst_pvc"`
}

// CopyTask 异步任务
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

// K8sClusterConfig K8s 集群配置
type K8sClusterConfig struct {
	Name       string `json:"name"`
	Kubeconfig string `json:"kubeconfig"` // kubeconfig 文件路径，或直接的 YAML 内容
}

// CephConfig Ceph 集群配置（全局一份，因为所有 K8s 共用）
type CephConfig struct {
	Monitors string `json:"monitors"` // e.g. "10.0.0.1:6789,10.0.0.2:6789"
	UserID   string `json:"user_id"`  // e.g. "admin"
	Key      string `json:"key"`      // Ceph 密钥
	Pool     string `json:"pool"`     // e.g. "rbd" 或 "kubernetes"
}

// ============================================================================
// Ceph Client（封装 go-ceph 的操作）
// ============================================================================

type CephClient struct {
	config *CephConfig
	conn   *rados.Conn
	ioctx  *rados.IOContext
}

// NewCephClient 创建 Ceph 客户端连接
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

	return &CephClient{
		config: config,
		conn:   conn,
		ioctx:  ioctx,
	}, nil
}

// Close 关闭连接（可重复调用）
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

// CopyImage 执行完整的 snapshot → clone → flatten 流程
// 整个过程都在 Ceph 层完成，非常快（秒到分钟级）
func (c *CephClient) CopyImage(ctx context.Context, srcImage, dstImage string, progressCb func(msg string, pct int)) error {
	if progressCb == nil {
		progressCb = func(string, int) {}
	}

	// ========== Step 1: 打开源镜像 ==========
	progressCb("Opening source image", 10)

	srcImg, err := rbd.OpenImage(c.ioctx, srcImage, rbd.NoSnapshot)
	if err != nil {
		return fmt.Errorf("failed to open source image %s: %w", srcImage, err)
	}
	defer srcImg.Close()

	// ========== Step 2: 创建快照 ==========
	snapName := fmt.Sprintf("pvc-copy-%d", time.Now().Unix())
	progressCb(fmt.Sprintf("Creating snapshot: %s", snapName), 20)

	snap, err := srcImg.CreateSnapshot(snapName)
	if err != nil {
		return fmt.Errorf("failed to create snapshot: %w", err)
	}

	// 失败时清理快照
	snapshotCreated := true
	defer func() {
		if snapshotCreated {
			log.Printf("Cleaning up snapshot: %s@%s", srcImage, snapName)
			_ = snap.Unprotect()
			_ = snap.Remove()
		}
	}()

	// ========== Step 3: 保护快照（Clone 前必须） ==========
	progressCb("Protecting snapshot", 30)

	if err := snap.Protect(); err != nil {
		return fmt.Errorf("failed to protect snapshot: %w", err)
	}

	// ========== Step 4: 检查取消 ==========
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// ========== Step 5: Clone ==========
	progressCb(fmt.Sprintf("Cloning to %s", dstImage), 50)

	// 使用 layering feature（format 2 必需）
	features := uint64(rbd.FeatureLayering)
	order := 0 // 0 = use default (通常 22，即 4MB 对象)

	clonedImg, err := srcImg.Clone(snapName, c.ioctx, dstImage, features, order)
	if err != nil {
		return fmt.Errorf("failed to clone image: %w", err)
	}
	clonedImg.Close()

	// ========== Step 6: Flatten ==========
	progressCb("Flattening cloned image (this may take a while)", 60)

	if err := c.flattenImage(ctx, dstImage, progressCb); err != nil {
		// flatten 失败，删除 clone
		_ = rbd.RemoveImage(c.ioctx, dstImage)
		return fmt.Errorf("failed to flatten image: %w", err)
	}

	// ========== Step 7: 清理快照 ==========
	progressCb("Cleaning up snapshot", 95)

	if err := snap.Unprotect(); err != nil {
		log.Printf("Warning: failed to unprotect snapshot: %v", err)
	}
	if err := snap.Remove(); err != nil {
		log.Printf("Warning: failed to remove snapshot: %v", err)
	}
	snapshotCreated = false // 已清理，defer 不再重复清理

	progressCb("Copy completed successfully", 100)
	return nil
}

// flattenImage 展平镜像，可检查 context 取消
func (c *CephClient) flattenImage(ctx context.Context, imageName string, progressCb func(msg string, pct int)) error {
	img, err := rbd.OpenImage(c.ioctx, imageName, rbd.NoSnapshot)
	if err != nil {
		return fmt.Errorf("failed to open image: %w", err)
	}
	defer img.Close()

	// Flatten 是同步阻塞操作，在 goroutine 中执行以支持取消信号
	done := make(chan error, 1)
	go func() {
		done <- img.Flatten()
	}()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		// librbd 的 Flatten 无法中断，必须等它跑完才能安全返回，
		// 否则上层 RemoveImage 会撞上 "image in use"。
		log.Println("Flatten cancel requested; waiting for librbd to finish (uninterruptible)")
		<-done
		return ctx.Err()
	}
}

// ============================================================================
// K8s Helper（从 PV 中提取 RBD image 信息）
// ============================================================================

// RBDImageInfo 从 PV 中提取的 RBD 信息
type RBDImageInfo struct {
	ImageName string
	Pool      string
	ClusterID string
}

// GetRBDImageFromPVC 从 PVC 获取对应的 RBD image 信息
func GetRBDImageFromPVC(ctx context.Context, client kubernetes.Interface, namespace, pvcName string) (*RBDImageInfo, *corev1.PersistentVolumeClaim, error) {
	// 获取 PVC
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

	// 获取 PV
	pv, err := client.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
	if err != nil {
		return nil, pvc, fmt.Errorf("failed to get PV %s: %w", pvc.Spec.VolumeName, err)
	}

	if pv.Spec.CSI == nil {
		return nil, pvc, fmt.Errorf("PV %s is not a CSI volume", pv.Name)
	}

	// ceph-csi 会在 volumeAttributes 中设置这些字段
	imageName := pv.Spec.CSI.VolumeAttributes["imageName"]
	pool := pv.Spec.CSI.VolumeAttributes["pool"]
	clusterID := pv.Spec.CSI.VolumeAttributes["clusterID"]

	if imageName == "" {
		return nil, pvc, fmt.Errorf("could not find imageName in PV CSI volumeAttributes")
	}

	return &RBDImageInfo{
		ImageName: imageName,
		Pool:      pool,
		ClusterID: clusterID,
	}, pvc, nil
}

// CreatePVCFromRBDImage 在目标 K8s 创建 PV + PVC，指向已有的 RBD image
func CreatePVCFromRBDImage(
	ctx context.Context,
	client kubernetes.Interface,
	namespace, pvcName string,
	rbdImage string,
	srcPVC *corev1.PersistentVolumeClaim,
	srcPV *corev1.PersistentVolume,
) error {
	// 1. 确保 namespace 存在
	if err := ensureNamespace(ctx, client, namespace); err != nil {
		return err
	}

	// 2. 生成唯一 PV 名
	pvName := fmt.Sprintf("pv-%s-%s-%d", namespace, pvcName, time.Now().Unix())

	// 3. 构造目标 PV（基于源 PV，但指向新的 RBD image）
	newPV := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: pvName,
			Annotations: map[string]string{
				"pv.kubernetes.io/provisioned-by": srcPV.Annotations["pv.kubernetes.io/provisioned-by"],
				"copied-from-image":               srcPV.Spec.CSI.VolumeAttributes["imageName"],
				"copied-at":                       time.Now().Format(time.RFC3339),
			},
		},
		Spec: corev1.PersistentVolumeSpec{
			Capacity:                      srcPV.Spec.Capacity,
			AccessModes:                   srcPV.Spec.AccessModes,
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
			StorageClassName:              srcPV.Spec.StorageClassName,
			VolumeMode:                    srcPV.Spec.VolumeMode,
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver:           srcPV.Spec.CSI.Driver,
					VolumeHandle:     rbdImage, // 使用新的 image 名
					FSType:           srcPV.Spec.CSI.FSType,
					VolumeAttributes: copyAndUpdateAttrs(srcPV.Spec.CSI.VolumeAttributes, rbdImage),
					// 引用原 PV 中的 secret
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

	// 4. 创建 PV
	log.Printf("Creating PV %s in destination cluster", pvName)
	if _, err := client.CoreV1().PersistentVolumes().Create(ctx, newPV, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("failed to create PV: %w", err)
	}

	// 5. 构造 PVC
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
			VolumeName:       pvName, // 绑定到刚创建的 PV
			VolumeMode:       srcPVC.Spec.VolumeMode,
		},
	}

	// 6. 创建 PVC
	log.Printf("Creating PVC %s/%s in destination cluster", namespace, pvcName)
	if _, err := client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, newPVC, metav1.CreateOptions{}); err != nil {
		// PVC 创建失败，删除已创建的 PV
		_ = client.CoreV1().PersistentVolumes().Delete(ctx, pvName, metav1.DeleteOptions{})
		return fmt.Errorf("failed to create PVC: %w", err)
	}

	return nil
}

func copyAndUpdateAttrs(src map[string]string, newImageName string) map[string]string {
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	dst["imageName"] = newImageName
	return dst
}

func ensureNamespace(ctx context.Context, client kubernetes.Interface, namespace string) error {
	_, err := client.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to check namespace %s: %w", namespace, err)
	}

	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: namespace},
	}
	if _, err := client.CoreV1().Namespaces().Create(ctx, ns, metav1.CreateOptions{}); err != nil {
		// 并发场景下可能已被别处创建
		if apierrors.IsAlreadyExists(err) {
			return nil
		}
		return fmt.Errorf("failed to create namespace %s: %w", namespace, err)
	}
	return nil
}

// ============================================================================
// HTTP Server
// ============================================================================

type Server struct {
	// K8s 集群配置
	k8sClusters  map[string]*K8sClusterConfig
	k8sClusterMu sync.RWMutex

	// K8s client 缓存
	k8sClients map[string]kubernetes.Interface
	k8sMu      sync.RWMutex

	// Ceph 全局配置（所有 K8s 共用一个 Ceph）
	cephConfig *CephConfig
	cephMu     sync.RWMutex

	// 任务
	tasks  map[string]*CopyTask
	taskMu sync.RWMutex

	router *mux.Router
	port   string
}

// 已结束的任务保留时长，超过即从内存清理
const finishedTaskTTL = 24 * time.Hour

func NewServer(port string) *Server {
	s := &Server{
		k8sClusters: make(map[string]*K8sClusterConfig),
		k8sClients:  make(map[string]kubernetes.Interface),
		tasks:       make(map[string]*CopyTask),
		port:        port,
		router:      mux.NewRouter(),
	}
	s.registerRoutes()
	go s.reapTasks(finishedTaskTTL)
	return s
}

// reapTasks 周期性清理已结束（completed/failed/cancelled）且超过 TTL 的任务
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

func (s *Server) registerRoutes() {
	s.router.HandleFunc("/health", s.handleHealth).Methods("GET")

	// K8s 集群管理
	s.router.HandleFunc("/api/v1/clusters", s.handleListClusters).Methods("GET")
	s.router.HandleFunc("/api/v1/clusters", s.handleAddCluster).Methods("POST")
	s.router.HandleFunc("/api/v1/clusters/{name}", s.handleDeleteCluster).Methods("DELETE")
	s.router.HandleFunc("/api/v1/clusters/{name}/pvcs", s.handleListPVCs).Methods("GET")

	// Ceph 配置（全局一份）
	s.router.HandleFunc("/api/v1/ceph", s.handleGetCeph).Methods("GET")
	s.router.HandleFunc("/api/v1/ceph", s.handleSetCeph).Methods("POST")

	// 复制操作
	s.router.HandleFunc("/api/v1/copy", s.handleStartCopy).Methods("POST")
	s.router.HandleFunc("/api/v1/copy/{task_id}", s.handleGetTask).Methods("GET")
	s.router.HandleFunc("/api/v1/copy/{task_id}/cancel", s.handleCancelTask).Methods("POST")
	s.router.HandleFunc("/api/v1/tasks", s.handleListTasks).Methods("GET")
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]string{"status": "ok"})
}

// ========== K8s Cluster Handlers ==========

func (s *Server) handleListClusters(w http.ResponseWriter, r *http.Request) {
	s.k8sClusterMu.RLock()
	list := make([]*K8sClusterConfig, 0, len(s.k8sClusters))
	for _, c := range s.k8sClusters {
		list = append(list, c)
	}
	s.k8sClusterMu.RUnlock()

	writeJSON(w, 200, map[string]interface{}{
		"clusters": list,
		"count":    len(list),
	})
}

func (s *Server) handleAddCluster(w http.ResponseWriter, r *http.Request) {
	var cfg K8sClusterConfig
	if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
		writeJSON(w, 400, map[string]string{"error": "invalid request body"})
		return
	}

	if cfg.Name == "" || cfg.Kubeconfig == "" {
		writeJSON(w, 400, map[string]string{"error": "name and kubeconfig are required"})
		return
	}

	// 测试连接
	if _, err := buildK8sClient(&cfg); err != nil {
		writeJSON(w, 400, map[string]string{
			"error": fmt.Sprintf("failed to connect: %v", err),
		})
		return
	}

	s.k8sClusterMu.Lock()
	s.k8sClusters[cfg.Name] = &cfg
	s.k8sClusterMu.Unlock()

	writeJSON(w, 201, map[string]string{"message": "cluster added"})
}

func (s *Server) handleDeleteCluster(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]

	s.k8sClusterMu.Lock()
	delete(s.k8sClusters, name)
	s.k8sClusterMu.Unlock()

	s.k8sMu.Lock()
	delete(s.k8sClients, name)
	s.k8sMu.Unlock()

	writeJSON(w, 200, map[string]string{"message": "cluster deleted"})
}

func (s *Server) handleListPVCs(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]
	ns := r.URL.Query().Get("namespace")
	if ns == "" {
		ns = "default"
	}

	client, err := s.getK8sClient(name)
	if err != nil {
		writeJSON(w, 400, map[string]string{"error": err.Error()})
		return
	}

	list, err := client.CoreV1().PersistentVolumeClaims(ns).List(r.Context(), metav1.ListOptions{})
	if err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
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

	writeJSON(w, 200, map[string]interface{}{
		"cluster": name, "namespace": ns, "pvcs": pvcs, "count": len(pvcs),
	})
}

// ========== Ceph Config Handlers ==========

func (s *Server) handleGetCeph(w http.ResponseWriter, r *http.Request) {
	s.cephMu.RLock()
	defer s.cephMu.RUnlock()

	if s.cephConfig == nil {
		writeJSON(w, 404, map[string]string{"error": "ceph not configured"})
		return
	}

	// 脱敏
	safe := *s.cephConfig
	if safe.Key != "" {
		safe.Key = "***"
	}
	writeJSON(w, 200, safe)
}

func (s *Server) handleSetCeph(w http.ResponseWriter, r *http.Request) {
	var cfg CephConfig
	if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
		writeJSON(w, 400, map[string]string{"error": "invalid request"})
		return
	}

	if cfg.Monitors == "" || cfg.Key == "" {
		writeJSON(w, 400, map[string]string{"error": "monitors and key are required"})
		return
	}
	if cfg.UserID == "" {
		cfg.UserID = "admin"
	}
	if cfg.Pool == "" {
		cfg.Pool = "rbd"
	}

	// 测试连接
	client, err := NewCephClient(&cfg)
	if err != nil {
		writeJSON(w, 400, map[string]string{
			"error": fmt.Sprintf("failed to connect to Ceph: %v", err),
		})
		return
	}
	client.Close()

	s.cephMu.Lock()
	s.cephConfig = &cfg
	s.cephMu.Unlock()

	writeJSON(w, 200, map[string]string{"message": "ceph configured"})
}

// ========== Copy Handlers ==========

func (s *Server) handleStartCopy(w http.ResponseWriter, r *http.Request) {
	var req CopyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, 400, map[string]string{"error": "invalid request"})
		return
	}

	if err := s.validateCopyRequest(&req); err != nil {
		writeJSON(w, 400, map[string]string{"error": err.Error()})
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

	writeJSON(w, 202, map[string]interface{}{
		"task_id":    task.ID,
		"status":     task.Status,
		"message":    "copy started",
		"start_time": task.StartTime,
	})
}

func (s *Server) handleGetTask(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["task_id"]

	s.taskMu.RLock()
	task, ok := s.tasks[id]
	s.taskMu.RUnlock()

	if !ok {
		writeJSON(w, 404, map[string]string{"error": "task not found"})
		return
	}

	task.mu.RLock()
	defer task.mu.RUnlock()

	writeJSON(w, 200, map[string]interface{}{
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

func (s *Server) handleCancelTask(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["task_id"]

	s.taskMu.RLock()
	task, ok := s.tasks[id]
	s.taskMu.RUnlock()

	if !ok {
		writeJSON(w, 404, map[string]string{"error": "task not found"})
		return
	}

	task.mu.Lock()
	if task.Status == "running" || task.Status == "pending" {
		task.cancel()
		task.Status = "cancelled"
		task.Message = "cancelled by user"
	}
	task.mu.Unlock()

	writeJSON(w, 200, map[string]string{"message": "cancel submitted"})
}

func (s *Server) handleListTasks(w http.ResponseWriter, r *http.Request) {
	s.taskMu.RLock()
	defer s.taskMu.RUnlock()

	list := make([]map[string]interface{}, 0, len(s.tasks))
	for _, t := range s.tasks {
		t.mu.RLock()
		list = append(list, map[string]interface{}{
			"task_id":    t.ID,
			"status":     t.Status,
			"progress":   t.Progress,
			"message":    t.Message,
			"start_time": t.StartTime,
			"end_time":   t.EndTime,
		})
		t.mu.RUnlock()
	}

	writeJSON(w, 200, map[string]interface{}{"tasks": list, "count": len(list)})
}

// ============================================================================
// 核心：执行 PVC 复制
// ============================================================================

func (s *Server) executeCopy(task *CopyTask) {
	defer task.cancel() // 释放 WithTimeout 的定时器，避免泄漏
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

	// ========== Step 1: 获取 K8s clients ==========
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

	// ========== Step 2: 获取源 PVC 对应的 RBD image ==========
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

	// ========== Step 3: 检查目标 PVC 是否已存在 ==========
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

	// ========== Step 4: 连接 Ceph ==========
	updateProgress("Connecting to Ceph", 10)

	s.cephMu.RLock()
	cephCfg := s.cephConfig
	s.cephMu.RUnlock()

	if cephCfg == nil {
		failTask("ceph is not configured")
		return
	}

	// 使用源 PVC 所在的 pool（如果与全局配置不同）
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

	// ========== Step 5: 在 Ceph 执行 snapshot → clone → flatten ==========
	// 生成目标 image 名（使用 csi 兼容格式）
	dstImageName := fmt.Sprintf("csi-vol-%d", time.Now().UnixNano())

	if err := cephClient.CopyImage(task.ctx, srcRBD.ImageName, dstImageName, updateProgress); err != nil {
		failTask("failed to copy image: %v", err)
		return
	}

	// ========== Step 6: 在目标 K8s 创建 PV + PVC ==========
	updateProgress("Creating destination PVC", 97)

	if err := CreatePVCFromRBDImage(task.ctx, dstK8s, task.Request.DstNS, task.Request.DstPVC,
		dstImageName, srcPVC, srcPV); err != nil {
		// 创建 PVC 失败，清理 Ceph image
		log.Printf("[%s] Cleanup: removing cloned image %s", task.ID, dstImageName)
		_ = rbd.RemoveImage(cephClient.ioctx, dstImageName)
		failTask("failed to create destination PVC: %v", err)
		return
	}

	// ========== 完成 ==========
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
// Helpers
// ============================================================================

func (s *Server) validateCopyRequest(r *CopyRequest) error {
	if r.SrcCluster == "" || r.SrcNS == "" || r.SrcPVC == "" {
		return fmt.Errorf("src_cluster, src_ns, src_pvc are required")
	}
	if r.DstCluster == "" || r.DstNS == "" || r.DstPVC == "" {
		return fmt.Errorf("dst_cluster, dst_ns, dst_pvc are required")
	}

	s.k8sClusterMu.RLock()
	_, srcOk := s.k8sClusters[r.SrcCluster]
	_, dstOk := s.k8sClusters[r.DstCluster]
	s.k8sClusterMu.RUnlock()

	if !srcOk {
		return fmt.Errorf("source cluster %q not registered", r.SrcCluster)
	}
	if !dstOk {
		return fmt.Errorf("destination cluster %q not registered", r.DstCluster)
	}

	s.cephMu.RLock()
	cephOk := s.cephConfig != nil
	s.cephMu.RUnlock()
	if !cephOk {
		return fmt.Errorf("ceph is not configured (POST /api/v1/ceph first)")
	}

	return nil
}

func (s *Server) getK8sClient(clusterName string) (kubernetes.Interface, error) {
	s.k8sMu.RLock()
	if c, ok := s.k8sClients[clusterName]; ok {
		s.k8sMu.RUnlock()
		return c, nil
	}
	s.k8sMu.RUnlock()

	s.k8sClusterMu.RLock()
	cfg, ok := s.k8sClusters[clusterName]
	s.k8sClusterMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("cluster %q not found", clusterName)
	}

	client, err := buildK8sClient(cfg)
	if err != nil {
		return nil, err
	}

	s.k8sMu.Lock()
	s.k8sClients[clusterName] = client
	s.k8sMu.Unlock()

	return client, nil
}

func buildK8sClient(cfg *K8sClusterConfig) (kubernetes.Interface, error) {
	// 支持两种方式：文件路径 或 直接的 kubeconfig 内容
	restConfig, err := clientcmd.BuildConfigFromFlags("", cfg.Kubeconfig)
	if err != nil {
		// 尝试作为内容解析
		apiConfig, err2 := clientcmd.Load([]byte(cfg.Kubeconfig))
		if err2 != nil {
			return nil, fmt.Errorf("invalid kubeconfig (tried both file path and content): %v / %v", err, err2)
		}
		restConfig, err = clientcmd.NewDefaultClientConfig(*apiConfig, &clientcmd.ConfigOverrides{}).ClientConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to build rest config: %w", err)
		}
	}

	return kubernetes.NewForConfig(restConfig)
}

func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

// ============================================================================
// Main
// ============================================================================

func (s *Server) Start() error {
	log.Printf("Starting PVC Copy API on :%s", s.port)
	log.Println("Scenario: Multiple K8s clusters sharing one Ceph cluster")
	return http.ListenAndServe(":"+s.port, s.router)
}

func main() {
	port := os.Getenv("SERVER_PORT")
	if port == "" {
		port = "8080"
	}

	srv := NewServer(port)
	if err := srv.Start(); err != nil {
		log.Fatal(err)
	}
}
