package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/remotecommand"
)

// ==================== Data Models ====================

type DirectoryRequest struct {
	ClusterName   string `json:"cluster_name"`
	Kubeconfig    string `json:"kubeconfig"`
	Namespace     string `json:"namespace" binding:"required"`
	PodName       string `json:"pod_name" binding:"required"`
	ContainerName string `json:"container_name" binding:"required"`
	DirPath       string `json:"dir_path" binding:"required"`
	Depth         int    `json:"depth" binding:"required,min=0,max=10"`
}

type DirectoryResponse struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
	Message string      `json:"message,omitempty"`
	Details ErrorDetail `json:"details,omitempty"`
}

type PathItem struct {
	Path string `json:"path"`
	Name string `json:"name"`
	Type string `json:"type"` // "file" or "directory"
}

type ErrorDetail struct {
	Stdout string `json:"stdout,omitempty"`
	Stderr string `json:"stderr,omitempty"`
}

// FileRequest 读取容器内文件内容的请求
type FileRequest struct {
	ClusterName   string `json:"cluster_name"`
	Kubeconfig    string `json:"kubeconfig"`
	Namespace     string `json:"namespace" binding:"required"`
	PodName       string `json:"pod_name" binding:"required"`
	ContainerName string `json:"container_name" binding:"required"`
	FilePath      string `json:"file_path" binding:"required"`
}

// ClusterConfig maps to the `cluster` table in MySQL.
// The Description field stores the kubeconfig YAML content.
type ClusterConfig struct {
	ID          int64   `db:"id"`
	Name        string  `db:"name"`
	Server      *string `db:"server"`
	Description string  `db:"description"`
	CreateUser  *int64  `db:"create_user"`
	UpdateUser  *int64  `db:"update_user"`
	UpdateTime  *string `db:"update_time"`
	CreateTime  *string `db:"create_time"`
	DelFlag     int     `db:"del_flag"`
}

type ClusterResponse struct {
	ID         int64   `json:"id"`
	Name       string  `json:"name"`
	Server     *string `json:"server,omitempty"`
	CreateUser *int64  `json:"create_user,omitempty"`
	UpdateUser *int64  `json:"update_user,omitempty"`
	UpdateTime *string `json:"update_time,omitempty"`
	CreateTime *string `json:"create_time,omitempty"`
}

type TerminalMessage struct {
	Type      string `json:"type"` // "connect" or "input"
	Data      string `json:"data"`
	Namespace string `json:"namespace"`
	Pod       string `json:"pod"`
	Container string `json:"container"`
	Cluster   string `json:"cluster"`
}

// wsWriter is a thread-safe WebSocket writer implementing io.Writer.
type wsWriter struct {
	conn *websocket.Conn
	mu   sync.Mutex
}

func (w *wsWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	err := w.conn.WriteMessage(websocket.TextMessage, p)
	return len(p), err
}

// ==================== Global Variables ====================

var (
	dbConnection *sqlx.DB
	upgrader     = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}
)

// ==================== Database Operations ====================

func InitDB(dsn string) error {
	var err error
	dbConnection, err = sqlx.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("failed to open database: %v", err)
	}
	if err := dbConnection.Ping(); err != nil {
		return fmt.Errorf("failed to connect to database: %v", err)
	}
	dbConnection.SetMaxOpenConns(10)
	dbConnection.SetMaxIdleConns(5)
	log.Println("Database connection established successfully")
	return nil
}

func GetClusterKubeconfig(clusterName string) (string, error) {
	if dbConnection == nil {
		return "", fmt.Errorf("database connection not initialized")
	}
	var cluster ClusterConfig
	err := dbConnection.Get(&cluster,
		"SELECT id, name, server, description, create_user, update_user, update_time, create_time, del_flag FROM `cluster` WHERE name = ? AND del_flag = 1 LIMIT 1",
		clusterName)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", fmt.Errorf("cluster '%s' not found", clusterName)
		}
		return "", fmt.Errorf("database query error: %v", err)
	}
	if cluster.Description == "" {
		return "", fmt.Errorf("cluster '%s' has no kubeconfig data", clusterName)
	}
	return cluster.Description, nil
}

func ListClusters() ([]ClusterConfig, error) {
	if dbConnection == nil {
		return nil, fmt.Errorf("database connection not initialized")
	}
	var clusters []ClusterConfig
	err := dbConnection.Select(&clusters,
		"SELECT id, name, server, description, create_user, update_user, update_time, create_time, del_flag FROM `cluster` WHERE del_flag = 1 ORDER BY create_time DESC")
	if err != nil {
		return nil, fmt.Errorf("database query error: %v", err)
	}
	return clusters, nil
}

// GetClusterRestConfig builds a rest.Config from a named cluster stored in the database.
// It parses the kubeconfig content in-memory without writing a temp file.
func GetClusterRestConfig(clusterName string) (*rest.Config, error) {
	content, err := GetClusterKubeconfig(clusterName)
	if err != nil {
		return nil, err
	}
	apiConfig, err := clientcmd.Load([]byte(content))
	if err != nil {
		return nil, fmt.Errorf("invalid kubeconfig for cluster %q: %v", clusterName, err)
	}
	return clientcmd.NewDefaultClientConfig(*apiConfig, &clientcmd.ConfigOverrides{}).ClientConfig()
}

// BuildRestConfig builds a rest.Config from a kubeconfig file path, or falls back to
// in-cluster config when kubeconfigPath is empty.
func BuildRestConfig(kubeconfigPath string) (*rest.Config, error) {
	if kubeconfigPath != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	}
	return rest.InClusterConfig()
}

// ==================== Kubernetes Operations ====================

func GetContainerDirectoryInfo(
	config *rest.Config,
	namespace, podName, containerName, dirPath string,
	depth int,
) (string, string, error) {
	command := fmt.Sprintf("find %s -mindepth %d -maxdepth %d -print 2>/dev/null", dirPath, depth, depth)
	return execInPod(config, namespace, podName, containerName, command)
}

func execInPod(
	config *rest.Config,
	namespace, podName, containerName, command string,
) (string, string, error) {
	clientSet, err := kubernetes.NewForConfig(config)
	if err != nil {
		return "", "", err
	}
	cmd := []string{"/bin/sh", "-c", command}
	req := clientSet.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace(namespace).
		SubResource("exec").
		Param("container", containerName)
	req.VersionedParams(
		&corev1.PodExecOptions{Command: cmd, Stdin: false, Stdout: true, Stderr: true, TTY: false},
		scheme.ParameterCodec,
	)
	executor, err := remotecommand.NewSPDYExecutor(config, "POST", req.URL())
	if err != nil {
		return "", "", err
	}
	var stdout, stderr strings.Builder
	err = executor.StreamWithContext(context.Background(), remotecommand.StreamOptions{
		Stdout: &stdout,
		Stderr: &stderr,
	})
	return stdout.String(), stderr.String(), err
}

// execInPodArgs execs a command directly in a pod without wrapping it in a shell.
// Use this instead of execInPod when the arguments are controlled by the caller
// to avoid shell injection entirely.
func execInPodArgs(
	config *rest.Config,
	namespace, podName, containerName string,
	args []string,
) (string, string, error) {
	clientSet, err := kubernetes.NewForConfig(config)
	if err != nil {
		return "", "", err
	}
	req := clientSet.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace(namespace).
		SubResource("exec").
		Param("container", containerName)
	req.VersionedParams(
		&corev1.PodExecOptions{Command: args, Stdin: false, Stdout: true, Stderr: true, TTY: false},
		scheme.ParameterCodec,
	)
	executor, err := remotecommand.NewSPDYExecutor(config, "POST", req.URL())
	if err != nil {
		return "", "", err
	}
	var stdout, stderr strings.Builder
	err = executor.StreamWithContext(context.Background(), remotecommand.StreamOptions{
		Stdout: &stdout,
		Stderr: &stderr,
	})
	return stdout.String(), stderr.String(), err
}

func ExecInteractiveTerminal(
	config *rest.Config,
	namespace, podName, containerName string,
	stdin io.Reader,
	stdout, stderr io.Writer,
) error {
	clientSet, err := kubernetes.NewForConfig(config)
	if err != nil {
		return err
	}
	req := clientSet.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace(namespace).
		SubResource("exec").
		Param("container", containerName)
	req.VersionedParams(
		&corev1.PodExecOptions{
			Command: []string{"/bin/sh"},
			Stdin:   true, Stdout: true, Stderr: true, TTY: true,
		},
		scheme.ParameterCodec,
	)
	executor, err := remotecommand.NewSPDYExecutor(config, "POST", req.URL())
	if err != nil {
		return err
	}
	return executor.StreamWithContext(context.Background(), remotecommand.StreamOptions{
		Stdin: stdin, Stdout: stdout, Stderr: stderr, Tty: true,
	})
}

// ==================== WebSocket Terminal Handler ====================

func wsHandler(c *gin.Context) {
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Printf("WebSocket upgrade error: %v", err)
		return
	}
	defer conn.Close()

	// Prefer URL query params; fall back to an initial "connect" message.
	namespace := c.Query("namespace")
	pod := c.Query("pod")
	container := c.Query("container")
	clusterName := c.Query("cluster")

	if namespace == "" || pod == "" || container == "" {
		var msg TerminalMessage
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		if err := conn.ReadJSON(&msg); err != nil {
			log.Printf("Failed to read initial message: %v", err)
			_ = conn.WriteJSON(gin.H{"error": "missing connection parameters in URL or message"})
			return
		}
		conn.SetReadDeadline(time.Time{})

		if msg.Type != "connect" {
			_ = conn.WriteJSON(gin.H{"error": "first message must be type 'connect'"})
			return
		}
		namespace = msg.Namespace
		pod = msg.Pod
		container = msg.Container
		if msg.Cluster != "" {
			clusterName = msg.Cluster
		}
	}

	if namespace == "" || pod == "" || container == "" {
		_ = conn.WriteJSON(gin.H{"error": "missing required fields: namespace, pod, container"})
		return
	}

	// Resolve rest.Config from DB cluster name or local/in-cluster kubeconfig.
	var config *rest.Config
	if clusterName != "" {
		config, err = GetClusterRestConfig(clusterName)
	} else {
		config, err = BuildRestConfig("")
	}
	if err != nil {
		_ = conn.WriteJSON(gin.H{"error": fmt.Sprintf("failed to build kube config: %v", err)})
		return
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		_ = conn.WriteJSON(gin.H{"error": fmt.Sprintf("failed to create k8s client: %v", err)})
		return
	}

	req := clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(pod).
		Namespace(namespace).
		SubResource("exec")
	req.VersionedParams(&corev1.PodExecOptions{
		Container: container,
		Command:   []string{"/bin/sh"},
		Stdin:     true, Stdout: true, Stderr: true, TTY: true,
	}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(config, "POST", req.URL())
	if err != nil {
		log.Printf("SPDY executor error: %v", err)
		return
	}

	reader, writer := io.Pipe()
	go func() {
		defer writer.Close()
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			if _, err := writer.Write(msg); err != nil {
				return
			}
		}
	}()

	ww := &wsWriter{conn: conn}
	if err := exec.StreamWithContext(context.Background(), remotecommand.StreamOptions{
		Stdin:  reader,
		Stdout: ww,
		Stderr: ww,
		Tty:    true,
	}); err != nil {
		log.Printf("terminal stream error: %v", err)
	}
}

// ==================== File Content Handler ====================

// maxFileReadSize limits how many bytes are returned from a single file read.
const maxFileReadSize = 10 * 1024 * 1024 // 10 MB

// getFileContent reads a file from inside a container and returns its content.
// The exec is done without a shell to prevent injection.
func getFileContent(c *gin.Context) {
	var req FileRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false, "error": "invalid request format", "message": err.Error(),
		})
		return
	}

	if strings.Contains(req.FilePath, "..") || strings.ContainsAny(req.FilePath, ";|&`$\\") {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false, "error": "invalid path",
			"message": "path traversal and shell metacharacters are not allowed",
		})
		return
	}

	var (
		config *rest.Config
		err    error
	)
	switch {
	case req.ClusterName != "":
		config, err = GetClusterRestConfig(req.ClusterName)
	case req.Kubeconfig != "":
		config, err = BuildRestConfig(req.Kubeconfig)
	default:
		config, err = BuildRestConfig("")
	}
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false, "error": "failed to get kubeconfig", "message": err.Error(),
		})
		return
	}

	// cat is called directly (no shell) so the file path cannot be used for injection.
	stdout, stderr, err := execInPodArgs(
		config, req.Namespace, req.PodName, req.ContainerName,
		[]string{"cat", req.FilePath},
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false, "error": "failed to read file",
			"message": err.Error(), "stderr": stderr,
		})
		return
	}
	if stdout == "" && stderr != "" {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false, "error": "file read returned error", "stderr": stderr,
		})
		return
	}

	truncated := false
	content := stdout
	if len(content) > maxFileReadSize {
		content = content[:maxFileReadSize]
		truncated = true
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data": gin.H{
			"content":   content,
			"size":      len(stdout),
			"path":      req.FilePath,
			"truncated": truncated,
		},
		"message": "file content retrieved successfully",
	})
}

// ==================== Directory Query Handler ====================

func queryDirectory(c *gin.Context) {
	var req DirectoryRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, DirectoryResponse{
			Success: false, Error: "invalid request format", Message: err.Error(),
		})
		return
	}

	// Reject path traversal and shell metacharacters to prevent command injection.
	if strings.Contains(req.DirPath, "..") || strings.ContainsAny(req.DirPath, ";|&`$\\") {
		c.JSON(http.StatusBadRequest, DirectoryResponse{
			Success: false, Error: "invalid path",
			Message: "path traversal and shell metacharacters are not allowed",
		})
		return
	}

	var (
		config *rest.Config
		err    error
	)
	switch {
	case req.ClusterName != "":
		config, err = GetClusterRestConfig(req.ClusterName)
	case req.Kubeconfig != "":
		config, err = BuildRestConfig(req.Kubeconfig)
	default:
		config, err = BuildRestConfig("")
	}
	if err != nil {
		c.JSON(http.StatusBadRequest, DirectoryResponse{
			Success: false, Error: "failed to get kubeconfig", Message: err.Error(),
		})
		return
	}

	stdout, stderr, err := GetContainerDirectoryInfo(
		config, req.Namespace, req.PodName, req.ContainerName, req.DirPath, req.Depth,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, DirectoryResponse{
			Success: false, Error: "failed to execute command", Message: err.Error(),
			Details: ErrorDetail{Stdout: stdout, Stderr: stderr},
		})
		return
	}

	if stdout == "" && stderr != "" {
		c.JSON(http.StatusInternalServerError, DirectoryResponse{
			Success: false, Error: "command returned error",
			Details: ErrorDetail{Stdout: stdout, Stderr: stderr},
		})
		return
	}

	pathItems := ParsePathsFromOutput(stdout)
	responseData := gin.H{"paths": pathItems, "count": len(pathItems)}
	if c.Query("raw") == "true" {
		responseData["raw"] = stdout
	}
	c.JSON(http.StatusOK, DirectoryResponse{
		Success: true, Data: responseData,
		Message: "directory information retrieved successfully",
		Details: ErrorDetail{Stderr: stderr},
	})
}

// ParsePathsFromOutput parses find(1) output into structured PathItems.
// Type detection uses name heuristics only; it is not authoritative.
func ParsePathsFromOutput(output string) []PathItem {
	lines := strings.Split(strings.TrimSpace(output), "\n")
	paths := make([]PathItem, 0, len(lines))
	for _, line := range lines {
		if line == "" {
			continue
		}
		parts := strings.Split(line, "/")
		name := parts[len(parts)-1]
		if name == "" {
			name = "/"
		}
		itemType := "directory"
		if strings.Contains(name, ".") {
			itemType = "file"
		}
		paths = append(paths, PathItem{Path: line, Name: name, Type: itemType})
	}
	return paths
}

// ==================== API Documentation ====================

func apiDoc(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"api_version": "2.0",
		"title":       "Ceph PVC Copier & Container Terminal API",
		"database": gin.H{
			"enabled":     dbConnection != nil,
			"description": "MySQL database for cluster kubeconfig storage",
		},
		"endpoints": []gin.H{
			{"method": "GET", "path": "/health", "description": "health check"},
			{"method": "GET", "path": "/api/v1/docs", "description": "API documentation"},
			{"method": "GET", "path": "/api/v1/clusters", "description": "list registered K8s clusters"},
			{"method": "POST", "path": "/api/v1/clusters", "description": "register a K8s cluster"},
			{"method": "DELETE", "path": "/api/v1/clusters/:name", "description": "remove a cluster"},
			{"method": "GET", "path": "/api/v1/clusters/:name/pvcs", "description": "list PVCs in cluster"},
			{"method": "GET", "path": "/api/v1/ceph", "description": "get Ceph configuration"},
			{"method": "POST", "path": "/api/v1/ceph", "description": "set Ceph configuration"},
			{"method": "GET", "path": "/api/v1/clusters/:cluster/namespaces/:ns/pvcs/:pvc/snapshots", "description": "list RBD snapshots of a PVC"},
		{"method": "POST", "path": "/api/v1/clusters/:cluster/namespaces/:ns/pvcs/:pvc/snapshots", "description": "create RBD snapshot (snap_name optional)"},
		{"method": "DELETE", "path": "/api/v1/clusters/:cluster/namespaces/:ns/pvcs/:pvc/snapshots/:snap", "description": "delete RBD snapshot"},
		{"method": "POST", "path": "/api/v1/copy", "description": "start PVC copy task"},
			{"method": "GET", "path": "/api/v1/copy/:task_id", "description": "get task status"},
			{"method": "POST", "path": "/api/v1/copy/:task_id/cancel", "description": "cancel task"},
			{"method": "GET", "path": "/api/v1/tasks", "description": "list all tasks"},
			{"method": "POST", "path": "/api/v1/directory", "description": "query container directory"},
			{"method": "POST", "path": "/api/v1/file", "description": "read file content from container (max 10 MB)"},
			{"method": "WebSocket", "path": "/api/v1/terminal", "description": "interactive container terminal"},
		},
	})
}
