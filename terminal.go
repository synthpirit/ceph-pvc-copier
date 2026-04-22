package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
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
	"k8s.io/client-go/util/homedir"
)

// ==================== 数据结构定义 ====================

// DirectoryRequest 容器目录信息查询请求
type DirectoryRequest struct {
	ClusterName   string `json:"cluster_name"`
	Kubeconfig    string `json:"kubeconfig"`
	Namespace     string `json:"namespace" binding:"required"`
	PodName       string `json:"pod_name" binding:"required"`
	ContainerName string `json:"container_name" binding:"required"`
	DirPath       string `json:"dir_path" binding:"required"`
	Depth         int    `json:"depth" binding:"required,min=0,max=10"`
}

// DirectoryResponse 响应结构体
type DirectoryResponse struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
	Message string      `json:"message,omitempty"`
	Details ErrorDetail `json:"details,omitempty"`
}

// PathItem 路径项
type PathItem struct {
	Path string `json:"path"`
	Name string `json:"name"`
	Type string `json:"type"` // "file" or "directory"
}

// ErrorDetail 错误详情
type ErrorDetail struct {
	Stdout string `json:"stdout,omitempty"`
	Stderr string `json:"stderr,omitempty"`
}

// ClusterConfig 集群配置
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

// ClusterResponse 用于 API 响应的集群配置（不包含敏感的 kubeconfig 数据摘要）
type ClusterResponse struct {
	ID         int64   `json:"id"`
	Name       string  `json:"name"`
	Server     *string `json:"server,omitempty"`
	CreateUser *int64  `json:"create_user,omitempty"`
	UpdateUser *int64  `json:"update_user,omitempty"`
	UpdateTime *string `json:"update_time,omitempty"`
	CreateTime *string `json:"create_time,omitempty"`
}

// TerminalMessage WebSocket 消息结构
type TerminalMessage struct {
	Type      string `json:"type"` // "input", "command", "connect"
	Data      string `json:"data"`
	Namespace string `json:"namespace"`
	Pod       string `json:"pod"`
	Container string `json:"container"`
	Cluster   string `json:"cluster"`
}

// WSWriter WebSocket 写入器
type WSWriter struct {
	conn *websocket.Conn
	mu   chan struct{}
}

// ==================== 全局变量 ====================

var (
	dbConnection *sqlx.DB
	upgrader     = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}
)

// ==================== 数据库操作 ====================

// InitDB 初始化数据库连接
func InitDB(dsn string) error {
	var err error
	dbConnection, err = sqlx.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("failed to open database: %v", err)
	}

	// 测试连接
	if err := dbConnection.Ping(); err != nil {
		return fmt.Errorf("failed to connect to database: %v", err)
	}

	// 设置连接池参数
	dbConnection.SetMaxOpenConns(10)
	dbConnection.SetMaxIdleConns(5)

	log.Println("Database connection established successfully")
	return nil
}

// GetClusterKubeconfig 通过集群名称从数据库获取 kubeconfig
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

	// Description 字段包含 kubeconfig 内容
	if cluster.Description == "" {
		return "", fmt.Errorf("cluster '%s' has no kubeconfig data", clusterName)
	}

	return cluster.Description, nil
}

// ListClusters 获取所有集群列表
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

// ==================== Kubernetes 操作 ====================

// GetKubeConfig 获取 kubeconfig，优先级：参数 > 数据库 > 本地 > 集群内
func GetKubeConfig(kubeconfigPath string, clusterName string) (string, error) {
	// 1. 如果直接提供了 kubeconfig 路径，验证其有效性
	if kubeconfigPath != "" {
		if _, err := os.Stat(kubeconfigPath); err == nil {
			return kubeconfigPath, nil
		}
	}

	// 2. 如果提供了集群名称，从数据库获取
	if clusterName != "" && dbConnection != nil {
		content, err := GetClusterKubeconfig(clusterName)
		if err == nil {
			// 将内容写入临时文件
			tempFile, err := os.CreateTemp("", "kubeconfig-*.yml")
			if err != nil {
				return "", err
			}
			defer tempFile.Close()
			if _, err := tempFile.WriteString(content); err != nil {
				return "", err
			}
			return tempFile.Name(), nil
		}
	}

	// 3. 尝试本地 kubeconfig
	if home := homedir.HomeDir(); home != "" {
		localConfig := home + "/.kube/config"
		if _, err := os.Stat(localConfig); err == nil {
			return localConfig, nil
		}
	}

	// 4. 返回空字符串，让 clientcmd 使用集群内配置
	return "", nil
}

// BuildRestConfig 构建 REST 配置
func BuildRestConfig(kubeconfigPath string) (*rest.Config, error) {
	if kubeconfigPath != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	}
	// 尝试集群内配置
	return rest.InClusterConfig()
}

// GetContainerDirectoryInfo 获取容器目录信息
func GetContainerDirectoryInfo(
	kubeconfig string,
	namespace string,
	podName string,
	containerName string,
	dirPath string,
	depth int,
) (string, string, error) {
	// 加载 kubeconfig
	fmt.Println("in GetContainerDirectoryInfo")
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return "", "", fmt.Errorf("failed to load kubeconfig: %v", err)
	}

	// 获取基本的目录列表
	command := fmt.Sprintf("find %s -mindepth %d -maxdepth %d 2>/dev/null", dirPath, depth, depth)
	stdout, stderr, err := execInPod(config, namespace, podName, containerName, command)

	return stdout, stderr, err
}

// execInPod 在 Pod 中执行命令
func execInPod(
        config *rest.Config,
        namespace string,
        podName string,
        containerName string,
        command string,
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
                &corev1.PodExecOptions{
                        Command: cmd,
                        Stdin:   false,
                        Stdout:  true,
                        Stderr:  true,
                        TTY:     false,
                },
                scheme.ParameterCodec,
        )

        executor, err := remotecommand.NewSPDYExecutor(config, "POST", req.URL())
        if err != nil {
                return "", "", err
        }

        var stdout, stderr strings.Builder
        err = executor.StreamWithContext(context.Background(), remotecommand.StreamOptions{
                Stdin:  nil,
                Stdout: &stdout,
                Stderr: &stderr,
        })

        return stdout.String(), stderr.String(), err
}


// ExecInteractiveTerminal 在容器中执行交互式终端会话
func ExecInteractiveTerminal(
	config *rest.Config,
	namespace string,
	podName string,
	containerName string,
	stdin io.Reader,
	stdout io.Writer,
	stderr io.Writer,
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
			Stdin:   true,
			Stdout:  true,
			Stderr:  true,
			TTY:     true,
		},
		scheme.ParameterCodec,
	)

	executor, err := remotecommand.NewSPDYExecutor(config, "POST", req.URL())
	if err != nil {
		return err
	}

	stream := remotecommand.StreamOptions{
		Stdin:  stdin,
		Stdout: stdout,
		Stderr: stderr,
		Tty:    true,
	}

	return executor.StreamWithContext(context.Background(), stream)
}

// ==================== WebSocket 操作 ====================

// Write 实现 io.Writer 接口
func (w *WSWriter) Write(p []byte) (int, error) {
	w.mu <- struct{}{}
	defer func() { <-w.mu }()

	err := w.conn.WriteMessage(websocket.TextMessage, p)
	return len(p), err
}

// Close 关闭连接
func (w *WSWriter) Close() error {
	return w.conn.Close()
}

// PipeReader 和 PipeWriter 的包装
type PipeReadWriter struct {
	*io.PipeReader
	*io.PipeWriter
}

// ==================== HTTP 处理器 ====================

// wsTerminalHandler WebSocket 终端处理器
func wsHandler(c *gin.Context) {

	w := c.Writer
	r := c.Request

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WebSocket upgrade error: %v", err)
		return
	}
	defer conn.Close()

	ns := c.Query("namespace")
	pod := c.Query("pod")
	container := c.Query("container")
	clusterName := c.Query("cluster")


	// 方式 1: 尝试从 URL 查询参数获取连接信息
	namespace := c.Query("namespace")
	//pod := c.Query("pod")
	//container := c.Query("container")
	//clusterName := c.Query("cluster")

	// 方式 2: 如果查询参数不完整，尝试从 WebSocket 消息获取
	if namespace == "" || pod == "" || container == "" {
		var msg TerminalMessage
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		err = conn.ReadJSON(&msg)
		conn.SetReadDeadline(time.Time{})

		if err != nil {
			log.Printf("Failed to read initial message: %v", err)
			conn.WriteJSON(gin.H{
				"error": "Missing connection parameters in URL or message",
			})
			return
		}

		if msg.Type != "connect" {
			conn.WriteJSON(gin.H{
				"error": "First message must be 'connect' type",
			})
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
		conn.WriteJSON(gin.H{
			"error": "Missing required fields: namespace, pod, container",
		})
		return
	}

	// 获取 kubeconfig
	kubeconfigPath, err := GetKubeConfig("", clusterName)
	if err != nil {
		conn.WriteJSON(gin.H{
			"error": fmt.Sprintf("Failed to get kubeconfig: %v", err),
		})
		return
	}

	// 构建 REST 配置
	config, err := BuildRestConfig(kubeconfigPath)
	if err != nil {
		conn.WriteJSON(gin.H{
			"error": fmt.Sprintf("Failed to build kube config: %v", err),
		})
		return
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatal(err)
	}

	req := clientset.CoreV1().
		RESTClient().
		Post().
		Resource("pods").
		Name(pod).
		Namespace(ns).
		SubResource("exec")

	req.VersionedParams(&corev1.PodExecOptions{

		Container: container,
		Command:   []string{"/bin/sh"},

		Stdin:  true,
		Stdout: true,
		Stderr: true,
		TTY:    true,

	}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(config, "POST", req.URL())
	if err != nil {
		log.Println(err)
		return
	}

	reader, writer := io.Pipe()

	go func() {

		for {

			_, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}

			writer.Write(msg)

		}

	}()

	stream := remotecommand.StreamOptions{

		Stdin:  reader,
		Stdout: wsWriter{conn},
		Stderr: wsWriter{conn},
		Tty:    true,
	}

	err = exec.StreamWithContext(context.Background(), stream)
	if err != nil {
		log.Println(err)
	}
}

type wsWriter struct {
	conn *websocket.Conn
}

func (w wsWriter) Write(p []byte) (int, error) {

	err := w.conn.WriteMessage(websocket.TextMessage, p)

	return len(p), err
}



// queryDirectory 处理查询目录的请求
func queryDirectory(c *gin.Context) {
	var req DirectoryRequest
    fmt.Println("in queryDirectory")
	// 绑定 JSON 请求体
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, DirectoryResponse{
			Success: false,
			Error:   "Invalid request format",
			Message: err.Error(),
		})
		return
	}

	// 验证路径安全性
	if strings.Contains(req.DirPath, "..") {
		c.JSON(http.StatusBadRequest, DirectoryResponse{
			Success: false,
			Error:   "Invalid path",
			Message: "Path traversal is not allowed",
		})
		return
	}

	// 获取 kubeconfig
	kubeconfig, err := GetKubeConfig(req.Kubeconfig, req.ClusterName)
	if err != nil {
		c.JSON(http.StatusBadRequest, DirectoryResponse{
			Success: false,
			Error:   "Failed to get kubeconfig",
			Message: err.Error(),
		})
		return
	}

	if kubeconfig == "" {
		c.JSON(http.StatusBadRequest, DirectoryResponse{
			Success: false,
			Error:   "Missing kubeconfig",
			Message: "Either provide 'kubeconfig' path or 'cluster_name'",
		})
		return
	}

	// 执行查询
	stdout, stderr, err := GetContainerDirectoryInfo(
		kubeconfig,
		req.Namespace,
		req.PodName,
		req.ContainerName,
		req.DirPath,
		req.Depth,
	)

	if err != nil {
		c.JSON(http.StatusInternalServerError, DirectoryResponse{
			Success: false,
			Error:   "Failed to execute command",
			Message: err.Error(),
			Details: ErrorDetail{
				Stdout: stdout,
				Stderr: stderr,
			},
		})
		return
	}

	if stdout == "" && stderr != "" {
		c.JSON(http.StatusInternalServerError, DirectoryResponse{
			Success: false,
			Error:   "Command execution returned error",
			Message: "Check stderr for details",
			Details: ErrorDetail{
				Stdout: stdout,
				Stderr: stderr,
			},
		})
		return
	}

	// 解析路径数据
	pathItems := ParsePathsFromOutput(stdout)

	// 检查是否要求返回原始数据
	includeRaw := c.Query("raw") == "true"

	responseData := gin.H{
		"paths": pathItems,
		"count": len(pathItems),
	}

	if includeRaw {
		responseData["raw"] = stdout
	}

	c.JSON(http.StatusOK, DirectoryResponse{
		Success: true,
		Data:    responseData,
		Message: "Directory information retrieved successfully",
		Details: ErrorDetail{
			Stderr: stderr,
		},
	})
}

// ParsePathsFromOutput 解析 find 命令输出为结构化数据
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
		} else if strings.HasPrefix(name, ".") && len(name) > 1 {
			itemType = "file"
		}

		paths = append(paths, PathItem{
			Path: line,
			Name: name,
			Type: itemType,
		})
	}

	return paths
}

// healthCheck 健康检查端点
func healthCheck(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":  "ok",
		"message": "Container Directory API is running",
	})
}

// listClusters 获取集群列表
func listClusters(c *gin.Context) {
	clusters, err := ListClusters()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   "Failed to fetch clusters",
			"message": err.Error(),
		})
		return
	}

	responses := make([]ClusterResponse, len(clusters))
	for i, cluster := range clusters {
		responses[i] = ClusterResponse{
			ID:         cluster.ID,
			Name:       cluster.Name,
			Server:     cluster.Server,
			CreateUser: cluster.CreateUser,
			UpdateUser: cluster.UpdateUser,
			UpdateTime: cluster.UpdateTime,
			CreateTime: cluster.CreateTime,
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data":    responses,
		"count":   len(responses),
		"message": "Clusters fetched successfully",
	})
}

// apiDoc 获取 API 文档
func apiDoc(c *gin.Context) {
	doc := gin.H{
		"api_version": "2.0",
		"title":       "Container Directory API with Terminal",
		"description": "Query directory information and access interactive terminals from Kubernetes containers",
		"database": gin.H{
			"enabled":     dbConnection != nil,
			"description": "MySQL database for cluster configuration storage",
		},
		"endpoints": []gin.H{
			{
				"path":        "/health",
				"method":      "GET",
				"description": "Health check endpoint",
				"response": gin.H{
					"status":  "ok",
					"message": "Container Directory API is running",
				},
			},
			{
				"path":        "/api/v1/clusters",
				"method":      "GET",
				"description": "Get list of all available clusters",
			},
			{
				"path":        "/api/v1/directory",
				"method":      "POST",
				"description": "Query directory information from a container",
				"request_body": gin.H{
					"cluster_name":   "string (optional) - Cluster name",
					"kubeconfig":     "string (optional) - Path to kubeconfig",
					"namespace":      "string (required) - Kubernetes namespace",
					"pod_name":       "string (required) - Pod name",
					"container_name": "string (required) - Container name",
					"dir_path":       "string (required) - Directory path",
					"depth":          "integer (required, 0-10) - Find depth",
				},
			},
			{
				"path":        "/api/v1/terminal",
				"method":      "WebSocket",
				"description": "Interactive terminal access to container",
				"protocol":    "ws:// or wss://",
				"initial_message": gin.H{
					"type":      "connect",
					"cluster":   "string (optional) - Cluster name",
					"namespace": "string (required) - Kubernetes namespace",
					"pod":       "string (required) - Pod name",
					"container": "string (required) - Container name",
				},
				"input_message": gin.H{
					"type": "input",
					"data": "string - Command input",
				},
			},
			{
				"path":        "/api/v1/docs",
				"method":      "GET",
				"description": "API documentation",
			},
		},
		"example_requests": []gin.H{
			{
				"description": "Query directory using cluster name",
				"method":      "POST",
				"url":         "/api/v1/directory",
				"body": gin.H{
					"cluster_name":   "production",
					"namespace":      "default",
					"pod_name":       "redis-master-0",
					"container_name": "redis",
					"dir_path":       "/",
					"depth":          2,
				},
			},
			{
				"description": "WebSocket terminal connection",
				"method":      "WebSocket",
				"url":         "ws://localhost:8088/api/v1/terminal",
				"steps": []string{
					"Connect to WebSocket",
					"Send: {\"type\": \"connect\", \"cluster\": \"production\", \"namespace\": \"default\", \"pod\": \"redis-master-0\", \"container\": \"redis\"}",
					"Send: {\"type\": \"input\", \"data\": \"ls -la\\n\"}",
				},
			},
		},
	}

	c.JSON(http.StatusOK, doc)
}

// ==================== 主函数 ====================

func main() {
	// 初始化数据库
	dbDSN := os.Getenv("DB_DSN")
	if dbDSN != "" {
		if err := InitDB(dbDSN); err != nil {
			log.Printf("Warning: Database initialization failed: %v", err)
			log.Println("Continuing without database support.")
		}
	} else {
		log.Println("No DB_DSN provided. Database support disabled.")
	}

	// 设置 Gin 运行模式
	gin.SetMode(gin.ReleaseMode)

	// 创建 Gin 引擎
	router := gin.New()

	// 添加中间件
	router.Use(gin.Logger())
	router.Use(gin.Recovery())

	// 添加 CORS 中间件
	router.Use(func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}

		c.Next()
	})

	// 注册路由
	router.GET("/health", healthCheck)
	router.GET("/api/v1/docs", apiDoc)
	router.GET("/api/v1/clusters", listClusters)
	router.POST("/api/v1/directory", queryDirectory)
	router.GET("/api/v1/terminal", func(c *gin.Context) {
		wsHandler(c)
	})

	// 启动服务器
	port := os.Getenv("PORT")
	if port == "" {
		port = "8088"
	}

	address := "0.0.0.0:" + port

	log.Printf("Starting Container Directory API server on %s", address)
	log.Printf("API documentation available at http://0.0.0.0:%s/api/v1/docs", port)
	log.Printf("WebSocket terminal available at ws://0.0.0.0:%s/api/v1/terminal", port)

	if err := router.Run(address); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}