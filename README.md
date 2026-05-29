# Mini-RaftKV：基于 Raft 协议的分布式 KV 存储系统

这是一个使用 Go 语言实现的分布式 KV 存储项目。系统通过 Raft 协议在多个节点之间复制日志，保证多数派节点提交后的数据一致。

## 项目功能

本项目已经实现以下能力：

- Raft 三种角色：`Follower`、`Candidate`、`Leader`
- Leader 选举、随机选举超时、投票机制、心跳维持
- 日志复制和 `commitIndex` 推进
- 基于 `prevLogIndex` 和 `prevLogTerm` 的日志一致性校验
- Leader 通过 `nextIndex` 回退机制修复 Follower 落后或冲突日志
- KV 状态机接口：`Get`、`Put`、`Delete`
- 写请求先进入 Raft 日志，多数派提交后再应用到 KV 状态机
- 基于 `clientId + requestId` 的客户端请求去重，避免重试导致重复写入
- 使用 bbolt 做持久化，保存 term、vote、log、snapshot 等状态
- Snapshot 快照和日志截断，降低日志膨胀
- gRPC 服务接口
- Protobuf 接口定义
- Prometheus 指标暴露
- 客户端命令行工具 `raftkvctl`
- YAML 配置文件启动
- Docker Compose 一键启动三节点集群和 Prometheus
- Follower 自动转发读写请求到 Leader，避免读旧副本
- Leader 读请求先确认多数派心跳，再读取状态机，提供更强的读一致性
- 写请求必须多数派提交后才返回成功，少数派不可提交
- 单元测试覆盖选主、复制、删除、冲突修复、重复请求、重启恢复、Leader 故障、网络分区和 Snapshot
- 真实 TCP gRPC 集成测试覆盖客户端读写和 Follower 转发
- 端到端 demo 脚本覆盖启动集群、读写、幂等请求和 Leader 故障恢复
- GitHub Actions 自动运行 Protobuf 生成校验、测试和构建
- 简单压测脚本和 Grafana Dashboard 示例

## 技术栈

- Go
- gRPC
- Protobuf
- bbolt
- Prometheus

## 目录说明

```text
api/raftkv.proto
```

Protobuf 接口定义，描述 Raft 节点之间通信和客户端 KV 操作的 RPC 接口。

```text
cmd/raftkv/
```

节点启动入口。运行这个目录下的程序可以启动一个 RaftKV 节点。

```text
cmd/raftkvctl/
```

客户端命令行工具，可以执行 `put`、`get`、`delete`。

```text
configs/
```

本地和 Docker 环境下的节点配置文件。

```text
deploy/
```

Prometheus 配置文件。

```text
deploy/grafana-dashboard.json
```

Grafana Dashboard 示例，可以导入后观察提交日志、应用日志、角色变化和复制错误。

```text
scripts/
```

本地一键启动三节点集群脚本。

```text
internal/raft/
```

Raft 核心实现，包括选举、日志复制、状态机、持久化、快照和测试。

```text
internal/server/
```

gRPC 服务层，把外部 RPC 请求转成 Raft 节点内部调用。

```text
internal/pb/
```

由 `api/raftkv.proto` 生成的标准 Protobuf 和 gRPC Go 代码。

## 运行测试

在项目根目录执行：

```bash
go test ./...
```

如果测试通过，会看到类似输出：

```text
ok   github.com/juanjuandog/mini-raftkv/internal/raft
ok   github.com/juanjuandog/mini-raftkv/internal/server
```

其中：

- `internal/raft` 测试 Raft 核心算法
- `internal/server` 测试真实 TCP gRPC 集群和 Follower 读写请求转发

## 重新生成 Protobuf 代码

项目使用 `buf` 调用 `protoc-gen-go` 和 `protoc-gen-go-grpc` 生成代码：

```bash
make proto
```

生成目标：

```text
internal/pb/raftkv.pb.go
internal/pb/raftkv_grpc.pb.go
```

## 编译项目

```bash
make build
```

编译后会生成：

```text
bin/raftkv
bin/raftkvctl
```

## 启动 3 个本地节点

### 方式一：使用 Makefile

分别打开三个终端：

```bash
make run-n1
```

```bash
make run-n2
```

```bash
make run-n3
```

### 方式二：使用一键脚本

```bash
make run-cluster
```

这个命令会同时启动 n1、n2、n3，并把日志写到：

```text
logs/n1.log
logs/n2.log
logs/n3.log
```

### 方式三：手动启动

也可以打开三个终端，分别执行下面三个命令。

启动节点 n1：

```bash
go run ./cmd/raftkv \
  -config configs/n1.yaml
```

启动节点 n2：

```bash
go run ./cmd/raftkv \
  -config configs/n2.yaml
```

启动节点 n3：

```bash
go run ./cmd/raftkv \
  -config configs/n3.yaml
```

三个节点启动后，会自动进行 Leader 选举。

## 使用客户端读写数据

启动集群后，可以用 `raftkvctl` 写入数据：

```bash
go run ./cmd/raftkvctl -addr 127.0.0.1:7001 put name raftkv
```

读取数据：

```bash
go run ./cmd/raftkvctl -addr 127.0.0.1:7002 get name
```

删除数据：

```bash
go run ./cmd/raftkvctl -addr 127.0.0.1:7003 delete name
```

即使请求打到 Follower，读写请求也会根据已知 Leader 自动转发。

如果要验证幂等去重，可以重复使用同一个 `client` 和 `request`：

```bash
go run ./cmd/raftkvctl -addr 127.0.0.1:7001 -client c1 -request 100 put idempotent first
go run ./cmd/raftkvctl -addr 127.0.0.1:7001 -client c1 -request 100 put idempotent second
go run ./cmd/raftkvctl -addr 127.0.0.1:7001 get idempotent
```

结果仍然应该是：

```text
first
```

## 运行端到端 Demo

```bash
make demo
```

这个脚本会自动完成：

- 编译 `raftkv` 和 `raftkvctl`
- 启动本地三节点集群
- 执行 `put/get/delete`
- 验证重复请求不会覆盖第一次写入
- 杀掉当前 Leader
- 等待重新选主
- 在新 Leader 上继续写入

日志会输出到：

```text
logs/n1.log
logs/n2.log
logs/n3.log
```

## 简单压测

集群启动后可以执行：

```bash
make bench
```

默认向 `127.0.0.1:7001` 写入 100 个 key。可以通过环境变量调整：

```bash
ADDR=127.0.0.1:7002 N=1000 make bench
```

输出示例：

```text
addr=127.0.0.1:7001 requests=100 ok=100 fail=0 seconds=1 qps=100
```

## 使用 Docker Compose 启动

```bash
make docker-up
```

这会启动：

- n1
- n2
- n3
- Prometheus

停止并清理：

```bash
make docker-down
```

Prometheus 页面：

```text
http://127.0.0.1:9090
```

## 查看 Prometheus 指标

启动节点后，可以在浏览器访问：

```text
http://127.0.0.1:9001/metrics
http://127.0.0.1:9002/metrics
http://127.0.0.1:9003/metrics
```

目前暴露的指标包括：

- Raft 角色切换次数
- 已提交日志数量
- 已应用日志数量
- Snapshot 生成次数
- 日志复制失败次数

## 和简历描述的对应关系

### 1. Raft 协议实现

代码位置：

```text
internal/raft/node.go
```

实现内容：

- Follower、Candidate、Leader 状态切换
- 随机选举超时，避免多个节点固定时间同时发起选举
- 投票请求
- Leader 心跳
- 日志复制
- `commitIndex` 推进

### 2. KV 状态机复制

代码位置：

```text
internal/raft/state_machine.go
internal/server/server.go
cmd/raftkvctl/main.go
```

实现内容：

- `Put`
- `Get`
- `Delete`
- 写请求通过 Raft 日志提交后再 apply 到状态机

### 3. 日志冲突处理

代码位置：

```text
internal/raft/node.go
```

实现内容：

- 使用 `prevLogIndex`
- 使用 `prevLogTerm`
- 如果 Follower 日志不匹配，Leader 回退 `nextIndex`
- 最终让 Follower 日志和 Leader 保持一致

### 4. 幂等与故障恢复

代码位置：

```text
internal/raft/state_machine.go
internal/raft/storage.go
internal/config/config.go
internal/raft/cluster_test.go
```

实现内容：

- 使用 `clientId + requestId` 对客户端请求去重
- 使用 bbolt 持久化 Raft 状态
- 持久化 `term`、`vote`、`log` 和 Snapshot 状态
- 单节点重启后可以恢复已提交数据
- 集群中的 Follower 重启后可以追赶 Leader 已提交日志

### 5. Snapshot 和测试验证

代码位置：

```text
internal/raft/node.go
internal/raft/cluster_test.go
internal/server/grpc_integration_test.go
```

实现内容：

- Snapshot 生成
- 日志截断
- Snapshot 恢复
- 测试场景包括 Leader 选举、随机选举超时、日志复制、日志冲突、重复请求、Follower 重启恢复和 Snapshot
- 测试 Leader 故障后的自动重新选主
- 测试落后 Follower 通过 Snapshot 追赶 Leader
- 真实 gRPC 测试验证 Follower 读写请求转发到 Leader

## 面试时可以重点讲的亮点

### 亮点一：自己实现了 Raft 的关键路径

项目不是简单调用现成 Raft 库，而是自己实现了选举、投票、心跳、日志复制和提交推进。

### 亮点二：处理了日志冲突

Follower 日志落后或冲突时，Leader 会通过 `nextIndex` 回退，结合 `prevLogIndex` 和 `prevLogTerm` 找到一致位置，再覆盖冲突日志。

### 亮点三：写请求具备幂等能力

客户端重试时使用 `clientId + requestId` 去重，避免网络重试导致同一个写请求被重复执行。

### 亮点四：节点可以重启恢复

Raft 的 term、vote、log、snapshot 状态会持久化到 bbolt。节点重启后可以恢复已提交数据。

### 亮点五：实现 Snapshot 降低日志膨胀

状态机生成 Snapshot 后会截断旧日志，避免日志无限增长。

### 亮点六：读写请求自动转发 Leader

客户端请求打到 Follower 时，服务层会根据已知 Leader 转发读写请求，避免直接从落后 Follower 读取旧数据。

### 亮点七：更严格的一致性语义

写请求必须多数派提交后才返回成功。如果 Leader 被隔离到少数派，写请求会返回 `quorum unavailable`，不会提前 apply 到状态机。Leader 读请求也会先向多数派发送心跳确认自己仍是 Leader，再读取本地状态机。

### 亮点八：工程化完整

项目包含标准 Protobuf 生成代码、gRPC、Prometheus、Grafana Dashboard、配置文件、Docker Compose、Makefile、客户端 CLI、端到端 demo、压测脚本、CI 和集成测试，具备完整工程项目形态。

## Protobuf 说明

`api/raftkv.proto` 是项目的 RPC 契约。当前仓库已经使用 `buf` 和 Go Protobuf 插件生成了标准代码：

```text
internal/pb/raftkv.pb.go
internal/pb/raftkv_grpc.pb.go
```

重新生成命令：

```bash
make proto
```
