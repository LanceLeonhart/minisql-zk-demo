# 基于 ZooKeeper 的分布式 MiniSQL 系统

## 项目简介  
本项目为单人实现的简化版分布式 MiniSQL 系统，利用 ZooKeeper（Curator）负责集群管理与路由调度，支持基本的 SQL 操作。

## 技术栈  
- Java 11+  
- Maven  
- ZooKeeper（Curator 客户端）  
- JUnit 5 单元测试  
- IntelliJ IDEA  

## 功能特性  
- **数据分布**：基于主键 `hash(id) % n` 路由 `INSERT/SELECT/UPDATE/DELETE`  
- **集群管理**：RegionServer 以 EPHEMERAL 节点注册，MasterNode 动态监听节点变化  
- **分布式查询**  
  - 全表查询（`SELECT *` 无 WHERE）→ 广播至所有 RegionServer  
  - 主键条件查询（`WHERE id=…`）→ 定向单个 RegionServer  
  - 非主键条件更新/删除 → 广播  
- **基础 SQL 支持**  
  - DDL：`CREATE TABLE`、`DROP TABLE`（列定义、主键）  
  - DML：`INSERT`、`SELECT [WHERE]`、`UPDATE SET … WHERE …`、`DELETE [WHERE]`  
- **线程安全**：内部采用 `ConcurrentHashMap` 存储表元数据与记录  
- **一键启动**：`RegionServerLauncher` 支持批量启动与优雅停止  

## 模块说明  
- **`client.Client`**  
  - 命令行交互，接收标准输入 SQL，输出执行结果  
- **`master.MasterNode`**  
  - 监听 ZooKeeper `/regions` 临时节点，负责 SQL 分发  
- **`region.RegionServer`**  
  - 注册自身节点，接收并执行来自 Master 的 SQL 请求  
- **`minisql`**  
  - 解析与执行 DDL/DML（类型校验、列名/主键校验）  
- **`launcher.RegionServerLauncher`**  
  - 通过 `ProcessBuilder` 启动/停止多个 RegionServer 进程  
- **`test/*.sql`**  
  - 集成测试脚本，批量执行并校验分布式行为  
