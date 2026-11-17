# 共享诊断 SQL 服务（FastAPI + PostgreSQL）

值班数据库工程师共用的诊断执行服务：提交至多 10 条可信 SQL，在**固定复用的两条
PostgreSQL 连接**之一上按序自动提交执行；支持逐句结果查询、取消未结束的单；
每单结束后清理会话状态（SET、临时表、会话级咨询锁等），清理失败则淘汰连接并补建。

## 架构

```
                ┌────────────── FastAPI (uvicorn, 单进程) ──────────────┐
  POST /jobs ──▶│  校验(sqlguard) → JobStore(内存注册表) → 共享队列       │
  GET  /jobs/*  │                                                        │
  POST ../cancel│  cancel: 置标志 + 202 即时受理 ──▶ CancelSender 线程    │
  GET  /pool    │       (可配置发送停顿) ──▶ conn.cancel()               │
                │                                                        │
                │  Worker 线程 #1 ── 连接 #1 (autocommit, 长连接)         │
                │  Worker 线程 #2 ── 连接 #2 (autocommit, 长连接)         │
                └────────────────────────┬───────────────────────────────┘
                                         ▼
                                   PostgreSQL
```

- **固定两条连接**：两个 worker 线程各自持有一条长连接（`autocommit=True`），
  从同一个队列取单。连接在启动时建立并反复复用，**绝不每单重连**；唯一的重建
  路径是"清理失败/连接损坏 → 淘汰旧连接 → 原地补建"（`generation` 递增，
  池子仍是两条）。
- **排队**：两条连接都忙时，新单在内存队列等待（默认上限 1000，超出返回 429）。
- **互不阻塞**：每条连接一个独立 worker；慢 SQL、慢清理只占用本连接的 worker，
  另一连接的执行、提交以及所有状态查询（读内存注册表）都不受影响。
- **记录保留**：未结束的单（排队中、执行中，或已有结论但取消发送/会话清理
  未完结）**永不因历史容量被淘汰**，始终可查、可取消；只有后台完全处理完毕
  （SQL 停止 + 取消发送完成 + 清理完成 + 连接归还，或从未运行）的记录才按
  `JOB_HISTORY` 容量淘汰最旧的。

## 快速开始

```bash
docker compose up --build
# API: http://localhost:8000  文档: http://localhost:8000/docs
```

compose 中特意把 `JOB_HISTORY` 设为 **2**，方便在 /docs 里直接演示容量压力：
连续提交多个单（可先用 `SELECT pg_sleep(30)` 占住两条连接），未结束的单始终
可查、可取消；已完成的记录只保留最新 2 条。正常使用可调大（默认 1000）。

本地开发（需自有 PostgreSQL）：

```bash
pip install -r requirements.txt
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/diag \
  uvicorn app.main:app --port 8000
```

演示脚本（覆盖：顺序执行、会话清理验证、取消、慢取消发送/慢清理钩子、迟到取消、
历史容量压力下未结束单不消失）：

```bash
./scripts/demo.sh            # BASE=http://localhost:8000 可覆盖
```

## API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/jobs` | 提交 1–10 条 SQL（202，返回 `job_id`） |
| GET | `/jobs` | 最近的单（摘要列表） |
| GET | `/jobs/{id}` | 单详情 + 逐句结果（状态/列/行/rowcount/耗时/错误） |
| POST | `/jobs/{id}/cancel` | 取消未结束的单（202 受理；已结束返回 409） |
| GET | `/pool` | 两条连接的状态（idle/busy/cleaning/reconnecting、代数、清理失败数） |
| GET | `/health` | 存活检查 |
| GET | `/docs` | Swagger UI |

### 提交

```bash
curl -XPOST localhost:8000/jobs -H 'content-type: application/json' -d '{
  "statements": ["SET work_mem = '"'"'64MB'"'"'", "SELECT count(*) FROM pg_stat_activity"],
  "options": {"cleanup_delay_ms": 0, "break_connection_after": false}
}'
```

- 每条元素必须是**一条**语句（`;` 分隔的多句会被 422 拒绝；字符串/美元引号/
  注释里的 `;` 不算）。
- **拒绝显式事务控制**：`BEGIN/START/COMMIT/ROLLBACK/ABORT/END/SAVEPOINT/
  RELEASE`、`SET TRANSACTION`、`SET SESSION CHARACTERISTICS`、
  `PREPARE TRANSACTION`（含注释伪装的写法），返回 422。每条语句各自自动提交。
- 单语句错误 → 该句记 `error`，后续句记 `skipped`，单状态 `failed`。

### 取消语义

- **及时受理**：`POST /jobs/{id}/cancel` 立即置标志并返回 202；真正的取消发送
  由独立线程完成（可用 `{"delay_ms": N}` 模拟慢取消管道），不阻塞受理。
- **受理后**：正在执行的语句被 `conn.cancel()` 中断（PostgreSQL 标准取消），
  后续语句全部 `skipped`；**此前已提交的语句效果保留**（自动提交，无回滚）。
  极端时序下已发往服务器的语句可能自然完成，属正常现象。
- **排队中的单**：直接标记 `cancelled`，不会执行。
- **迟到取消**：单已结束时取消返回 409（无操作）；发送线程在发送前按连接校验
  "该单仍是此连接当前单且未结束"，因此**迟到的取消绝不会误停同一连接上的下一单**。
- **连接归还条件**：SQL 停止 + 取消发送完成 + 会话清理完成，三者齐备后连接才
  回到空闲（可用延迟钩子直接观测）；此时任务记录才被"释放"，此前它不会被
  历史容量淘汰，取消发送线程也一定能找到它（不会漏发、不会空等超时）。

### 记录保留与历史容量

- **未结束的单永不被淘汰**：排队中、执行中，以及 SQL 已有结论但取消发送或
  会话清理尚未完结的单，始终可通过 `GET /jobs/{id}` 查询、可取消，与
  `JOB_HISTORY` 无关，也不占用历史容量名额。
- **已完成的单按容量淘汰**：后台不再使用的完成记录最多保留 `JOB_HISTORY`
  条（默认 1000；compose 演示环境设为 2），超出时最旧的先被淘汰。
- **取消发送不漏发**：已受理取消的单在取消发送完成前不会被释放，因此发送
  线程按 `job_id` 查找时记录一定存在——不会漏发，worker 也不会空等
  `CANCEL_SEND_WAIT_S` 超时。

### 会话清理（每单结束后，成功/失败/取消都执行）

`RESET ALL`、`RESET ROLE`、`RESET SESSION AUTHORIZATION`、`DEALLOCATE ALL`、
`DISCARD SEQUENCES`、删除本会话全部临时表/视图/序列、
`pg_advisory_unlock_all()`。清理失败 → 关闭并淘汰该连接，原地补建新连接
（`/pool` 可见 `generation` 与 `cleanups_failed` 递增）。

## 测试钩子（可控停顿/故障注入）

| 钩子 | 位置 | 作用 |
|---|---|---|
| `options.cleanup_delay_ms` | 提交单时 | 清理阶段停顿 N 毫秒（模拟慢清理） |
| `{"delay_ms": N}` | 取消请求体 | 受理后延迟 N 毫秒再向 PG 发送取消（模拟慢取消发送） |
| `options.break_connection_after` | 提交单时 | 单结束后断开本连接（模拟连接故障），触发淘汰+补建 |
| `CANCEL_SEND_DELAY_MS` / `CLEANUP_DELAY_MS` | 环境变量 | 上述两个延迟的全局默认值 |

## 自测

```bash
pip install -r requirements-dev.txt
pytest tests/test_sqlguard.py tests/test_store.py   # 纯单元测试，无需数据库
# 集成测试需要运行中的服务与数据库：
API_BASE=http://localhost:8000 TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/diag \
  pytest tests/test_integration.py
```

集成测试覆盖：按序自动提交与逐句结果、行数截断、失败即停、参数校验、
会话状态不泄漏（SET/临时表/咨询锁/预备语句，含失败单）、取消运行中/排队中的单、
迟到取消（409 与延迟发送不误伤下一单）、慢 SQL/慢清理不阻塞另一连接、
清理失败淘汰并补建连接（钩子注入 + `pg_terminate_backend` 真实杀连接两种）、
历史容量压力下未结束单不消失（排队单可查可取消、取消发送待发的单不被清退）。

> 注：PostgreSQL 的语句取消依赖向后端进程投递 SIGINT。在禁止进程间信号的
> 沙箱里，"中断正在执行的语句"无法生效（`pg_terminate_backend` 同样无效），
> 相关测试用例设计为在两种环境下都通过：沙箱内验证协作式停止（语句间停止），
> Docker/正常 Linux 下同一代码路径会真正中断执行中的语句。

## 设计说明与限制

- 面向**可信 SQL**（值班 DBA）：守卫只强制执行契约（单句、禁显式事务），
  不是安全边界；`CALL`/`DO` 体内的事务控制无法静态识别，属约定范围。
- 任务注册表在内存中（重启即失）。保留策略：未结束（未释放）的单永不淘汰；
  已完成的单最多保留 `JOB_HISTORY` 条（默认 1000），超出淘汰最旧的。
- 结果行数上限 `ROW_LIMIT`（默认 200，超出截断并标记 `truncated`）。
- 单进程单实例：连接池在进程内，勿以多 worker 方式启动 uvicorn。
- 连接使用 `prepare_threshold=None`，避免 psycopg 的客户端预备语句缓存与
  清理中的 `DEALLOCATE ALL` 冲突。
