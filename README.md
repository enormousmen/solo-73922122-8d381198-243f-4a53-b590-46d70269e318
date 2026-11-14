# 气象站小文件入湖与后台合并服务

单进程湖仓维护 API（FastAPI + PyArrow + SQLite），把气象站持续入湖的零散
Parquet 小文件合并为单个 Parquet，降低下游分析的文件打开开销。

## 核心契约与保证

| 需求 | 实现方式 |
| --- | --- |
| 追加上传，每份 ≤ 8MiB，固定三列 | `POST /uploads` 按字节流式接收，超限立即 413；校验并规范化为 `station_id(string)` / `observed_at(timestamp[us, UTC])` / `temperature(float64)`，缺列、多列、空表、非 Parquet 一律 422 |
| 按上传顺序 + 文件内行序分页，保留重复行 | 全局 `read_order` 决定文件次序，游标在文件内按偏移顺序读，全程不去重 |
| 游标固定创建时已提交的行 | 创建游标时把当时所有 `active` 文件快照写入 `cursor_files`，之后新上传/合并产物都不会进入旧游标 |
| 后台把“开始时”的文件合成一个 Parquet，同期只合并一次 | 合并启动瞬间捕获输入清单（`merge_files`），进行中的合并不吞新文件；存在活动任务时再次启动返回 409 |
| 慢读期间上传、建游标、合并切换不被拖住 | 慢 Parquet 读盘在进程全局锁**之外**执行，全局锁只覆盖短时元数据事务；读盘期间任何元数据操作都能立即拿锁完成 |
| 同一游标并发翻页不重读/跳行 | 每个游标一把 FIFO 锁，并发翻页严格按请求顺序串行，每个请求只在自己持有的 `[rows_read, rows_read+limit)` 区间读盘并推进位置 |
| 完整结果才可切换，失败保留原数据 | 先做行数 + 完整重读 + 列结构校验，通过后在单个 SQLite 事务里原子切换；任何失败输入文件始终 `active`，tmp 自动清理 |
| 正在读的页不受切换/关闭/回收影响 | 读盘前把本页文件 pin 到进程内 pin 注册表；回收方在全局锁内同时校验“无未关闭游标 SQL 引用且无在读 pin”才删除；读盘结束后复检游标状态——已关闭则本页明确失败（404）且不推进进度，读盘异常返回 500 同样不推进 |
| 旧文件等游标读完或关闭后才回收 | 旧文件转 `obsolete`；回收 SQL 用 `NOT EXISTS` 校验没有任何未关闭游标引用，再叠加 pin 检查；慢读线程在阶段 3 释放 pin 后会补一次回收，实现“读者不再引用后即可回收” |
| 切换/回收中退出，重启后可恢复 | 启动恢复：清理 tmp 半成品；`switching` 任务（事务未提交）标记 failed 且原数据完好；`running/paused` 任务重起后台线程续跑；继续回收无引用旧文件；active 文件永不删除 |

数据与元数据全部落在挂载卷 `/data`：

```
/data
├── files/      # 正式 Parquet（上传文件与合并产物）
├── tmp/        # 上传/合并的临时文件（*.tmp，崩溃后启动清理）
└── meta/lakehouse.db   # SQLite（WAL 模式）：文件清单、合并任务、游标快照、读取位置
```

## 快速开始（Docker Compose）

```bash
docker compose up -d --build
# 交互式文档（可直接在页面上演示全部接口）
open http://localhost:8000/docs
```

健康检查：`GET /health` 返回启动恢复结果。

## API 一览

| 方法 & 路径 | 说明 |
| --- | --- |
| `POST /uploads` | 原始 Parquet 字节流上传（**二进制 body，不是 multipart**），≤8MiB |
| `GET  /files` | 文件清单（active/obsolete、行数、读取次序、合并产物） |
| `POST /cursors` | 创建游标，固定当前已提交行 |
| `GET  /cursors` | 全部游标及读取进度 |
| `GET  /cursors/{id}/pages?limit=N` | 顺序分页，返回 `rows / exhausted / rows_read / remaining` |
| `POST /cursors/{id}/close` | 显式关闭，可能立即触发旧文件回收 |
| `POST /merges` | 启动后台合并（202；已有活动任务时 409） |
| `GET  /merges/status` | 最近合并任务状态与每个输入文件的写入进度 |
| `POST /merges/pause` / `/resume` | 暂停/恢复合并（演示用） |
| `POST /merges/reclaim` | 立即回收无游标引用、且无在读 pin 的 obsolete 文件 |
| `POST /merges/fail-inject/{stream|switch}` | 演示用故障注入，验证失败保留原数据 |
| `GET  /debug/slow-read` | 查看当前慢读延迟与在读 pin 快照 |
| `POST /debug/slow-read` | 动态设置每次分页读盘前的额外延迟（秒，0 关闭），制造可控慢读窗口 |

## 慢读交错验证

```bash
./scripts/run_interleave_demo.sh
# 或本地/容器内直接运行（INTERLEAVE_SLOW_SECONDS 控制慢读秒数）
INTERLEAVE_SLOW_SECONDS=0.8 python scripts/interleave_demo.py
```

脚本通过 `/debug/slow-read` 注入确定性的读盘延迟，在“一页正在慢读”的
时间窗内依次验证：

1. 补传上传与新游标创建立即完成（耗时远小于慢读延迟），慢读页仍完整返回；
2. 同一游标 3 个并发读者（不同 page limit）翻页，按 FIFO 串行后各页区间
   首尾相接——无重读、无跳行，按读取位置重建的行序与顺序基线完全一致；
3. 慢读期间发起合并并完成切换：在途页完整返回，旧游标行序/行数不变，
   游标关闭前 obsolete 旧文件不回收；
4. 慢读期间关闭游标：在途页明确返回 404 且读取进度不推进，被 pin 的文件
   跳过回收，读者释放 pin 后 obsolete 文件最终全部回收。

也可直接用环境变量给全部分页读盘加固定延迟：`SLOW_READ_SECONDS=0.8`。

## 一键端到端演示

```bash
./scripts/run_demo.sh
```

脚本依次演示：

1. 构建启动服务，生成 6 份合成 Parquet（含故意构造的跨文件重复行）；
2. 上传 → 建游标 → 校验快照行数、重复行保留、跨文件边界分页；
3. 启动合并并**暂停**，暂停期间继续上传和翻页，再恢复完成切换；
4. 校验旧游标的行序/内容不变、新游标看到合并产物 + 新文件；
5. 关闭游标触发回收，确认 active 文件不被误删；
6. **切换前退出**：合并进行中 `kill -9` 容器，重启后原数据完好、任务续跑；
7. **切换后退出**：保留未关闭游标和 obsolete 文件后 `kill -9`，重启后游标继续读、
   继续回收、现用文件不误删；
8. 故障注入：stream / switch 两阶段失败均保留原数据，清除后合并成功。

### 不用 Docker 时本地运行

```bash
pip install -r requirements.txt
export DATA_DIR=/tmp/lakehouse
python scripts/generate_data.py --out "$DATA_DIR/synthetic" --files 6 --rows 200
uvicorn app.main:app --host 127.0.0.1 --port 8000 --workers 1
# 另开终端
DATA_DIR=$DATA_DIR python scripts/demo.py demo "$DATA_DIR"/synthetic/weather-*.parquet
DATA_DIR=$DATA_DIR python scripts/demo.py fail-demo
```

## 测试

```bash
pip install -r requirements.txt pytest
pytest            # 16 个用例：校验/分页边界/快照/合并/回收/故障/重启恢复
                  #          + 慢读交错（不阻塞/FIFO 分页/关闭与切换竞争/pin）
```

## 关键设计说明

- **慢读不占全局锁（本次修复核心）**：`app.db.LOCK` 只保护短时元数据事务；
  慢 Parquet 读盘 (`storage.read_rows`) 在锁外执行。分页请求分三阶段——
  ①锁内取快照并 pin 本页文件 → ②锁外慢读盘 → ③锁内复检游标状态、
  释放 pin、推进位置。因此慢读期间上传、新游标创建、合并切换都能立即完成。
- **游标级 FIFO 锁**：`app.concurrency.FifoLock` 保证同一游标的并发翻页
  严格按到达顺序串行；每个请求基于上一个请求持久化后的 `rows_read`
  读取自己的区间，从机制上杜绝重读与跳行。不同游标之间互不阻塞。
- **在读文件 pin + 双条件回收**：`app.concurrency.FilePinRegistry` 记录
  正在读盘的文件引用计数；`reclaim` 在全局锁内同时检查“无未关闭游标
  SQL 引用”与“无在读 pin”，两个条件都满足才删除文件，读盘与删除不可能
  竞争。读盘中关闭游标时，在途请求在阶段 3 复检到 `closed=1`，本页
  返回 404 且不推进位置，并在释放 pin 后补一次回收。
- **原子切换**：合并产物先写 `tmp/` 并通过完整性校验，再 `os.replace` 落位，
  最后在一个 SQLite 事务里插入新产物 + 旧文件置 obsolete；事务失败自动回滚并退回产物。
- **崩溃安全**：所有进度（合并输入、游标位置、文件状态）在每页/每输入文件处理后
  立即提交；重启时按状态机恢复，不依赖进程内存。pin 是纯进程内状态，
  进程退出即消失，重启恢复的回收逻辑不受其影响。
- **合并产物继承最早输入的读取位置**，新上传文件 `read_order` 恒在其后，
  保证切换前后全局行序对任何游标都稳定。
