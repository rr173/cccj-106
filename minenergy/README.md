# minenergy — 工业采样 → 分钟能耗事实表

把“不规则到达的累计电表读数”加工成可分析的 **分钟能耗事实表**（Polars + Parquet），
同时提供 **全量** 与 **增量** 两个入口，增量只重算真正受影响的相邻采样区间，
并替换对应的分钟分区。

- 语言/库：Python ≥3.11、Polars、Parquet（zstd）
- 时间统一存为 **UTC 微秒整数**（`*_us`），能耗为 float64 Wh，覆盖时长为整数微秒，
  因此“每分钟不超过 60 秒覆盖”可以被精确断言。

---

## 1. 目录

```
minenergy/
├── pyproject.toml
├── minenergy/            # 库与 CLI
│   ├── schema.py         # 所有表结构/常量/原因码
│   ├── canonical.py      # 原始样本与倍率配置的去重、修订、冲突
│   ├── intervals.py      # 相邻样本 → 可积分区间 + 倍率分段
│   ├── minutes.py        # 区间 → 分钟事实（跨分钟分摊、无效秒数、日历字段）
│   ├── engine.py         # full_run / incremental_run
│   ├── storage.py        # 原子写 Parquet 分区 + 跨文件事务/恢复
│   ├── settings.py       # 表计时区 + 空档阈值
│   ├── reference.py      # 独立纯 Python 参考实现（精确有理数）
│   ├── verify.py         # 差分对比与不变式检查、手写样例、随机场景
│   ├── cli.py            # full / incremental / verify / show
│   ├── generate.py       # 生成乱序多批输入
│   ├── ingest.py         # 整目录分批灌入（首批 full，其后 incremental）
│   ├── compare.py        # 增量 vs 全量 vs 参考 三段对照
│   ├── flight.py         # Arrow Flight 服务端（按需扫描事实表、批式 DoGet）
│   ├── flight_server.py  # 独立前台进程入口（连接现有 store）
│   ├── flight_client.py  # 演示客户端（schema 发现 / 查询 / DoGet / stats）
│   ├── flight_demo_data.py # 跨多分区/行组/批次的大规模演示数据
│   └── comtrade/         # IEEE C37.111-1999 故障录波接入（见第 10 节）
│       ├── config.py     # CFG 解析/校验、模拟/数字通道、分段采样率、timemult
│       ├── clock.py      # 时间戳/分段重建、DST 歧义与缺失判定
│       ├── frames.py     # ASCII/BINARY 流式帧解码（有界内存）
│       ├── recording.py  # 物理换算、skew、数字解包、窗口/投影/Arrow 批
│       ├── export.py     # 先检查后暂存的 Parquet 发布与溯源元数据
│       ├── fixtures.py   # 无外部下载的录波/损坏/大文件夹具
│       ├── errors.py     # 带文件与帧/样本位置的错误类型
│       └── cli.py        # catalog / inspect / export / make-fixtures
├── samples/
│   ├── fixtures/         # 6 个命名小样例（生成产物 + settings）
│   └── randomized/       # 4 批乱序随机样例（生成产物 + settings）
└── tests/                # pytest：样例精确值 + 增量/全量/参考对照 + 属性
```

---

## 2. 规则（与需求逐条对应）

| 需求 | 实现 |
|---|---|
| 同表计同事件时刻只保留 `ingest_seq` 最大 | `canonical.py`：按 `(meter_id, event_time)` 取 `max(ingest_seq)`；`revision_count` 记录该时刻共有几个修订 |
| 同序号不同载荷报冲突 | 同 `ingest_seq` 出现 >1 个 `(counter_wh, boot_id)` → 写入 `canonical/sample_conflicts.parquet`；若冲突发生在**保留的**最大序号，样本 `conflicted=true` 且区间判无效 |
| 相邻有效样本同一 `boot_id`、计数不降、间隔 ≤ 阈值 | `intervals.py`：三个条件任一不满足 → `boot_id_change` / `counter_reset` / `gap_exceeded`，整段覆盖记无效秒数，**不推测用量** |
| 增量按实际经过秒数均匀分摊到所跨 UTC 分钟 | `minutes.py`：区间按 `[start,end)` 切 UTC 分钟，每片 `delta_wh * 秒数 / 总秒数` |
| 倍率变化点切段 | `intervals.py`：区间内每个 `effective_from` 把区间切成 segment，每段用自己的倍率；`weighted_multiplier = Σ m_i·dur_i / total` |
| 单个孤立样本不贡献持续时长 | 区间由相邻两个样本定义；孤立样本没有相邻样本 → 不产生区间（不写分钟行） |
| 分钟结果含 energy、有效秒、无效秒、质量原因 | `energy_wh, valid_seconds, invalid_seconds, valid_us, invalid_us, quality_reasons, invalid_by_reason, coverage_status, is_zero_usage …` |
| 零用量与无覆盖可区分 | `coverage_status ∈ {full_valid, partial_valid, invalid_only, mixed}`；`is_zero_usage = 有有效覆盖且 energy==0`；**完全没有覆盖的分钟不出现在事实表**（不会被填 0） |
| 按表计 IANA 时区派生日历日期与本地分钟标签 | `local_date/localtime/local_minute_label/utc_offset_seconds` 由 `zoneinfo` 逐表计 `convert_time_zone` |
| DST 回拨两个同名分钟用偏移区分 | `local_minute_label = 2025-11-02T01:30-0400 / …-0500`（`local_minute` 允许重复，标签唯一） |
| 迟到样本/倍率更正只重算受影响区间并替换分钟分区 | `engine.incremental_run`：受影响窗口 = 变更时刻的 `[前驱, 后继]` 并集；配置变更额外覆盖 `[变更点前的样本, 下一配置点]`；只重算窗口内的区间，splice 回区间表，重写窗口触及的 `(meter_id, timezone, local_date)` 分区；`affected/<run_id>/intervals.parquet` + `minutes.parquet` 留存受影响清单 |
| 原始样本与配置可落盘，退出再跑仍能识别修订 | 原始日志在 `raw/samples.parquet`、`raw/configs.parquet`；**每次运行都从完整原始日志重新 canonicalize**，因此进程重启、迟到修订都能识别 |
| 多文件作业要么整体提交、要么整体回滚，中断后自动补完/撤销 | `storage.Transaction` + `recover_pending()`：全部产物先暂存，manifest 为完成边界，`COMMITTED` 为提交点；重跑以最后一次已提交 canonical 为基线，因此中断作业不会被误判 no-op（README 4.1） |
| 重复启动/重传同一批不跳过待处理、不累加第二遍 | raw 追加按整行去重；已提交作业重跑是真正的 no-op；未提交作业在入口被恢复后再判定 |
| 全量 + 增量两种入口 | `minenergy.cli full` / `minenergy.cli incremental`；另有 `ingest` 整目录分批 |
| 随机多轮乱序输入 + 独立全量算法对照 | `tests/test_incremental.py`、`minenergy.compare`、`minenergy.cli verify`（`reference.py` 是不掩 bit 的纯 Python 精确有理数实现） |
| 能量守恒、分钟覆盖 ≤60s、无采样分钟不被填零 | `verify.check_conservation`（全局）、`check_interval_conservation`（逐区间重切分钟）、`check_invariants`（覆盖 ≤60s、无零覆盖行、零用量需有有效覆盖） |

### 原因码

| 原因 | 判定 | 是否使区间不可积分 |
|---|---|---|
| `boot_id_change` | 两端 `boot_id` 不同 | 是 |
| `counter_reset` | `Δcounter < 0` | 是 |
| `gap_exceeded` | `end-start > gap_threshold_seconds` | 是 |
| `ingest_conflict` | 任一端点是“保留序号冲突” | 是 |
| `config_conflict` | 区间内有冲突倍率配置 | 是 |
| `multiplier_defaulted` | 区间起点之前没有任何配置 → 用 1.0 | 否（仅质量提示） |

> 有效区间在 `multiplier_defaulted` 下仍按 1.0 积分，同时 `quality_reasons` 里带该提示。

---

## 3. 精确小样例（可手算核对）

### 3.1 跨分钟分摊（`cross_minute`）

输入（`M1`，UTC，阈值 3600s）：

```
2025-01-01T00:00:00Z seq1 counter=0.0  B1
2025-01-01T00:02:30Z seq2 counter=5.0  B1     ← 同刻 seq2 胜出
2025-01-01T00:02:30Z seq1 counter=5.0  B1     ← 被同刻修订淘汰
```

区间 `[00:00:00, 00:02:30)`，Δ=5 Wh，150 s：

| minute (UTC) | energy_wh | valid_seconds | invalid_us | coverage_status |
|---|---|---|---|---|
| 00:00 | 2.0 | 60 | 0 | full_valid |
| 00:01 | 2.0 | 60 | 0 | full_valid |
| 00:02 | 1.0 | 30 | 0 | partial_valid |

`2 + 2 + 1 = 5 = Δ`，没有 00:03 行（无覆盖，不生成）。

### 3.2 复位（`reset`）

```
00:00 counter=100  B1
00:02 counter=130  B1     区间 [00:00,00:02) Δ=30   → 30 Wh，2 分钟有效
00:04 counter=0    B1     区间 [00:02,00:04) Δ=-130 → counter_reset
00:06 counter=30   B1     区间 [00:04,00:06) Δ=30   → 30 Wh
```

分钟 00:02、00:03 为 `invalid_only`、`energy_wh=0`、`invalid_us=60s`、
`quality_reasons=["counter_reset"]`、`is_zero_usage=false`。
总能量 60 Wh（不是 “猜出 30 Wh 后按时间摊”。

### 3.3 倒退 / 长空档（`long_gap`，阈值 300s）

```
00:00 c=10, 00:01 c=20, 00:10 c=25
```

`00:00–00:01`（60s）合法 → 10 Wh；`00:01–00:10`（540s）> 300s →
`gap_exceeded`，分钟 00:01…00:09 全部 `invalid_only`，共 540 无效秒，能量 0。

### 3.4 同刻修订（`same_instant_revision`）

```
00:00 seq1 c=0
00:01 seq1 c=10
00:01 seq2 c=14   ← 同刻更大序号胜出，00:01 的有效取值 = 14
00:02 seq1 c=14
```

结果：00:00 → 14 Wh，00:01 → 0 Wh（`is_zero_usage=true`，有 60s 有效覆盖但无增量）。

### 3.5 倍率中途切换（`mid_interval_multiplier`）

```
样本: 00:00 c=0, 00:03 c=30          区间 Δ=30 Wh / 180 s
配置: 12-31 00:00 m=1.0
      01-01 00:01 m=2.0              → 切段 [00:00,00:01) m=1，[00:01,00:03) m=2
```

- 00:00：30×(60/180)×1 = 10
- 00:01：30×(60/180)×2 = 20
- 00:02：30×(60/180)×2 = 20 → 合计 50，与 `weighted_multiplier=1.6666…` 的区间能量一致。

### 3.6 夏令时回拨（`dst_fallback`，`America/New_York`，阈值 7200s）

```
05:00Z → 01:00 EDT    06:00Z → 01:00 EST    07:00Z → 02:00 EST
样本 05:00Z c=0, 06:00Z c=60, 07:00Z c=120
```

分钟事实中同时出现：

```
local_minute = 01:30 （两行）
local_minute_label = 2025-11-02T01:30-0400   (utc_offset_seconds=-14400)
local_minute_label = 2025-11-02T01:30-0500   (utc_offset_seconds=-18000)
```

即：同名墙上分钟用偏移区分，UTC 分钟键唯一且连续。

---

## 4. 存储布局

```
<store>/
  raw/samples.parquet                 # 原始样本（append-only 逻辑日志）
  raw/configs.parquet
  canonical/samples.parquet           # 去重+修订后的唯一样本
  canonical/configs.parquet
  canonical/sample_conflicts.parquet  # 同序号冲突明细
  canonical/config_conflicts.parquet
  intervals/meter_id=<id>/data.parquet
  facts/meter_id=<id>/timezone=<tz%2F>/local_date=<YYYY-MM-DD>/data.parquet
  affected/<run_id>/{intervals,minutes}.parquet + run.json
  state/settings.json                 # 上次运行的时区/阈值指纹
  state/txn/<txid>/                   # 仅在作业进行中存在（见 4.1）
```

所有单文件写操作都是“临时文件 + `os.replace`”的原子替换；分钟区按
`(meter_id, timezone, local_date)` 分片，因此增量替换只触碰受影响日片。
时区名路径按 URL 编码（`America%2FNew_York`），避免 `/` 破坏 hive 目录。

### 4.1 跨文件提交协议

单文件原子写**不等于**一次作业原子：一次增量会依次覆盖 raw、canonical、
多个 `intervals/meter_id=…`、多个分钟分区、`state/settings.json`、
`affected/<run_id>/` 与 `run.json`。旧实现先落 canonical、再与磁盘上的
canonical 比较来决定“受影响范围”，所以进程若在 canonical 落盘后、区间/事实
写盘前退出，下一次启动会把“尚未计算的新版本”当成“已经算过的版本”，整轮作业
被判成 no-op，留下旧事实。

现在的提交协议（`storage.py`）：

1. **暂存（staging）**：作业开始时创建 `state/txn/<txid>/`，所有写入/删除都
   先写到 `staged/` 并登记在内存操作表；读取 raw/canonical 时能看到本作业自己
   的暂存版本，读取 intervals/facts 分区树时仍看**已提交基线**。
2. **完成边界（manifest）**：作业的全部产物（含 `run.json`）都暂存完毕后，
   `prepare()` 原子写出 `manifest.json`，列出每个目标路径与删除项。
   它的存在即“工作已全部就绪”的判据。
3. **提交点（COMMITTED）**：`commit()` 在文件锁内按 manifest 先移动所有暂存
   文件、再执行删除，最后原子写 `COMMITTED` 标记。这个标记是整轮作业**唯一**
   的提交点；之前任何时刻退出，磁盘上的有效状态都仍是上一次提交的完整状态。
4. **恢复（recover）**：每个入口在读写前调用 `recover_pending()`：
   * 有 `COMMITTED` 标记 → 作业已完成，只清理残留目录（**不重放** manifest，
     否则会覆盖后续作业写下的同名文件）；
   * 有 manifest、无标记 → 作业死在发布阶段，**向前重放** manifest
     （暂存文件已移动的跳过，删除幂等），补写标记后清理；
   * 无 manifest → 作业死在暂存阶段，未触碰任何正式路径，直接丢弃。
   恢复过程再次被杀掉，下一次入口仍按同样判据继续，可反复恢复。

**恢复判据一句话**：manifest 是“全部产物已暂存”的原子边界，COMMITTED 是
“全部产物已发布”的原子边界；两者之间是可安全重放的发布阶段，两者之外都
按“未提交→丢弃”或“已提交→清理”处理。因此重跑原命令时比较基线永远是上一
次已提交的 canonical，不会把崩溃写下的中间 canonical 误认成“已完成”。

**失败清理**：崩溃后残留的只有 `state/txn/<txid>/`（私有目录，读路径永不
扫描它）。下一次任意入口的 `recover` 会依据上述判据补完或删除；正常异常
（Python 异常）在 manifest 之前直接丢弃暂存目录，manifest 之后留给恢复流程
补完。`raw` 追加按整行内容去重，重发同一批不会累加。

**旧目录迁入**：旧版本写出的目录没有 `state/txn/`，无需转换——其现有文件
就是第一次事务的“已提交基线”。第一次运行新版本的作业会照常比较并提交；
若该目录在旧版本下写到了 canonical 但没写完事实，新版本会把 canonical 的
差异当作待处理变更并重算受影响范围（因为比较对象是磁盘上旧的 canonical，
而不是新代码某次中间写下的结果）。测试
`test_legacy_store_without_txn_directory_is_usable` 覆盖了这一点。

**并发**：`recover` 与 `commit` 在同一把 `state/.txn.lock` 文件锁下进行，因此
恢复只会看到已死进程留下的目录，不会去动另一个进程正在发布的事务。同一
store 上仍不建议两个**同时开始**的作业并发写；多作业串行时恢复会先补完前一
个目的 manifest，后一个再以补完后的状态为基线。

### 4.2 故障回归

```bash
./regression_crash_recovery.sh /tmp/minenergy-crash-regression
```

用两只电表、跨本地日边界的区间构造一轮会触及多个 interval 文件和多个分钟
分区的作业，在**真实子进程**中以 `os._exit(77)` 在下列断点退出：raw 暂存后、
canonical 后、首个 interval 前后、首个事实分区前后、变更说明前后、`run.json`
前后、manifest 前后、发布过程中移动 1/2/3 个文件后、发布完成但无标记、提交
完成后。每行打印该断点的真实退出码、恢复状态、是否落出 `run.json`、重跑是否
为 no-op，以及整轮数据/变更说明是否与无中断的干净运行一致；发布阶段还会
“恢复途中再退出一次再恢复”。退出码 0 表示所有断点均收敛。

---

## 5. 命令

安装（本仓库已在 `/workspace/.venv` 装好 polars/pyarrow/pytest）：

```bash
pip install -e /workspace/minenergy
```

### 5.1 生成

```bash
# 4 批乱序随机输入（含同刻修订、同序号冲突、boot 变化、倒退、长空档、倍率网格）
python -m minenergy.generate --out samples/randomized --scenario randomized \
       --seed 7 --batches 4 --meters 3 --points 40

# 6 个命名小样例（上面 3.1–3.6）
python -m minenergy.generate --out samples/fixtures --scenario all --seed 7
```

每个输出目录含 `settings.json`、`batches/batch_XX.parquet`、`configs_all.parquet`。

### 5.2 全量 + 增量

```bash
# 首批：全量（可顺带灌入首批原始数据与配置）
python -m minenergy.cli full \
  --store /tmp/demo_store --settings samples/randomized/settings.json \
  --samples samples/randomized/batches/batch_00.parquet \
  --configs samples/randomized/configs_all.parquet

# 后续批次：增量（可一次给多个文件）
python -m minenergy.cli incremental \
  --store /tmp/demo_store --settings samples/randomized/settings.json \
  --samples samples/randomized/batches/batch_01.parquet \
            samples/randomized/batches/batch_02.parquet

# 或一步到位：整目录分批（首批 full，其余 incremental）
python -m minenergy.ingest \
  --store /tmp/demo_store --settings samples/randomized/settings.json \
  --batch-dir samples/randomized/batches \
  --configs samples/randomized/configs_all.parquet --fresh
```

`incremental` 会打印本次受影响的区间清单（`change_kind`, `changed_fields`,
`previous/expected_energy_wh`…）与受影响分钟分区；完整清单落在
`<store>/affected/<run_id>/intervals.parquet` 与 `minutes.parquet`，也可 `--export-report DIR` 另存。

### 5.3 验收 / 对照

```bash
# 独立参考实现逐字段对照 + 不变式（60s 上限、无覆盖分钟不被填零、逐区间守恒）
python -m minenergy.cli verify \
  --store /tmp/demo_store --settings samples/randomized/settings.json

# 三段对照：增量结果 vs 同原始日志的全量重算 vs 独立参考实现
python -m minenergy.compare --store /tmp/demo_compare \
  --settings samples/randomized/settings.json \
  --configs samples/randomized/configs_all.parquet \
  --batches samples/randomized/batches/*.parquet

# pytest：6 个样例精确值 + 增量/全量/参考差分 + 属性（多随机种子）
#  + 跨文件提交协议的多断点崩溃恢复（真实子进程 os._exit(77)）
python -m pytest tests -q

# 故障回归：逐断点打印真实退出码与恢复后的核对结果（README 4.2）
./regression_crash_recovery.sh /tmp/minenergy-crash-regression

# 或一条命令跑完整个验收流程（生成 → full → incremental → verify → compare → 6 样例 → pytest）
./demo.sh /tmp/minenergy-demo
```

> 未安装包时，在仓库根目录加 `PYTHONPATH=.` 即可运行全部 `python -m ...` 命令。

### 5.4 查看

```bash
python -m minenergy.cli show --store /tmp/demo_store --table samples
python -m minenergy.cli show --store /tmp/demo_store --table conflicts
python -m minenergy.cli show --store /tmp/demo_store --table intervals
python -m minenergy.cli show --store /tmp/demo_store --table facts --meter M0
python -m minenergy.cli show --store /tmp/demo_store --table affected --run-id <id>
python -m minenergy.cli show --store /tmp/demo_store --table runs
```

---

## 6. 事实表字段

| 字段 | 含义 |
|---|---|
| `meter_id`, `timezone`, `local_date` | 表计、该表计 IANA 时区、本地日历日 |
| `minute_epoch_us` | UTC 分钟起点（微秒） |
| `minute_start_utc` | 同上，`Datetime[us, UTC]` |
| `local_minute` | `2025-11-02T01:30`（DST 回拨时会重复） |
| `local_minute_label` | `2025-11-02T01:30-0400`（含偏移，唯一） |
| `utc_offset_seconds` | 该分钟生效的 UTC 偏移 |
| `energy_wh` | 该分钟分摊能量（仅有效覆盖；无效覆盖写 0） |
| `valid_seconds` / `valid_us` | 有效覆盖秒数 / 微秒 |
| `invalid_seconds` / `invalid_us` | 无效覆盖秒数 / 微秒（**区间级并集，不会重复计数**） |
| `coverage_us` | `valid_us + invalid_us`，恒 ≤ 60 000 000 |
| `coverage_status` | `full_valid` / `partial_valid` / `invalid_only` / `mixed` |
| `quality_reasons` | 去重排序的字符串原因列表 |
| `invalid_by_reason` | 每个原因贡献的无效微秒（同一秒可归属多个原因，因此各原因时长不可相加） |
| `has_quality_issue`, `is_zero_usage`, `interval_count` | 便捷布尔/计数 |

区间表（`intervals/*`）还含：`segments`（倍率分段列表）、
`weighted_multiplier`、`expected_energy_wh`、`reason_codes`、`is_valid`、
`prev/next_counter_wh`、`first_minute_us/last_minute_us`、`content_hash`（用于精确判定“是否真的变了”）。

---

## 7. 增量算法

1. 入口先 `recover` 上一次中断的作业（见 4.1），再追加新原始数据；从**完整原始日志**
   重建 canonical 样本/配置（因此进程重启仍识别修订）。
2. 与磁盘上**上次已提交**的 canonical 逐 `(meter_id, event_time_us)` 比较签名，得到
   变更时刻集合（新增、删除、载荷/序号/冲突位变化）。本作业新写的 canonical 只在
   暂存区，不参与基线比较。
3. 受影响窗口：每个变更时刻取 `[前驱, 后继]`（在旧∪新时刻集合内），合并相邻窗口。
   配置变更额外给出 `[变更点前的样本, 下一配置点前/后]` 的窗口。
4. 只在这些窗口内重跑 `derive_intervals`，并把结果 splice 回每表计的区间文件。
5. 受影响分钟分区 = 旧受影响区间 ∪ 新区间触及的 `(meter_id, timezone, local_date)`；
   只重算并重写这些分区（仅用该日相关的区间切片），其余分区保持不动。
6. 受影响区间清单与分钟清单写 `affected/<run_id>/`；`run.json` 记录窗口、变更时刻数、
   重算区间数、写/删分区数、耗时。
7. 以上全部产物都在同一事务中暂存，`prepare` 后由 `commit` 一次性发布；只有
   `COMMITTED` 标记出现后新进程才会把这一轮视为已完成。

> 缓冲区只取“真正相邻”的一个前驱/后继：窗口外的区间与本次变更完全无关，因此不重算。
> 正常作业仍走局部重算路径：事务只改变“何时可见”，不改变“算多少”。

---

## 8. 独立参考实现

`minenergy/reference.py` 不使用任何管道内代码（除输入校验语义）：
纯 Python 字典 + `fractions.Fraction` 精确累加，逐分钟、逐区间、逐段手写拆分；
`zoneinfo` 逐分钟换算本地时间。`verify` / `compare` / pytest 都拿它当“独立全量算法”。

`check_*` 覆盖：

- 全局能量：分钟之和 == 有效区间之和；
- 逐区间：按分钟重切后的能量之和 == 区间 `expected_energy_wh`，且分段覆盖恰好铺满整段；
- 每个有效区间：`expected_energy_wh == delta_wh × weighted_multiplier`；
- 无效区间总时长 == 事实表 `invalid_us` 总和（无效不丢秒）；
- 每表计每分钟 `coverage_us ≤ 60 000 000`、无零覆盖行、`is_zero_usage ⇒ valid_us>0`；
- 分钟键集合与参考实现完全一致（**没有采样的分钟不会被填成 0**）。

---

## 9. Arrow Flight 事实数据服务

分析团队可以**直接从另一台机器**消费事实数据，不需要复制 store 目录，也不需要
解析终端表格：服务把已提交的 `facts/` 分区树作为只读、按需扫描的数据集暴露为
Arrow Flight。它只做传输，不新增/修改任何能耗指标（分钟事实仍由 `minutes.py`
按原规则生成）。

### 9.1 启动（独立前台进程）

```bash
# 对现有 store 起服务（只读；同一 store 上的 full / incremental 照常可写）
python -m minenergy.flight_server --store /tmp/demo_store --host 127.0.0.1 --port 8815
#   端口 0（默认）由系统分配；进程启动后打印一行 JSON：
#   {"status":"ready","store":...,"host":...,"port":...,"location":"grpc://..."}
```

`--store` 必须是已存在的事实 store（有 `facts/` 树）。服务是只读的：发布方
（`minenergy.cli full|incremental|ingest`）与它可同时运行。

### 9.2 查询参数

```json
{
  "meters": ["M0", "M2"],
  "start": "2025-01-01T00:00:00Z",
  "end":   "2025-01-02T00:00:00+08:00",
  "select": ["meter_id", "minute_start_utc", "energy_wh"],
  "batch_size": 8192
}
```

| 键 | 含义 | 默认值 / 上限 |
|---|---|---|
| `meters` | 表号数组，**必须非空**，不能“串流所有表号”；每个表号必须已有 `meter_id=` 分区 | 必填；≤256 个，不得重复 |
| `start` | 对事实行的 **UTC 分钟起点**（`minute_epoch_us`）的闭区间下界；**必须带偏移**（`Z` / `+hh:mm` / `+hhmm`） | 必填 |
| `end` | 同一键的开区间上界；同样必须带偏移 | 必填；必须 `> start` |
| `select` | 需要的列名数组，**按请求顺序返回**并保持存储 Arrow 类型；不含 `start`/`end`/`meters` 之外任何隐式列 | `null`/省略 = 全部 20 列，按存储顺序；≤20 列，不得重复 |
| `batch_size` | 每个 `RecordBatch` 的目标行数 | `8192`；允许 `1..65536` |

不支持未列出的键。`meters` 是**显式列表**，没有“全表号”通配模式。

### 9.3 RPC 用法

| RPC | 用法 |
|---|---|
| `GetSchema` | `FlightDescriptor.for_path("facts")` → 完整事实 schema；`for_command(query)` → 该查询投影后的 schema |
| `GetFlightInfo` | 命令描述符 = 查询 JSON：**先校验参数**，再返回 `FlightInfo`（投影 schema、单一 ticket endpoint、`app_metadata` 含 `query_id`） |
| `DoGet` | ticket = 查询 JSON（`GetFlightInfo` 会给它加上同一个 `query_id`）→ `RecordBatch` 流；每批 `app_metadata` 带 `query_id` / `batch_index` |
| `DoAction` | `minenergy.stats` → 计数器与最近查询记录（扫描文件数、行组取舍、行/字节、峰值内存、当前打开文件与活跃查询）；`minenergy.health` → 存活信息 |
| `ListFlights` | 返回一张 `facts` flight 供发现 |

**空结果也带正确 schema**：`GeneratorStream` 在传输开始前就声明投影 schema，
所以 0 行结果仍能拿到列名与类型（`read_all()` 得到 0 行、schema 与 `FlightInfo` 一致）。

**参数错误在传输开始前被拒绝**：未知列、重复列、倒置/相等区间、无偏移时间、
越界 `batch_size`、未知表号、未知键、空 `meters` 全部在 `GetFlightInfo`/`DoGet`
入口校验，失败 RPC 消息以 `MINENERGY_PARAM_INVALID:` 开头并带
`{"code": ...}`，`DoGet` 侧呈现为 gRPC `INVALID_ARGUMENT`。扫描器在参数通过后
才创建，因此不存在“传到一半才报错”。

### 9.4 扫描、内存与一致性

- **先裁剪再读**：先按 `meter_id=` 分区裁剪（无关表号的目录根本不打开），再用
  parquet 行组 `minute_epoch_us` 统计量裁剪时间范围，只读取选中的行组；然后
  在 int64 过滤键上做一次 `[start,end)` 过滤、按请求顺序投影。为了满足小列投影，
  内部会额外读取过滤键，但它**不会出现在返回列**中。
- **不做“先全读进 Polars/Pandas 再切块”**：逐行组、逐批生成 `RecordBatch`，
  内存占用约等于一个批次 + 一个行组的投影列，不随总结果线性增长（见 9.6）。
- **一次流 = 完整一轮数据，准入边界先于一切目录判断**：每个请求（`GetFlightInfo`
  与 `DoGet` 都一样）先调用 `admit_read` 跨过**准入边界**，再在边界内做表号判断、
  列目录与扫描。准入的依据是提交协议本身的不变量——事实文件只有在
  `Transaction.commit`/`apply_manifest` 持有 `state/.txn.lock` **排他锁**期间才会
  从暂存区搬进正式树：
  1. 先做无锁预检 `settle_store`：明显空闲的 store 不取任何锁；遇到**存活的已
     prepare 发布者**就等它结束，遇到**已死的已 prepare 事务**直接按既有协议接管；
  2. 然后对同一把 `state/.txn.lock` 取**共享读锁**，并**在持锁期间重新判定**事务
     目录。预检与取锁之间若有写方搬入了第一个替换分区（两代事实的故障窗口），
     持锁后的复检必然看到那个 prepared-but-unmarked 事务；
  3. 持锁下没有 prepared-but-unmarked 事务 → 冻结的树就是一轮完整提交，立即放行，
     读许可持续到本次传输结束；有死事务 → **先释放共享锁**，让 `recover_pending`
     在无锁栈帧上取排他锁完成接管，再重新取共享锁复检；有存活发布者 → 释放后
     有界等待其完成再复检。
  恢复需要排他锁、常规读取需要共享锁，因此**绝不在共享锁上原地升级**（多读者并存
  时升级会互锁）：恢复永远发生在释放之后的新栈帧上。表号（UNKNOWN_METER）判断
  使用准入后的同一状态，所以暂存在死事务里的新表号会先被接管、再被承认；准入前
  的目录永远不作为拒绝依据。遇到仍在分析、尚未触碰已提交树的写方可以直接读上一轮
  完整数据（该写方提交时会被共享读锁挡住）。
- **取消/断网即释放**：客户端取消、断开或服务关机都会关闭生成器；其 `finally`
  关闭所有 parquet 句柄并释放共享读锁（统计接口的 `open_parquet_files` /
  `active_queries` 归零），等待中的发布随即可以提交。没有后台任务残留。
- **下一条查询看到更新**：不缓存文件或结果；每次查询重新列目录并重新裁剪，
  因此随后完成的发布对新查询可见。不要求持久快照、断点续传或离线副本。

### 9.5 客户端示例

```bash
# schema 发现
python -m minenergy.flight_client --uri grpc://127.0.0.1:8815 --schema

# 窄时间段 + 列投影，逐批消费 DoGet
python -m minenergy.flight_client --uri grpc://127.0.0.1:8815 \
  --meters M0 --start 2024-01-02T03:00:00Z --end 2024-01-02T04:00:00Z \
  --select meter_id minute_start_utc energy_wh valid_seconds --batch-size 20

# 机器可读（含 batch 元数据与可选服务端统计）
python -m minenergy.flight_client --uri grpc://127.0.0.1:8815 \
  --meters M0 --start 2024-01-01T00:00:00Z --end 2024-01-05T00:00:00Z --json --stats
```

`minenergy.flight_client` 同时提供 `connect / discover_schema / run_query /
query_records / server_stats / expect_param_error`，验收脚本与测试复用它，
与人工使用走完全相同的 RPC 路径。

### 9.6 一条命令跑完 Flight 演示与验收

```bash
./flight_demo.sh /tmp/minenergy-flight-demo
```

脚本会：用 `minenergy.flight_demo_data` 造一份**跨多分区、行组、批次**的真实
store（6 只表计、4 个时区、21 天 → 130+ 分钟分区、760+ 行组、18 万分钟事实）；
在真实端口启动 `minenergy.flight_server`；用真实 Flight 客户端演示 schema 发现、
窄范围列投影、空结果 schema、非法参数 `INVALID_ARGUMENT`、两个消费者不同表号
并发，并打印服务端的实际扫描观测（被裁掉的行组/文件、批大小、峰值内存）。

`python -m pytest tests/test_flight.py -q` 覆盖以下**真实端口、真实客户端**的验收点：

- schema 与存储 parquet 完全一致；投影 schema 与返回类型/列序一致；
- 半开区间按 UTC 分钟起点解释（含秒级边界、带偏移时间串）；
- 空结果的 schema 正确；
- 未知列 / 重复列 / 倒置区间 / 无偏移时间 / 过大 `batch_size` / 未知表号 /
  未知键均以带 `MINENERGY_PARAM_INVALID` 与 `INVALID_ARGUMENT` 的 RPC 错误拒绝，
  且失败请求不会产生任何流记录；
- 两个消费者请求不同表号时数据严格隔离；
- 慢消费者流中途取消后：另一个查询完成、等待中的写入完成、句柄与读锁归零；
- 用同步点安排“流已打开”与“发布开始”：发布被挡住直到流结束，同一流内数据不
  混版，随后新查询读到更新；
- 崩溃留下的 prepared-but-unmarked 发布在查询前被既有协议补完（只补一次）；
- 大目录扫描只触及相关分区与行组，全程投影/宽投影的峰值内存不随总结果量线性增长。

**请求准入边界故障回归**（真实 socket、真实 prepare/commit 搬移、真实子进程退出码）：

```bash
./regression_admission_boundary.sh /tmp/minenergy-admission-regression
# 或直接：python -m pytest tests/admission -q
```

`tests/admission/` 把两张故障单和同一条调用链上的衍生场景全部落成可执行用例：

- **A（两代事实混入一次响应）**：用 `sitecustomize` 注入的纯时序屏障把请求线程停在
  “`settle_store` 已返回、`FlightReadHold` 尚未建立”的精确窗口；另一个写进程走真实
  `prepare/commit` 搬移，在**首个事实分区替换落地后**以 `os._exit(77)` 死亡，留下
  prepared-but-unmarked 事务。屏障只安排时序、不伪造任何扫描结果；修复前消费者从
  socket 收到跨日期的 10 与 20 两批值，修复后只收到接管后的单代值（每日 20 Wh）。
- **B（新表号被误判 UNKNOWN_METER 且事务无人接管）**：一只新表的完整事务被暂存并
  prepare，随后结束所有者、事实尚未进入正式位置；第一次索取该表号必须先接管再承认，
  而真正未知的表号仍以 `MINENERGY_PARAM_INVALID`/`UNKNOWN_METER` 拒绝。
- **衍生回归**：所有者在读者等待期间**正常退出**、所有者在等待期间**死亡**、
  **接管动作自身被杀**（exit 77）后由下一个请求续完、**两名消费者同时获得读许可**
  （共享锁并存）、**两名读者竞争同一笔死事务接管**（不发生共享→排他升级互锁）。
  每个子进程都断言退出状态，所有等待都有截止时间；由 socket 对端逐批核对跨日期
  数值，并核对遗留事务的最终去向（全部收敛、无残留）。

---

## 10. COMTRADE 故障录波接入（IEEE C37.111-1999）

检修拿到的是成对的 `*.cfg` / `*.dat` 高频录波，而不是分钟累计读数。
`minenergy.comtrade` 把跳闸前后的模拟量与开关量直接加工成 **Arrow / Parquet
数据**供后续算法使用——**不另算任何分钟指标，也不改动既有能耗 store 的发布协议**。

### 10.1 支持的格式边界

| 维度 | 支持范围 | 边界之外的处理 |
|---|---|---|
| 标准修订 | 仅 IEEE C37.111-**1999**（CFG 修订字段必须为 `1999`） | 其他修订年份直接拒绝 |
| DAT 编码 | **ASCII** 与 **BINARY** 两种 | `BINARY32` / `FLOAT32` 等拒绝 |
| 二进制帧布局 | 小端：`uint32 序号, uint32 时间戳, N×int16 模拟量, G×uint16 数字字组` | 非小端平台不支持；残帧拒绝 |
| 数字通道 | 按 CFG 通道顺序解包，字组最低位 = 该组第一个通道，16 通道/字组 | 通道/分组数与帧布局不符即拒绝 |
| 采样率 | 多段 `samp,endsamp`，结束样本号按**含端点累计计数** | 非递增结束样本号拒绝 |
| 时间戳 | 全零 → 按 CFG 分段采样率重建；非零 → 乘 `timemult` 后按微秒解释 | 部分为零（首点除外）拒绝，不静默切换 |
| 时区 | 调用方强制指定 IANA 名 | DST 回拨重复时刻报歧义、春跳缺失时刻报不存在，绝不猜测 |
| 物理换算 | `a*x+b` 还原标记侧工程量，按 P/S 标记与一次/二次变比换算 | 非正/非有限变比拒绝，结果再校验有限性，杜绝无穷值 |
| 扩展名 | `.cfg`/`.dat` 大小写不敏感（`wave.CFG` + `wave.DAT` 可用） | 找不到同主干 DAT 拒绝 |

### 10.2 时间解释（最易出错的部分）

1. **非零 DAT 时间戳**：`绝对时刻 = CFG 起始时刻 + 原始时间戳 × timemult`，单位微秒，
   `timemult` 用 `Fraction` 精确表示，无浮点漂移。
2. **全零时间戳**：才允许按 CFG 分段采样率重建。第一点相对起始时刻为 0；
   后一段第一点的偏移 = 前面各整段 `样本数 / 采样率` 之和。例：1000 Hz × 6 点
   接 500 Hz × 4 点，则第 7 点偏移 = 6/1000 s = 6000 µs。
3. **混合零时间戳**：除首点（偏移 0，两种模式都合法）外，任何一帧为零而其他帧非零，
   立即报错并指出帧号/样本号，不允许读到一半切换算法。
4. **时间回退、序号跳号、末尾残帧**：均带文件路径与帧/样本位置拒绝整个导入。
5. **时区**：CFG 时刻无时区。回拨（fold 0/1 映射到两个偏移且往返 UTC 仍等于自身）
   报 *ambiguous*；春跳（往返后墙上时刻改变）报 *does not exist*。
6. **通道 skew**：每个模拟通道把自己的 `skew`（微秒）加到**自己的**采样时刻；
   窗口筛选按加 skew 后的时刻进行，不用一个公共时刻覆盖所有通道。

### 10.3 物理量与开关量语义

- 模拟量先用 `y = a*x + b` 还原，`y` 的侧别由 CFG 的 **P/S 标记**决定：
  `P` 标记表示还原值即一次值（二次 = 一次 × 二次变比/一次变比）；
  `S` 标记表示还原值即二次值（一次 = 二次 × 一次变比/二次变比）。
  换算方向严格跟随标记；输出可选 `primary` / `secondary`，任何非有限结果都报错。
  CFG 的单位、相别、一次/二次变比、P/S 标记全部保留到数据与溯源元数据。
- 数字位按 CFG 顺序解开，同时给出实际 `state`（0/1）、`normal_state` 与
  `deviates_from_normal`，调用方可区分“实际电平”和“是否偏离正常态”。

### 10.4 流式读取接口（不吞整文件、不把全部采样变成 Python 对象）

```python
from minenergy.comtrade import Recording

rec = Recording.open("trip/rec.cfg", timezone="Asia/Shanghai")
rec.catalog()                       # 通道目录（不读 DAT 内容）
rec.validate()                      # 全量有界内存导入检查

# 相对触发时刻的闭区间窗口（微秒），可选通道、Arrow 批大小
for batch in rec.iter_analog_window(
    start_rel_us=-2000, end_rel_us=4000,
    channels=["Ia", 2], side="primary", batch_size=8192,
):
    ...  # pyarrow.RecordBatch，模拟量按“加 skew 后的时刻”筛选
for batch in rec.iter_digital_window(-2000, 4000, channels=[17]):
    ...
```

读取按 DAT 帧分批（BINARY 用定长块 + `struct` 小端解包，ASCII 逐行），
内存占用约等于一个批次 + 一个 Arrow RecordBatch，不随文件大小增长
（`tests/comtrade/test_memory.py` 用峰值 RSS 与 tracemalloc 单笔最大分配双重验证）。

### 10.5 命令

```bash
# 通道目录
python -m minenergy.comtrade.cli catalog --cfg path/rec.cfg --timezone UTC
# 导入检查（不写数据；--json 机器可读）
python -m minenergy.comtrade.cli inspect --cfg path/rec.cfg --timezone UTC
# 触发相对窗口分批写 Parquet（先全量检查，再暂存发布）
python -m minenergy.comtrade.cli export --cfg path/rec.cfg --timezone UTC \
       --out out/ --start-us -2000 --end-us 4000 \
       --analog-channels Ia Vb --digital-channels DIG17 \
       --side primary --batch-size 8192
# 重新生成离线夹具（无外部下载）
python -m minenergy.comtrade.cli make-fixtures --out fixtures/
```

坏文件以退出码 2 拒绝，信息含**出错文件与帧/样本/CFG 行号**。

### 10.6 Parquet 发布与“半份数据”防护

`export_parquet` 先跑一遍有界内存的全量 `validate()`，确认无任何帧级/时间级错误后，
才把 `analog.parquet` / `digital.parquet` 写到目标旁的私有暂存目录
（`.tmp-comtrade-*`）；全部写完后 `manifest.json` 原子替换为唯一完成标记。
任何异常都会删除暂存目录，目标目录永远不会把半份数据呈现为成功导入。
每个 Parquet 的 schema metadata 与 manifest 都带溯源信息：CFG/DAT 的 SHA-256、
时间解释方式（时区、时间戳模式、timemult、分段采样率）、物理换算（侧别、a/b、
变比、P/S 标记）、窗口与通道投影。

### 10.7 离线夹具与演示

`minenergy/comtrade/fixtures.py` 不依赖任何外部下载，生成同一波形的四种编码
（全零/显式时间戳 × ASCII/BINARY），含两段采样率、不同 skew、正负模拟值、
int16 极值与 **17 个数字通道（跨两个 uint16 字组，含 D16/D17 字组边界）**；
损坏样例覆盖截断（ASCII+BINARY）、序号跳号、错通道数、混合零时间戳、时间回退、
DST 回拨歧义与春跳缺失；另有 ~20 MiB、100 万帧的大录波。

```bash
# 一条命令：夹具 → 目录 → 四编码检查 → 手算核对 → 窄窗口/投影/分批
#          → 11 类损坏拒绝 → 暂存 Parquet → 大文件实测峰值内存
./comtrade_demo.sh /tmp/minenergy-comtrade-demo

# 回归（84 个 COMTRADE 用例 + 既有 104 个用例，共 188 个）
python -m pytest tests/comtrade -q
python -m pytest tests -q
```

大录波实测（参考环境）：DAT 19.1 MiB、100 万帧、流式产出 2100 万行，
进程峰值 RSS 约 69 MiB，与文件大小解耦；tracemalloc 观测到的单笔最大分配
保持在单批缓冲量级（约 0.25 MiB），不存在整 DAT 级缓冲。
