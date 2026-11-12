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
│   └── compare.py        # 增量 vs 全量 vs 参考 三段对照
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
