# 渡轮结算 · Iceberg v2 票号删除核对 / 整日核验

用于核对湖仓源快照中，某航次日分区里的票号是否已被 **Iceberg v2 位置删除**
（positional delete）标记。网页提供两套工具：

1. **逐票删除核对**：接收有效文件清单和对应 Parquet 副本，输入整数票号，
   逐条给出原文件、从 0 起算的文件内行号、是否已删除以及命中的删除文件。
2. **整日核验**（试跑后补计 / 去重 / 重跑）：上传同一航次日的有效文件清单、
   Parquet 副本和计费 CSV，按整日数据输出错引、误计、重计、漏计四类差异
   CSV 下载。

## 快速启动

```bash
docker compose up --build
```

打开 <http://localhost:8000>，点 **“载入样例”** 即可运行内置演示。接口健康检查：

```bash
curl http://localhost:8000/healthz
```

停止：

```bash
docker compose down
```

## 网页用法

1. 上传 **有效文件清单**（CSV 或 JSON）。
2. 上传清单中所有 `data` / `position_delete` 文件的 **Parquet 副本**，可多选。
   文件名必须与清单的 `uploaded_filename` 一致。
3. 填写整数票号（逗号、空格、分号或换行分隔），以及票号列名（默认
   `ticket_no`）。
4. 开始核对。

上传文件只保存在一次请求的临时目录中，请求结束后删除；不会按用户文件名
拼接服务器路径。整日核验的差异报告是唯一跨请求保留的产物，放在带随机
token 的临时目录并按 TTL 自动清理（见下文）。

### 清单格式

CSV（推荐）：

```csv
original_path,uploaded_filename,file_type,data_sequence_number
s3://ferry-lake/warehouse/settlement/voyage_date=2026-11-05/data/data-0001.parquet,data-0001.parquet,data,7
s3://ferry-lake/warehouse/settlement/voyage_date=2026-11-05/delete-pos/delete-pos-0001.parquet,delete-pos-0001.parquet,position_delete,12
```

JSON 也支持，顶层可以是数组，或包含 `files` / `entries` 数组的对象：

```json
[
  {
    "original_path": "s3://.../data/data-0001.parquet",
    "uploaded_filename": "data-0001.parquet",
    "file_type": "data",
    "data_sequence_number": 7
  }
]
```

列名可使用以下别名：

| 含义 | 可用列名 |
| --- | --- |
| 原始路径 | `original_path`, `original_file_path`, `source_path`, `path` |
| 上传文件名 | `uploaded_filename`, `uploaded_file`, `filename` |
| 文件类型 | `file_type`, `content`, `type` |
| 数据序列号 | `data_sequence_number`, `sequence_number`, `seq` |

`file_type` 可选值：

| 值 | 行为 |
| --- | --- |
| `data` / `0` | 数据文件，参与扫描 |
| `position_delete` / `positional_delete` / `1` / `delete` | 位置删除文件，参与匹配 |
| `equality_delete` / `2` | 等值删除，**忽略**；不要求上传 Parquet 副本 |

只处理位置删除。位置删除 Parquet 必须包含 `file_path`（字符串）和 `pos`
（整数）两列，其他列（例如 Iceberg 可选的 `row`）会被忽略。

## 核对规则

- **行组跳读**：对每个数据文件的每个行组读取 `ticket_no` 列的 min/max 统计。
  票号落在统计范围外时跳过该行组；统计缺失（`statistics` 为 `None` 或没有
  min/max）时读取该行组。统计范围包含票号时也读取。
- **原始行号**：行号始终是数据文件中从 0 起算的原始行号 =
  前面所有行组的行数之和 + 行组内原始下标。筛选或只读取某个行组都不会重置、
  压缩或重新编号。
- **位置删除匹配**：删除记录必须同时满足
  1. 删除记录的 `file_path` 精确等于清单中的 `original_path`；
  2. `pos` 精确等于该命中行的从 0 起算的行号；
  3. 删除文件的数据序列号 **不小于** 数据文件的数据序列号。

  不满足序列号条件的候选删除文件会显示为“被规则忽略的删除记录”，但不会
  把票判成已删除。
- **全有或全无**：清单中的任何一个 `data` / `position_delete` 副本缺失，
  或任一文件打不开、行组读取失败，整次核对都会中止并明确列出文件名，不显示
  任何票号的部分结论。未出现在清单中的上传文件会被忽略并给出警告。

## 整日核验

试跑后要查清哪几笔该补计、去重或重跑时，使用页面下半部的 **“整日核验”**
表单（或打开 <http://localhost:8000/reconcile/sample> 载入样例）。除清单和
Parquet 副本外，还要上传一份 **计费 CSV**，表头固定包含三列：

| 列名 | 含义 |
| --- | --- |
| `ticket_no` | 计费票号，非负整数 |
| `file_path` | 计费所引用的源数据文件原始路径，按字符串精确匹配 |
| `pos` | 源文件内从 0 起算的行号，非负整数 |

允许带 UTF-8 BOM、表头空白和额外列；完全空白的行不计入数据行号。数据行号
从 **1** 起算（不含表头）。

### 差异分类

按 `(file_path, pos)` 找源行，沿用上面的位置删除规则（路径 + 行号 + 删除
文件数据序列号不小于数据文件）：

| 类型 | 判定 |
| --- | --- |
| **错引** | 按原路径和行号找不到源行；或找到源行但票号与计费行不符 |
| **误计** | 票号相符，但该源行已被有效的位置删除 |
| **重计** | 一个**未删**源行有多条票号相符的计费行，这些计费行全部列出 |
| **漏计** | 一个**未删**源行没有任何票号相符的计费行 |

错引、误计、重计附计费 CSV 的数据行号（从 1 起）；漏计附源路径、从 0 起算
的行号和票号。已删除的源行不会列为漏计；票号不符时，计费行记错引、对应未删
源行同时记漏计。

### 差异报告 CSV

只有**全部文件读验通过**后才会生成差异 CSV 并出现下载按钮（
`/reconcile/report/<token>`，随机 32 位十六进制 token）。列：

```csv
diff_type,billing_row_no,billing_ticket_no,file_path,pos,source_ticket_no,detail
```

报告保存在服务端临时目录（`var/reconcile-reports`，Docker Compose 挂载为
命名卷），默认 1 小时过期自动清理，且最多保留最近 100 份；可用环境变量
`REPORT_DIR`、`REPORT_TTL_SECONDS` 调整。

### 全有或全无（整日核验）

出现以下任一情况时，页面只说明原因、**不发报告**，已生成的半成品报告会被
删除：

- 清单含 **等值删除（equality delete）**：整日核验只接受位置删除规则；
- 清单要求的 Parquet 副本缺件、打不开、缺列或行组读取失败；
- 位置删除文件的 `file_path` 为空、`pos` 非整数或为负数等坏行；
- 数据文件票号列为空或非整数；
- 计费 CSV 缺表头列、`ticket_no` / `pos` 不是非负整数、必填值为空等坏行；
- 本地临时数据库或报告文件读写失败。

### 分批读写，内存不随整日数据增长

核验过程不会在 Python 内存里攒下整日的票据、计费行或差异：

- 所有 Parquet 按 **行组** 读取，计费 CSV 按行解析，均以固定批次
  （`BATCH_SIZE = 4096`）写入一个临时的、文件型 SQLite 缓冲库；
- 两遍扫描（第一遍按计费行判错引 / 误计 / 重计，第二遍按源行判漏计）都用
  `fetchmany` 分批游标，查询使用独立游标，不会重置流式结果集；
- 差异行直接流式写入报告 CSV；请求结束后 SQLite 缓冲文件立即删除。

## 内置样例

`sample/` 中的演示数据由 `tools/make_sample.py` 生成：

- `data-0001.parquet`：5 个行组，每个 20 行。
  - 行组 0–3 的票号范围分别是 1000–1019、2000–2019、3000–3019、4000–4019。
  - 票号 **4242** 在第 5 个行组（索引 4），全局行号 **99**。
  - 核对 4242 时，前 4 个行组全部被统计范围排除并跳过；读取第 5 个行组后
    仍按原始行号 99 命中 `delete-pos-0001.parquet`。
- `data-0002.parquet`：写入时关闭了列统计，票号 **8888** 在第 2 个行组
  （全局行号 **19**）。因为没有统计，两个行组都会被读取，随后命中
  `delete-pos-0002.parquet`。
- `delete-pos-stale-0001.parquet`：也指向 4242 / 行 99，但数据序列号是 6，
  小于数据文件的 7，因此被忽略。
- 清单中的 `equality_delete` 项没有上传副本，逐票核对仍正常完成，因为它被
  忽略。整日核验使用不含该行的 `manifest-reconcile.csv`（整日核验遇等值
  删除会直接拒绝）。
- `billing-sample.csv`：整日核验样例计费文件，四类差异各至少一笔——
  - 误计 3 笔：已删除的 1003、4242、8888 仍被计费；
  - 重计 4 笔（2 组各 2 条）：未删源行 1005、7005 各被计费两次；
  - 错引 3 笔：越界 `pos=777`、未知路径、以及源行 2005 被错记成票号 9999；
  - 漏计 2 笔：未删源行 2005（票号不符所致）和 8006 没有相符计费行。

样例默认票号：`4242, 1003, 4218, 8888, 9999`。其中 1003 已删除，4218
存在但未删除，9999 不存在。

## 本地开发与测试

需要 Python 3.11+、Flask、PyArrow。

```bash
python3 -m pip install -r requirements.txt
python3 app.py                     # http://localhost:8000
python3 -m pytest -q               # 66 个测试
```

重新生成样例（会覆盖 `sample/` 下的 Parquet、清单和计费 CSV）：

```bash
python3 tools/make_sample.py
```

## 目录

```
app.py                     Flask 路由、上传处理、票号解析、差异报告下载
checker/manifest.py        清单解析与校验
checker/engine.py          PyArrow 行组跳读、行号计算、位置删除匹配
checker/reconcile.py       整日核验：分批磁盘缓冲、四类差异分类、报告流式写出
checker/errors.py          CheckError（始终带文件名）
templates/index.html       网页（逐票核对 + 整日核验）
static/style.css           样式
sample/                    内置样例（清单 + Parquet + 计费 CSV）
tools/make_sample.py       样例生成脚本
tests/                     pytest 测试
var/reconcile-reports/     差异报告临时目录（运行时生成，已 gitignore）
Dockerfile                 容器镜像
docker-compose.yml         启动配置（含报告卷挂载）
```

## 已知边界

- 位置删除文件会整体读入内存建立 `(file_path, pos)` 索引；适用于常见规模。
- 每个票号独立扫描数据文件。一次输入大量票号时耗时随票号数线性增长。
- 路径按字符串精确比较，不解析 `s3://`、`file://` 等 URI 等价关系。
  请保证清单的 `original_path` 与删除记录中的 `file_path` 写法完全一致。
