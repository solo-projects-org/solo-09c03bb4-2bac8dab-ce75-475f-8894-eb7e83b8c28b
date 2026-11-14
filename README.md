# DICOM CT 离线取数流水线

面向影像科研数据员的院内 PACS 取数工具：提交 `StudyInstanceUID`，经
**C-FIND 固定本批 SOP 清单 → C-MOVE 发起回传 → C-STORE 接收落盘 →
核对收齐 → 打包 ZIP 下载** 的完整流水线，支持多检查并行、慢回传交错、
重复投送去重、清单外实例拒收、保存失败判失败与重启恢复。

在原始取数之上，另提供**按批生成与下载的定向去标识包**：把已完成批的
原 ZIP 转成可交给院外分割组的去标识 ZIP（删除私有标签与直接标识、
重新分配 UID 并同步改写批内引用），详见第 4 节。

* **API 框架**：FastAPI（自带 Swagger/OpenAPI）
* **DICOM 栈**：pynetdicom 3.x（C-STORE SCP + C-FIND/C-MOVE SCU）、pydicom
* **持久化**：SQLite（WAL，全量落在数据卷）
* **模拟 PACS**：pynetdicom 实现的真实 DICOM 网络对端，内置虚构合成 CT，
  可配置慢回传与重复投送；另带 rogue 投送器演示拒收

---

## 1. 快速开始（Docker Compose）

```bash
# 构建并启动 API + 模拟 PACS（首次会自动生成 8/6/5/2 个实例的四个虚构 CT
# 检查；第 4 个检查含“指向批外实例”的引用，用于演示拒绝发布）
docker compose up --build -d

# Swagger 文档
open http://localhost:8000/docs

# 一键端到端演示：交错回传 / 重复投送 / 清单外拒收 / 压缩拒收 /
# 保存失败 / 失败批迟到拒收 / 重启恢复 / 定向去标识包（含并发生成与
# 批外引用拒绝发布）
docker compose run --rm demo
```

端口：

| 服务 | 端口 | 用途 |
|---|---|---|
| api | 8000 | REST API + Swagger |
| api | 11113 | DICOM C-STORE SCP（AE=`RETRIEVER`）|
| pacs | 11112 | 模拟 PACS（AE=`MYPACS`）|

数据卷：`api-data`（SQLite、DICOM 文件、ZIP）、`pacs-data`（虚构 CT）。

---

## 2. 业务规则与实现位置

| 规则 | 实现 |
|---|---|
| 只收**非压缩 CT** | SCP 只公布 Implicit/Explicit VR Little Endian 的 CT 存储上下文，压缩 TS 在协商阶段即被拒；`app/storage.py::validate_dataset` 二次校验 |
| 源端批内**不增删实例** | C-FIND（IMAGE 级）一次性固定清单写入 `instances` 表，`set_manifest` 只允许固定一次 |
| 回传**归入对应批** | C-STORE 按 `StudyInstanceUID` 经 `db.find_batch_for_study` 路由（按 rowid 取最新批） |
| **清单外实例不入包** | `db.is_manifest_uid` 校验，拒收并回 `0xC000`，事件 `STORE_REJECTED` |
| **重复实例不重复计数** | `(batch_id, sop_instance_uid)` 唯一约束 + `register_instance` 条件更新；重复直接回 SUCCESS，不写盘不计数，事件 `STORE_DUPLICATE` |
| **文件写完且登记成功才回 C-STORE 成功** | `handle_c_store` 严格顺序：临时文件 → fsync → rename → 目录 fsync → DB 登记；任一步失败回 `0xA700/0xC000`，登记失败会删除已写文件 |
| **C-MOVE 成功且清单收齐才可下载** | 下载接口仅允许 `COMPLETED` 且 zip_path 存在；MOVING/FAILED 返回 409 |
| 传输/保存失败、结束后缺件均**判失败** | C-MOVE 非 0x0000（含 0xA702/0xB000）失败；`missing_uids` 非空失败；写盘异常立即 `fail_batch` |
| **迟到回传不能恢复失败批** | 终态 FAILED 的批收到 C-STORE 一律拒收；DB 层 `transition_state` 禁止任何状态迁出终态 |
| 多检查**并行、慢回传互不阻塞** | 每批独立线程；C-STORE SCP 每关联一线程；C-MOVE 回传与其它批的 API/DICOM 处理无共享阻塞 |
| **重启恢复** | 启动时所有非终态批置 FAILED；COMPLETED 批保留可下载，ZIP 丢失自动按登记重建 |

### 批状态机

```
PENDING ──C-FIND──▶ FINDING ──清单固定+故障武装──▶ MOVING
                                          │
                  收齐+C-MOVE 成功 ────────┴──▶ COMPLETED（可下载，终态）
                  任何失败/缺件 ──────────────▶ FAILED（终态，迟到回传拒收）
重启时 PENDING/FINDING/MOVING ───────────────▶ FAILED
```

---

## 3. REST API

Swagger：`GET /docs`（OpenAPI JSON：`/openapi.json`）

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/batches` | 提交检查，body：`{"study_instance_uid": "...", "fault_inject": ""}`，`fault_inject` 可选 `save_failure` |
| GET | `/batches` | 列出全部批 |
| GET | `/batches/{id}` | 批状态与进度（`expected_count/received_count/state/downloadable`）|
| GET | `/batches/{id}/events` | 事件流水（接收、重复、拒收、状态迁移、故障）|
| GET | `/batches/{id}/instances` | 清单、已收、缺失 SOP 列表 |
| GET | `/batches/{id}/download` | 下载 ZIP（仅 COMPLETED），ZIP 内含 `dicom/` 与 `MANIFEST.csv` |
| POST | `/batches/{id}/deid-package` | 生成并原子发布**定向去标识包**（幂等；失败可重试），详见第 4 节 |
| GET | `/batches/{id}/deid-package` | 去标识包状态：`None/RUNNING/COMPLETED/FAILED` 及失败原因 |
| GET | `/batches/{id}/deid-package/instances` | 服务端保留的旧 SOP → 新 SOP 映射 |
| GET | `/batches/{id}/deid-download` | 下载已发布的去标识 ZIP（原包下载不受影响） |
| GET | `/health` | 健康检查 |
| GET | `/internal/pacs-studies` | （演示）C-FIND 列出模拟 PACS 检查 |
| POST | `/internal/suicide` | （演示）退出进程，由 compose 自动重启以演示恢复 |

手工调用示例：

```bash
BASE=http://localhost:8000
STUDY=$(curl -s $BASE/internal/pacs-studies | jq -r '.[0].study_instance_uid')

BID=$(curl -s -X POST $BASE/batches -H 'Content-Type: application/json' \
  -d "{\"study_instance_uid\":\"$STUDY\"}" | jq -r .id)

watch "curl -s $BASE/batches/$BID | jq"
curl -s $BASE/batches/$BID/events | jq
curl -OJ $BASE/batches/$BID/download

# 去标识包：生成 -> 状态 -> 下载（原包仍可下载）
curl -s -X POST $BASE/batches/$BID/deid-package | jq
curl -s $BASE/batches/$BID/deid-package | jq
curl -OJ $BASE/batches/$BID/deid-download

# 保存失败演示
curl -s -X POST $BASE/batches -H 'Content-Type: application/json' \
  -d "{\"study_instance_uid\":\"$STUDY\",\"fault_inject\":\"save_failure\"}"
```

---

## 4. 定向去标识包

交付对象是院外分割组：需要保留**切片引用关系**，但不能携带可识别信息。
入口是 `POST /batches/{id}/deid-package`，输入为该批**原 ZIP**，输出为新 ZIP。

### 4.1 处理规则

| 规则 | 做法 |
|---|---|
| **递归删除私有标签** | 遍历整棵元素树（含嵌套序列），删除所有 group 奇数标签及私有创建者块（`app/anonymize.py::_strip_private`） |
| **删除直接标识** | `PatientName`、`PatientBirthDate`、`AccessionNumber`，递归删除（含序列内出现） |
| **重新分配 UID** | `PatientID` 与 Study/Series/SOPInstanceUID 换新值；新值 = `SHA-256(批 ID, 旧值)` 派生的十进制 UID，**同批重试结果一致、跨批不同**（`uid_for` / `patient_id_for`） |
| **同步改写引用** | 序列内 `ReferencedSOPInstanceUID` / `ReferencedSeriesInstanceUID` / `ReferencedStudyInstanceUID` 指向本批实例的，一律改为对应新 UID |
| **同步改写文件元信息** | `MediaStorageSOPInstanceUID` / `MediaStorageSOPClassUID` 与主数据集一致；保持原未压缩传输语法 |
| **同步改写文件名与清单** | 文件名只由新 Series/SOP UID 组成；`MANIFEST.csv` 只列新 UID（旧→新映射仅保存在服务端 DB，供审计） |
| **批外引用拒绝发布** | 任何引用 UID 不在本批映射内 → `DedupError`，HTTP 422，说明具体实例/标签/目标 UID；不写任何最终包 |
| **像素与几何保持原值** | 不触碰 `PixelData`、Rows/Columns、PixelSpacing、ImagePositionPatient、SliceOrientation、SliceLocation、Rescale 等 |

### 4.2 并发、原子性与恢复

* **同批并发生成只发布一份完整包**：`deid_packages.claim_deid` 以数据库
  事务原子领取生成权（`None|FAILED → RUNNING`），只有一个请求真正写盘；
  其余请求等待并复用它发布的同一份包（首次 201，其余 200）。
* **写盘失败不暴露半成品**：新包写入 `zips/{batch}.deid.zip.part`，完整
  写入并 fsync 后才 `os.replace` 到最终路径；失败只清理临时文件，最终
  路径要么不存在、要么完整。发布后登记失败会撤销最终文件。
* **重启安全**：启动时把残留 `RUNNING` 置 `FAILED`（原因明确、可重试），
  清理所有 `*.deid.zip.part`；已发布 `COMPLETED` 包保留可下载，若文件
  丢失则置 `FAILED` 以便重新生成；若最终文件存在但数据库中没有对应的
  `COMPLETED` 记录（改名后、登记前被杀），按“从未发布”删除，绝不暴露。
* **失败可重试**：`FAILED` 状态允许再次 POST；UID 映射按批稳定，重试与
  首次产出的新 UID 完全一致。
* **原包始终可下载**：去标识只读原 ZIP，绝不改写或删除原包。

### 4.3 状态与接口语义

```
(无记录) ──POST──▶ RUNNING ──发布成功──▶ COMPLETED（可下载，幂等复用）
                       │
                       └──批外引用/写盘失败/重启──▶ FAILED（可再次 POST 重试）
```

| 场景 | HTTP |
|---|---|
| 首次生成并发布 | `201` + 状态体 |
| 已存在完整包，重复调用 | `200` + 同一份结果（`attempt` 不变） |
| 原批未 COMPLETED | `409` |
| 另一请求生成中且等待超时 | `409`（生成中） |
| 批外引用 / 生成失败 | `422`，`detail.message` 说明原因，`retryable=true` |
| 下载未发布/失败包 | `409`；已发布包缺失 | `410` |

失败状态体含 `reason_code`：`EXTERNAL_REFERENCE`（引用指向批外，拒绝
发布）或 `GENERATION_FAILED`（读写/解析失败），两者 `retryable=true`。

### 4.4 演示

`docker compose run --rm demo` 会：

1. 对已完成批并发生成 4 次，验证只产生一份完整包（1×201 + 3×200），
   UID 一致；
2. 校验新包：直接标识已删、私有标签（含嵌套序列）清除、Study/Series/
   SOP 换新、序列内引用同步改写、文件名/清单只含新 UID，且**原包字节
   级不变**；
3. 对含批外引用的第 4 个虚构检查执行生成，验证 `422` 与明确原因，
   且不暴露任何新包。

演示数据（`simulator/generate_data.py`）为完全虚构的合成 CT，每个实例
带私有创建者块、私有元素与嵌套序列私有标签，并在 `ReferencedImageSequence`
中引用批内其它实例；第 4 个检查额外引用一个批外实例 `…777002`。

---

## 5. 演示场景说明

模拟 PACS 支持环境变量（见 `docker-compose.yml`）：

* `MOVE_DELAY=1.0`：每个实例发送前停顿 1 秒，制造慢回传，便于观察多批交错
* `MOVE_DUPLICATE_EVERY=3`：每 3 个实例额外用一条新关联重发一次，模拟源端重试
  （接收端应回 SUCCESS 但不计数）

rogue 投送器（演示容器/API 镜像内均可运行）：

```bash
# 清单外实例：期望 0xC000 拒收
python -m simulator.rogue_sender off-list   --study <StudyUID> --host api --port 11113
# 声明 JPEG 压缩：期望关联协商被拒
python -m simulator.rogue_sender compressed --study <StudyUID> --host api --port 11113
```

保存失败演示的实现是**真实故障**而非 mock：提交时把批目录 `chmod 555`，
非 root 容器用户写入必然 `PermissionError`，可观察到：
首个实例 C-STORE 回 `0xA700` → 批立即 FAILED → 后续实例全部以“已失败”拒收
→ C-MOVE 最终 `0xA702` → 下载返回 409。

定向去标识包的演示、并发与重启语义见第 4.4 节。

---

## 6. 目录结构

```
app/
  config.py      环境变量配置（AE、端口、数据卷路径、超时、去标识 UID 根）
  db.py          SQLite schema、状态机、清单/登记去重、去标识包领取与登记
  storage.py     非压缩 CT 校验、原子写盘+fsync、故障注入、ZIP 打包
  anonymize.py   定向去标识：递归删私有/直接标识、稳定 UID 映射、引用改写
  deid.py        去标识包：原子发布、并发单发布者、失败重试、重启恢复
  dicom.py       C-STORE SCP 处理器、C-FIND/C-MOVE SCU
  pipeline.py    每批后台线程编排与启动恢复
  main.py        FastAPI 路由、lifespan、Swagger
simulator/
  generate_data.py  虚构合成 CT 生成器（私有标签+序列内引用+批外引用样本）
  pacs.py           模拟 PACS（C-ECHO/C-FIND/C-MOVE，慢回传+重复投送）
  rogue_sender.py   清单外/压缩实例投送器
demo/
  run_demo.py       端到端演示编排（含去标识包与拒绝发布）
tests/
  test_db.py       状态机/去重/终态保护单元测试
  test_storage.py  校验/原子写/ZIP 单元测试
  test_deid.py     去标识标签处理/UID 稳定性/原子发布/并发/恢复单元测试
  test_e2e.py      真实 pynetdicom 网络栈端到端测试（11 个场景）
docker-compose.yml  Dockerfile  Dockerfile.pacs  entrypoint.sh
```

## 7. 本地测试（无需 Docker）

```bash
pip install -r requirements.txt pytest
python -m pytest tests/ -v
```

端到端测试会在临时目录启动完整的 PACS + API 真实进程对，覆盖：
正常收齐打包、并行交错、重复去重、清单外/压缩/未知 Study 拒收、
保存失败与迟到拒收、409 冲突、重启恢复，以及去标识包生成/幂等/下载、
原包不变、批外引用拒绝发布（共 32 个测试）。

## 8. 接入真实 PACS 的注意事项

1. 在真实 PACS 上注册 AE Title `RETRIEVER` 及本服务主机/11113 端口作为
   C-MOVE 目标，并放通防火墙。
2. 用 `PACS_AET/PACS_HOST/PACS_PORT` 指向真实源端；确认其 C-FIND 支持
   Study Root IMAGE 级查询并返回 `TransferSyntaxUID`。
3. 流水线只接受未压缩 little-endian CT；若源端只存压缩 TS，需在源端配置
   转码，或扩展 `app/dicom.py` 的上下文与 `storage.UNCOMPRESSED_TS` 策略。
4. 数据卷需保证持久化挂载与足够 inode/容量；SQLite 与 DICOM 文件均在
   `DATA_DIR` 下，备份该目录即可完整备份批状态、原包与去标识包。
5. 去标识范围严格限定为需求列出的标签与 UID 类别；若院内策略还要求清除
   `StudyDescription` 等自由文本或设备字段，在 `app/anonymize.py` 的
   `DIRECT_IDENTIFIER_TAGS` / 处理流程中扩展，并用第 7 节测试回归。
   `ANON_UID_ROOT` / `ANON_PATIENT_PREFIX` 可经环境变量配置。
