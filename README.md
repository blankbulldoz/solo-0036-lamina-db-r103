# LaminaDB

LaminaDB 是一个 Go 实现的单进程关系数据库内核和 HTTP 服务，面向数据库工程学习、存储实验和可复现的语义研究。它实现自己的类型系统、快照版本、提交校验、二级索引、查询执行、日志和恢复；不依赖 SQLite、PostgreSQL 或在线服务。

领域：**基础设施 / 数据库**。主要技术：Go 标准库、HTTP、版本链、CRC32 帧日志、SHA-256 图像。

## 构建和启动

要求 Go 1.25+，Linux 或 macOS。生产工作流演示另外使用 Python 3 标准库。

```sh
go build -o bin/laminad ./cmd/laminad
./bin/laminad -data var/lamina -listen 127.0.0.1:7386
```

数据目录只能由一个进程打开。默认仅监听回环地址；服务没有身份认证，不应直接暴露在不可信网络。进程收到 SIGTERM/SIGINT 后停止接受新请求，等待 HTTP 请求结束，再关闭数据库。没有在退出时强制生成检查点，已成功返回的提交依赖日志恢复。

```sh
python3 examples/walkthrough.py --workflow all
```

该命令启动临时实例，执行 14 条真实生产入口工作流，输出逐条观测 JSON，最后关闭实例并删除本次临时目录。可用 `--workflow query-plan` 等方式只运行一条。它是有界运行演示，不是基线测试套件。测试代码和自动化测试基础设施有意留待后续工作，本基线 `testing=deferred`。

## 一个事务

所有业务接口前缀为 `/v1/lamina`，JSON 标量仅支持有符号 64 位整数、文本、布尔值和 null。

```sh
curl -s http://127.0.0.1:7386/v1/lamina/relations -H 'Content-Type: application/json' -d '{"name":"spectra","primary":"sample","columns":[{"name":"sample","type":"int"},{"name":"band","type":"text"},{"name":"amplitude","type":"int","nullable":true}]}'
curl -s http://127.0.0.1:7386/v1/lamina/transactions -H 'Content-Type: application/json' -d '{}'
```

取返回的事务 `id`，在下面 URL 中替换 `<id>`：

```sh
curl -s http://127.0.0.1:7386/v1/lamina/transactions/<id>/put -H 'Content-Type: application/json' -d '{"relation":"spectra","row":{"sample":1,"band":"infrared","amplitude":40},"insert_only":true}'
curl -s -X POST http://127.0.0.1:7386/v1/lamina/transactions/<id>/commit
curl -s http://127.0.0.1:7386/v1/lamina/sql -H 'Content-Type: application/json' -d '{"sql":"SELECT sample, amplitude FROM spectra WHERE amplitude >= 20 ORDER BY amplitude DESC LIMIT 10;"}'
```

`put` 是完整行替换；缺失的可空列补为 null，缺失的非空列拒绝。主键从新行中取得，不提供原主键重命名操作。删除接口为事务 URL 下的 `delete`，正文为 `{"relation":"spectra","key":1}`；不存在的键也可以形成墓碑。DDL 自身是单个耐久操作，不在用户事务中暂存。

## 查询与隔离

`POST /query` 接收 `relation`、`columns`、`where`、`order`、`limit`，以及可选的 `transaction` 或 `at`。两种快照选择互斥。`where` 是 AND 连接的比较，支持 `= != < <= > >= is_null not_null`；`order` 指定列及 `desc`。

```json
{"relation":"spectra","where":[{"column":"amplitude","op":">=","value":20}],"order":[{"column":"amplitude","desc":true}],"limit":10}
```

读事务固定开始时的提交序号，重复读稳定，支持读自己的暂存写入。普通读使用当前序号。按 `at` 查询只接受保留区间内的序号，并要求关系在该序号已存在。事务无法访问在它开始之后创建的关系。无显式排序时按类型化主键排序；排序相同时也用主键确定次序。升序 null 在前，降序 null 在后。null 普通比较不匹配；使用 `is_null` 检查。

数据库提供快照隔离和同键 first-committer-wins，不承诺串行化、谓词锁或跨行写偏差防护。提交前对最终可见状态检查所有受影响关系的唯一索引；一次事务内可以置换唯一值。失败提交会关闭该事务，不发布任何私有行。只读事务通过 `{"read_only":true}` 开始。

SQL 只支持单条 SELECT：投影或 `count(*)`、`count(column)`、`sum(integer_column)`，可选 WHERE AND、ORDER BY、LIMIT。字符串使用单引号，内部单引号写两次；关键字及未引号标识符转为小写。没有 JOIN、GROUP BY、别名、表达式、SQL DDL/DML。语义检查与结构化查询共用绑定和执行。`sum` 忽略 null，空输入返回 null，溢出报错；`count(column)` 忽略 null，`count(*)` 计算全部匹配行。

## 索引

`POST /indexes` 的示例正文：

```json
{"name":"by_amplitude","relation":"spectra","column":"amplitude","unique":false}
```

单列索引由类型化有序切片维护，保留历史非空值到主键的候选关联。唯一约束允许多个 null。索引在当前提交序号同步构建；构建失败不会发布目录。规划器在可用索引中选择候选键最少的一条，之后读取指定快照的真实版本并执行全部谓词。对早于索引创建时刻的查询使用扫描；含私有写入的事务查询也使用扫描。回收后重建历史候选。

结果的 `plan.candidates` 是访问路径候选键数，`scanned` 是存在可见行并进入残余判断的行数，`matched` 是限制输出前的匹配数。保留历史候选可能导致这些数字差异很大。`force_scan:true` 可用于对照观测。`plan` 是执行事实，不是 I/O 耗时估计。

## 日志、检查点、还原

提交顺序为校验、完整帧写入、fsync、内存发布。每个日志帧包括长度、CRC32 和完整提交记录。日志序号连续，当前记录格式版本为 2，可读范围为 2..2（`GET /identity` 或 `/stats` 的 `readable_formats` 会报告）。写盘或同步失败后实例拒绝继续服务；调用方可能无法确定该次提交最终是否落盘，应重启后核对实际状态，基线没有请求级去重协议。

启动先验证检查点，再按序重放剩余记录。只自动截断最后一个不完整帧；完整帧校验不匹配、非法长度或序号异常都会拒绝启动。完整日志一直保留，检查点不会截短日志。数据库状态在内存中，日志和镜像会随数据量增长。

### 库身份与格式兼容

每个 v2 目录有 128 位十六进制实例 id，日志头（`LMW2`，216 字节）与每个图像（checkpoint/backup）的 `provenance` 块共同携带：`instance`（本目录）、`origin`（数据最初来源，从无可选）、`parent`（恢复时继承自谁，可选）和 `lineage`（产生此格式的转换标签，如 `legacy-v1-to-v2:<前缀摘要>`）。日志头另有 SHA-256 自校验和，并与检查点绑定同一实例与 lineage；序号相同但来自不同目录的日志和检查点会以“belongs to instance … but checkpoint to …”拒绝，而不是仅凭序号拼接。恢复产生的目录获得新实例、记录 `parent`/`origin`，从自己的序号继续提交；源库不会因此看到 restored 目录的提交。`GET /identity` 返回完整身份块，`/stats` 和 `/changes` 也暴露 `instance`。

### 旧格式升级、回退与诊断

旧格式（`LMW1` 日志头、payload/图像 `format:1`）没有身份信息，正常启动会拒绝并提示升级，绝不推断来源。运维入口是三个子命令，均只操作离线目录（独占锁）：

```sh
./bin/laminad recover -data var/lamina -json        # 只读诊断，不改字节
./bin/laminad upgrade -data var/lamina -json        # 原地转换，输出接续与回退事实
./bin/laminad rollback -data var/lamina -json       # 回退到升级前的旧目录
```

`recover` 报告每个工件的格式、摘要、序号、身份，以及“可读历史止于哪个序号、多少键/版本”；日志损坏时明确说明止点，不静默截断。

`upgrade` 先在内存中按正常准入规则完整重放旧目录，并用**旧日志从 0 重放的状态与旧检查点在检查点序号处逐行/逐链/逐目录项比对**。两者一致才认定它们同源；不一致说明检查点和日志来自不同来源却恰好序号相接，此时拒绝并列出差异，目录保持原样。若旧日志起点大于 0（例如旧版恢复出的目录），无法交叉核对，必须显式 `-assume-continuous` 才采纳，并在报告 `legacy_cross_check.result=unverifiable` 中记录这是声明而非验证。

转换策略是**原地转换**（运维只有一份图像大小的余量）：旧日志的 v1 前缀一个字节都不重写，仍留在 `journal.lmw` 原位；旧检查点改名为 `checkpoint.v1.lmi` 保留在同目录；新检查点写在原文件名下；最后原子写入 `journal.lmw.id`（adoption 记录）作为提交点，其中记录旧日志前缀的 SHA-256、长度、记录数、旧检查点摘要和转换 lineage。因此转换本身不产生第二份数据副本。

中断边界可重入：每一步后目录都不可服务，且下一次 `upgrade` 能识别所处阶段并确定性完成或回退——旧图像已改名而新检查点未写时放回旧图像再继续；已写 v2 检查点但无 adoption 记录时拒绝猜测身份并保持不可启动，需按 `recover` 报告人工确认；adoption 记录已写但 v2 检查点缺失时按已提交的身份补齐。任何阶段都不会留下“两版本各取一半”的可启动目录，`recover` 会显示 `serving_ok=false` 及原因。

`rollback` 是 `upgrade` 的显式逆操作：校验保留前缀与旧图像仍与 adoption 记录一致（字节级），把 `checkpoint.v1.lmi` 放回并截掉转换后追加的 v2 帧，再删除身份记录。回退后的目录与升级前逐字节相同，旧二进制仍可读取；转换之后产生的新提交只存在于被截掉的后缀中，报告会给出丢弃字节数。若前缀已被改动，回退拒绝执行而不是覆盖证据。

旧备份（`format:1` 的 `.lmi`）可用 `-restore` 直接还原：会获得新身份和 `legacy-v1-to-v2:<摘要>` lineage，保留图像内的历史读版本，并从恢复基点继续写自己的日志；它不包含源库历史日志记录。

```sh
curl -s http://127.0.0.1:7386/v1/lamina/backup -o /tmp/lamina-backup.lmi
./bin/laminad -restore /tmp/lamina-backup.lmi -data var/lamina-restored -listen 127.0.0.1:7387
```

还原目标必须不存在。启动前验证图像摘要、格式、目录、版本顺序及行类型，再安装图像和带恢复基点的日志头。还原保留图像内的历史读版本，但不包含源库历史日志记录；变更消费者必须从恢复基点重新对齐。禁止将备份文件当成一个新根日志目录手动拼接。

用真实旧材料演练迁移（第一参数必须是迁移前提交构建的基线二进制）：

```sh
git worktree add /tmp/lamina-baseline cb08bd0
(cd /tmp/lamina-baseline && go build -o /tmp/laminad-legacy ./cmd/laminad)
python3 examples/migration_drill.py /tmp/laminad-legacy ./bin/laminad
```

脚本用基线二进制生成真实旧日志/检查点/备份，覆盖 40 项检查：拒绝无身份启动、只读诊断、同序号异来源配对、原地升级与跨检查点启动、检查点替换、便携还原的继承与分叉、三阶段中断重开、字节级回退与旧二进制复读、尾部截断、整帧损坏、外来日志身份拒绝。任一检查失败时退出码非零。

## 变更与回收

`GET /changes?after=0&limit=100` 返回按提交序号递增的目录、提交和回收记录。分页单位是完整记录，不拆分事务，`cursor` 是已返回的最后序号，`head` 是观测时的数据库头部序号。还原实例的 `history_base` 之前没有变更历史，旧游标返回 410。

`POST /vacuum` 正文为 `{"horizon":<sequence>}`。实际水位取请求值和所有活动事务快照的最小值；活动读者阻挡时 `pinned=true`。每条版本链保留水位时的最后版本和所有更新版本；水位前的最终墓碑可以全部移除。低于持久回收水位的按序号查询返回 410，水位不能回退。回收并不清理日志磁盘占用。

## 资源与错误

请求体上限 1MiB，列数 64，文本值 64KiB，活动事务 128，每事务最终写集 1000 个键，单日志帧 16MiB，镜像 128MiB。查询最多 32 个 AND 谓词、8 个排序项，默认输出 1000 行，可显式设到 10000；截断时 `truncated=true`。查询执行仍可能物化全部匹配行，事务没有自动过期，排序和备份没有外溢机制，应显式关闭不用的会话并为演练设置合理数据规模。

错误 JSON 为 `{"error":{"code":"...","message":"..."}}`。无效输入 400，已关闭或未知事务及缺失行 404，写冲突或唯一冲突 409，快照和变更历史不可用 410，容量拒绝 429，存储不确定或已关闭 503。`GET /stats` 返回会话、版本、候选索引大小、恢复尾字节、实例身份与可读格式范围等领域事实，进程计数在重启后归零；`GET /identity` 是身份与接续事实的稳定读取点。

## 容器

```sh
docker build -t lamina-db .
docker run --rm -p 127.0.0.1:7386:7386 -v lamina-volume:/data lamina-db
```

镜像使用非特权用户 10001。容器工作流需本机镜像构建可用；原生运行不依赖 Docker。持久目录使用 Unix 文件锁，当前不声明 Windows 支持。备份只能缓解存储故障；没有复制、自动切换或加密。
