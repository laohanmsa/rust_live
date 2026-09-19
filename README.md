# Polym Rust Demo

一个能从信号走到签名下单的最小 Rust 原型。
默认只运行本机模拟交易所，实盘入口需要显式 `--live` 和自行提供的账户凭据。
不依赖原 Polym 项目、Python、Docker、数据库或消息服务器。

## 直接运行

需要 Rust 1.96 或更新版本。

```sh
cargo run --release -- demo
```

这个命令会启动两个临时本地接口，提交一条信号，完成真实的第二版订单签名和请求认证，校验后退出。
模拟交易所会恢复签名者地址并检查认证，返回模拟成交结果。
输出中的 `accepted` 只代表模拟接口接受，不是真实交易所成交。
日志和结果保存在新建的 `data/demo-<随机编号>/` 目录。

验证一秒内 67 条信号，按每 10 毫秒一条开放循环发送，不等待上一条请求完成：

```sh
cargo run --release -- demo 67
```

模拟交易所每单固定等待 50 毫秒才回复，用于验证等待网络时其他信号仍能处理。
输出同时列出输入数、接受数、未接受数和各条请求的计时，不隐藏拒绝来美化延迟。
这是本机实验，不是生产容量或公网延迟承诺。

## 常驻服务

```sh
cargo run --release -- serve config.demo.json
```

在另一个终端发送信号：

```sh
cargo run --release -- send 1 0.50 0.60 example-1
```

参数依次为结果代币、观察到的卖价、外部提供的公允价值、可选信号编号。
服务器只绑定 `127.0.0.1:8787`。
模拟模式的服务访问口令固定为 `demo-local-only`，此口令不会用于实盘。

完整输入格式：

```json
{
  "id": "example-1",
  "token_id": "1",
  "ask": "0.50",
  "fair_value": "0.60",
  "observed_at_ms": 1789240000000,
  "book_valid": true
}
```

时间戳必须替换为当前 Unix 毫秒。
金额和价格使用十进制字符串。
同一个编号重试时必须发送完全相同的内容，包括原始时间戳。
同编号不同内容返回冲突；已记账的相同信号直接返回已有状态，不重新签名或发单。

| 接口 | 用途 |
| --- | --- |
| `GET /health` | 运行模式和是否允许处理新信号 |
| `POST /signal` | 验证信号、签名、可靠记录、提交订单 |
| `GET /orders/{id}` | 查询本地记录，编号为输入信号编号 |
| `POST /stop` | 停止新信号及尚未通过提交检查的订单，已进入提交阶段的请求仍可能发出或完成 |

除健康检查外，接口要求 `Authorization: Bearer <访问口令>`。
输入超过 8 KiB（二进制千字节）会被拒绝。

## 最小策略与结构

```text
本机信号接口
  → 有界内存队列
  → 公允价值减卖价的门槛检查
  → 固定预算买入意图
  → 官方客户端构造并签名
  → 持久化去重与预算占用
  → 复用连接，异步提交
  → 记录接受、拒绝或结果未知
```

每个信号仅支持买入，订单类型为 FAK（立即成交，取消剩余量），输入卖价作为成交价格上限。
`min_edge` 是未扣手续费的价格差门槛，不是原系统的争议概率或完整收益模型。
原型使用外部提供的公允价值，不预测事件结果，也不自动监听预言机。
`book_valid` 与报价来自受信任的信号生产者，本项目不独立重建或证明盘口正确性。

启动时读取允许交易代币的价格步长、最小数量、负风险类型和手续费信息，并预热官方客户端缓存。
发单路径不重新查询这些元数据；缺失手续费信息或协议不是第二版时拒绝启动。
元数据超过配置有效期后拒绝新信号，需要重启重新加载，最长允许 5 分钟。
这是短时原型的明确边界，连续运行时应改成独立更新状态的行情接入。

官方客户端用于订单格式、金额舍入、费用预留和钱包签名。
其 0.8.0 版本的高级提交方法还会查询成交交易哈希，所以原型用常驻请求客户端提交签好的原始请求，直接拿到提交回报。
请求认证遵守官方的时间戳、方法、路径和原始请求体签名规则。
传输禁止自动重试、重定向和环境代理。

## 风险与恢复边界

- 单账户，一个活动进程，显式代币白名单。
- 每单预算和全程累计尝试预算都有上限。
- 官方客户端按缓存的手续费信息调整买入金额，给费用留出空间。
- 累计预算对每次已准备的订单永久计费，拒绝或未成交也不自动释放。
- 这不是可用余额账本，不追踪其他程序的挂单或资金变化。
- 不应与旧自动交易系统共用正在交易的账户；原型不协调跨程序余额。
- 本地日志先同步到磁盘，成功后才发单；磁盘操作在线程池中完成，网络等待不持有日志锁。
- 日志仅一个写入者，使用文件锁，权限要求为 `0600`，并绑定运行模式、签名者和资金账户。
- 日志包含已签名订单的经济参数和签名，不保存私钥或请求认证凭据，仍应当作为敏感本地文件保护。
- 网络超时、服务器故障或不明确回报记为 `unknown`，保留原订单去重和预算预留；其他订单继续交易，重启也不会重发原单。
- 五分钟后独立查询 Dashboard 对账结果：完整账户 activity（成交活动）和精确订单成交记录都为空时，按 `no_activity_after_5m` 规则标记失败；查询失败保留未知并重试。
- 对账结果读取独立于账户就绪轮询，未知订单或查询失败不会阻塞其他订单。
- 崩溃时只有准备记录的订单，恢复后同样作为结果未知处理。
- 不自动重试，不自动释放未知订单预算，不自动续接未知订单。
- 有不完整日志尾行或损坏记录时拒绝启动，不猜测、截断或跳过。
- 原型没有自动对账、赎回、转账或撤单功能。

结果未知时应根据记录中的订单哈希通过现有可信工具核实实际订单和成交，再由操作者决定如何继续。
不要删除日志来绕过未知订单或预算限制。
确认被交易所拒绝的尝试也保留预算占用，这是为了减少原型中的资金状态分支。

## 实盘准备

本项目交付验证没有使用真实账户，也没有提交实盘订单。
实盘仅支持第二版结果代币订单，以及普通外部账户、代理钱包和 Safe 钱包的签名类型 0、1、2。
新存款钱包的签名类型 3 和第三版仓位订单暂不支持。

1. 复制 `config.demo.json` 为 `config.live.json`，设置实际结果代币白名单、每单预算和累计尝试预算。
2. 将日志路径改为独立的 `data/live-orders.jsonl`，不能复用模拟日志。
3. 提供已存在的账户凭据，不会自动创建凭据或执行链上授权。
4. 确认该账户的资金和交易授权已准备好。
5. 设置至少 32 字节的本地访问口令。

所需环境变量列在 `.env.example`。
真实值只放环境或未跟踪的 `.env` 文件，不要提交进版本控制。
如果使用 `.env`，需要由终端显式加载；程序不会自动读取它。

```sh
set -a
source .env
set +a
cargo run --release -- serve config.live.json --live
```

实盘服务只有收到符合配置的信号才会下单。
`send` 命令在连接实盘服务时会触发真实交易，也要求相同的 `DEMO_ACCESS_TOKEN` 环境变量。
执行端固定为 `https://clob.polymarket.com`，本地模拟的公开测试密钥禁止在实盘入口使用。

## 计时口径

| 字段 | 含义 |
| --- | --- |
| `queue_ms` | 请求体解析进入处理器，到开始执行信号 |
| `sign_ms` | 构造订单与签名 |
| `journal_ms` | 等待日志写入线程、去重和磁盘同步 |
| `dispatch_ms` | 进入信号处理器到开始交易所请求，包含上述阶段 |
| `total_ms` | 进入信号处理器到判定提交结果，未包含最后一次结果日志同步 |

`dispatch_ms` 是应用提交边界，不是网卡发出数据包的硬件时间戳。
信号中的观察时间用于过期判断，与上述单进程单调时钟计时分开。
收到提交回报不等于成交或链上结算完成。

## 验证

```sh
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
cargo fmt --check
cargo run --release --locked -- demo 67
```

端到端测试通过真实本机请求接口覆盖第二版密码签名、请求认证、重复请求、修改同编号内容、过期、错误价格步长、不可信盘口、代币白名单、预算耗尽和重启。
另覆盖并发上限、发送超时后不重试、未知状态跨重启保持停止、日志文件锁和不完整记录。
所有检查均使用本机模拟交易所，不访问实盘账户。

## 后续扩展点

当前只有一个进程，直接使用 Tokio（异步运行库）的有界通道即可。
NATS（消息总线）和 PostgreSQL（关系数据库）在拆分多个服务、多个执行者或需要共同资金账本时接入。
不要用多个独立日志的实例模拟多账户扩容。
先补独立行情更新与对账，再扩大实盘范围。

协议依据：[官方接口与认证](https://docs.polymarket.com/getting-started/api)、[官方交易入口](https://docs.polymarket.com/trading/overview)、[固定版本的 Rust 客户端](https://docs.rs/polymarket_client_sdk_v2/0.8.0/polymarket_client_sdk_v2/)。

## 本次本机验证结果

发布版本连续输入 67 条信号，67 条全部通过密码签名和请求认证并被模拟交易所接受，无拒绝。
模拟交易所每单额外等待 50 毫秒再回复。
从信号处理器入口到开始提交，中位数为 3.36 毫秒，95% 不超过 3.78 毫秒，最大为 11.04 毫秒。
这些数值包含订单构造、签名与日志同步，排除公网传播与真实交易所处理，不外推为实盘保证。

本次 `cargo test --locked` 的 5 项测试、严格静态检查与格式检查全部通过。
原始逐单结果保存在 [data/demo-c27d1a41-ba61-4f8a-83fc-4858172eca55/result.json](data/demo-c27d1a41-ba61-4f8a-83fc-4858172eca55/result.json)。
实盘提交路径已经实现，但没有读取真实凭据、提交实盘订单或验证真实成交。

## MP 独立容器部署

在本项目目录运行下面的命令。
部署目标为 `amster-p`，构建与测试全部在 `brahma`，本机只传送已提交的源代码和发起部署。
镜像使用现有 Vultr 仓库 `ams.vultrcr.com/polym/rust-demo`，无需新建 GitHub 镜像仓库或启动本机 Docker。

| 命令 | 作用 |
| --- | --- |
| `./deploy.sh --dry-run` | 显示待部署分支、提交、镜像和目标，不请求凭据或修改服务 |
| `./deploy.sh` | 远程测试、构建、推送、拉取固定摘要、更新 Compose、验证并汇报资源 |
| `./deploy.sh --build-only` | 在 Brahma 测试、构建并推送，暂不部署 |
| `./observe.sh` | 一次读取热路径耗时、结果计数、健康与容器资源；默认只读 |
| `./observe.sh --seconds 15 --window 600` | 采样 15 秒资源，统计最近 10 分钟保留的计时 |
| `./observe.sh --json` | 输出可保存或供其他工具读取的结构化结果 |
| `./observe.sh --seconds 10 --exercise-demo 67` | 明确向模拟服务发送 67 个信号，同时测量；拒绝在实盘模式执行 |

源代码必须先提交且工作目录干净。
部署按当前分支的精确提交打包，不传递未跟踪文件、真实 `.env` 或本地交易日志。
目前仓库没有远程代码仓库，源代码通过加密连接发送给 Brahma；镜像推送到既有镜像仓库。

构建阶段在容器内执行全部测试、静态检查和格式检查，通过后才产生运行镜像。
镜像标注源提交和分支，部署使用内容摘要固定版本，并验证架构和源提交。
已启用同项目构建缓存，首次构建比后续构建慢。

镜像仓库凭据通过现有 Vultr 配置取得，申请一小时有效的临时 Docker 凭据，只通过标准输入传输到临时目录，结束后清理。
也可以使用 `--registry-env <已有配置文件>` 或 `VULTR_REGISTRY_API_KEY` 环境变量。
脚本仅解析所需字段，不执行配置文件，不输出凭据。

远端目录为 `/opt/polym-rust-demo`，Compose 项目名为 `polym-rust-demo`。
当前只有 `trader` 服务，连接现有内部数据网络，后续配套服务可继续加入同一份 `deploy/compose.yaml`。
服务现运行真实数据旁路模式，执行目标仍是本地模拟接收端，接口映射为 MP 本机的 `127.0.0.1:18787`，没有公网入口，也不读取原系统的账户凭据。
模拟市场元数据固定不变，因此模拟服务不再因五分钟的元数据寿命而失效；实盘模式仍保留原有限制。

容器使用非管理员用户、只读根目录、独立持久数据卷，限制为半个处理器核心、256 MiB（二进制兆字节）内存和 64 个进程或线程。
容器日志最多两份，每份 5 MB（十进制兆字节）。
SIGTERM（容器停止信号）会进入停止新单并等待在途请求的流程。
部署不重建其他 Compose 项目，不停止旧交易服务，不删除数据卷。

每次部署保存新旧镜像和 Compose 配置到远端 `releases` 目录。
新容器验证失败时尝试恢复上一份配置和镜像，数据卷保持不变，命令仍返回失败。
首次部署失败时没有旧版本可回退，需要依据错误修复。

### 观测口径

程序的 `/metrics` 接口需要访问口令。
计数器覆盖本进程启动后已认证、已成功解析的信号；未认证、请求体格式错误和超过体积限制的请求不属于这些策略计数。
样本最多保留最近 2,048 次处理结果，命令会标注查询窗口是否因容量被截断。
重启后计数器和样本重新开始，持久订单日志仍然保留。
重复请求单独计数，不把旧订单耗时重新计入延迟分布。

观测包含排队、策略校验、签名、准备日志同步、提交前总延迟、信号年龄、请求至回报、结果日志同步、处理器整体耗时。
每段都显示样本数量、中位数、95% / 99% 分位及最大值。
未执行的阶段显示缺失，不使用零值代替。
提交边界是应用开始请求，不是网卡时间戳；信号年龄使用墙上时钟，不能与单调时钟的阶段耗时混淆。
容器处理器百分比以一个核心为 100%，观测同时显示半核上限；网络与磁盘读写是容器累计计数。
模拟交易所在部署配置中固定延迟 50 毫秒回复，以便观察异步等待；这不代表公网耗时。

## 真实数据旁路模式

部署命令当前启动 `shadow /app/shadow.json`。
输入订阅 `ober.*.best`、`uma:resolution`、`uma:dispute_price`、`uma:settle`，各有独立连接，不加入原消费者的分发组。
市场资料来自 Django 内网 `/api/trading-context/`，默认与 OBer 一样限定最近三小时的提案，之后每 30 秒核对。
新信号缺少市场资料或提案版本不一致时，在原有 200 毫秒信号寿命内定向补取，不等待全表扫描。
原型不修改原生产的订阅注册、市场或资金。
模拟提交结果会通过内网接口写入 Dashboard 的订单历史。

资格来自已提案、未结算、未争议的请求状态，并验证赢家代币。
请求轮次、事件位置、终态标记以及 Django 提供的按请求结算区块一起防止迟到数据恢复旧资格。
生命周期断连后必须重新完成上下文同步，过期或不明确的状态不执行。

内存判断包含当前标签拦截、争议、卖墙、价格、订单数量、价格步长、费用和收益条件。
沿用原消费者 60 秒市场冷却，模拟订单计数在三小时窗口内保留，并从独立日志恢复。
M5 估值在 Rust 内即时计算，使用 Django 维护的成交量、提案信息，信号中的赢家前五档盘口，以及从 OBer 内存 best 接口补读的另一侧买价。
同一套系数和时间曲线已用旧订单的输入及边界样本对照 Django 公式验证。
补读报价和必要的资料查询计入输入准备阶段，并受整个信号 200 毫秒期限限制，任何时候都不会过期后发单。
OBer best 接口拒绝未同步或被隔离的盘口，获取失败时跳过。
这种最小实现仍在输入准备时进行一次内网报价读取，并不是全程无网络读取的最终版本。
模拟订单次数仅统计独立模拟日志，不消耗或使用旧应用的实盘次数；总会话预算和市场冷却仍有效。
价格步长优先使用 OBer 的明确数值，与原策略一致，避免旧市场资料覆盖新步长。

金额上限为每次模拟 10 pUSD（平台美元代币）、独立模拟会话累计 10,000。
这是保守的原型会话限制，到达上限停止，不自动清空日志。
签名使用公开测试密钥，提交地址由程序固定为同进程的回环模拟服务，不加载真实账户环境变量。
一般与负风险两种第二版签名域都有离线校验。

`./observe.sh --json` 的 `sources` 包含连接、同步代次、候选数量、待补资料、生命周期事件计数、时间戳类型、最近模拟订单及最多 32 条候选拒绝样本。
`GET /markets` 返回最多 200 条内存市场预览及总数，需要同样的本地访问口令。
`--exercise-demo` 仅对人工 demo 模式开放，当前 shadow 模式会拒绝注入。
提交前的上游总延迟只统计能确认为 OBer 本机接收微秒戳的消息，交易所毫秒快照不混入该指标。
连接中断、源消息发布前的丢失及上游盘口正确性仍需要独立证据，本原型没有宣称完整无丢包或真实成交。

### Dashboard 模拟订单历史

真实 `ober.*.best` 信号通过当前内存条件后，完成签名并提交同进程模拟接收端。
准备记录和提交结果先写入独立持久日志，后台每两秒向 Django `/api/shadow-orders/` 补写最多 25 笔。
写入确认保存在单独的 `shadow-orders.history-acks.jsonl`，重启后继续未确认记录；接口按信号编号唯一去重。
确认文件与原订单日志分开，上一版本仍能读取原日志进行回退。
部署前已有的真实数据模拟订单也会自动补写。

在 Dashboard 的订单历史选择 `Dry Run`，策略输入 `rust_shadow`。
记录显示为 `Rust shadow`，不关联真实钱包，实际成交数量为零，不进入真实订单核对或该策略的真实限额计数。
展开订单可看模拟提交结果、原信号时间、提交时间及策略、签名、日志和模拟响应耗时。
列表的 `ober` 延迟是 OBer 接收到模拟提交；缺失这个时钟口径时显示空值，详情仍显示应用收到消息至提交的耗时。
记录时间是 Django 实际入库时间，历史补写不会伪造入库时间。
`./observe.sh` 同时显示已同步、待同步数量和最近同步错误。

## airdrop_224 实盘

`./deploy.sh --live-account airdrop_224` 在同一独立容器运行实时 NATS 消费及 Rust 直接签名提交。
默认 `./deploy.sh` 仍部署模拟模式，实盘必须显式选择账户。
凭据仅在 MP 从已有 Django 账户生成，保存在受限文件并作为只读 secret 挂载，不发送至 Brahma、镜像仓库或本机。
首次实盘部署会做该账户的链上现金与授权只读检查，并使用现有方式获取交易 API 凭据。
账户继续可供旧应用使用，不做独占分配。

实盘每笔最多 30 pUSD（平台美元代币），具体预算按独立配置中的 5 / 10 / 20 档位计算，每市场沿用当前次数上限。
按用户 2026-09-14 指示取消实盘累计额度限制，`deploy/live.json` 的 `total_budget_pusd: null` 表示不限制累计金额。
日志仍持续记录累计预留金额，包括拒绝或不明请求，不能把它当作实际花费。
模拟配置仍有累计额度限制；单笔额度、余额检查、信号寿命和不明提交暂停规则不变。
买入金额按旧应用规则向下保留两位小数，例如 `0.999 × 5` 提交 `4.99`。
订单历史使用签名中的现金金额，避免用取整后的股数反推金额；旧日志重试仍保持原回执格式。
交易所的 `error` 和 `errorMsg` 统一保存为经过凭据遮蔽、长度限制的 `errorMsg`，并显示在失败原因中。
实时输入、市场状态、账户就绪状态或历史同步异常时停止产生新单。
网络回报不明时保留占用并停止，不会自动重发。
交易所明确返回 FAK（即时成交、未成交部分取消）无对手单时，即使回报带有同一订单编号，也只记录本单未成交并继续处理新信号。
此判断要求订单编号一致、没有成交数量或交易证据，也没有成功回报；编号冲突、网络错误和服务端异常仍进入暂停核对。
实盘使用独立的 live-airdrop-224-orders.jsonl，模拟记录不会作为实盘重放。

实盘回报通过有账户范围的签名凭证写入 `/api/rust-live-orders/`，显示为 Rust live，接入已有订单核对流程。
接受回报仅记为已提交，成交必须由实际执行证据确认。
实盘部署验证失败时保留容器和日志供检查，不自动回滚而掩盖可能发生的真实提交。
`./observe.sh` 自动读取当前模式及其受限访问凭证，显示订单同步、预算使用和延迟。

### 生命周期缓存清理与暂停原因

每次完整市场资料同步成功后，清理已不在当前市场表、超过三小时没有更新、且没有未解决争议的生命周期缓存。
到达 20,000 条保护上限时先执行相同清理；仍超限才停止，不自动忽略有效市场或争议。
结算和争议的终态证据继续保留，以拒绝迟到提案和落后的市场快照；该证据计数及内存用量需要持续观察。
`./observe.sh` 现在显示独立的 `stop_reason`，后续资料同步成功不会擦掉第一次暂停原因。
结构化观测新增 `lifecycle_marks`、`lifecycle_pruned` 和 `lifecycle_terminal_proofs`，分别表示当前缓存、累计清理数量和保留的终态证据数量。
修复于 2026-09-15，故障表现为容器健康但 `ready=false`、`stopped=true`、订单停止增长。
交易参数、单笔额度、累计额度设置及未知订单的暂停规则不变。

## 独立 Rust UMA 与运行监控

`./deploy.sh --live-account airdrop_224` 同时部署 MP 上的 `uma` 与 `trader` 两个独立容器，并在 Brahma 部署 `rust-224-monitor`。
所有镜像构建、测试和推送均在 Brahma 完成，采用当前干净分支的精确提交。
UMA 配置仅保存在 MP 的受限文件，当前使用独立核验过的公共节点，不需要账户或交易凭据。

Rust UMA 监听两路链上连接，同时每两秒检查最新区块并扫描最近 64 个区块的日志。
只解码已有两个 UMA 合约及受支持的 Polymarket 适配器发布的 ProposePrice、DisputePrice 和 Settle。
重启时重建最近四小时状态；缓存按链上时间滚动删除，不依靠不断增加市场数量上限维持运行。
每批日志与区块头由同一节点核对，不一致时换节点验证；已核对区块发生重组时使数据失效，重建后才重新提供可交易资格。
连接静默时仍由独立扫描补充事件；扫描过期、节点区块过期或补抓失败时，旁路拒绝新单。
新服务使用专用 `rust.uma.events` 消息主题和内网 `/snapshot`，不向旧应用的 UMA 通道发布消息。
消息编号缺口、服务代次变化或重组要求重新加载快照，普通重复消息不会重复执行。
旁路每三十秒用完整快照替换生命周期缓存，包括终态证据，避免永久累积。

Django 继续提供市场信息、标签、费率、策略参数以及额外的争议和结算保护。
链上事件直接更新已有市场的内存状态，新市场资料按需补取。
OBer 仍是共享的订单信号来源；本次没有重写 OBer 或旧应用的市场订阅流程。
交易参数、每单额度、同市场次数限制及未知提交保护保持不变。

`./observe.sh --json` 包含 `sources.uma_source`、`sources.native_uma`、账户就绪年龄、日志容量和未知订单数，并汇总 Brahma 监控状态及两个运行服务的资源快照。
MP 本机 `18788/health` 提供 UMA 诊断数据，`18788/ready` 只有在数据可用时才成功。
交易容器的就绪检查也使用 `/ready`，不再把进程存活当作交易就绪。

监控在 Brahma 每十五秒读取一次 MP 的受限只读快照，因此 MP 宿主机不可达时仍可告警。
监控容器启用 Docker 自带的进程回收，限制为 0.25 核、96 MiB 内存，避免连接子进程积累。
监控经已有白名单中的 Stockholm 出口连接 MP，保持原有地址限制不变。
专用连接密钥在 Stockholm 只能转接 MP 的固定端口，在 MP 只能运行固定的监测读取程序，禁止交互命令及任意端口转发，不携带交易凭据。
策略明确暂停时立即告警。
运行中的未确认计数持续三十秒后通知核对，其他订单继续交易；连接中断、数据过期和历史积压也按持续时间确认，避免短暂抖动刷屏。
同一事故通知去重，持续事故每三十分钟提醒一次，连续恢复三十秒后发送恢复通知。
没有成交本身不会触发告警，正常的策略过滤不视为故障。
告警状态和最近快照保存在 Brahma `/home/anchen/ops/rust-224-monitor/state`，Pushover 接受消息后才记为已通知。
监控只报告，不解除交易保护、不重发订单，也未接入 agent。
监控自身所在的 Brahma 整机不可用时无法自行发送告警，这需要另一台外部探测器。

Pushover 通知格式参考[官方消息接口](https://pushover.net/api)，事件解码使用[Alloy 的类型化事件接口](https://docs.rs/alloy-sol-types/latest/alloy_sol_types/trait.SolEvent.html)。

## UMA 替换门槛测试

替换测试位于 `tests/uma_replacement/`，只使用回环 RPC（链节点接口）、固定链上日志和隔离 NATS（消息总线）及 Redis（内存数据库）。
它们不读取账户凭据，不向交易所发单，也不连接生产服务。

从项目目录运行以下命令，源码会传到 Brahma 编译并执行普通回归及全部替换门槛测试：

```sh
bash tests/uma_replacement/run-brahma.sh
```

需要已有的 `brahma` 连接别名及免交互 Docker 权限。
编译期间允许下载依赖，测试运行容器使用 `--network none`，内部启动真实消息服务，不挂载生产目录、凭据或宿主机端口。
门槛测试逐项独立进程执行，避免共用消息主题造成并行串扰。
测试结果、逐项日志、编译日志及完整源码包保留在 Brahma，并复制结果到本机临时目录；命令结束时显示两个位置。
退出码 0 表示全通过，1 表示回归或验收不通过，2 表示测试运行环境异常；构建或连接错误也会非零退出，以日志为准。
普通 `cargo test` 会跳过替换门槛，不能用普通套件通过代替切换验收。

2026-09-15 在 Brahma 的隔离运行结果为普通回归 24 项通过，替换验收 17 项中 10 项通过、7 项失败。
失败包括 RequestPrice（请求价格）未支持、旧消息未投递、旧字段缺失、错误主节点阻碍备用恢复、静默空响应仍就绪、四小时外缺口未重放、未变化窗口重复下载。
详见 [复审与实际失败清单](tests/uma_replacement/REVIEW.md)。
本套件验证解码、状态处理及回环故障注入，不等于完整生产替换证明。
实际进程崩溃后的持久游标、消息服务停机后的可靠重放、旧消费者入库、长时间静默和高峰长稳仍需独立验收。

## Rust live sizing and million-order journal (2026-09-15)

`deploy/live.json` now owns the independent live sizing bands through `order_sizing`.
Prices below 0.05 use 5 pUSD; prices from 0.05 inclusive to 0.80 exclusive use 20; prices from 0.80 through 0.98 inclusive use 10; exactly 0.99 with fewer than 51 shares across the first five asks uses 20; all other eligible prices use 5.
The live per-order cap is 30, and the cumulative budget remains unlimited.
The ordinary 5-unit band and all non-sizing guards remain unchanged.
Django shared strategy amounts are not modified; shadow configurations without an override continue using the previous policy.
The configured bands must be positive and no greater than the configured cap.
The balance/readiness poll now also requires `trade_capacity_pusd` to cover the cap, using the minimum of cached cash and both exchange allowances.
Deploy the Dashboard receipt/capacity endpoint change before this client; a missing capacity field blocks new live orders.
This remains a periodic readiness check, not a shared concurrent funds reservation.

The journal allows 1,000,000 unique preparations.
Later result corrections for the same preparation are replayed in file order, matching the legacy journal, and do not consume another order or another budget reservation.
Original JSONL order and acknowledgement files remain authoritative and are not converted, deleted, or truncated.
A restricted `*.index.sqlite` file holds record offsets and indexes for deduplication, pending history, unresolved submissions, and recent orders.
The derived index is rebuilt from the authoritative files at every startup with an 8 MiB SQLite page cache; full old order payloads are no longer kept in memory.
Only the derived index is replaced on startup, so it is safe to rebuild after a crash.
The index uses no durability journal because its recovery source is the synchronously written JSONL audit.
All order lookup and history batch reads run on blocking workers.
History synchronization reads at most 25 pending records rather than scanning the full lifetime population.
Monitoring uses the reported journal capacity and alerts at 80 percent, now 800,000 orders.
Unknown submissions, malformed order logs, scope mismatches, and write failures retain fail-closed behavior.
Rollback to a binary with the old 50,000-order limit is only possible while the journal is still below that old limit.

The regular build runs the legacy-log recovery regression with 50,001 orders.
Run the same regression with `JOURNAL_SCALE_ORDERS=1000000 cargo test --locked --test journal_scale -- --nocapture` to check the requested capacity, pending-history lookup, deduplication, budget totals, acknowledgements, and reopening.
This generates synthetic local files and does not access production or the exchange.

The million-order synthetic check passed on the workstation with a 45.1 MiB maximum resident set.
First index recovery took 35.1 seconds; generation, recovery, queries, and a second reopen took 121.7 seconds in the debug test build.
These measurements are local test evidence, not a production latency guarantee.

### 信号一致性与局部更新

FAK（立即成交、剩余取消）允许部分成交，正的卖盘深度即可尝试；保持请求数量和预算上限，不要求放大后的整单都能立即成交。
这与 Django 使用向上取整计算可参与账户数的规则一致。
空卖盘先过滤，执行槽位繁忙时使用消息订阅已有的有界缓冲，在原信号 200 ms 有效期内等待；过期信号不会重放。
跳过记录包含市场、代币、原始信号时间和原因，写入既有容器日志，最近 256 条也可从观测命令读取。
每市场的成交次数上限继续有效，老应用模拟单不占实际成交次数，因此它们可能与已成交的旁路出现合法差异。

只更新交易容器时运行 `./deploy.sh --live-account airdrop_224 --trader-only`。
该命令保留现有 UMA（预言机数据）容器镜像和监控容器，并检查运行版本是待部署版本的祖先，防止覆盖其他分支已上线的变更。

## Proposal-triggered second lane

The `rust_uma` lane shares the existing UMA service, queries Dashboard on each proposal, reads trusted OBer order books, applies the shared guards and local M5 model, and uses the same price/depth budgets and signing calculation as Rust live with an independent account.
Its receipts include both book snapshots and per-stage timings in Dashboard.
See [rust_uma implementation and activation](docs/rust_uma.md) and `deploy/compose.uma.yaml`.
The lane is disabled until an independent account and the compatible Dashboard receipt endpoint are configured.

### Dashboard global dry run

Both execution lanes independently subscribe to `trading.control.dry_run` on the configured NATS broker.
The payload is `{"schema_version":1,"source":"dashboard","dry_run_enabled":true,"triggered_at_ms":1789600000000}`.
An enabled message latches the current process into dry run; false messages never enable live trading.
The same-origin `/api/trading-control/` snapshot is checked at startup and every five seconds, after subscribing, to repair lost messages.
Until a fresh snapshot is available, or when the control connection fails, new live submissions are disabled.
Snapshots older than fifteen seconds are rejected.
This task starts for both lanes, including `rust_uma`, which has no OBer subscription.

The shared final submission check still signs and journals simulated orders as `dry_run`, without an exchange request or live-history export.
These forced simulations remain in the local durable journal and metrics; existing fixed-shadow execution keeps its mock and dashboard simulation receipts.
Health reports `mode=dry_run` for a live-configured process under this guard, and metrics expose `dashboard_dry_run` and `dashboard_dry_run_latched`.
Already submitted orders continue receipt processing.
To recover a latched live process, disable dashboard dry run and explicitly restart it under the normal trading authorization.
Deploy the dashboard publisher and snapshot endpoint before this consumer.
The isolated broker regression is `RUST_CONTROL_TEST_NATS_URL=nats://127.0.0.1:4222 cargo test --locked --test trading_control -- --ignored` and is included in the existing Rust checks workflow.

## 网球 O/U 暂停（2026-09-17）

OBer 与 UMA 两条路径在共用决策 guard 中拒绝网球总局数、分盘局数及总盘数大小盘，原因码为 `tennis_ou_paused`。
优先使用上下文中的 `sports_market_type`，缺少类型时用 Tennis 标签和 O/U 标题识别。
Django API 提供市场类型；PostgreSQL 路径使用已有 Tennis 标签和 O/U 标题，不扩大数据库读取权限。
胜负盘、让盘与其他体育大小盘不受影响。
这是临时无条件暂停，恢复需要明确修改并部署代码，不随 live/dry-run 开关解除。
