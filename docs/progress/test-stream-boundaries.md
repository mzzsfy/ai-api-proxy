# 取消与流式边界补查

基线 `b0c417e`。本轮只新增测试、独立复现与问题记录，未修改生产代码；第三轮报告中的四项产品缺陷仍未修复。本轮再确认四项生产行为问题，以及一个仅限辅助函数停止契约的问题。不能由默认测试通过推导这些缺陷已修复。

## 已确认问题

| 优先级建议 | 条件与预期 | 当前可观察结果 | 复现测试 |
|---|---|---|---|
| 高 | 快速路径收到上游 `429 + text/event-stream`，应保留错误状态 | 下游收到200及`data: rate limited`，请求日志也记200 | `TestStreamBoundaryProbe_FastPathPreservesSSEErrorStatus` |
| 高 | 普通管道收到错误状态SSE，终局返回后生产者应能退出 | 传输层先启动分帧，管道丢弃Events；帧队列满后生产者阻塞在发送，即使关闭body也不退出 | `TestStreamBoundaryProbe_TerminalSSEDoesNotLeaveBlockedProducer` |
| 中 | handler Event/End/ErrorFrame 写入失败，应返回错误让执行器感知 | 三个调用均已实际尝试写入，底层返回合成错误，sink仍返回nil | `TestStreamBoundaryProbe_HandlerPropagatesWriteFailure`（三个子场景） |
| 中 | keys-check单并发首键触发取消后，排队键应停止派发 | 共9个键全部调用传输接口，原本只应派发首键 | `TestStreamBoundaryProbe_CanceledKeysDoNotDispatchQueuedWork` |
| 辅助函数契约 | FrameSSEResult的stop关闭后应退出（按函数现有注释） | 上游等待下一次读取时，仅关stop不会关闭通道；需额外关闭body | `TestStreamBoundaryProbe_StopExitsSSEProducer` |

### 状态误报与流通道泄漏是不同路径

快速路径在 `internal/gateway/gateway.go` 的 fastPath 按 Content-Type 进入 passthroughStream，后者无条件 WriteHeader(200)。本地 httptest 上游返回429，实际网关入口向下游转发200；这一项已有真实本地HTTP调用证据。

普通管道的 `internal/transport/stream.go` consumeResponse 对错误状态的SSE也立即启动frameSSE，随后 `internal/pipeline/executor.go` Run 对状态≥300使用terminalResponse(resp.Body)，不消费Events。frameSSE向容量有限的通道无条件发送；队列满后，取消网络读取也不能释放已经阻塞的发送。探针复用真实consumeResponse与Executor.Run，用io.Pipe产生完整帧；关闭body后通过记录生产者自身的Close副作用确认它仍未退出，最后主动排空通道清理。没有用测试超时或进程崩溃冒充泄漏证据。

错误SSE在该路径的Body为空，源代码表明终局可丢失错误正文；本轮终局探针重点断言生产者退出，没有独立断言正文恢复。不能把快速路径的429变200套到普通管道：普通管道的状态仍是429。

### 取消与出站请求的证据边界

KeysCheck获取信号量时没有监听ctx，获得名额后也没有取消检查，仍Resolve并运行管道。探针让首个假传输调用同步取消ctx，并发设为1，因此后续8个派发发生在取消之后。测试证明的是传输接口仍被调用；真实HTTP传输使用NewRequestWithContext，可能拒绝已取消请求。本轮没有证明取消后真实上游收到9次请求，也没有执行JS/handler存储副作用探针。

已有server取消测试只有两个键、默认并发四，不产生排队；因此该测试通过不能覆盖本轮缺陷。真实JS热池取消、handler冷/热池副作用仍是明确的测试缺口，不计为已复现产品缺陷。

### 写失败与stop的限定

handlerSink的Event、End、ErrorFrame都调用不返回错误的writeSSE，写错误被丢弃。探针使用实现http.Flusher的ResponseWriter，Write返回合成错误并记录写入次数，防止“没有实际写入也判失败”的假阳性。这只证明直接Write错误丢失；Flush失败检测本轮未验证。

FrameSSEResult的stop只作用于发送select，不能中断scanSSE正在进行的Read。其生产调用者outboundSource.close会同时关闭stop、body并cancel，因此不能从辅助函数stop探针直接推导handler存在生产泄漏。另有常规绿测确认调用者按现有方式同时关闭body和stop时，已满队列的生产者能在消费者排空前自行退出。

生产侧遗留：`stream.go` 对FrameSSEResult的注释声称stop关闭后发射方退出，实现只在帧/错误发送点选择stop，注释相对实现过度承诺。该文档与实现不一致属生产问题，本轮不改生产源码，与`TestStreamBoundaryProbe_StopExitsSSEProducer`探针绑定记录。

## 可重复执行

已知缺陷探针位于两个测试文件：

- `internal/gateway/stream_boundary_probe_test.go`
- `internal/transport/stream_boundary_probe_test.go`

它们使用单独的`audit`构建标签，作为主动执行的已知缺陷复现入口；没有t.Skip，也没有将错误现状改成预期成功。两个文件头部均注明运行命令与迁移条件（缺陷修复后迁入默认回归套件并删除探针文件，不得仅删失败断言）。默认套件不加载这些探针；普通新增回归测试不带标签。探针分布：gateway文件含写失败传播（event/end/error三子场景）、429状态保留、取消后排队派发三项；transport文件含终局弃读生产者滞留、stop-only契约两项。

在仓库根目录运行：

```bash
go test -race -tags=audit ./internal/gateway ./internal/transport -run '^TestStreamBoundaryProbe_' -count=1 -v -timeout=45s
```

本轮实际退出码1。预期五个顶层探针全部失败，handler探针的event/end/error子场景分别失败：输出包含`write failure lost: <nil>`、`429 changed to 200`、`transport dispatches=9`、`body close did not release blocked channel send`和`stop did not terminate producer blocked on next read`。无DATA RACE，全部用例正常结束。失败后无需移走文件；构建标签只隔离独立审计入口，不能把默认绿测称为产品整体通过。

所有资源来自本地临时SQLite、httptest、io.Pipe、合成ResponseWriter或假传输，无真实外网。探针中synthetic.invalid仅作为Request字段，不进行DNS或HTTP连接。

## 常规回归补强与可证伪性

新增 `internal/transport/stream_contract_test.go` 的三个常规测试：

- 完整帧先送达，再按FIFO送达原始读取错误，关闭body。
- 自然EOF保留无空行的尾帧，兼容CRLF和多行data，保留额外前导空格，不伪造错误。
- 满队列背压下同时关闭body和stop，生产者须在消费者排空前退出；失败路径也释放资源。

三个测试均在原实现通过，并分别实际注入以下变异观察到具体断言失败，再逐项恢复生产源码：丢弃Err得到`terminal error ... Err:<nil>`；删除EOF flush得到`frames=[]`；stop分支仍阻塞发送得到`producer did not close body ... closes=0`。都为可编译、正常终止的行为失败，不是超时或基础设施错误。

## 验证与局限

定向常规race回归与带audit标签的五探针预期红已分别通过/复现。提交前最终验证实际结果(.work/stream-final-verify.log):三个新测试文件gofmt无输出,`go vet ./...` exit0,`go test -race ./... -count=1 -timeout=180s` 18个含测试包全部ok(gateway 96.9s、server 130.7s、plugin 81.4s),无DATA RACE。带标签五探针按预期失败的证据见上文audit命令小节。这是检查结果,不是修复交付,前轮与本轮共八项生产缺陷保持未修。

独立审查结论:文档审查会话8ff0d6fa完成六维核对,结论无内容阻断,认可写失败前证Write与排空前证producer退出两类假阳性防护及终局探针真实换行修正。代码审查会话ab91a1ba完成七维审查,全部PASS、无阻断项,确认断言确定性、失败路径资源不泄漏、取消计数串行化充分、错误写入器先证后断;非阻断建议采纳三条:audit运行方式与迁移条件落库探针文件头、stop探针注释补充handlerhttp双关闭豁免、帧数量改为缓冲数推导表达;verdict分布断言为可选增强未采纳;生产侧FrameSSEResult注释与stop语义不一致作为遗留问题记录于上文,不在本轮修复。审查改进后复验:双tag组合vet通过、三个契约测试定向race通过、gofmt清洁;最终字节的全量race与五探针复跑结果见 .work/stream-final-verify2.log:18个含测试包全部ok(full_exit=0),五探针全部按预期红(audit_exit=1),五条失败消息与audit命令小节一致。
