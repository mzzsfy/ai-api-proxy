# 第三轮测试审计

## 适用范围与证据更正

审计基线为 a308e5d，Go 1.27.0、Windows amd64、CGO 开启。此次交付测试补强和缺陷报告，生产代码保持原样，下面四项产品缺陷尚未修复。

前两轮所谓“65 个变异全部杀死、测试已饱和”的结论无效。旧执行器构造 `tgt = ["go", "test", ...]`，实际执行 `subprocess.run(tgt[1:])`，运行的是系统 `test` 命令。实际复现输出为 `test: extra argument '-count=1'`、退出码 2、stdout 为空；旧分类器只要输出末行不以 `ok` 开头就计为 killed。因此它没有证明测试实际运行，更没有证明对应断言失败。旧 server 鉴权种子只增加 `_ = keys`，没有改变鉴权行为。

本轮读取旧脚本中的64个可恢复种子，修正部分明显不能定位/编译的写法后，用真实 `go test -json -count=1` 重跑，并单独编译测试包：

| 分类 | 数量 | 含义 |
|---|---:|---|
| 命名测试失败 | 40 | 已编译，出现具体测试 fail 事件 |
| 幸存 | 19 | 实际测试执行且通过；需进一步区分测试缺口与等价变异 |
| 无效种子 | 4 | 匹配不到或匹配不唯一，不计为 killed |
| 编译失败 | 1 | 不计为 killed |

此次重跑不是原“65”逐字节实验的重放，旧脚本与旧报告已经存在不一致。不能据此补算原实验成功率。

## 已复现的产品缺陷

### 编码 token 经订阅错误泄漏

`internal/subscribe/subscribe.go` 的 `authURL` 使用 `url.UserPassword` 编码用户信息，`maskToken` 却使用 `url.QueryEscape`。token 为测试字符串 `a b` 时，前者是 `a%20b`，后者是 `a+b`，掩码匹配不到。

本地复现：创建 `https://example.invalid/repo` 订阅，token=`a b`；使用引擎已有 `CheckURL` 注入点在 URL 检查后取消 context，使 git clone 在联网前失败。`Sync` 返回错误及数据库 `last_error` 均包含 `https://token:a%20b@example.invalid/repo`。这是合成凭据，不涉及真实密钥。

失败测试为 `TestRound3Probe_EncodedTokenRedaction`。修复应在 git 错误载体和 URL 用户信息编码边界统一脱敏，并同时检查返回值和持久化错误。

### 默认分支改名后订阅无法恢复

浅克隆 `--depth=1` 隐含单分支 fetch refspec。首次同步 main 后，在本地裸仓库创建 trunk，将远端 HEAD 指向 trunk，删除 main，再次同步，实际失败：

```text
fetch: git fetch origin: exit status 128: fatal: couldn't find remote ref refs/heads/main
```

失败测试为 `TestRound3Probe_DefaultBranchRename`。重新克隆的恢复逻辑只位于 reset 失败分支，fetch 已先返回错误，因而没有到达恢复逻辑。复现不依赖公网或第三方仓库。

### 在途目标请求修改已经取走的快照

`metrics.Recorder.EnterTarget` 的结束闭包持有计数器指针；`Snapshot` 将包含同一指针的 map 交给调用者后，结束闭包继续修改旧计数器。

复现序列：`end := r.EnterTarget("u", "t")` → `Snapshot()` → 保存目标计数器值 → `end(true)`。已经返回的快照从 `{Requests:1 Errors:0 MaxConc:1 concurrent:1}` 变为 `{Requests:1 Errors:1 MaxConc:1 concurrent:0}`。失败测试为 `TestRound3Probe_InFlightTargetDoesNotMutateTakenSnapshot`。

由此可确定已发布快照不稳定；与快照序列化并发时存在读写风险。新窗口错误归属、在途并发跨窗口如何计数需要在修复时统一定义，不能仅复制 map 后忽略闭包归属。

### 无后端的 StorageTx 提交触发 panic

复现序列分别为 `NewStorageTx(nil)` → `Set("k", "v")` → `Commit()`，以及 `Delete("k")` → `Commit()`。两个子场景均捕获到 `runtime error: invalid memory address or nil pointer dereference`，失败测试为 `TestRound3Probe_StorageWithoutBackendDoesNotPanic/set` 与 `/delete`。

无存储 HooksDeps 会包装出非 nil 的 StorageTx，成功任务的提交阶段可到达此路径。handler 也使用 nil base 作为纯缓冲并经 Snapshot/Merge 归并；修复应保留这种缓冲用途，在错误提交边界返回可处理错误，不能简单禁止所有缓冲写。

## 补强的既有行为

新增/强化测试覆盖：实时快照值和 map 隔离、非重置读、固定分钟键和三维 JSON；KV 覆盖值重开后仍持久；任务完成后重新调度、两个 worker 上限与排队执行、未来任务不提前、Next 不经 cron；302 终局保留、传输缺失失败回调、双向未声明形态拒绝；空模型会话键材料；IPv6 组播；单候选 n=1；指针存在但键池为空；登录锁到期恢复；短模型名与组前缀相似时正常路由；入口鉴权有效/无效双头及优先级。

`slotstore` 原快照测试允许其声称禁止的 `(旧值, authoritative=true)`。现仅允许完整前态 `(旧值,false)` 或完整后态 `(新值,true)`。该并发测试仍依赖调度，尚未通过 SQL 驱动屏障强制重现两次 SELECT 交错，不能称确定性证明。

## 不能用强杀解决的变异

- upstream 的 `>` 改 `>=`：前缀有唯一约束，相同长度但不同前缀不可能同时匹配同一完整模型路径。当前有效输入下等价，不补断言内部比较符的测试。
- pipeline 删除 MapEvent 的 ErrPoolBusy 特判：紧随的普通错误 fallback 发送相同 RawPass 帧并 continue，属于等价变异。
- admin 常时比较改为 `==`：普通功能测试无法证明时间常数，真实重跑幸存，不能伪称测试捕获时序泄漏。
- store 将连接数上限改为8：本地 WAL 和 busy_timeout 下既有并发测试仍成功；不能用断言私有配置常量冒充并发正确性证据。

## 新断言的实际变异复验

16个原幸存变异已在补强后触发具体行为断言失败，另新增一个真正禁用 server 鉴权的变异并由测试捕获：

| 变异 | 失败测试 |
|---|---|
| IPv6 组播放行 | TestForbidden |
| 实时快照直接返回内部 map | TestRecorder_SnapshotLiveReturnsDetachedCounters |
| 分钟格式误用秒 | TestRecorder_TakePreservesMinuteAndDimensionPayloads |
| 任务结束不释放 busy | TestScheduler_Round3_CompletedTaskRunsOnNextTick |
| worker 上限从2变64 | TestScheduler_Round3_WorkerLimitAndQueuedTasksRun |
| Next 任务进入 cron | TestScheduler_Round3_NextTaskWithValidCronDoesNotRun |
| 未来任务提前一分钟 | TestScheduler_Round3_FutureMinuteDoesNotRunEarly |
| KV 冲突时不更新 | TestKV_OverwritePersistsAfterReopen |
| 302 进入成功映射 | TestRun_RedirectIsTerminalWithoutSuccessMapping |
| 缺传输报告成功 | TestRun_MissingTransportReportsFailureWithoutRoundTrip |
| 未声明形态放行 | TestRun_UndeclaredEntryFormRejectsBeforeComponents（双向） |
| 空模型额外加入分隔符 | TestSessionKey_EmptyModelUsesSingleMaterial/empty |
| 组前缀不要求斜杠 | TestPick_ShortNameSharingPrefixDoesNotBecomeFullPath |
| n=1 被当作多候选 | TestOpenAICodec_InspectSingleChoice |
| 已有指针的空池取模 | TestKeys_PeekEmptyAfterRotation |
| 登录锁过期后仍继续锁定 | TestLogin_LockExpiresAndCorrectCredentialsRecover |
| server 鉴权分支整体跳过（新增种子） | TestKeyAuth_RejectsInvalidCredentialsBeforeHandler |

另修正无效种子后，未知长度 IP 放行由新增 TestForbidden_InvalidAddressFailsClosed 捕获；KV 命名空间删除、显式头覆盖、缺 model 放行由既有或新增测试捕获。pipeline 池忙种子仍未匹配，不算执行成功；等价判断来自控制流对照。

测试均先在原实现通过，再单独注入一个缺陷观察断言失败，最后按原始字节恢复生产文件。它们是既有行为特征测试，不宣称新增了产品功能。普通功能测试不用于强行杀死常时比较等不可观测差异。原始日志保留旧标签，正文已纠正语义：A3 实际把锁延长约一分钟，不是锁永不生效；C3 是多候选能力误报，不是多轮；T1 证明亲和键材料改变，没有证明哈希碰撞。

## 验证方式与边界

初始基线 `go test -race -coverprofile=... ./... -count=1 -timeout=180s` 所有测试包通过；最终工作树新增17个顶层测试函数并强化既有断言后，`go test -race ./... -count=1 -timeout=180s` 的18个测试包全部通过，退出码0，未报告数据竞争（本轮最终工作树实跑；临时原始日志为 `.work/round3-final-race.log`，此日志只作审计证据，不是复现前提）。`go vet ./...` 无诊断，`git diff --check` 通过，所有生产 Go 文件均已恢复、无 diff。前端无改动，`npm test` 为1文件10个测试通过。包内覆盖率不包括其他测试包对它的调用，不能据包内零覆盖直接认定没有端到端测试。

产品缺陷探针未作为默认套件的红测提交；本报告附录保留完整复现代码及命令，不依赖本地忽略区。报告明确保留缺陷状态，默认回归通过不代表这四项缺陷消失。后续修复应将对应复现迁入常规测试后再执行 RED/GREEN。

旧变异脚本只存在于本地临时目录，本轮执行器仍依赖这些旧种子，未作为可维护的全仓变异工具交付。原始日志用于本次审计核对，表格是此次实验记录，不承诺换环境后可自动重放全部旧种子。新增测试可直接由项目正常测试入口运行。

## 四项缺陷的完整复现

以下文件均为临时探针，应分别保存到标出的包内路径。使用项目现有测试 helper，不需网络或第三方服务。执行后预期失败；这些失败属于已知产品缺陷。检查结束移走这三个临时文件，避免混入默认绿测。原实现修复前不要通过 Skip 或改为断言错误现状来制造通过。

保存为 `internal/subscribe/round3_probe_test.go`：

```go
package subscribe

import (
    "context"
    "strings"
    "testing"
    "github.com/mzzsfy/ai-api-proxy/internal/store"
)

func TestRound3Probe_EncodedTokenRedaction(t *testing.T) {
    e, st, _, _ := newEngine(t)
    id, err := st.CreateSubscription(context.Background(), &store.Subscription{
        Name: "encoded", RepoURL: "https://example.invalid/repo", Token: "a b",
    })
    if err != nil { t.Fatal(err) }
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    e.CheckURL = func(string) error { cancel(); return nil }
    err = e.Sync(ctx, id)
    if err == nil { t.Fatal("expected canceled git failure") }
    sub, readErr := st.GetSubscription(context.Background(), id)
    if readErr != nil { t.Fatal(readErr) }
    if strings.Contains(err.Error(), "a%20b") || strings.Contains(sub.LastError, "a%20b") {
        t.Fatalf("encoded credential remains: returned=%s stored=%s", err, sub.LastError)
    }
}

func TestRound3Probe_DefaultBranchRename(t *testing.T) {
    gitAvailable(t)
    e, st, _, _ := newEngine(t)
    repo := seedRepo(t, "demo", "plugins/demo")
    id := createSub(t, st, "rename", repo, "plugins/demo")
    if err := e.Sync(context.Background(), id); err != nil { t.Fatal(err) }
    runGit(t, repo, "branch", "trunk", "main")
    runGit(t, repo, "symbolic-ref", "HEAD", "refs/heads/trunk")
    runGit(t, repo, "branch", "-D", "main")
    if err := e.Sync(context.Background(), id); err != nil {
        t.Fatalf("sync after branch rename: %v", err)
    }
    sub, err := st.GetSubscription(context.Background(), id)
    if err != nil { t.Fatal(err) }
    if sub.Branch != "trunk" { t.Fatalf("branch = %q", sub.Branch) }
}
```

保存为 `internal/metrics/round3_probe_test.go`：

```go
package metrics

import "testing"

func TestRound3Probe_InFlightTargetDoesNotMutateTakenSnapshot(t *testing.T) {
    r := NewRecorder()
    end := r.EnterTarget("u", "t")
    _, _, _, _, target, _ := r.Snapshot()
    before := *target["u/t"]
    end(true)
    if *target["u/t"] != before {
        t.Fatalf("taken snapshot mutated: before=%+v after=%+v", before, *target["u/t"])
    }
}
```

保存为 `internal/plugin/round3_probe_test.go`：

```go
package plugin

import "testing"

func TestRound3Probe_StorageWithoutBackendDoesNotPanic(t *testing.T) {
    for _, operation := range []string{"set", "delete"} {
        t.Run(operation, func(t *testing.T) {
            defer func() {
                if value := recover(); value != nil { t.Errorf("commit panicked: %v", value) }
            }()
            tx := NewStorageTx(nil)
            if operation == "set" {
                if err := tx.Set("k", "v"); err != nil { t.Fatal(err) }
            } else { tx.Delete("k") }
            if err := tx.Commit(); err == nil {
                t.Error("commit without backend should return an error")
            }
        })
    }
}
```

在仓库根目录运行：

```bash
go test ./internal/subscribe ./internal/metrics ./internal/plugin -run '^TestRound3Probe_' -count=1 -v -timeout=60s
```

需安装 git；分支测试用临时裸仓库，凭据测试在 URL 检查后主动取消，不会连接 example.invalid。本轮实际输出包括编码 token 同时留在 returned/stored、fetch 找不到 main、快照 before/after 不同，以及 set/delete 两个 nil panic。
