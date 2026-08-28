# Agent Note: 会话持久化单写者守卫——锁 sidecar 加持久尾部核对

Status: implemented

[English](2026-08-19-single-writer-session-logs.md) | 中文

## 问题

会话日志是事件溯源的，`seq = log.length` 连续，而每个存活进程都从它**自己**的内存副本推导下一个 `seq`（`Session.seq` getter）。没有任何东西能阻止两个 dsh 进程挂载同一个 store：JSONL 的 `appendLines` 以 `'a'` 模式打开工件盲目追加，SQLite 的 WAL 也接纳并发写入者。这在生产环境中表现为一个 dsh-web 会话无法加载：`corrupt session log: seq gap in committed region at line 13166 (expected 90666, got 90662)`。该工件持有两个写入者的尾部：一个进程已种到 `session/end-seed` 并写入了被中断的 `tool/result@90662`，而第二个带着陈旧视图的进程十六分钟后写入了 `approval/decided@90662`——正是用户实际允许的那个分支。两个事件共享一个 seq，因此已提交区域不再连续。

修复里还藏着一个更隐蔽的缺口：`commitRepair` 信任了准备读取，因此一个在准备读取与修复之间已提交并退出的写入者，会把自己的行丢失给删除操作。

## 决策

**建议式锁 sidecar 指名写入者；活性决定接管。** 在对日志的首次写入（初始实体化、append 或修复）之前，后端用 `O_EXCL` 创建 `<artifact>.lock`，其中含 `{pid, createdAt}`。遇到已存在的 sidecar 时：同 pid 重新收养；已死的 pid（`kill(pid, 0)`）被移除并重新获取，并附带指明陈旧 pid 的警告；存活的外来 pid 拒绝写入并指名其 pid。一个无法解析为有效记录的 sidecar 会被拒绝而不是猜测：一个存活写入者写了一半的 sidecar 绝不该被当成陈旧。`release()` 只在 sidecar 载荷仍指名本进程时移除它，且是尽力而为；遗留 sidecar 由下一个获取者回收。

**首次写入核对持久基线；死亡写入者的撕裂尾部被丢弃。** 每个进程对每个日志一次，持久尾部被解码并与内存视图比对：在本进程之外前进、收缩或消失的尾部会响亮地拒绝，而不是分叉 seq 空间。一个由死亡写入者留下的结构不完整尾部会被截断到其完整记录，并连同通常的恢复 closer 一起重新 append。锁与核对恰好覆盖那些变更日志的操作——读取与列表从不取锁，进程内 `:memory:` SQLite 也不取。

**`commitRepair` 现在接收所观察到的 revision。** 该 seam 变为 `commitRepair(meta, tornMarker, closers, revision)`：后端在守卫下复核存储仍携带准备 `loadStored` 读取所观察到的 `revision`（JSONL 后端比对文件身份，SQLite 比对行 revision），然后才删除任何东西。一个期间已提交并退出的写入者会被检测到，其行得以幸存。

守卫逻辑是 `@deepseek-ai/dsh-session-persistence` 中的一个共享模块（`single-writer.ts`，以 `./single-writer` 导出），被两个第一方后端使用；磁盘格式未变（`SESSION_FORMAT_VERSION` 保持 0）。

## 后果

该事故的故障现在表现为第二次进程首次写入时响亮、自解释的拒绝——指名存活所有者的 pid，或前进/收缩/消失的尾部——而不是在下次加载时发现的损坏。用户受损的会话通过保留共享前缀（seq 0..90661）加已批准分支（seq 90662..98054）、丢弃未批准的 fork 尾部而获救；原工件保留为 `session.jsonl.zstd.corrupt.bak`。运行修复前代码的进程仍持有其陈旧的内存视图，因此必须重启它们才能从磁盘恢复修复后的日志，并随之取得该守卫。`commitRepair` 的 seam 变更是跨包接口变更：第三方 `PersistenceBackend` 实现必须新增 `revision` 参数。

该守卫刻意是本机局部的、建议式的：它协调共享同一 root 或数据库文件的 dsh 进程，对其他用户或工具直接写入工件不提供任何防护（SQLite README 既有的主体边界为数据库条目覆盖了同样的范围）。两个后端 README 与共享 seam README 都记录了该守卫及其边界。

## 曾考虑的替代方案

- **独占 OS 锁（`flock`/锁文件句柄）**——平台语义各异（release-on-close、fork 继承、跨 socket 行为），且一个从不干净退出的进程持有的锁仍需一套活性方案；sidecar 统一地提供了它，并可被人或工具阅读。
- **存在外来写入者时拒绝启动**——为了防御真正会损坏的那一个操作（第二个变更进程），却拒绝了合法拓扑（只读观察者、并行的读侧消费者）。
- **每次 append 都复核持久尾部**——每批次付出一次完整解码的代价；首次写入基线核对加锁持有已关闭陈旧视图的 fork，因为锁在整个进程生命周期内都归本进程，且每个新获取者都会重新核对。
- **共享协调服务**——为写入者选举增加一个跨进程存储或 socket，会给 harness 最关键的路径增加依赖与失败模式；一个建议式文件守卫能优雅退化（最坏情况：回到旧的损坏行为，而绝不会拒绝合法的单一写入者启动）。