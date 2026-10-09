# mapreduce-go

用 Go 实现的 MapReduce:一个 coordinator 通过 RPC 把 map / reduce 任务分发给多个并行 worker,
并能容忍 worker 崩溃或变慢。基于 MIT 6.5840(原 6.824)Lab 1,附带一组性能实验。

> **说明**:课程提供了启动程序、示例应用、测试和 RPC 框架骨架。本仓库中由我实现的部分是
> `mr/coordinator.go`、`mr/worker.go`、`mr/rpc.go`;`bench/` 是我为性能实验补充的基准测试脚本和数据。
> 完整的来源划分见下方"目录结构"。

## 工作方式

```
输入文件 i ──Map──> 按 ihash(key) % nReduce 分桶 ──> mr-i-0 … mr-i-(nReduce-1)
reduce 任务 j:读取 mr-0-j … mr-(M-1)-j ──排序、分组──Reduce──> mr-out-j
```

- **Coordinator**(1 个)维护每个任务的状态(空闲 / 进行中 / 已完成)和派发时间,处理两类 RPC:
  - `GetTask`:worker 来要任务。map 全部完成前只派 map;之后派 reduce;全部完成返回 `exit`;暂时没有可派的返回 `wait`。
  - `ReportTask`:worker 汇报任务完成。
- **Worker**(任意多个)循环:要任务 → 执行 → 汇报。联系不上 coordinator 或收到 `exit` 即退出。

## 设计要点

| 问题 | 做法 |
|---|---|
| worker 崩溃或变慢 | 任务派出 10 秒仍未汇报就视为失败,重新派给其他 worker(只依赖超时,不需要心跳) |
| 崩溃时留下残缺文件 | 先写临时文件(`os.CreateTemp`),写完再 `os.Rename`,rename 是原子的,读方只会看到完整文件或看不到 |
| 同一任务被执行两次 | 任务是幂等的:重复执行产生相同内容的输出,重复汇报只是把状态再置为"已完成" |
| 并发访问共享状态 | RPC handler 在独立 goroutine 中运行,coordinator 的所有状态由一把 `sync.Mutex` 保护;测试带 `-race` |
| reduce 读到不完整的 map 输出 | map 阶段全部完成后才派发 reduce 任务 |

## 目录结构

```
mr/
  coordinator.go   我实现:任务状态、GetTask / ReportTask / Done
  worker.go        我实现:worker 主循环、doMap、doReduce
  rpc.go           我实现:RPC 参数与返回值的定义
  mr_test.go       课程提供:测试(正确性、并行、提前退出、崩溃恢复)
  util.go          课程提供:测试辅助函数
mrapps/            课程提供:示例应用(wc、indexer、crash 等),编译为 Go plugin 供 worker 加载
main/              课程提供:mrcoordinator / mrworker / mrsequential 启动程序,及 8 本测试文本
bench/             我补充:基准测试脚本与实验数据
```

## 运行与测试

```bash
make mr                      # 全部测试(带 -race)
make RUN="-run Wc" mr        # 单个测试:Wc / Indexer / MapParallel / ReduceParallel / JobCount / EarlyExit / CrashWorker
```

手动运行(第一个参数是 socket 路径):

```bash
cd main
go build -buildmode=plugin ../mrapps/wc.go
go run mrcoordinator.go /tmp/mr-sock pg-*.txt     # 终端 1
go run mrworker.go wc.so /tmp/mr-sock             # 终端 2、3 …
```

7 项测试全部通过,并在 `-race` 下连续 3 轮无数据竞争。

## 性能实验

**环境**:2 vCPU 的 VMware 虚拟机,Linux,Go 1.22。worker 与 coordinator 都在同一台机器上(不同进程,通过 Unix socket 通信)。
**程序**:`wc`(词频统计),不带 `-race` 编译。`nReduce` 固定为 10(由 `mrcoordinator.go` 决定)。
**数据**:`small` 为 8 个文件(3.2MB);`large` 为 80 个文件(32MB,小数据集复制 10 份)。
**方法**:每个配置运行 5 次取平均;每次运行都把输出与 `mrsequential` 的结果逐字节比对,全部一致。
**计时**:用"最后一个 `mr-out-*` 文件写完的时间"作为作业完成时间。`mrcoordinator` 每秒才轮询一次 `Done()`,
进程退出时间会被向上取整到整秒并多出 1~2 秒,不能反映真实耗时。

复现:`bench/bench.sh build && bench/bench.sh data && bench/bench.sh run 5`。

### 结果(作业完成时间,秒)

| 数据集 | 单机 mrsequential | 1 worker | 2 workers | 4 workers |
|---|---|---|---|---|
| small(3.2MB) | 0.27 | 1.15 | 1.00 | 0.97 |
| large(32MB) | 3.44 | 10.61 | 8.30 | 6.86 |

### 观察

1. **在单机上,分布式版本比顺序版本慢。** large 数据集上 4 个 worker(6.86s)仍慢于单机(3.44s)。分布式多出了单机没有的工作:
   所有中间键值对要编码成 JSON 写入磁盘,reduce 再读回并解码,另有 RPC 与进程启动开销。MapReduce 的价值在于数据规模超出单机时横向扩展,而不是在一台机器上提速。
2. **加速比远低于线性**:large 上 1→2 个 worker 加速 1.28×,1→4 个 1.55×。虚拟机只有 2 个核,超过 2 个 worker 后只是排队;worker 共用同一块磁盘;map 到 reduce 之间有串行的阶段切换。
3. **map 阶段是主要耗时。** 对 large 数据集拆分阶段(最后一个中间文件出现的时间为 map 结束):

   | workers | map 阶段 | reduce 阶段 | 合计 |
   |---|---|---|---|
   | 1 | 6.81s | 3.99s | 10.80s |
   | 2 | 5.72s | 2.38s | 8.09s |
   | 4 | 5.62s | 2.50s | 8.12s |

   map 占总时间约 70%,且 2 个 worker 之后几乎不再下降。

### 实验:worker 轮询间隔 1s → 100ms

worker 在没有可领任务时会睡眠后再询问。把睡眠从 1s 改为 100ms(`git tag baseline` 为改动前):

| 数据集 | workers | 1s | 100ms | 变化 |
|---|---|---|---|---|
| small | 1 | 1.15 | 1.15 | 0% |
| small | 2 | 1.00 | 0.94 | −6% |
| small | 4 | 0.97 | 0.89 | −8% |
| large | 1 | 10.61 | 10.50 | −1% |
| large | 2 | 8.30 | 8.28 | 0% |
| large | 4 | 6.86 | 7.98 | +16% |

- 1 个 worker 从不进入等待,结果不变,可作为对照。
- 小数据集上有小幅改善(6~8%),来自 map→reduce 切换处少等待。
- large 上几乎无收益,4 个 worker 时反而更慢。同一次测量里单机基准本身也有约 10% 的漂移,所以这个退化需要更多次重复才能确认;
  一个未验证的猜测是 2 核上空闲 worker 的高频轮询占用了 CPU。
- 结论:轮询间隔不是主要瓶颈。large 的耗时集中在 map 阶段的中间文件写入。

## 已知局限

- coordinator 是单点,崩溃后作业整体失败。
- 中间文件走本地磁盘,不支持跨机器(需要共享文件系统并把 RPC 从 Unix socket 换成 TCP)。
- 没有实现备份任务(论文 3.6 节)来缓解落后者问题。
- `nReduce` 由课程提供的启动程序固定为 10。
