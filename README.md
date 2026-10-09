# mapreduce-go

用 Go 实现的 MapReduce:一个 coordinator 通过 RPC 把 map / reduce 任务分发给多个并行 worker,
并能容忍 worker 崩溃或变慢。基于 MIT 6.5840(原 6.824)Lab 1,附带一组基准测试与性能优化记录。

> **说明**:课程提供了启动程序、示例应用、测试和 RPC 框架骨架。本仓库中由我实现的部分是
> `mr/coordinator.go`、`mr/worker.go`、`mr/rpc.go`;`bench/` 是为性能实验补充的基准测试脚本和数据
> (脚本在 AI 辅助下编写)。完整的来源划分见下方"目录结构"。

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
| 中间文件格式 | 长度前缀的二进制:`<uvarint len><key><uvarint len><value>`,与内容无关,任意字符串(含换行、引号)都能无损存取 |

## 目录结构

```
mr/
  coordinator.go   我实现:任务状态、GetTask / ReportTask / Done
  worker.go        我实现:worker 主循环、doMap、doReduce、中间文件读写
  rpc.go           我实现:RPC 参数与返回值的定义
  mr_test.go       课程提供:测试(正确性、并行、提前退出、崩溃恢复)
  util.go          课程提供:测试辅助函数
mrapps/            课程提供:示例应用(wc、indexer、crash 等),编译为 Go plugin 供 worker 加载
main/              课程提供:mrcoordinator / mrworker / mrsequential 启动程序,及 8 本测试文本
bench/             补充:基准测试脚本 bench.sh 与各阶段的原始数据(results-*.csv)
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

7 项测试全部通过;此前的版本在 `-race` 下连续 3 轮无数据竞争,最终版本在 `-race` 下完整通过 1 轮。

## 性能实验

**环境**:2 vCPU 的 VMware 虚拟机,Linux,Go 1.22。worker 与 coordinator 在同一台机器上(不同进程,通过 Unix socket 通信)。
**程序**:`wc`(词频统计),不带 `-race` 编译。`nReduce` 固定为 10(由 `mrcoordinator.go` 决定)。
**数据**:`small` 为 8 个文件(3.2MB);`large` 为 80 个文件(32MB,小数据集复制 10 份)。
**方法**:每个配置重复运行并取平均(次数见下);每次运行都把输出与 `mrsequential` 的结果逐字节比对,全部一致。
**计时**:用"最后一个 `mr-out-*` 文件写完的时间"作为作业完成时间。`mrcoordinator` 每秒才轮询一次 `Done()`,
进程退出时间会被向上取整到整秒并多出 1~2 秒,不能反映真实耗时。

复现:`bench/bench.sh build && bench/bench.sh data && bench/bench.sh run 5`。

### 优化过程(large 数据集,作业完成时间,秒)

| 版本 | 1 worker | 2 workers | 4 workers | 数据文件 |
|---|---|---|---|---|
| ① 初始:JSON 中间文件、无缓冲、100ms 轮询 | 10.80 | 8.09 | 8.12 | `results-phases.csv`(3 次) |
| ② + map 写中间文件加 `bufio` | 6.76 | 4.38 | 4.16 | `results-bufio.csv`(3 次) |
| ③ + 中间文件改二进制格式(最终版) | **3.66** | **2.55** | **2.31** | `results-final.csv`(5 次) |
| 单机 `mrsequential`(同期) | 3.19 | | | `results-final.csv` |

最终版相对 ① 总耗时降低约 66%~72%;4 个 worker 时(2.31s)比单机(3.19s)快约 1.4 倍。small 数据集:4 个 worker 由 0.91s 降到 0.34s。

作业完成时间拆为 map 阶段与 reduce 阶段(large,4 workers):

| 版本 | map 阶段 | reduce 阶段 |
|---|---|---|
| ① | 5.62s | 2.50s |
| ② | 1.95s | 2.21s |
| ③ | 1.21s | 1.11s |

### 每一步为什么这么做

1. **先测量**。拆分阶段耗时后发现 map 阶段占总时间约 70%。
2. **`bufio`(②)**:`json.Encoder` 每编码一条记录就对文件 `Write` 一次,即一次系统调用,large 数据集约有 600 万条。
   加缓冲后 map 阶段耗时降低约 65%,reduce 阶段几乎不变,说明改动的效果确实落在被改的代码上。
   (系统调用次数的下降是根据阶段耗时推断的,没有用 `strace` 直接计数。)
3. **二进制格式(③)**:在 reduce 的各阶段临时加计时后发现,读取并解码 JSON 占 reduce 阶段的约 75%~80%,排序仅约 14%,
   于是改用长度前缀的二进制格式,避免 JSON 的反射与解析,解码耗时降为原来的约 1/4。
4. **没有收益、因此没有保留的尝试**:
   - 给 reduce 的读取也加 `bufio`:无收益,`json.Decoder` 内部本来就带缓冲;
   - 优化排序:不做,只占 reduce 的约 14%;
   - worker 轮询间隔 1s → 100ms:小数据集 2~4 个 worker 下降 6%~8%,large 数据集无明显收益
     (数据:`results-baseline.csv` 对 `results-wait100ms.csv`),收益有限,但保留。

### 观察与局限

- **分布式并不总比单机快**:small 数据集上最终版 4 个 worker 为 0.34s,单机为 0.30s。进程启动、RPC、中间文件读写等固定开销在小数据上占主导。
  MapReduce 的价值在于数据规模超出单机时的横向扩展,而不是单机提速。
- **加速比仍不是线性**:large 上 1→4 个 worker 只快 1.6 倍。虚拟机只有 2 个核,worker 共用同一块磁盘,map 到 reduce 之间有串行的阶段切换。
- 样本量较小(各配置 3~5 次),未做统计检验;各版本之间的差距远大于观察到的约 10% 的单次波动,结论可靠,但具体数字仅代表这台 2 核虚拟机。

## 已知局限

- coordinator 是单点,崩溃后作业整体失败。
- 中间文件走本地磁盘,不支持跨机器(需要共享文件系统并把 RPC 从 Unix socket 换成 TCP)。
- 二进制中间文件不是人类可读的,调试时不能直接 `cat`;读取方假设文件完整,依赖原子 rename 保证这一点。
- 没有实现备份任务(论文 3.6 节)来缓解落后者问题。
- `nReduce` 由课程提供的启动程序固定为 10。
