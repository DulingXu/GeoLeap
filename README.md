# GeoLeap

GeoLeap 的核心工作流缓存实现，包含子图结构检查、记录级可靠性筛选、执行规划和空间受限的驻留管理。

本仓库发布可独立运行的核心代码、消融开关和离线检测。运行核心不需要第三方依赖、API key 或数据集。

## 快速运行

需要 Python 3.10 或更新版本；本次发布使用 Python 3.13 验证。

```bash
git clone https://github.com/DulingXu/GeoLeap.git
cd GeoLeap
python3 checks/run_core_checks.py
PYTHONPATH=src python3 examples/core_demo.py
```

检测报告写入 `output/core_checks/summary.txt` 和 `summary.json`，每个模块有独立输出目录。检测仅调用本地函数。

## 核心模块

| 模块 | 内容 |
| --- | --- |
| `spec.py`、`dag.py`、`subgraph_extractor.py` | 声明式 DAG、子图结构与边界 |
| `records.py` | 不可变记录、缓存快照、pin 和版本化发布 |
| `reliability.py`、`b1.py` | 按记录版本、epoch、相似度区间隔离证据；Similarity / NoMargin / Min1 / Full |
| `planner.py`、`scheduling.py`、`c1.py`、`c2.py` | NoCache / Greedy / Full；有界前瞻与小 DAG 的同信息 Exhaustive 参照 |
| `residency.py`、`d1.py`、`d2.py` | LRU / LegacyDensity / AdditiveValue / Full，以及需求和经济画像变化驱动 |
| `runtime.py`、`observability.py` | 实际跳过缓存子图、恢复出口、下游续算、参考反馈和独立命中捕获 |

统一运行时名称为 `GeoLeapRuntime`：

```python
from workflow.runtime import GeoLeapRuntime, RuntimeConfig
from workflow.spec import OperatorSpec, WorkflowSpec

workflow = WorkflowSpec("demo", (
    OperatorSpec("increment", lambda state: {"y": state["x"] + 1},
                 read_fields=("x",), write_fields=("y",)),
))
runtime = GeoLeapRuntime(workflow, ())
result = runtime.run({"x": 1}, use_cache=False, collect_records=False)
assert result.state["y"] == 2
```

## 开关示例

以下命令只查看或导出配置：

```bash
PYTHONPATH=src python3 -m workflow.b1 --b1-variant full
PYTHONPATH=src python3 -m workflow.c1 --c1-variant no_cache
PYTHONPATH=src python3 -m workflow.c2 --c2-variant bounded --c2-depth 3 --c2-beam-width 8
PYTHONPATH=src python3 -m workflow.d1 --d1-variant full --d1-capacity-mib 4
PYTHONPATH=src python3 -m workflow.d2 --d2-stream demand_shift --d2-variant full
```

Python 中可用相应配置类的 `make_runtime` / `make_planner` 构造执行对象。`C1Config` 和 `C2Config` 固定缓存用于对照；需要在线积累证据时，使用 `GeoLeapRuntime` 或 `B1Config` 并保持 `fixed_cache=False`。

## 实现范围

执行器当前为单请求、串行实现；多 worker 调度仅用于规划器静态诊断。Exhaustive 在规模超限时明确报错。
可靠性分数是分组历史与经验余量，不构成严格失败概率保证。缓存内存指标是序列化载荷加元数据预留，不是进程 RSS。
静态费用画像与实际 API 账单分开；外部服务通过算子函数和观测接口接入。LegacyDensity 是旧密度公式在当前记录框架上的适配。

详细检测对象见 [checks/README.md](checks/README.md)。
