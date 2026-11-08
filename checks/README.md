# GeoLeap 核心跑通检测

从仓库根目录执行 `python3 checks/run_core_checks.py`。所有检测只用 Python 标准库和本地声明工作流，不调用 API，不运行真实实验。

| 脚本 | 检测对象 |
| --- | --- |
| `check_a_subgraph_structure.py` | DAG、SESE 子图与边界 |
| `check_b_reliability_matching.py` | 硬检查、分组反馈、版本/epoch/bin 隔离 |
| `check_b1_screening_switches.py` | 四种 B 资格规则及 lookup/commit 一致 |
| `check_b_experiment_interfaces.py` | 冻结证据、独立 clone、实际命中捕获与后置学习 |
| `check_b2_runtime_feedback.py` | 自然出口反馈、支持量与后续请求生效 |
| `check_c_lookahead_planning.py` | 前瞻组合、失配、实际入口重检 |
| `check_c1_planning_switches.py` | 不缓存、贪心、Full 的真实执行路径与计时 |
| `check_c2_search_cost.py` | depth/beam、同信息完整搜索与规模限制 |
| `check_d_opportunity_residency.py` | 集合价值、字节预算、审计配额、pin 和版本 |
| `check_d1_residency_switches.py` | 四种驻留策略、真实访问与维护统计 |
| `check_d2_change_response.py` | 需求/经济画像变化、顺序执行和窗口/账本统计 |
| `check_core_integration.py` | B/C/D 联通与出口恢复 |

可使用 `--module c1_planning_switches` 等参数只运行一个模块，或 `--output-dir /tmp/geoleap-checks` 指定输出位置。
每个模块输出 `report.txt`、`report.json` 和相应明细；总入口生成 `summary.txt/json`。
这些数据是功能验证，不能作为性能、任务质量或实际费用收益结论。
