"""Run clearly named local functional checks and collect one summary."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
MODULES = (
    ("a_subgraph_structure", "check_a_subgraph_structure.py", "基础：子图结构"),
    ("b_reliability_matching", "check_b_reliability_matching.py", "B：可靠性匹配"),
    ("b1_screening_switches", "check_b1_screening_switches.py", "B1：筛选模式开关"),
    ("b_experiment_interfaces", "check_b_experiment_interfaces.py", "B：冻结证据与独立命中捕获"),
    ("b2_runtime_feedback", "check_b2_runtime_feedback.py", "B2：自然反馈与资格状态"),
    ("c_lookahead_planning", "check_c_lookahead_planning.py", "C：前瞻规划"),
    ("c1_planning_switches", "check_c1_planning_switches.py", "C1：执行规划消融开关"),
    ("c2_search_cost", "check_c2_search_cost.py", "C2：搜索范围与同信息参照"),
    ("d_opportunity_residency", "check_d_opportunity_residency.py", "D：缓存驻留"),
    ("d1_residency_switches", "check_d1_residency_switches.py", "D1：驻留策略消融开关"),
    ("d2_change_response", "check_d2_change_response.py", "D2：需求与经济画像变化"),
    ("core_integration", "check_core_integration.py", "B/C/D：联通检查"),
    ("memory_ledger", "check_memory_ledger.py", "内存：共享对象与事务预留"),
    ("evidence_memory", "check_evidence_memory.py", "内存：证据与预测表"),
    ("cache_manager_memory", "check_cache_manager_memory.py", "内存：队列与驻留管理"),
    ("cache_memory_budget", "check_cache_memory_budget.py", "内存：发布、读者与回退"),
    ("runtime_memory_failures", "check_runtime_memory_failures.py", "内存：运行时失败路径"),
    ("similarity_projection", "check_similarity_projection.py", "输入：字段投影与版本隔离"),
    ("input_encoder", "check_input_encoder.py", "输入：编码器接口与有界向量缓存"),
)


def main():
    parser = argparse.ArgumentParser(description="GeoLeap 核心模块本地跑通检查（不调用外部 API）")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "output" / "core_checks")
    parser.add_argument("--module", choices=("all",) + tuple(item[0] for item in MODULES), default="all")
    parser.add_argument("--verbose", action="store_true", help="显示每个检查项的完整输出")
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    results = []
    env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
    for module, script, label in MODULES:
        if args.module != "all" and args.module != module:
            continue
        output = args.output_dir / module
        output.mkdir(parents=True, exist_ok=True)
        for name in ("report.json", "report.txt"):
            (output / name).unlink(missing_ok=True)
        print(f"[RUN] {script} — {label}", flush=True)
        started = time.perf_counter()
        process = subprocess.run([sys.executable, str(ROOT / "checks" / script), "--output-dir", str(output.resolve())],
                                 cwd=ROOT, env=env, text=True, encoding="utf-8", errors="replace",
                                 stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        (output / "console.log").write_text(process.stdout, encoding="utf-8")
        try:
            report = json.loads((output / "report.json").read_text(encoding="utf-8"))
        except (OSError, ValueError):
            report = {"status": "FAIL", "passed": 0, "failed": 1}
        success = process.returncode == 0 and report["status"] == "PASS"
        result = {"module": module, "script": script, "label": label,
                  "status": "PASS" if success else "FAIL", "passed": report["passed"], "failed": report["failed"],
                  "exit_code": process.returncode, "output_dir": str(output.resolve()),
                  "check_elapsed_seconds": round(time.perf_counter() - started, 4)}
        results.append(result)
        print(f"[{result['status']}] {result['passed']} passed, {result['failed']} failed | {output}")
        if args.verbose or not success:
            print(process.stdout)
    success = all(item["status"] == "PASS" for item in results)
    summary = {"status": "PASS" if success else "FAIL", "check_type": "local_functional_smoke",
               "network_used": False, "passed": sum(item["passed"] for item in results),
               "failed": sum(item["failed"] for item in results), "modules": results}
    (args.output_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    lines = [f"GeoLeap core checks: {summary['status']}",
             f"Passed: {summary['passed']} | Failed: {summary['failed']}", ""]
    lines.extend(f"[{item['status']}] {item['script']}: {item['passed']} passed / {item['failed']} failed\n"
                 f"  {item['output_dir']}" for item in results)
    (args.output_dir / "summary.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"\n{summary['status']}: {summary['passed']} passed, {summary['failed']} failed")
    print(f"Summary: {(args.output_dir / 'summary.txt').resolve()}")
    return 0 if success else 1


if __name__ == "__main__":
    raise SystemExit(main())
