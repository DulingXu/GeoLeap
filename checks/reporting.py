"""Shared CLI/report format for local module checks, without remote services."""

from __future__ import annotations

import argparse
import json
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


class CheckReport:
    def __init__(self, module: str, description: str, output_dir=None):
        self.module = module
        self.description = description
        self.output_dir = Path(output_dir) if output_dir else ROOT / "output" / "core_checks" / module
        self.items = []

    def check(self, name: str, passed: bool, details=None):
        self.items.append({"name": name, "passed": bool(passed), "details": details})
        print(f"[{'PASS' if passed else 'FAIL'}] {name}")
        if details is not None:
            print("       " + json.dumps(details, ensure_ascii=False, sort_keys=True))

    def finish(self) -> int:
        passed = sum(item["passed"] for item in self.items)
        success = bool(self.items) and passed == len(self.items)
        payload = {
            "module": self.module, "description": self.description,
            "status": "PASS" if success else "FAIL", "passed": passed,
            "failed": len(self.items) - passed, "checks": self.items,
            "check_type": "local_functional_smoke", "network_used": False,
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        self.output_dir.mkdir(parents=True, exist_ok=True)
        (self.output_dir / "report.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        lines = [self.description, f"Status: {payload['status']} ({passed}/{len(self.items)})", ""]
        for item in self.items:
            lines.append(f"[{'PASS' if item['passed'] else 'FAIL'}] {item['name']}")
            if item["details"] is not None:
                lines.append(json.dumps(item["details"], ensure_ascii=False, sort_keys=True))
        (self.output_dir / "report.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")
        print(f"\n{payload['status']}: {passed}/{len(self.items)} checks")
        print(f"Reports: {self.output_dir.resolve()}")
        return 0 if success else 1


def run_report(module: str, description: str, callback) -> int:
    parser = argparse.ArgumentParser(description=description + "（仅本地功能检查）")
    parser.add_argument("--output-dir", type=Path, help="检查报告目录，生成 report.json 和 report.txt")
    args = parser.parse_args()
    report = CheckReport(module, description, args.output_dir)
    print(description)
    print("Local functional check; no external APIs.\n")
    try:
        callback(report)
    except Exception:
        report.check("检查脚本完整执行", False, {"traceback": traceback.format_exc()})
    return report.finish()
