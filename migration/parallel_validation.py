#!/usr/bin/env python3
"""
parallel_validation.py — Phase 3 并行验证脚本
每日运行：比对新系统 API 输出与旧脚本 daily_report.json，偏差为零则通过

用法：
  python parallel_validation.py \
    --new-api http://localhost:8000 \
    --token <ADMIN_JWT_TOKEN> \
    --old-json /path/to/daily_report.json

偏差容忍：金额字段 <= 0.01 万元（浮点精度误差）
"""
import sys, json, argparse
from pathlib import Path
import requests
from deepdiff import DeepDiff

FLOAT_TOLERANCE = 0.01   # 万元


def load_old(path: str) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def load_new(api_base: str, token: str) -> dict:
    resp = requests.get(
        f"{api_base}/api/v1/dashboard/daily-report",
        headers={"Authorization": f"Bearer {token}"},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def compare(old: dict, new: dict) -> list[str]:
    """逐字段比对，返回所有偏差描述"""
    issues = []

    def cmp_float(path: str, v_old, v_new):
        if v_old is None and v_new is None:
            return
        if v_old is None or v_new is None:
            issues.append(f"[NULL差异] {path}: old={v_old}, new={v_new}")
            return
        diff = abs(float(v_old) - float(v_new))
        if diff > FLOAT_TOLERANCE:
            issues.append(f"[金额偏差] {path}: old={v_old}, new={v_new}, diff={diff:.4f}万")

    def cmp_pct(path: str, v_old, v_new):
        if v_old is None and v_new is None:
            return
        if v_old is None or v_new is None:
            issues.append(f"[百分比NULL差异] {path}: old={v_old}, new={v_new}")
            return
        diff = abs(float(v_old) - float(v_new))
        if diff > 0.1:  # 百分比允许 0.1 pp 误差
            issues.append(f"[百分比偏差] {path}: old={v_old}, new={v_new}, diff={diff:.2f}pp")

    # ── KPI 收款 ──────────────────────────────────────────────
    for period in ["daily", "weekly", "monthly", "fiscal_year"]:
        for key in ["value", "target", "completion_rate", "gap"]:
            v_o = old["kpi_payment"].get(period, {}).get(key)
            v_n = new["kpi_payment"].get(period, {}).get(key)
            cmp_float(f"kpi_payment.{period}.{key}", v_o, v_n)
        for key in ["wow_pct", "yoy_pct", "mom_pct"]:
            cmp_pct(f"kpi_payment.{period}.{key}",
                    old["kpi_payment"].get(period, {}).get(key),
                    new["kpi_payment"].get(period, {}).get(key))

    # ── KPI 净签 ──────────────────────────────────────────────
    for period in ["daily", "weekly", "monthly", "fiscal_year"]:
        for key in ["value", "gross_sign", "refund", "target", "completion_rate", "gap", "yoy_abs"]:
            v_o = old["kpi_signing"].get(period, {}).get(key)
            v_n = new["kpi_signing"].get(period, {}).get(key)
            cmp_float(f"kpi_signing.{period}.{key}", v_o, v_n)

    # ── 资金预警 ──────────────────────────────────────────────
    cmp_float("fund_warning.total_unarchived",
              old["fund_warning"].get("total_unarchived"),
              new["fund_warning"].get("total_unarchived"))
    cmp_float("fund_warning.total_unconfirmed",
              old["fund_warning"].get("total_unconfirmed"),
              new["fund_warning"].get("total_unconfirmed"))

    old_depts = {d["name"]: d for d in old["fund_warning"].get("departments", [])}
    new_depts = {d["name"]: d for d in new["fund_warning"].get("departments", [])}
    for name in set(old_depts) | set(new_depts):
        if name not in old_depts:
            issues.append(f"[新增部门] fund_warning.departments: {name}")
        elif name not in new_depts:
            issues.append(f"[缺失部门] fund_warning.departments: {name}")
        else:
            cmp_float(f"fund_warning.{name}.unarchived",
                      old_depts[name]["unarchived"], new_depts[name]["unarchived"])
            cmp_float(f"fund_warning.{name}.unconfirmed",
                      old_depts[name]["unconfirmed"], new_depts[name]["unconfirmed"])

    # ── 顾问净签榜 ────────────────────────────────────────────
    old_ns = {r["name"]: r for r in old.get("advisor_net_sign", [])}
    new_ns = {r["name"]: r for r in new.get("advisor_net_sign", [])}
    for name in set(old_ns) | set(new_ns):
        if name not in new_ns:
            issues.append(f"[顾问净签榜缺失] {name}")
        elif name not in old_ns:
            issues.append(f"[顾问净签榜新增] {name}")
        else:
            for key in ["net_sign", "gross_sign", "refund"]:
                cmp_float(f"advisor_net_sign.{name}.{key}", old_ns[name][key], new_ns[name][key])

    return issues


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--new-api",  required=True)
    parser.add_argument("--token",    required=True)
    parser.add_argument("--old-json", required=True)
    args = parser.parse_args()

    print("=" * 60)
    print("  广州前途财务日报 · 并行验证")
    print("=" * 60)

    old = load_old(args.old_json)
    print(f"  旧系统: {args.old_json}")
    print(f"  新系统: {args.new_api}")

    new = load_new(args.new_api, args.token)
    print(f"  执行日: {new.get('header', {}).get('execution_date')}")

    issues = compare(old, new)

    if not issues:
        print("\n  ✅ 全部字段验证通过，偏差为零！")
        print("  可以继续 Phase 3 并行验证计划。")
        sys.exit(0)
    else:
        print(f"\n  ❌ 发现 {len(issues)} 处偏差：")
        for i, issue in enumerate(issues, 1):
            print(f"  {i:3d}. {issue}")
        print("\n  ⚠ 请排查偏差原因后再继续。")
        sys.exit(1)


if __name__ == "__main__":
    main()
