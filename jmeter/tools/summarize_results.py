#!/usr/bin/env python3
"""Summarize Apache JMeter CSV JTL results for Online Boutique tests."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import math
import os
import re
import statistics
import sys
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple


CORE_LABELS = [
    "T01_Home",
    "T02_Product_Detail_With_Reviews",
    "T03_Add_To_Cart",
    "T04_View_Cart",
    "T05_Change_Currency",
    "T06_Submit_Review",
    "T07_Verify_Review_Fragment",
    "T08_Verify_Review_Product_Page",
    "T09_Checkout",
]


@dataclass
class Sample:
    timestamp_ms: int
    elapsed_ms: float
    label: str
    response_code: str
    response_message: str
    success: bool
    failure_message: str
    bytes_received: int
    bytes_sent: int

    @property
    def end_timestamp_ms(self) -> int:
        return self.timestamp_ms + int(self.elapsed_ms)

    @property
    def is_negative(self) -> bool:
        return self.label.startswith("NEG_")


def parse_bool(value: str) -> bool:
    return str(value).strip().lower() == "true"


def parse_int(value: object, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(str(value)))
    except (TypeError, ValueError):
        return default


def parse_timestamp_ms(value: str) -> int:
    value = str(value or "").strip()
    if not value:
        return 0
    if value.isdigit():
        number = int(value)
        if number < 10_000_000_000:
            return number * 1000
        return number
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        return int(parsed.timestamp() * 1000)
    except ValueError:
        return 0


def read_jtl(path: str) -> List[Sample]:
    with open(path, newline="", encoding="utf-8-sig") as f:
        first = f.readline()
        if not first:
            return []
        f.seek(0)
        has_header = "timeStamp" in first and "label" in first
        if not has_header:
            raise ValueError("Only CSV JTL with a header row is supported.")
        reader = csv.DictReader(f)
        samples: List[Sample] = []
        for row in reader:
            timestamp = parse_timestamp_ms(row.get("timeStamp", "0"))
            label = row.get("label", "") or row.get("samplerData", "")
            sample = Sample(
                timestamp_ms=timestamp,
                elapsed_ms=float(parse_int(row.get("elapsed"), 0)),
                label=label,
                response_code=row.get("responseCode", "") or "",
                response_message=row.get("responseMessage", "") or "",
                success=parse_bool(row.get("success", "false")),
                failure_message=row.get("failureMessage", "") or "",
                bytes_received=parse_int(row.get("bytes"), 0),
                bytes_sent=parse_int(row.get("sentBytes"), 0),
            )
            samples.append(sample)
    return samples


def percentile(values: List[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    rank = math.ceil((pct / 100.0) * len(ordered)) - 1
    rank = max(0, min(rank, len(ordered) - 1))
    return ordered[rank]


def duration_seconds(samples: List[Sample]) -> float:
    if not samples:
        return 0.0
    start = min(s.timestamp_ms for s in samples)
    end = max(s.end_timestamp_ms for s in samples)
    return max((end - start) / 1000.0, 0.001)


def is_http_code(sample: Sample, start: int, end: int) -> bool:
    if not re.fullmatch(r"\d{3}", sample.response_code or ""):
        return False
    code = int(sample.response_code)
    return start <= code <= end


def is_connection_error(sample: Sample) -> bool:
    text = f"{sample.response_code} {sample.response_message} {sample.failure_message}".lower()
    return any(token in text for token in ["connectexception", "connection refused", "unknownhost", "no route to host"])


def is_timeout(sample: Sample) -> bool:
    text = f"{sample.response_code} {sample.response_message} {sample.failure_message}".lower()
    return any(token in text for token in ["timeout", "timed out", "sockettimeoutexception"])


def is_json_assertion_failure(sample: Sample) -> bool:
    return "json assertion" in (sample.failure_message or "").lower()


def is_page_assertion_failure(sample: Sample) -> bool:
    text = (sample.failure_message or "").lower()
    return any(token in text for token in ["page content", "text assertion", "response assertion", "expected text"])


def effective_success(sample: Sample) -> bool:
    if sample.is_negative and is_http_code(sample, 400, 499):
        return True
    return sample.success


def stats_for(samples: List[Sample], *, negative_expected: bool = False) -> Dict[str, object]:
    elapsed = [s.elapsed_ms for s in samples]
    total = len(samples)
    if negative_expected:
        success_count = sum(1 for s in samples if effective_success(s))
    else:
        success_count = sum(1 for s in samples if s.success)
    fail_count = total - success_count
    duration = duration_seconds(samples)
    received = sum(s.bytes_received for s in samples)
    sent = sum(s.bytes_sent for s in samples)
    return {
        "samples": total,
        "success": success_count,
        "failures": fail_count,
        "error_rate_pct": (fail_count / total * 100.0) if total else 0.0,
        "avg_ms": statistics.fmean(elapsed) if elapsed else 0.0,
        "min_ms": min(elapsed) if elapsed else 0.0,
        "max_ms": max(elapsed) if elapsed else 0.0,
        "median_ms": statistics.median(elapsed) if elapsed else 0.0,
        "p90_ms": percentile(elapsed, 90),
        "p95_ms": percentile(elapsed, 95),
        "p99_ms": percentile(elapsed, 99),
        "throughput_s": total / duration if total else 0.0,
        "received_kb_s": (received / 1024.0) / duration if total else 0.0,
        "sent_kb_s": (sent / 1024.0) / duration if total else 0.0,
        "4xx": sum(1 for s in samples if is_http_code(s, 400, 499)),
        "5xx": sum(1 for s in samples if is_http_code(s, 500, 599)),
        "connection_errors": sum(1 for s in samples if is_connection_error(s)),
        "timeouts": sum(1 for s in samples if is_timeout(s)),
        "json_assertion_failures": sum(1 for s in samples if is_json_assertion_failure(s)),
        "page_assertion_failures": sum(1 for s in samples if is_page_assertion_failure(s)),
    }


def read_events(path: Optional[str]) -> List[Dict[str, str]]:
    if not path or not os.path.exists(path):
        return []
    with open(path, newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def parse_event_time(value: str) -> Optional[int]:
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        return int(parsed.timestamp() * 1000)
    except Exception:
        return None


def phase_bounds(events: List[Dict[str, str]]) -> Optional[Tuple[int, int]]:
    fault_start = None
    fault_end = None
    for event in events:
        name = (event.get("event") or "").strip()
        timestamp = parse_event_time(event.get("timestamp_utc") or "")
        if timestamp is None:
            continue
        if name == "FAULT_START" and fault_start is None:
            fault_start = timestamp
        elif name == "FAULT_END" and fault_end is None:
            fault_end = timestamp
    if fault_start is None or fault_end is None or fault_end <= fault_start:
        return None
    return fault_start, fault_end


def samples_by_phase(samples: List[Sample], events: List[Dict[str, str]]) -> Dict[str, List[Sample]]:
    bounds = phase_bounds(events)
    if not bounds:
        return {}
    fault_start, fault_end = bounds
    phases = {"normal": [], "fault": [], "recovery": []}
    for sample in samples:
        if sample.timestamp_ms < fault_start:
            phases["normal"].append(sample)
        elif sample.timestamp_ms <= fault_end:
            phases["fault"].append(sample)
        else:
            phases["recovery"].append(sample)
    return phases


def write_summary_csv(path: str, rows: List[Dict[str, object]]) -> None:
    fieldnames = [
        "scope",
        "label",
        "samples",
        "success",
        "failures",
        "error_rate_pct",
        "avg_ms",
        "min_ms",
        "max_ms",
        "median_ms",
        "p90_ms",
        "p95_ms",
        "p99_ms",
        "throughput_s",
        "received_kb_s",
        "sent_kb_s",
        "4xx",
        "5xx",
        "connection_errors",
        "timeouts",
        "json_assertion_failures",
        "page_assertion_failures",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            formatted = dict(row)
            for key in [
                "error_rate_pct",
                "avg_ms",
                "min_ms",
                "max_ms",
                "median_ms",
                "p90_ms",
                "p95_ms",
                "p99_ms",
                "throughput_s",
                "received_kb_s",
                "sent_kb_s",
            ]:
                formatted[key] = f"{float(formatted.get(key, 0.0)):.3f}"
            writer.writerow(formatted)


def row(scope: str, label: str, samples: List[Sample], *, negative_expected: bool = False) -> Dict[str, object]:
    data = stats_for(samples, negative_expected=negative_expected)
    data["scope"] = scope
    data["label"] = label
    return data


def review_special_rows(samples: List[Sample]) -> List[Dict[str, object]]:
    labels = defaultdict(list)
    for sample in samples:
        labels[sample.label].append(sample)
    read_samples = labels["T02_Product_Detail_With_Reviews"] + labels["T07_Verify_Review_Fragment"]
    submit_samples = labels["T06_Submit_Review"]
    verify_samples = labels["T07_Verify_Review_Fragment"] + labels["T08_Verify_Review_Product_Page"]
    negative_samples = [s for s in samples if s.is_negative]
    return [
        row("review", "review_read", read_samples),
        row("review", "review_submit", submit_samples),
        row("review", "review_verify_display", verify_samples),
        row("negative", "NEG_Review_All", negative_samples, negative_expected=True),
    ]


def write_summary_md(path: str, rows: List[Dict[str, object]], events: List[Dict[str, str]], samples: List[Sample]) -> None:
    row_map = {(r["scope"], r["label"]): r for r in rows}
    overall = row_map.get(("overall", "all_samples"), {})
    normal = row_map.get(("overall", "normal_business_excluding_negative"), {})
    submit = row_map.get(("review", "review_submit"), {})
    read = row_map.get(("review", "review_read"), {})
    verify = row_map.get(("review", "review_verify_display"), {})
    negative = row_map.get(("negative", "NEG_Review_All"), {})

    def fmt(value: object, suffix: str = "") -> str:
        if isinstance(value, float):
            return f"{value:.3f}{suffix}"
        return f"{value}{suffix}"

    lines = [
        "# JMeter 性能测试汇总",
        "",
        "## 总体结果",
        "",
        f"- 全部样本数: {overall.get('samples', 0)}",
        f"- 正常业务样本数: {normal.get('samples', 0)}",
        f"- 正常业务错误率: {fmt(float(normal.get('error_rate_pct', 0.0)), '%')}",
        f"- 平均响应时间: {fmt(float(normal.get('avg_ms', 0.0)), ' ms')}",
        f"- P95: {fmt(float(normal.get('p95_ms', 0.0)), ' ms')}",
        f"- 吞吐量: {fmt(float(normal.get('throughput_s', 0.0)), ' samples/s')}",
        "",
        "## 评论专项",
        "",
        f"- 评论读取样本数: {read.get('samples', 0)}",
        f"- 评论读取 P95: {fmt(float(read.get('p95_ms', 0.0)), ' ms')}",
        f"- 评论提交样本数: {submit.get('samples', 0)}",
        f"- 评论提交成功率: {fmt(100.0 - float(submit.get('error_rate_pct', 0.0)), '%')}",
        f"- 评论提交错误率: {fmt(float(submit.get('error_rate_pct', 0.0)), '%')}",
        f"- 评论提交 P95: {fmt(float(submit.get('p95_ms', 0.0)), ' ms')}",
        f"- 评论提交 P99: {fmt(float(submit.get('p99_ms', 0.0)), ' ms')}",
        f"- 评论展示验证成功率: {fmt(100.0 - float(verify.get('error_rate_pct', 0.0)), '%')}",
        f"- 4xx 数量: {submit.get('4xx', 0)}",
        f"- 5xx 数量: {submit.get('5xx', 0)}",
        f"- 连接错误: {submit.get('connection_errors', 0)}",
        f"- 超时: {submit.get('timeouts', 0)}",
        f"- JSON 断言失败: {submit.get('json_assertion_failures', 0)}",
        f"- 页面内容断言失败: {verify.get('page_assertion_failures', 0)}",
        "",
        "## 负面评论用例",
        "",
        f"- 负面样本数: {negative.get('samples', 0)}",
        f"- 按预期 4xx 通过数: {negative.get('success', 0)}",
        f"- 未按预期通过数: {negative.get('failures', 0)}",
        "",
    ]

    phase_rows = [r for r in rows if r["scope"] == "phase"]
    if phase_rows:
        lines.extend(["## 事件阶段", ""])
        for phase in ["normal", "fault", "recovery"]:
            item = row_map.get(("phase", phase))
            if not item:
                continue
            lines.append(
                f"- {phase}: Average={float(item['avg_ms']):.3f} ms, "
                f"P95={float(item['p95_ms']):.3f} ms, "
                f"Throughput={float(item['throughput_s']):.3f}/s, "
                f"Error Rate={float(item['error_rate_pct']):.3f}%"
            )
        lines.append("")
    elif events:
        lines.extend([
            "## 事件阶段",
            "",
            "events.csv 存在，但没有同时提供有效的 FAULT_START 和 FAULT_END，未猜测阶段边界。",
            "",
        ])

    if not samples:
        lines.extend([
            "## 数据状态",
            "",
            "JTL 中没有样本。汇总工具没有补造任何结果。",
            "",
        ])

    lines.extend([
        "## 说明",
        "",
        "- JMeter 响应时间是客户端观察到的端到端时间，不是服务端纯处理时间。",
        "- 样本量少时，P90/P95/P99 仅作参考。",
        "- 缺失的数据不会被补造；相关统计会显示为 0 或在明细 CSV 中为空。",
        "- `NEG_Review_*` 标签的预期 4xx 单独统计，不纳入正常业务错误率。",
    ])

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


def build_rows(samples: List[Sample], events: List[Dict[str, str]]) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    normal_business = [s for s in samples if not s.is_negative]
    negative = [s for s in samples if s.is_negative]
    rows.append(row("overall", "all_samples", samples, negative_expected=True))
    rows.append(row("overall", "normal_business_excluding_negative", normal_business))
    rows.append(row("overall", "negative_excluding_business", negative, negative_expected=True))

    by_label: Dict[str, List[Sample]] = defaultdict(list)
    for sample in samples:
        by_label[sample.label].append(sample)

    for label in CORE_LABELS:
        rows.append(row("label", label, by_label.get(label, [])))

    for label in sorted(k for k in by_label if k.startswith("NEG_")):
        rows.append(row("negative_case", label, by_label[label], negative_expected=True))

    rows.extend(review_special_rows(samples))

    for phase, phase_samples in samples_by_phase(samples, events).items():
        rows.append(row("phase", phase, [s for s in phase_samples if not s.is_negative]))

    return rows


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Summarize Online Boutique JMeter CSV JTL results.")
    parser.add_argument("--jtl", required=True, help="Path to CSV JTL result file.")
    parser.add_argument("--events", help="Optional events.csv path.")
    parser.add_argument("--output-dir", required=True, help="Directory for summary.csv and summary.md.")
    args = parser.parse_args(argv)

    os.makedirs(args.output_dir, exist_ok=True)
    samples = read_jtl(args.jtl)
    events = read_events(args.events)
    rows = build_rows(samples, events)

    write_summary_csv(os.path.join(args.output_dir, "summary.csv"), rows)
    write_summary_md(os.path.join(args.output_dir, "summary.md"), rows, events, samples)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
