"""随机接线与操作组合生成器（只生成合法数据，供对照验收使用）。

生成结果刻意覆盖需求里的边角结构：平行边、闭环、自环、同接点多表/多源、
孤立接点与孤立表。操作簿保证自身合法（同刻同目标不矛盾），并可按概率
混入“完全一致的操作编号重传”。
"""
from __future__ import annotations

import random

BASE_TIME = "2026-01-01T00:00:00Z"


def _time_at(minute: int) -> str:
    hour, minute = divmod(minute, 60)
    return f"2026-01-01T{hour:02d}:{minute:02d}:00Z"


def build_random_case(rng: random.Random) -> tuple[dict, dict]:
    node_count = rng.randint(3, 9)
    node_ids = [f"N{i}" for i in range(node_count)]
    nodes = [{"id": nid, "name": f"接点{i}"} for i, nid in enumerate(node_ids)]

    lines: list[dict] = []
    line_seq = 0

    def add_line(a: str, b: str, kind: str | None = None,
                 initial_state: str | None = None) -> None:
        nonlocal line_seq
        lid = f"L{line_seq:02d}"
        line_seq += 1
        if kind is None:
            kind = "switch" if rng.random() < 0.6 else "wire"
        entry = {"id": lid, "kind": kind, "endpoint_a": a, "endpoint_b": b}
        if kind == "switch":
            entry["initial_state"] = initial_state or rng.choice(
                ["closed", "open"])
        lines.append(entry)

    # 先保证有一个连通骨架（随机走一串接点），再随机补边制造环/平行边/自环
    order = node_ids[:]
    rng.shuffle(order)
    for a, b in zip(order, order[1:]):
        add_line(a, b)
    for _ in range(rng.randint(0, node_count * 2)):
        a = rng.choice(node_ids)
        b = rng.choice(node_ids)  # 允许 a == b（自环）
        add_line(a, b)
    # 明确加一对平行边
    if node_count >= 2:
        a, b = rng.sample(node_ids, 2)
        add_line(a, b, kind="wire")
        add_line(a, b, kind="wire")

    # 源：1..3 个，允许挂同一接点；保证至少一个源
    source_count = rng.randint(1, min(3, node_count))
    source_ids = [f"S{i}" for i in range(source_count)]
    sources = [
        {"id": sid, "node_id": rng.choice(node_ids),
         "initial_state": rng.choice(["engaged", "retired"])}
        for sid in source_ids
    ]

    # 表：覆盖支路/公共接点，并强制至少一只孤立表（挂在没有边的接点上）
    meter_count = rng.randint(1, min(4, node_count))
    meter_ids = [f"M{i}" for i in range(meter_count)]
    meters = [
        {"id": mid, "node_id": rng.choice(node_ids)}
        for mid in meter_ids
    ]
    isolated_node = f"N{node_count}"  # 新建一个不参与任何连线的接点
    nodes.append({"id": isolated_node, "name": "孤立接点"})
    meters.append({"id": f"M{meter_count}", "node_id": isolated_node})

    model = {
        "base_time": BASE_TIME,
        "nodes": nodes,
        "lines": lines,
        "sources": sources,
        "meters": meters,
    }

    # 操作簿：在 1..60 分钟内随机挑时刻；同刻同目标只允许一种最终状态
    switch_ids = [line["id"] for line in lines if line["kind"] == "switch"]
    operations: list[dict] = []
    claimed: set[tuple[int, str, str]] = set()
    op_seq = 0
    for _ in range(rng.randint(0, 14)):
        minute = rng.randint(1, 59)
        if rng.random() < 0.7 and switch_ids:
            kind, target, state = (
                "switch", rng.choice(switch_ids),
                rng.choice(["closed", "open"]))
        else:
            kind, target, state = (
                "source", rng.choice(source_ids),
                rng.choice(["engaged", "retired"]))
        key = (minute, kind, target)
        if key in claimed:
            continue
        claimed.add(key)
        op_seq += 1
        operations.append({
            "at_time": _time_at(minute),
            "op_id": f"OP{op_seq:02d}",
            "target_kind": kind,
            "target_id": target,
            "new_state": state,
        })

    # 混入完全一致的重传（不同文件位置、同 op_id 同内容）
    if operations and rng.random() < 0.5:
        dup = dict(rng.choice(operations))
        operations.append(dup)

    rng.shuffle(operations)  # 文件顺序打乱
    return model, {"operations": operations}
