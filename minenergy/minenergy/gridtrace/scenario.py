"""值班验收案例：两源、一圈联络线、一对平行线、一只孤立表。

拓扑（接点清册内 ID 唯一，边为无向多重边）::

          E1(switch,初合)         E3(wire)
    N1 ─────────────────── N2 ─────────────── N3
     │                     │                    │
   SA 挂N1               E2(switch,初合)      SB 挂N3
     │                     │                    │
     │ E7(switch,初分)     N4 ─ E5(wire) ─ N5 ─ E4(switch,初合)
     └──────────────────► N4                    │
                           │                    │
                          M6 挂N4              M3 挂N5
                           │  P1(wire) 平行双线  │
                           └────────────────────┘
                           │  P2(wire)          │
                          N6 挂 M5（支路接点）
    N9 挂 M9，无任何连线（孤立表）

* 联络线环：N1-E1-N2-E3-N3-E4-N5-E5-N4-E2-N2（闭环）；
* 平行双线：N4 与 N6 之间拉 P1、P2 两根常接导线；
* E7 是 N1-N4 的备用联络开关（初分），工单二同刻断 E1（旧路）合 E7（新路）；
* 源：SA 挂 N1、SB 挂 N3；表：M1 挂 N1（公共接点）、M3 挂 N5、
  M6 挂 N4（支路）、M5 挂 N6（平行双线末端）、M9 挂 N9（孤立表）。

基准时刻 2026-01-01T00:00:00Z。操作簿（语义按发生时刻回放）：

    T1=01:00 OP21 switch E2 open      联络环一处开断（仍有备用路径）
    T2=02:00 OP22 source SA retired   退 SA
    T3=03:00 OP23 source SB retired   退 SB -> 全断
    T4=04:00 OP24 source SA engaged   SA 恢复送电
             OP25 source SB engaged   SB 恢复送电（同刻整体作用）
             OP26 switch E2 closed    联络环恢复
    T5=05:00 OP31 switch E1 open      工单二：同刻断旧路
             OP32 switch E7 closed    合新路（N1-N4 备用联络开关，初分）
    T6=06:00 OP41 switch E1 closed    恢复原方式
             OP42 switch E7 open

手算依据（边号字典序：E1<E2<E3<E4<E5<E7<P1<P2）：

T4 快照（环恢复、双源投入）下，除 M9 外全部表同时可达 SA/SB（双路并供），
M9 恒为失电。M5 到任一源的最短路径都以 P1/P2 之一起步：两条等长，首条边
P1<P2，故 P1 胜出；M5->SA 为 [P1,E2,E1]（3 边，绕 E5 一侧为 5 边），
M5->SB 有 [P1,E2,E3] 与 [P1,E5,E4] 两条等长（均 3 边），第二位 E2<E5，
取 [P1,E2,E3]。

T5 同刻断 E1、合 E7：SA 经 E7 直接落在 N4，所有表的**来源集合不变**
（N1 不再孤立，SA 改由新路送出），历史在 [T4,T6) 归并为一段；但 M6 的
走线改变——T5 前 M6(N4)->SA 为 [E2,E1]（N4-E2-N2-E1-N1），T5 后 E1
断开、E7 闭合，M6->SA 变为单边 [E7]（N4-E7-N1）。这正是“归并段内部
追线须还原被点中瞬间、不能复用归并段首条路线”的考点。
"""
from __future__ import annotations

import json
from pathlib import Path

BASE_TIME = "2026-01-01T00:00:00Z"
T1 = "2026-01-01T01:00:00Z"
T2 = "2026-01-01T02:00:00Z"
T3 = "2026-01-01T03:00:00Z"
T4 = "2026-01-01T04:00:00Z"
T5 = "2026-01-01T05:00:00Z"
T6 = "2026-01-01T06:00:00Z"
T7 = "2026-01-01T07:00:00Z"


def build_model_dict() -> dict:
    return {
        "base_time": BASE_TIME,
        "nodes": [
            {"id": "N1", "name": "1号母线"},
            {"id": "N2", "name": "2号母线"},
            {"id": "N3", "name": "3号母线"},
            {"id": "N4", "name": "4号接点"},
            {"id": "N5", "name": "5号接点"},
            {"id": "N6", "name": "6号接点"},
            {"id": "N9", "name": "9号孤立接点"},
        ],
        "lines": [
            {"id": "E1", "kind": "switch", "endpoint_a": "N1", "endpoint_b": "N2",
             "initial_state": "closed"},
            {"id": "E2", "kind": "switch", "endpoint_a": "N2", "endpoint_b": "N4",
             "initial_state": "closed"},
            {"id": "E3", "kind": "wire", "endpoint_a": "N2", "endpoint_b": "N3"},
            {"id": "E4", "kind": "switch", "endpoint_a": "N3", "endpoint_b": "N5",
             "initial_state": "closed"},
            {"id": "E5", "kind": "wire", "endpoint_a": "N4", "endpoint_b": "N5"},
            {"id": "E7", "kind": "switch", "endpoint_a": "N1", "endpoint_b": "N4",
             "initial_state": "open"},
            {"id": "P1", "kind": "wire", "endpoint_a": "N4", "endpoint_b": "N6"},
            {"id": "P2", "kind": "wire", "endpoint_a": "N4", "endpoint_b": "N6"},
        ],
        "sources": [
            {"id": "SA", "node_id": "N1", "initial_state": "engaged"},
            {"id": "SB", "node_id": "N3", "initial_state": "engaged"},
        ],
        "meters": [
            {"id": "M1", "node_id": "N1"},
            {"id": "M3", "node_id": "N5"},
            {"id": "M5", "node_id": "N6"},
            {"id": "M6", "node_id": "N4"},
            {"id": "M9", "node_id": "N9"},
        ],
    }


def build_ledger_dict() -> dict:
    return {
        "operations": [
            # 文件顺序故意打乱，语义仍按发生时刻回放
            {"at_time": T3, "op_id": "OP23", "target_kind": "source",
             "target_id": "SB", "new_state": "retired"},
            {"at_time": T1, "op_id": "OP21", "target_kind": "switch",
             "target_id": "E2", "new_state": "open"},
            {"at_time": T4, "op_id": "OP24", "target_kind": "source",
             "target_id": "SA", "new_state": "engaged"},
            {"at_time": T2, "op_id": "OP22", "target_kind": "source",
             "target_id": "SA", "new_state": "retired"},
            {"at_time": T4, "op_id": "OP25", "target_kind": "source",
             "target_id": "SB", "new_state": "engaged"},
            {"at_time": T4, "op_id": "OP26", "target_kind": "switch",
             "target_id": "E2", "new_state": "closed"},
            {"at_time": T5, "op_id": "OP31", "target_kind": "switch",
             "target_id": "E1", "new_state": "open"},
            {"at_time": T5, "op_id": "OP32", "target_kind": "switch",
             "target_id": "E7", "new_state": "closed"},
            {"at_time": T6, "op_id": "OP41", "target_kind": "switch",
             "target_id": "E1", "new_state": "closed"},
            {"at_time": T6, "op_id": "OP42", "target_kind": "switch",
             "target_id": "E7", "new_state": "open"},
            # 完全一致的操作编号重传：必须被忽略，不产生第二条动作
            {"at_time": T1, "op_id": "OP21", "target_kind": "switch",
             "target_id": "E2", "new_state": "open"},
        ]
    }


def write_case_files(directory: str | Path) -> dict[str, Path]:
    """把案例模型与操作簿写到一个全新空目录，返回文件路径。"""
    out = Path(directory)
    out.mkdir(parents=True, exist_ok=True)
    model_path = out / "grid_model.json"
    ledger_path = out / "operations.json"
    model_path.write_text(json.dumps(build_model_dict(), ensure_ascii=False, indent=2),
                          encoding="utf-8")
    ledger_path.write_text(json.dumps(build_ledger_dict(), ensure_ascii=False, indent=2),
                           encoding="utf-8")
    return {"model": model_path, "ledger": ledger_path}
