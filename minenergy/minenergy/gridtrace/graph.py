"""无向多重图上的连通性与最短路径搜索。

只实现模块自身使用的 BFS：

* :meth:`Graph.reachable_sources` —— 从某接点出发，经当前导通边能到达的、
  处于投入状态的源集合；
* :meth:`Graph.lexicographic_shortest_paths` —— 到一组目标接点的最短简单
  路径，边数最少者优先，同长时按**整条边号序列**字典序比较取最小。

独立对照程序（``oracle.py``）不允许调用这里的任何函数，须自行实现图搜索。
"""
from __future__ import annotations

from collections import deque

from .model import GridModel


class Graph:
    """接线的无向多重图视图（含全部边；导通性由 Snapshot 判定）。"""

    def __init__(self, model: GridModel,
                 removed_lines: frozenset[str] | None = None):
        self.model = model
        self.removed_lines = removed_lines or frozenset()
        # node_id -> list[(edge_id, other_node_id)]
        self.adjacency: dict[str, list[tuple[str, str]]] = {
            node_id: [] for node_id in model.nodes
        }
        for edge_id, line in model.lines.items():
            if edge_id in self.removed_lines:
                continue
            self.adjacency[line.endpoint_a].append((edge_id, line.endpoint_b))
            self.adjacency[line.endpoint_b].append((edge_id, line.endpoint_a))

    def with_removed_lines(self, removed_lines: frozenset[str]) -> "Graph":
        merged = self.removed_lines | removed_lines
        return Graph(self.model, merged)

    def _conducting_edges(self, node_id: str, snapshot) -> list[tuple[str, str]]:
        return [
            (edge_id, other)
            for edge_id, other in self.adjacency[node_id]
            if snapshot.line_conducts(edge_id)
        ]

    def reachable_sources(self, start_node: str, snapshot) -> set[str]:
        """返回 ``start_node`` 所在导通分量内、当前投入的源 ID 集合。

        分量内一个源也到不了即返回空集——调用方据此把该表标为失电，
        孤立表不会被当成不存在。
        """
        if start_node not in self.adjacency:
            return set()
        visited = {start_node}
        queue = deque([start_node])
        while queue:
            node_id = queue.popleft()
            for _edge_id, other in self._conducting_edges(node_id, snapshot):
                if other not in visited:
                    visited.add(other)
                    queue.append(other)
        return {
            sid
            for sid, source in self.model.sources.items()
            if snapshot.source_is_engaged(sid) and source.node_id in visited
        }

    def lexicographic_shortest_paths(
        self, start_node: str, target_nodes: set[str], snapshot
    ) -> dict[str, tuple[tuple[str, ...], tuple[str, ...]]]:
        """一次 BFS 求 ``start_node`` 到每个目标接点的最优简单路径。

        路径以 ``(边号序列, 节点序列)`` 表示。择优规则：

        1. 边数最少（BFS 按层推进天然保证）；
        2. 边数相同则整条边号序列字典序最小——每条邻接边按边号升序扩展，
           同层首次到达的路径即字典序最小者，后续同层到达不会更优。

        平行边在这里是两条不同的邻接边，按各自边号参与字典序比较。
        """
        if start_node not in self.adjacency:
            return {}
        # node -> (edge tuple, node tuple)
        best: dict[str, tuple[tuple[str, ...], tuple[str, ...]]] = {
            start_node: ((), (start_node,))
        }
        found: dict[str, tuple[tuple[str, ...], tuple[str, ...]]] = {}
        if start_node in target_nodes:
            found[start_node] = best[start_node]
        frontier = deque([start_node])
        while frontier:
            current = frontier.popleft()
            cur_edges, cur_nodes = best[current]
            neighbors = sorted(self._conducting_edges(current, snapshot),
                               key=lambda item: item[0])
            for edge_id, other in neighbors:
                if other in best:
                    continue  # 更短或同层字典序更小的路径已先到达
                new_edges = cur_edges + (edge_id,)
                new_nodes = cur_nodes + (other,)
                best[other] = (new_edges, new_nodes)
                if other in target_nodes:
                    found[other] = (new_edges, new_nodes)
                frontier.append(other)
        return found
