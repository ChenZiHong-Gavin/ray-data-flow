import ray
import ray.data
from collections import deque, defaultdict
from typing import List, Dict, Any, Set, Callable

from .bases import Config, Node


class Engine:
    def __init__(
        self,
        config: Dict[str, Any],
        functions: Dict[str, Callable],
        **ray_init_kwargs):
        self.config = Config(**config)
        self.functions = functions
        self.datasets: Dict[str, ray.data.Dataset] = {}

        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True, **ray_init_kwargs)

    @staticmethod
    def _topo_sort(nodes: List[Node]) -> List[Node]:
        id_to_node: Dict[str, Node] = {}
        for n in nodes:
            id_to_node[n.id] = n
        
        indeg: Dict[str, int] = {nid: 0 for nid in id_to_node}
        adj: Dict[str, List[str]] = defaultdict(list)

        for n in nodes:
            nid = n.id
            deps: List[str] = n.dependencies
            uniq_deps: Set[str] = set(deps)
            for d in uniq_deps:
                if d not in id_to_node:
                    raise ValueError(f"节点 {nid} 依赖的节点 {d} 不存在")
                indeg[nid] += 1
                adj[d].append(nid)

        zero_deg: deque = deque(
            [id_to_node[nid] for nid, deg in indeg.items() if deg == 0]
        )
        sorted_nodes: List[Node] = []

        # Kahn 主循环
        while zero_deg:
            cur = zero_deg.popleft()
            sorted_nodes.append(cur)
            cur_id = cur.id
            for nb_id in adj.get(cur_id, []):
                indeg[nb_id] -= 1
                if indeg[nb_id] == 0:
                    zero_deg.append(id_to_node[nb_id])

        if len(sorted_nodes) != len(nodes):
            remaining = [nid for nid, deg in indeg.items() if deg > 0]
            raise ValueError(
                f"配置中存在环，无法执行。剩余入度>0的节点 id: {remaining}"
            )

        return sorted_nodes

    def _get_input_dataset(self, node: Node, initial_ds: ray.data.Dataset) -> ray.data.Dataset:
        """解析依赖关系并获取输入数据集"""
        deps = node.dependencies
        
        if not deps:
            return initial_ds
        
        if len(deps) == 1:
            return self.datasets[deps[0]]
        
        # 多依赖处理：合并 (Union)
        # 注意：Union 要求 Schema 一致
        main_ds = self.datasets[deps[0]]
        other_dss = [self.datasets[d] for d in deps[1:]]
        return main_ds.union(*other_dss)

    def _execute_node(self, node: Node, initial_ds: ray.data.Dataset):
        # 获取上游数据
        input_ds = self._get_input_dataset(node, initial_ds)

        if node.func_name not in self.functions:
            raise ValueError(f"函数 {node.func_name} 未注册")

        raw_func = self.functions[node.func_name]
        node_params = node.params

        # 包装函数以处理 kwargs
        def func_wrapper(row_or_batch: Dict[str, Any]) -> Dict[str, Any]:
            return raw_func(row_or_batch, **node_params)
        
        # 应用算子
        if node.type == "map":
            self.datasets[node.id] = input_ds.map(func_wrapper)
        elif node.type == "filter":
            self.datasets[node.id] = input_ds.filter(func_wrapper)
        elif node.type == "flatmap":
            # flatmap要求函数返回可迭代对象
            self.datasets[node.id] = input_ds.flat_map(func_wrapper)
        # ]Ray Data 为了高性能，默认在 map_batches 中传递的数据格式是 列式（Columnar） 的（即 Dict[str, List]）
        elif node.type == "aggregate":
            # 全局聚合
            self.datasets[node.id] = input_ds.repartition(1).map_batches(
                lambda batch: func_wrapper(batch),
                batch_format="default"
            )
        elif node.type == "map_batch":
            self.datasets[node.id] = input_ds.map_batches(func_wrapper)
        else:
            raise ValueError(f"未知任务类型 {node.type}")

    def _find_leaf_nodes(self, nodes: List[Node]) -> Set[str]:
        """找到所有叶子节点（无后续依赖）"""
        all_ids = {n.id for n in nodes}
        deps_set = set()
        for n in nodes:
            deps_set.update(n.dependencies)
        return all_ids - deps_set

    def execute(self, initial_ds: ray.data.Dataset) -> Dict[str, List[Any]]:
        # 1. 拓扑排序
        sorted_nodes = self._topo_sort(self.config.nodes)

        # 2. 执行节点
        for node in sorted_nodes:
            self._execute_node(node, initial_ds)
        
        # 3. 触发计算
        leaf_nodes = self._find_leaf_nodes(sorted_nodes)

        @ray.remote
        def _fetch_result(ds: ray.data.Dataset) -> List[Any]:
            # 这一步会触发 ds 对应的整个依赖链的计算
            return ds.take_all()
        
        # 4. 触发计算并获取结果
        results = ray.get([_fetch_result.remote(self.datasets[node_id]) for node_id in leaf_nodes])
        return dict(zip(leaf_nodes, results))


if __name__ == "__main__":
    config = {
        "nodes": [
            {"id": "node1", "func_name": "func1", "type": "map", "dependencies": [], "params": {"factor": 2}},
            {"id": "node2", "func_name": "func2", "type": "filter", "dependencies": ["node1"]},
        ]
    }

    functions = {
        "func1": lambda x, factor: {"id": x["id"], "value": x["value"] * factor},
        "func2": lambda x: x["value"] > 1,
    }

    engine = Engine(config, functions)
    results = engine.execute()
    print(results)

# TODO： Pydantic 验证
# TODO： 使用示例：etl、agent_workflow
# TODO: 性能分析
# TODO： 监控指标（如处理速度、延迟、错误率）