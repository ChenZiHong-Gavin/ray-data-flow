

# ------------------------------------------------------------
#  基于 Ray Dataset 的 DAG 执行器
# ------------------------------------------------------------
def execute_dag_with_ray_data(config: Dict[str, Any]):
    ray.init()

    # 1. 加载函数
    funcs = config["functions"]

    # 2. 创建初始数据集（自动分块，流式处理）
    ds = ray.data.from_items(config["input_data"], parallelism=2)  # 可调整并行度

    # 3. 存储每个节点的数据集
    datasets: Dict[str, ray.data.Dataset] = {}

    # 4. 按拓扑序执行（惰性构建计算图）
    for node in topological_sort(config["nodes"]):
        node_id = node["id"]
        deps = node.get("dependencies", [])

        # ---- 处理依赖关系 ----
        if not deps:
            input_ds = ds  # 无依赖，使用初始数据
        elif len(deps) == 1:
            input_ds = datasets[deps[0]]  # 单依赖，直接连接
        else:
            # 多对一：合并多个上游数据集（union 不会触发 shuffle）
            input_ds = datasets[deps[0]].union(*[datasets[d] for d in deps[1:]])

        # ---- 根据类型应用算子 ----
        func = funcs[node["func_name"]]
        task_type = node["type"]

        if task_type == "map":
            # 一对多：在这里可以分叉，比如 ds.map(...).map(...)
            datasets[node_id] = input_ds.map(func)
        elif task_type == "filter":
            datasets[node_id] = input_ds.filter(func)
        elif task_type == "flatmap":
            # 注意：Ray Data 的 flat_map 需要返回 generator
            datasets[node_id] = input_ds.flat_map(func)
        elif task_type == "aggregate":
            datasets[node_id] = input_ds.repartition(1).map_batches(
                lambda batch: func(batch),
                batch_size=None,  # 整个数据集作为一批
                batch_format="default"
            )
        elif task_type == "map_batch":
            pass
        else:
            raise ValueError(f"未知的任务类型: {task_type}")

    # 5. 触发执行并收集结果
    final_output = {}
    for node_id, dataset in datasets.items():
        # take_all() 会触发整个 DAG 的执行
        final_output[node_id] = dataset.take_all()

    ray.shutdown()
    return final_output

# ------------------------------------------------------------
#  测试代码
# ------------------------------------------------------------
if __name__ == "__main__":
    def square(row: dict):
        val = row['item']  # 从 dict 中提取值
        return {'item': val * val}  # 返回 dict

    def is_even(row: dict):
        val = row['item']
        return val % 2 == 0

    def count(data_list: List[dict]):
        # data_list 格式: [{'item': 4}, {'item': 16}, ...]
        return {'item': [len(data_list)]}

    def generate_data(row: dict):
        # 输入: {'item': 7}
        count_val = row['item']
        # flat_map 必须 yield dict
        for i in range(count_val):
            yield {'item': i}

    configs = {
        "functions": {
            "square": square,
            "is_even": is_even,
            "count": count,
            "generate_data": generate_data
        },
        "nodes": [
            { "id": "n1", "type": "map", "func_name": "square", "params": {}, "dependencies": [] },
            { "id": "n2", "type": "filter", "func_name": "is_even", "params": {}, "dependencies": ["n1"] },
            { "id": "n3", "type": "map",  "func_name": "square", "params": {}, "dependencies": ["n1"] },
            { "id": "n4", "type": "aggregate", "func_name": "count", "params": {}, "dependencies": ["n2", "n3"] },
            { "id": "n5", "type": "flatmap", "func_name": "generate_data", "params": {}, "dependencies": ["n2"] }
        ],
        "input_data": [1, 2, 3, 4, 5]
    }

    out = execute_dag_with_ray_data(configs)
    print("--- 各节点结果 ---")
    print(json.dumps(out, indent=2, ensure_ascii=False))

# TODO： Pydantic 验证


# TODO： 使用示例：etl、llm
# TODO: 性能分析
# TODO： 监控
