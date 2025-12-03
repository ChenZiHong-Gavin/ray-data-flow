# Ray Data Flow

一个基于 Ray Data 的轻量级 DAG 工作流执行引擎，将“业务逻辑”与“资源分配/并发策略”解耦，支持用配置编排与扩展。

## 为什么用它
1. 业务与资源分离：在算子代码中只写“业务处理”，并发副本数、批大小、GPU 等资源在配置里声明。
2. 配置驱动：使用 JSON + Pydantic 校验，支持 `map`、`filter`、`flatmap`、`aggregate`、`map_batch` 五类任务。
3. 支持多分支与汇聚：节点可有多个上游，自动执行 `Union`，节点的结果自动分发给下游。
4. 类算子复用：以 Actor 方式托管“重型资源”（如 LLM，数据库连接等），一次加载，多次复用，避免重复初始化。
5. 惰性执行与高性能：基于 Ray Data 的列式批处理与惰性计算，仅在取结果时触发。

## 安装
确保已安装 Python 与 pip，然后执行：
```
pip install .
```

## 快速开始
 ETL 场景示例：
```
python -m examples.etl.run
```
Agent Workflow 场景示例：
```
python -m examples.agent_workflow.run
```

## 核心用法
1. 定义算子函数或类，并注册到字典中：
```python
operators = {
    "standardize_currency": standardize_currency,   # 函数型算子
    "BertIntentClassifier": BertIntentClassifier,   # 类/Actor 型算子
}
```
2. 编写 DAG 配置（JSON）：
```json
{
  "nodes": [
    {"id": "clean", "func_name": "preprocess_text", "type": "map", "dependencies": []},
    {"id": "classify", "func_name": "BertIntentClassifier", "type": "map_batch", "dependencies": ["clean"],
     "params": {"replicas": 2, "batch_size": 4, "model_version": "bert-base-v1", "compute_resources": {"num_gpus": 0}}}
  ]
}
```
3. 在代码中加载并执行：
```python
import ray, ray.data
from ray_data_flow.engine import Engine
from ray_data_flow.utils import load_config

config = load_config("examples/agent_workflow/config.json")
engine = Engine(config, operators)
ds = ray.data.from_items([{"id": 1, "raw_text": "I need a refund"}])
results = engine.execute(ds)  # 只会在取叶子节点结果时触发整个 DAG 计算
```

## 配置说明（节点）
- `id`：节点唯一标识
- `func_name`：算子名称（在 `operators` 字典中注册）
- `type`：任务类型，支持 `map`、`filter`、`flatmap`、`aggregate`、`map_batch`
- `dependencies`：上游节点 ID 列表；多个上游将执行 `Union`（需列 schema 一致）  
- `params`：传给算子的参数；对于“类算子”，支持：
  - `replicas`：并发 Actor 副本数（默认 1）
  - `batch_size`：批大小（`map_batch` 时有效）
  - `compute_resources`：计算资源声明（如 `{"num_gpus": 1}`）
  - 其余键将作为构造参数传入算子类的 `__init__`

## 工作原理
- 拓扑排序：对节点进行 Kahn 算法拓扑排序，校验环与缺失依赖。
- 数据流执行：根据节点类型应用 Ray Data 的 `map`/`filter`/`flat_map`/`map_batches` 等操作。
- 多依赖合并：当存在多个上游时，使用 `union` 合并数据集（要求 Schema 一致）。
- 触发与收集：仅在读取叶子节点结果时触发整链计算，最终返回 `{叶子节点ID: 结果}` 的字典。

任务流示例：
<img width="400" alt="image" src="https://github.com/user-attachments/assets/d40b158e-e04f-4714-9bc9-a510f2913320" />

