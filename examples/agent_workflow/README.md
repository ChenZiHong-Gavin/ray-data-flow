# Agent Workflow 示例：批量意图分类与回复生成

本示例演示如何用 `ray_data_flow` 基于 Ray Data 构建“文本工单→意图分类→回复生成”的 agent 工作流。
需要加载模型的算子，会根据配置参数自动创建多个 Actor 实例，每个 Actor 负责加载一个模型副本，模型加载后可以被重用。

## 场景概述
- 输入：一批工单文本（模拟 20 条，包含退款、登录问题、通用咨询）。
- 清洗：规范化文本（去空格、转小写）。
- 意图分类：用“BERT 分类器”识别 `REFUND`、`TECH_SUPPORT`、`GENERAL` 并输出置信度。
- 回复生成：根据意图调用“LLM 响应器”生成模板回复。

## 目录结构
```
examples/agent_workflow/
├─ README.md         # 本说明文档
├─ config.json       # DAG 配置（节点、依赖、算子名与参数）
├─ operators.py      # 算子：预处理、BERT 分类器、LLM 响应器
└─ run.py            # 运行入口：构造数据→执行→统计→采样输出
```

## 快速开始
在仓库根目录执行：
```
python -m examples.agent_workflow.run
```


## DAG 配置（`config.json`）
```json
{
  "nodes": [
    {"id": "clean_node",    "func_name": "preprocess_text",     "type": "map",       "dependencies": []},
    {"id": "classify_node", "func_name": "BertIntentClassifier", "type": "map_batch", "dependencies": ["clean_node"],
     "params": {"model_version": "bert-base-v1", "replicas": 2, "batch_size": 4}},
    {"id": "generate_node", "func_name": "LLMResponder",         "type": "map_batch", "dependencies": ["classify_node"],
     "params": {"model_name": "DeepSeek-Lite", "replicas": 2, "batch_size": 4}}
  ]
}
```
