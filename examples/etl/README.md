# ETL 示例：金融交易风控与营销分流

本示例展示如何使用 `ray_data_flow` 基于 Ray Data 搭建一个简洁的 ETL 流水线：对原始交易进行统一预处理后，分别走向风控、营销、合规三条分支，最终在汇聚节点统一写入下游消息队列。

## 场景概述
- 输入：原始“交易”事件流。
- 前置标准化：将不同币种金额统一换算为 `USD`。
- 并行分支：
  - 风控分支：筛选高风险（大额且跨国）交易，生成 `FRAUD_ALERT` 事件。
  - 营销分支：筛选生活类消费（餐饮/旅游/娱乐），生成 `MARKETING_PROMO` 事件。
  - 合规分支：对全量交易进行脱敏归档，生成 `COMPLIANCE_ARCHIVE` 事件。
- 汇聚：将三条分支的输出进行 Union，统一写入“消息队列”（示例中以打印+统计的形式模拟）。

## 目录结构
```
examples/etl/
├─ README.md         # 本说明文档
├─ config.json       # DAG 配置（节点、依赖、算子名）
├─ operators.py      # 业务算子（风控/营销/合规/汇聚）
└─ run.py            # 运行入口（构造数据→执行→打印统计）
```

## 快速开始
在仓库根目录下执行：
```
python -m examples.etl.run
```

## DAG 配置（`config.json`）
示例配置：
```json
{
  "nodes": [
    {"id": "standardize", "func_name": "standardize_currency", "type": "map", "dependencies": []},

    {"id": "fraud_filter", "func_name": "filter_high_risk", "type": "filter", "dependencies": ["standardize"]},
    {"id": "fraud_alert",  "func_name": "mark_fraud_review", "type": "map",    "dependencies": ["fraud_filter"]},

    {"id": "mkt_filter",   "func_name": "filter_lifestyle",  "type": "filter", "dependencies": ["standardize"]},
    {"id": "mkt_promo",    "func_name": "generate_promo",    "type": "map",    "dependencies": ["mkt_filter"]},

    {"id": "comp_archive", "func_name": "anonymize_data",     "type": "map",    "dependencies": ["standardize"]},

    {"id": "central_sink", "func_name": "sink_to_queue",      "type": "aggregate", "dependencies": ["fraud_alert", "mkt_promo", "comp_archive"]}
  ]
}
```
