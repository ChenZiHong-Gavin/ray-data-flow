import ray
import ray.data

from ray_data_flow.engine import Engine
from ray_data_flow.utils import load_config

from .operators import operators

def run_workflow(config_path: str):
    config = load_config(config_path)
    engine = Engine(config, operators)

    # 模拟复杂的混合交易流
    transactions = [
        # 交易 A: 高危欺诈 (金额大，跨国) -> 应该进入 Fraud + Archive
        {"tx_id": "TX_1001", "amount": 3000, "currency": "USD", "src_country": "US", "dst_country": "RU", "category": "unknown"},
        
        # 交易 B: 营销机会 (旅游，金额适中) -> 应该进入 Marketing + Archive
        {"tx_id": "TX_1002", "amount": 500, "currency": "EUR", "src_country": "FR", "dst_country": "FR", "category": "travel"},
        
        # 交易 C: 普通交易 (无特殊特征) -> 只进入 Archive
        {"tx_id": "TX_1003", "amount": 10, "currency": "CNY", "src_country": "CN", "dst_country": "CN", "category": "groceries"},
        
        # 交易 D: 混合特征 (大额 + 餐饮) -> 既高危 又是 营销目标 -> Fraud + Marketing + Archive
        {"tx_id": "TX_1004", "amount": 5000, "currency": "USD", "src_country": "US", "dst_country": "CN", "category": "food"},
    ]

    ds = ray.data.from_items(transactions)
    results = engine.execute(ds)


    # 分析结果
    final_stats = results["central_sink"][0]["stats"]
    print("\n执行统计报告 (汇聚节点输出):")
    print(f"   - 欺诈报警生成: {final_stats['FRAUD_ALERT']}")
    print(f"   - 营销活动触达: {final_stats['MARKETING_PROMO']}")
    print(f"   - 合规归档记录: {final_stats['COMPLIANCE_ARCHIVE']}")
    
    print("\n详细流向解释:")
    print("   - TX_1001 (欺诈): 触发了 Fraud(1) 和 Archive(1)")
    print("   - TX_1002 (旅游): 触发了 Marketing(1) 和 Archive(1)")
    print("   - TX_1003 (普通): 仅触发 Archive(1)")
    print("   - TX_1004 (混合): 触发了 Fraud(1), Marketing(1), Archive(1)")
    
    # 验证总数
    total_events = sum(final_stats.values())
    print(f"\n   总计产生事件数: {total_events} (理论值: 2+2+1+3 = 8)")

if __name__ == "__main__":
    run_workflow("examples/etl/config.json")
    ray.shutdown()

