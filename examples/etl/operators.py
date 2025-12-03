import time
import pandas as pd

def standardize_currency(row: dict) -> dict:
    """
    标准化交易金额为 USD。
    """
    rate_map = {"CNY": 0.14, "EUR": 1.1, "USD": 1.0, "JPY": 0.0067}
    currency = row.get("currency", "USD")
    amount = row.get("amount", 0)
    
    row["amount_usd"] = amount * rate_map.get(currency, 1.0)
    return row

def filter_high_risk(row: dict) -> bool:
    """
    只保留高风险交易（大于2000刀 且 跨国）
    """
    is_cross_border = row["src_country"] != row["dst_country"]
    return row["amount_usd"] > 2000 and is_cross_border


def mark_fraud_review(row: dict) -> dict:
    """
    标记为标记为欺诈审核
    """
    return {
        "tx_id": row["tx_id"],
        "event_type": "FRAUD_ALERT",
        "payload": f"High risk transaction detected: ${row['amount_usd']:.2f}",
        "priority": "HIGH",
        "processed_at": time.time()
    }

def filter_lifestyle(row: dict) -> bool:
    """
    只保留生活交易类记录
    """
    return row["category"] in ["food", "travel", "entertainment"]

def generate_promo(row: dict) -> dict:
    """
    生成营销优惠券
    """
    coupon_value = row["amount_usd"] * 0.05 # 返现5%
    return {
        "tx_id": row["tx_id"],
        "event_type": "MARKETING_PROMO",
        "payload": f"Send coupon worth ${coupon_value:.2f} for {row['category']}",
        "priority": "LOW",
        "processed_at": time.time()
    }

def anonymize_data(row: dict) -> dict:
    """
    数据脱敏归档
    """
    masked_id = row["tx_id"][:4] + "****"
    return {
        "tx_id": row["tx_id"], # 保留原始ID用于索引，但在 payload 里脱敏
        "event_type": "COMPLIANCE_ARCHIVE",
        "payload": f"Archived tx {masked_id} from {row['src_country']}",
        "priority": "NONE",
        "processed_at": time.time()
    }

def sink_to_queue(batch: dict) -> dict:
    """
    将批量数据发送到队列
    """

    records = pd.DataFrame(batch).to_dict('records')

    print(f"    [System] Writing {len(records)} events to Message Queue...")
    
    stats = {"FRAUD_ALERT": 0, "MARKETING_PROMO": 0, "COMPLIANCE_ARCHIVE": 0}
    
    # 现在 item 是一个标准的字典了，例如 {'tx_id': '...', 'event_type': '...'}
    for item in records:
        etype = item.get("event_type") # 使用 get 防止 key 不存在报错
        if etype in stats:
            stats[etype] += 1
            
    # Ray 的 map_batches 要求返回列式数据 (Dict[str, List])
    return {"status": ["success"], "stats": [stats]}

operators = {
    "standardize_currency": standardize_currency,
    "filter_high_risk": filter_high_risk,
    "mark_fraud_review": mark_fraud_review,
    "filter_lifestyle": filter_lifestyle,
    "generate_promo": generate_promo,
    "anonymize_data": anonymize_data,
    "sink_to_queue": sink_to_queue,
}
