import ray
import ray.data

from ray_data_flow.engine import Engine
from ray_data_flow.utils import load_config

from .operators import operators

def run_workflow(config_path: str):
    config = load_config(config_path)
    engine = Engine(config, operators)

    # 1. 模拟一批工单数据
    raw_data = []
    for i in range(20):
        if i % 3 == 0:
            txt = "I need a refund for my order"
        elif i % 3 == 1:
            txt = "Cannot login forgot password"
        else:
            txt = "What is the weather today"
        raw_data.append({"id": i, "raw_text": txt})

    ds = ray.data.from_items(raw_data)

    results = engine.execute(ds)
    final_data = results["generate_node"]

    print("\n" + "="*50)
    print("执行分析报告")
    print("="*50)
    
    # 统计分类器的 PID
    cls_pids = set([row["classifier_pid"] for row in final_data])
    llm_pids = set([row["responder_pid"] for row in final_data])
    
    print(f"总处理工单数: {len(final_data)}")
    print(f"分类器 Actor PIDs: {cls_pids}")
    print(f"生成器 Actor PIDs: {llm_pids}")
    
    if len(cls_pids) <= 2 and len(llm_pids) <= 2:
        print("\n验证成功: 所有 20 条数据仅由有限的几个 Actor 处理，模型没有重复加载！")
    else:
        print("\n验证失败: 似乎创建了过多 Actor。")

    print("\n部分结果采样:")
    for row in final_data[:3]:
        print(f"ID:{row['id']} | Intent:{row['intent']} | Reply:{row['response']}")

if __name__ == "__main__":
    run_workflow("examples/agent_workflow/config.json")
    ray.shutdown()
