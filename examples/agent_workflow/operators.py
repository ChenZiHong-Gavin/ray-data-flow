import time
import os


def preprocess_text(row: dict) -> dict:
    """简单的文本清理"""
    row["clean_text"] = row["raw_text"].strip().lower()
    return row


class BertIntentClassifier:
    """
    模拟一个重型的 BERT 分类器。
    只需要初始化一次，之后可以处理任意数量的 batch。
    """
    def __init__(self, model_version: str):
        self.pid = os.getpid()
        print(f"[PID:{self.pid}] Loading BERT Classifier ({model_version})... (耗时操作)")
        # 模拟加载模型耗时
        time.sleep(2.0) 
        self.model_version = model_version
        print(f"[PID:{self.pid}] BERT Model Loaded!")

    def __call__(self, batch: dict) -> dict:
        """
        Ray Data 传入的 batch 是 columnar format: 
        {'clean_text': ['txt1', 'txt2'], 'id': [1, 2]}
        """
        texts = batch["clean_text"]
        intents = []
        scores = []
        worker_pids = []

        # 模拟推理逻辑
        for t in texts:
            if "refund" in t or "money" in t:
                intents.append("REFUND")
                scores.append(0.95)
            elif "password" in t or "login" in t:
                intents.append("TECH_SUPPORT")
                scores.append(0.88)
            else:
                intents.append("GENERAL")
                scores.append(0.50)
            worker_pids.append(self.pid)

        # 返回扩充后的列
        return {
            "id": batch["id"],
            "clean_text": texts,
            "intent": intents,
            "confidence": scores,
            "classifier_pid": worker_pids # 记录是谁处理的
        }

class LLMResponder:
    """
    模拟一个 LLM (如 Llama/GPT) 用于生成回复。
    """
    def __init__(self, model_name: str, temperature: float = 0.7):
        self.pid = os.getpid()
        print(f"[PID:{self.pid}] Loading LLM ({model_name})... (显存占用大)")
        time.sleep(2.0)
        self.model_name = model_name
        print(f"[PID:{self.pid}] LLM Ready!")

    def __call__(self, batch: dict) -> dict:
        intents = batch["intent"]
        ids = batch["id"]
        responses = []
        worker_pids = []

        for i, intent in enumerate(intents):
            # 模拟 LLM 生成
            if intent == "REFUND":
                res = f"[{self.model_name}] We can help with your refund (Ticket {ids[i]})."
            elif intent == "TECH_SUPPORT":
                res = f"[{self.model_name}] Please reset your router (Ticket {ids[i]})."
            else:
                res = f"[{self.model_name}] How can I help you? (Ticket {ids[i]})."
            
            responses.append(res)
            worker_pids.append(self.pid)

        # 这里不需要返回所有列，只需返回新增或修改的
        batch["response"] = responses
        batch["responder_pid"] = worker_pids
        return batch

operators = {
    "preprocess_text": preprocess_text,
    "BertIntentClassifier": BertIntentClassifier,
    "LLMResponder": LLMResponder,
}
