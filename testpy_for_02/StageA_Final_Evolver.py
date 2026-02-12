import json
import pandas as pd
from collections import Counter, defaultdict
from google.cloud import storage
import vertexai
from vertexai.generative_models import GenerativeModel
import tag_config as tc 

class StageAFinalEvolver:
    def __init__(self, project_id, location, bucket_name, gcs_result_path):
        vertexai.init(project=project_id, location=location)
        self.model = GenerativeModel("gemini-2.0-flash-001")
        self.bucket_name = bucket_name
        self.gcs_result_path = gcs_result_path
        self.all_results = []
        self.client = storage.Client(project=project_id)

    def run_pipeline(self):
        print(f"🚀 [Step 1] 回收雲端數據並建立語義池...")
        bucket = self.client.bucket(self.bucket_name)
        blob = bucket.blob(self.gcs_result_path)
        content = blob.download_as_text()
        lines = content.strip().split('\n')
        
        # 關鍵修正：建立語義追蹤字典
        # key: 清洗後的詞, value: 原始出現過的各種寫法集合
        feature_trace = defaultdict(set)
        stop_words = ["有", "提供", "店內", "具備", "的", "一個", "處", "一杯", "飲品", "飲料"]

        for line in lines:
            try:
                entry = json.loads(line)
                raw_response_text = entry["response"]["candidates"][0]["content"]["parts"][0]["text"]
                clean_json_str = raw_response_text.strip("```json\n").strip("```").strip()
                audit_logic = json.loads(clean_json_str)
                
                new_feats = audit_logic.get('new_incremental_features', [])
                for feat in new_feats:
                    raw_name = feat.get('feature_name', '').strip()
                    if not raw_name: continue
                    
                    # 統計用的 key (粗洗)
                    clean_key = raw_name
                    for word in stop_words:
                        clean_key = clean_key.replace(word, "")
                    
                    if len(clean_key) >= 2:
                        feature_trace[clean_key].add(raw_name)
                
                self.all_results.append({
                    "place_id": entry.get("place_id"),
                    "place_name": entry.get("place_name"),
                    "audit_results": audit_logic
                })
            except Exception: continue

        # 2. 準備給 AI 的數據集 (包含原始範例)
        enriched_stats = []
        # 取前 40 名高頻特徵進行演進
        top_features = Counter({k: len(v) for k, v in feature_trace.items()}).most_common(40)
        
        for name, count in top_features:
            enriched_stats.append({
                "proposed_label": name,
                "frequency": count,
                "raw_examples": list(feature_trace[name])[:8] # 給 AI 看最多 8 種原始寫法
            })

        print(f"✅ [Step 2] 語義池建立完成。準備啟動 AI 歸一化決策...")
        self._ask_vertex_ai_to_evolve(enriched_stats)

    def _ask_vertex_ai_to_evolve(self, enriched_stats):
        # 將豐富的統計數據轉為 JSON
        raw_input_json = json.dumps(enriched_stats, ensure_ascii=False)
        
        prompt = f"""
        [ROLE] 你是資深數據架構師，負責優化咖啡廳標籤系統 (Schema Evolution)。
        
        [INPUT]
        1. 現有規則 (NORM_RULES): {json.dumps(tc.NORM_RULES, ensure_ascii=False)}
        2. 新特徵語義池 (含原始範例): {raw_input_json}

        [TASK]
        請分析 [新特徵語義池]，並產出擴充代碼。執行邏輯如下：
        1. 語義聚合：觀察 [raw_examples]，將相似意義的項目歸類。
           範例：若 raw_examples 有 ["植物奶", "燕麥奶", "oat milk"]，應合併為 "燕麥奶" 標籤。
        2. 關鍵字擴充：根據 [raw_examples] 提取所有具備識別度的關鍵字，放入 norm_rules。
        3. 友善度納入：若發現「老闆親切」、「環境友善」等高頻特徵，請務必納入。
        4. 格式對齊：
           - NORM_RULES 格式: "顯示名稱": ["關鍵字1", "關鍵字2"]
           - FEATURE_DEFINITION 格式: "顯示名稱": ("key_name", True)

        [OUTPUT REQUIREMENT]
        請直接輸出 JSON，結構如下：
        {{
            "updated_existing_rules": {{ "已有的顯示名稱": ["新增關鍵字1", "新增關鍵字2"] }},
            "new_tags": {{
                "新顯示名稱": {{
                    "norm_rules": ["關鍵字1", "關鍵字2", "關鍵字3"],
                    "feature_def": ["snake_case_key", true]
                }}
            }}
        }}
        """

        print("🤖 [Step 3] 正在調用 Vertex AI 進行語義歸一化...")
        response = self.model.generate_content(
            prompt,
            generation_config={
                "response_mime_type": "application/json",
                "temperature": 0.1 # 低隨機性確保格式穩定
            }
        )
        
        evolution_plan = json.loads(response.text)
        print("\n✨ Vertex AI 產出的「關鍵字豐富版」進化建議：")
        print(json.dumps(evolution_plan, indent=4, ensure_ascii=False))
        
        with open("schema_evolution_proposal.json", "w", encoding="utf-8") as f:
            json.dump(evolution_plan, f, ensure_ascii=False, indent=4)
        
        with open("stage_a_final_audit_results.json", "w", encoding="utf-8") as f:
            json.dump(self.all_results, f, ensure_ascii=False, indent=4)
        
        print(f"\n✅ 進化方案已存至: schema_evolution_proposal.json")

if __name__ == "__main__":
    CONFIG = {
        "project_id": "project-tjr104-cafe", 
        "location": "us-central1",
        "bucket_name": "tjr104-cafe-datalake",
        "gcs_result_path": "batch_output/stage_a/20260212_145205/prediction-model-2026-02-12T06:52:08.835011Z/predictions.jsonl"
    }
    evolver = StageAFinalEvolver(**CONFIG)
    evolver.run_pipeline()