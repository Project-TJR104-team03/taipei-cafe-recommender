import json
import os
import logging

# 配置日誌
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class StageB_Processor:
    def __init__(self, original_payload, official_data, stage_a_output):
        self.original_payload = original_payload
        self.official_data = official_data
        self.stage_a_output = stage_a_output

    def load_and_merge(self):
        """核心邏輯：建立索引並合併數據"""
        
        # 1. 載入官方數據 (Baseline)
        logger.info("載入官方數據...")
        official_map = {}
        with open(self.official_data, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)
            # 假設 official_data 是 CafeTagEngine 的產出 list
            for item in raw_data:
                official_map[item['place_id']] = item

        # 2. 載入 Stage A AI 產出 (Audit Result)
        logger.info("載入 Stage A AI 驗證結果...")
        stage_a_map = {}
        if os.path.exists(self.stage_a_output):
            with open(self.stage_a_output, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        res = json.loads(line)
                        # Vertex AI Batch 結果通常在 prediction 欄位，metadata 用來對齊
                        # 注意：這裡需依照實際下載格式微調
                        pid = res.get('metadata', {}).get('place_id') 
                        if pid:
                            # 提取 AI 的核心預測內容
                            stage_a_map[pid] = res.get('prediction', {})
                    except Exception:
                        continue
        else:
            logger.warning("❌ 找不到 Stage A 結果檔！Stage B 將缺乏 AI 驗證數據。")

        return official_map, stage_a_map

    def _build_stage_b_instruction(self):
        return """
[ROLE] Senior_Space_Editor
[TASK] Write a 150-word store profile based on the [MERGED_DATA].
[LOGIC]
1. **Foundation**: Use [OFFICIAL_INFO] as the base structure.
2. **Correction**: If [AI_AUDIT] shows a conflict (e.g., Official says Quiet, AI says Noisy), TRUST the AI but mention the discrepancy tactfully.
3. **Enhancement**: Weave in [AI_AUDIT] new discoveries (e.g., Oat milk, Old house vibe) to make the description rich.
4. **Tone**: Objective, detailed, ready for Vector Search.
"""

    def generate_stage_b_shards(self, output_file):
        """生成 Stage B 的任務檔案"""
        
        official_map, stage_a_map = self.load_and_merge()
        
        # 載入原始任務以取得評論 Context
        with open(self.original_payload, 'r', encoding='utf-8') as f:
            tasks = json.load(f)

        logger.info(f"正在生成 Stage B 任務封包: {output_file}")
        
        with open(output_file, 'w', encoding='utf-8') as f_out:
            for task in tasks:
                pid = task['place_id']
                
                # --- [數據接合 Data Join] ---
                # 這裡是最關鍵的一步：把官方與 AI A 的結果結合
                merged_data = {
                    "OFFICIAL_INFO": official_map.get(pid, {}),
                    "AI_AUDIT": stage_a_map.get(pid, {})
                }
                
                # 準備 Prompt
                user_content = f"""
[TARGET STORE]: {task['place_name']}

[MERGED_DATA (Official + AI Verified)]:
{json.dumps(merged_data, ensure_ascii=False, indent=2)}

[SAMPLE_REVIEWS (For Vibe Reference)]:
{task['context']['sample_reviews'][:3]}
"""

                request_item = {
                    "request": {
                        "contents": [
                            {"role": "system", "parts": [{"text": self._build_stage_b_instruction()}]},
                            {"role": "user", "parts": [{"text": user_content}]}
                        ],
                        "generationConfig": {"temperature": 0.4} # 稍微提高一點創造力寫畫像
                    },
                    "metadata": {"place_id": pid, "stage": "B_PROFILE_GENERATION"}
                }
                f_out.write(json.dumps(request_item, ensure_ascii=False) + '\n')
                
        logger.info(f"✅ Stage B 任務生成完畢！")

if __name__ == "__main__":
    # 檔案路徑配置 (請依照你的實際路徑修改)
    ORIGINAL_PAYLOAD = "ai_ready_payload.json"
    OFFICIAL_DATA = "cafe_data_final.json"      # 來自 CafeTagEngine
    STAGE_A_RESULT = "downloaded_stage_a_results.jsonl" # 來自雲端下載
    
    OUTPUT_FILE = "vertex_job_stage_b.jsonl"    # 準備上傳的新檔案
    
    processor = StageB_Processor(ORIGINAL_PAYLOAD, OFFICIAL_DATA, STAGE_A_RESULT)
    processor.generate_stage_b_shards(OUTPUT_FILE)