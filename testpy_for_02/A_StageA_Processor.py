import pandas as pd
import json
import os
import logging
import tag_config as tc  # 確保目錄下有你的標籤定義

# 配置日誌
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class StageA_OneStop_Processor:
    def __init__(self, distilled_csv_path, official_baseline_path):
        """
        一站式處理器：直接從純化 CSV 產出 Vertex AI 任務包
        """
        self.distilled_csv_path = distilled_csv_path
        self.official_baseline_path = official_baseline_path
        self.official_map = {}

    def _load_data(self):
        """讀取 CSV 評論與官方 JSON 基準"""
        if not os.path.exists(self.distilled_csv_path):
            logger.error(f"❌ 找不到純化評論檔: {self.distilled_csv_path}")
            return None
        
        # 1. 讀取第一階段產出的 CSV
        df = pd.read_csv(self.distilled_csv_path)
        
        # 2. 讀取官方基準並建立索引 (Key: place_id)
        if os.path.exists(self.official_baseline_path):
            with open(self.official_baseline_path, 'r', encoding='utf-8') as f:
                raw_data = json.load(f)
                self.official_map = {item.get('place_id'): item for item in raw_data}
            logger.info(f"✅ 載入官方基準，共 {len(self.official_map)} 筆資料。")
        else:
            logger.warning(f"⚠️ 找不到官方基準檔: {self.official_baseline_path}")

        return df

    def _build_system_instruction(self):
        """AI 1 的劇本：數據審計員 (Data Auditor)"""
        return f"""
[SYSTEM_SPECIFICATION]
ROLE: Data_Auditor_Engine
TASK: Verify official claims against user reviews & Discover new features.
CONFIG: {tc.FEATURE_DEFINITION} / {tc.NORM_RULES}

[EXECUTION_LOGIC]
1. **Calibration (基準校驗)**: 若 [Baseline] 與評論嚴重衝突，必須在 conflict_alerts 提出修正。
2. **Incremental Discovery (增量發現)**: 挖掘標籤清單外的特色 (如：燕麥奶、特定低消、景觀)。

[OUTPUT_SCHEMA]
Strict JSON only.
"""

    def generate_jsonl(self, output_file):
        """執行一站式封裝與轉換"""
        df = self._load_data()
        if df is None: return

        system_instruction = self._build_system_instruction()
        
        # 按 place_id 分群，這就是我們原本 Packer 在做的事
        grouped = df.groupby('place_id')
        logger.info(f"🚀 開始為 {len(grouped)} 家店家生成一站式任務封包...")

        with open(output_file, 'w', encoding='utf-8') as f_out:
            count = 0
            for pid, group in grouped:
                place_name = group['place_name'].iloc[0]
                
                # [DATA JOIN] 獲取該店官方基準
                baseline = self.official_map.get(pid, {"official_tags": {}, "features": {}})
                
                # 準備評論
                review_texts = "\n".join([f"- {r}" for r in group['content'].astype(str)])
                
                user_content = f"""
### [TARGET STORE]
Name: {place_name} (ID: {pid})
### [1. OFFICIAL BASELINE]
{json.dumps(baseline, ensure_ascii=False)}
### [2. USER REVIEWS]
{review_texts}
"""

                # 封裝為 Vertex AI 格式 [關鍵修正：識別碼扁平化]
                request_item = {
                    "request": {
                        "contents": [
                            {"role": "user", "parts": [{"text": f"System Instruction: {system_instruction}\n\nUser Content: {user_content}"}]}
                        ],
                        "generationConfig": { "response_mime_type": "application/json", "temperature": 0.0 }
                    },
                    "custom_id": str(pid),
                    "place_id": str(pid),
                    "place_name": str(place_name)
                }
                
                f_out.write(json.dumps(request_item, ensure_ascii=False) + '\n')
                count += 1

        logger.info(f"✅ 一站式封裝完成：{output_file} (共 {count} 筆)")

if __name__ == "__main__":
    CONFIG = {
        "distilled_csv_path": "reviews_top50_distilled.csv", # 直接讀取 Step 1 的 CSV
        "official_baseline_path": "cafe_data_final.json",
    }
    processor = StageA_OneStop_Processor(**CONFIG)
    processor.generate_jsonl("vertex_job_stage_a.jsonl")