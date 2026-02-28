import pandas as pd
import json
import os
import logging
from configs import tag_config as tc 
import datetime
from io import BytesIO
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv()

# 配置日誌
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class StageA_OneStop_Processor:
    def __init__(self, project_id, bucket_name, gcs_distilled_path, gcs_baseline_path, gcs_output_path):
        self.client = storage.Client(project=project_id)
        self.bucket = self.client.bucket(bucket_name)
        self.gcs_distilled_path = gcs_distilled_path
        self.gcs_baseline_path = gcs_baseline_path
        self.gcs_output_path = gcs_output_path
        self.official_map = {}

    def _load_data(self):
        logger.info(f"正在從 GCS 讀取純化評論: {self.gcs_distilled_path}")
        blob = self.bucket.blob(self.gcs_distilled_path)
        return pd.read_csv(BytesIO(blob.download_as_bytes()))


    def _load_official_baseline(self):
        logger.info(f"正在從 GCS 讀取官方基準: {self.gcs_baseline_path}")
        blob = self.bucket.blob(self.gcs_baseline_path)
        raw_data = json.loads(blob.download_as_text(encoding='utf-8'))
        self.official_map = {str(item.get('place_id')): item for item in raw_data}
 

    def _build_system_instruction(self):
        feature_def = json.dumps(tc.FEATURE_DEFINITION, ensure_ascii=False)
        norm_rules = json.dumps(tc.NORM_RULES, ensure_ascii=False)
        cat_map_context = json.dumps(tc.CAT_MAP, ensure_ascii=False)

        #  Prompt 內容不要動
        return f"""
[ROLE] Lead Data Auditor. Audit [OFFICIAL_BASELINE] against [USER_REVIEWS].

[SCHEMA REGISTRY (CRITICAL)]
1. [features] 所有的 Key 必須嚴格對應 {{feature_def}} 中的英文 ID。
2. [official_tags_audit] 必須依照 {cat_map_context} 的分類進行歸納，內容為繁體中文標籤。

[LANGUAGE RULE]
- JSON Keys: 必須維持英文（不可翻譯）。
- JSON Values: 所有內容、理由、證據、總結必須使用 **繁體中文**。

[CONFIG] 
- Feature Definition: {feature_def}
- Category Map: {cat_map_context}
- Norm Rules: {norm_rules}

[TASK]
1. **Feature Logic Audit**: 
   - 根據 {{feature_def}} 更新 `features` 狀態。
   - TRUE: 評論證實存在 | FALSE: 評論證實不存在 | NULL: 未提及。
2. **Official Tags Grouping**: 
   - 根據評論提到的關鍵字，參考 {cat_map_context} 的分類，將其歸類到 `official_tags_audit`。
3. **Evidence & Analysis**: 
   - `conflict_alerts`: 記錄官方與現實不符的理由。
   - `evidence_map`: 針對 `features` 的 **英文 Key** 提供 20 字內原始節錄。

[OUTPUT SCHEMA (Strict JSON)]
{{
  "audit_results": {{
    "audit_summary": {{ "total_reviews": 50, "overall_vibe": "繁體中文總結" }},
    "official_tags_audit": {{
        "atmosphere": ["安靜", "氛圍舒適"],
        "facilities": ["洗手間", "插座"],
        "..." : "依照 CAT_MAP 的英文分類填入對應的繁體中文標籤"
    }},
    "features": {{
        "has_wifi": Boolean or Null,
        "is_quiet": Boolean or Null,
        "..." : "必須使用 feature_def 中的英文 ID，嚴禁中文 Key"
    }},
    "conflict_alerts": [
      {{
        "key": "英文代碼",
        "official_claim": "String",
        "reality_check": "String",
        "reason": "繁體中文分析理由",
        "consensus_level": 5,
        "sentiment": -1
      }}
    ],
    "new_incremental_features": [
      {{
        "feature_name": "繁體中文標籤",
        "raw_keywords": ["關鍵字"],
        "evidence": "20-30字評論節錄",
        "frequency": "High/Low"
      }}
    ],
    "evidence_map": {{ 
        "英文代碼": "20字內原始評論精華" 
    }}
  }}
}}
"""

    def generate_jsonl(self):
        df = self._load_data()
        self._load_official_baseline()

        # 防呆：確保官方基準檔存在，這是我們的 Master Table
        if not self.official_map:
            logger.error("❌ 官方基準為空，無法執行 Left Join，程式終止。")
            return

        system_instruction = self._build_system_instruction()
        today_str = datetime.date.today().isoformat()

        grouped = df.groupby('place_id') if df is not None else None
        
        logger.info(f"🚀 開始全量處理 {len(grouped)} 家店家(啟用 Left Join 防呆機制)...")

        cold_start_count = 0
        valid_payloads = 0
        output_lines = []

        # ⭐️ 核心修正：改由「官方主表」帶動迴圈，保證所有店家都會進 AI 管線
        for pid, baseline in self.official_map.items():
        
            # 嘗試從官方資料取得店名 (請依據你 cafe_data_final.json 的實際 Key 調整，通常是 name 或 title)
            place_name = baseline.get("name", baseline.get("title", f"未知店名_{pid}"))
            clean_reviews = []
            
            # 嘗試去評論庫找資料 (Left Join)
            if grouped is not None and pid in grouped.groups:
                group = grouped.get_group(pid).head(50)
                # 如果有評論，優先使用評論表中的店名確保一致性
                place_name = str(group['place_name'].iloc[0])
                for r in group['content'].dropna().tolist():
                    r_str = str(r).replace('\n', ' ').replace('\r', ' ').strip()
                    clean_reviews.append(r_str)
            
            # ==========================================
            # 🛡️ 動態組裝 User Content (觸發防呆機制)
            # ==========================================
            if not clean_reviews:
                cold_start_count += 1
                review_text_block = (
                    "【系統防呆機制觸發】\n"
                    "此店家目前缺乏有效的使用者評論。請完全依據上方的 [1. OFFICIAL BASELINE] 進行推論。\n"
                    "嚴禁幻覺：對於無法從官方標籤確認的主觀特徵（如：安靜程度、服務態度、咖啡品質等），請務必將其 Boolean 值填寫為 null。"
                )
            else:
                review_text_block = "\n".join([f"- {r}" for r in clean_reviews])

            user_content = (
                f"### [TARGET STORE]\n"
                f"Name: {place_name} (ID: {pid})\n\n"
                f"### [1. OFFICIAL BASELINE]\n"
                f"{json.dumps(baseline, ensure_ascii=False)}\n\n"
                f"### [2. USER REVIEWS]\n"
                f"{review_text_block}"
            )
            
            final_prompt = f"System Instruction:\n{system_instruction}\n\nUser Content:\n{user_content}"
            
            request_item = {
                "request": {
                    "contents": [
                        {"role": "user", "parts": [{"text": final_prompt}]}
                    ],
                    "generationConfig": { 
                        "response_mime_type": "application/json", 
                        "temperature": 0.0,
                        "max_output_tokens": 8192
                    }
                },
                "custom_id": str(pid),
                "place_name": str(place_name),
                "review_count": int(len(clean_reviews)),
                "audit_date": str(today_str)
            }
        
            # [DE 關鍵修正] 只有在這裡做 json.dumps 才是最安全的
            # 它會把字串內所有的 \n 自動轉義為 \\n，保證整筆資料在檔案中「物理上只有一行」
            json_line = json.dumps(request_item, ensure_ascii=False)
            output_lines.append(json_line.strip())
            valid_payloads += 1

        final_jsonl_content = "\n".join(output_lines)
        output_blob = self.bucket.blob(self.gcs_output_path)
        output_blob.upload_from_string(final_jsonl_content, content_type='application/jsonl')
        logger.info(f"✅ 全量封裝完成！共處理 {valid_payloads} 筆 (其中無評論冷啟動 {cold_start_count} 筆)")
        logger.info(f"✅ 已上傳至: gs://{self.bucket.name}/{self.gcs_output_path}")

if __name__ == "__main__":
    CONFIG = {
        "project_id": os.getenv("PROJECT_ID"),
        "bucket_name": os.getenv("BUCKET_NAME"),
        "gcs_distilled_path": os.getenv("GCS_DISTILLED_CSV_PATH"),
        "gcs_baseline_path": os.getenv("GCS_CAFE_DATA_FINAL_PATH"),
        "gcs_output_path": os.getenv("GCS_STAGE_A_JSONL_PATH", "transform/stageA/vertex_job_stage_a.jsonl")
    }
    processor = StageA_OneStop_Processor(**CONFIG)
    processor.generate_jsonl()