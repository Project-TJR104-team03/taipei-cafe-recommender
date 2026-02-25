import pandas as pd
import json
import os
import logging
import tag_config as tc 
import datetime

# 配置日誌
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class StageA_OneStop_Processor:
    def __init__(self, distilled_csv_path, official_baseline_path):
        self.distilled_csv_path = distilled_csv_path
        self.official_baseline_path = official_baseline_path
        self.official_map = {}

    def _load_data(self):
        if not os.path.exists(self.distilled_csv_path):
            logger.error(f"❌ 找不到純化評論檔: {self.distilled_csv_path}")
            return None
        return pd.read_csv(self.distilled_csv_path)

    def _load_official_baseline(self):
        if os.path.exists(self.official_baseline_path):
            with open(self.official_baseline_path, 'r', encoding='utf-8') as f:
                raw_data = json.load(f)
                self.official_map = {str(item.get('place_id')): item for item in raw_data}
            logger.info(f"✅ 載入官方基準，共 {len(self.official_map)} 筆。")

    def _build_system_instruction(self):
        # 定義 feature_def 與 norm_rules (這原本就有了)
        feature_def = json.dumps(tc.FEATURE_DEFINITION, ensure_ascii=False)
        norm_rules = json.dumps(tc.NORM_RULES, ensure_ascii=False)
        
        # [核心修正] 補上 cat_map_context 的定義，將 tc.CAT_MAP 轉為 JSON 字串
        cat_map_context = json.dumps(tc.CAT_MAP, ensure_ascii=False)

        # 這裡完全保留妳要求的 Prompt 內容
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

    def generate_jsonl(self, output_file):
        df = self._load_data()
        self._load_official_baseline()
        if df is None: return

        system_instruction = self._build_system_instruction()
        today_str = datetime.date.today().isoformat()

        grouped = df.groupby('place_id')
        logger.info(f"🚀 開始全量處理 {len(grouped)} 家店家...")

        # [DE 關鍵修正] 使用 newline='\n' 並強制不帶 BOM 的 utf-8
        with open(output_file, 'w', encoding='utf-8', newline='\n') as f_out:
            for pid, group in grouped:
                group = group.head(50)
                place_name = str(group['place_name'].iloc[0])
                baseline = self.official_map.get(str(pid), {"official_tags": {}, "features": {}})
                
                # [DE 關鍵修正] 更徹底的清洗，移除可能破壞 JSON 格式的隱藏符號
                clean_reviews = []
                for r in group['content'].dropna().tolist():
                    r_str = str(r).replace('\n', ' ').replace('\r', ' ').strip()
                    clean_reviews.append(r_str)
                
                review_text_block = "\n".join([f"- {r}" for r in clean_reviews])
                
                # 保留妳的 User Content 結構
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
                            "temperature": 0.0 
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
                f_out.write(json_line.strip() + '\n')

        logger.info(f"✅ 全量封裝完成：{output_file}")

if __name__ == "__main__":
    processor = StageA_OneStop_Processor("reviews_top50_distilled.csv", "cafe_data_final.json")
    processor.generate_jsonl("vertex_job_stage_a_final.jsonl")