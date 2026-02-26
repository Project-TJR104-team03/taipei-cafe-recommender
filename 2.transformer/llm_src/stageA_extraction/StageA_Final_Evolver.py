import json
import re
import time
import vertexai
import logging
import os
from google.cloud import storage
from dotenv import load_dotenv
from collections import defaultdict
from vertexai.generative_models import GenerativeModel, SafetySetting
from configs import tag_config as tc 

load_dotenv()

# ==========================================
# [設定區]
# ==========================================

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

PROJECT_ID = os.getenv("PROJECT_ID")
LOCATION = "us-central1"
BUCKET_NAME = os.getenv("BUCKET_NAME")
INPUT_PATH = os.getenv("GCS_FINAL_AUDIT_JSON_PATH")
OUTPUT_PATH = os.getenv("GCS_TAG_UPDATE_DASHBOARD", "transform/stageA/TAG_UPDATE_DASHBOARD.py")

# 取前 150 個最高頻的新詞
TOP_K_CANDIDATES = 150
INTERNAL_BATCH_SIZE = 50 

try:
    vertexai.init(project=PROJECT_ID, location=LOCATION)
    # 關閉安全過濾
    safety_settings = [
        SafetySetting(
            category=SafetySetting.HarmCategory.HARM_CATEGORY_HATE_SPEECH,
            threshold=SafetySetting.HarmBlockThreshold.BLOCK_NONE,
        ),
        SafetySetting(
            category=SafetySetting.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
            threshold=SafetySetting.HarmBlockThreshold.BLOCK_NONE,
        ),
        SafetySetting(
            category=SafetySetting.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT,
            threshold=SafetySetting.HarmBlockThreshold.BLOCK_NONE,
        ),
        SafetySetting(
            category=SafetySetting.HarmCategory.HARM_CATEGORY_HARASSMENT,
            threshold=SafetySetting.HarmBlockThreshold.BLOCK_NONE,
        ),
    ]
    model = GenerativeModel("gemini-2.0-flash-001")
except Exception as e:
    print(f"❌ Vertex AI 初始化失敗: {e}")
    exit()

def run_strict_evolution():
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)

    try:
        logger.info(f"📂 正在從 GCS 讀取審計結果: {INPUT_PATH}")
        blob = bucket.blob(INPUT_PATH)
        data = json.loads(blob.download_as_text(encoding='utf-8'))
    except Exception as e:
        logger.error(f"❌ 讀取 GCS 檔案失敗: {e}")
        return

    # --- [1. 重新找回關鍵字統計邏輯] ---
    logger.info("🔍 正在進行全量關鍵字統計...")
    # 建立一個已知的變體清單，避免重複處理 tag_config 裡已有的詞
    known_variants = {v for variants in tc.NORM_RULES.values() for v in variants}
    unmapped_stats = defaultdict(lambda: {"count": 0, "origins": set()})

    for pid, content in data.items():
        audit_res = content.get('audit_results', {})
        if not isinstance(audit_res, dict): audit_res = {}
        new_features = audit_res.get('new_incremental_features', [])
        
        for feat in new_features:
            raw_words = feat.get('raw_keywords', [])
            feat_name = feat.get('feature_name', '未知維度')
            for word in raw_words:
                if word not in known_variants:
                    unmapped_stats[word]["count"] += 1
                    unmapped_stats[word]["origins"].add(feat_name)

    candidates = []
    for word, info in sorted(unmapped_stats.items(), key=lambda x: x[1]['count'], reverse=True):
        if info['count'] >= 1: 
            candidates.append({
                "raw_word": word,
                "count": info['count'],
                "suggested_category": list(info['origins'])
            })

    if not candidates:
        print("✅ 無新關鍵字需處理。")
        return

    # --- [2. 進入全局語義聚合] ---
    top_candidates = candidates[:TOP_K_CANDIDATES]
    logger.info(f"📊 統計完成：鎖定全量 {len(top_candidates)} 個高頻詞，進行一次性全局語義聚合...")

    cat_map_context = json.dumps(tc.CAT_MAP, ensure_ascii=False)
    candidate_list_text = json.dumps(top_candidates, ensure_ascii=False)

    prompt = f"""
    [ROLE] Senior Data Architect & Ontologist.
    [CONTEXT] Current Categories: {cat_map_context}
    [TASK] Group all related keywords into singular "Normalized Features".
    
    [INPUT ALL KEYWORDS] 
    {candidate_list_text}

    [REQUIRED JSON FORMAT]
    {{
        "suggested_updates": [
            {{
                "type": "EXTEND",
                "cat_zh": "服務",
                "tag_zh": "服務品質不佳",
                "var_name": "low_service_quality",
                "variants": ["態度差", "臭臉", "口氣不好"],
                "code": "NORM_RULES['服務'].extend(['態度差', '臭臉', '口氣不好'])",
                "reason": "Global aggregation of negative service indicators.",
                "count": 45
            }}
        ]
    }}
    """

    try:
        logger.info("🚀 正在發射全局語義請求 (Gemini 2.0 Flash)...")
        response = model.generate_content(
            prompt, 
            safety_settings=safety_settings,
            generation_config={
                "response_mime_type": "application/json", 
                "temperature": 0.0,
                "max_output_tokens": 8192 
            }
        )

        # 這裡用正則表達式確保解析 JSON 穩定
        raw_output = response.text.strip()
        json_match = re.search(r"\{.*\}", raw_output, re.DOTALL)
        if not json_match:
            logger.error("❌ AI 回傳格式不包含有效 JSON")
            return
            
        res_json = json.loads(json_match.group())
        updates = res_json.get("suggested_updates", [])

        # --- [3. 寫入儀表板] ---
        dashboard_content = "# === 🚀 全局架構演進儀表板 ===\n\n"
        for item in updates:
            tag_zh = item.get('tag_zh') # 修正：定義變數
            code_snippet = item.get('code')
            if not tag_zh or not code_snippet: continue
            dashboard_content += f"## 📂 聚合結果：{tag_zh}\n"
            dashboard_content += f"'''\n包含原詞: {item.get('variants')}\n理由: {item.get('reason')}\n'''\n"
            dashboard_content += f"{code_snippet}\n"
            dashboard_content += f"FEATURE_DEFINITION.update({{ '{tag_zh}': ('{item.get('var_name')}', True) }})\n"
            dashboard_content += f"{'='*50}\n\n"

    # 上傳至 GCS
        bucket.blob(OUTPUT_PATH).upload_from_string(dashboard_content, content_type='text/x-python')
        logger.info(f"✅ 全局分析完成！演進建議已存至: gs://{BUCKET_NAME}/{OUTPUT_PATH}")
    
    except Exception as e:
        print(f"❌ 全局分析執行錯誤: {e}")
        
if __name__ == "__main__":
    run_strict_evolution()