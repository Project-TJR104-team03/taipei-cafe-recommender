import json
import re
import time
import vertexai
from collections import defaultdict
from vertexai.generative_models import GenerativeModel, SafetySetting
import tag_config as tc 

# ==========================================
# [設定區]
# ==========================================
PROJECT_ID = "XXX"
LOCATION = "us-central1"
INPUT_FILE = "final_readable_audit.json"
OUTPUT_FILE = "TAG_UPDATE_DASHBOARD.py"

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
    print(f"📂 正在全量讀取: {INPUT_FILE} ...")
    try:
        with open(INPUT_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"❌ 找不到檔案: {INPUT_FILE}")
        return

    # --- [1. 重新找回關鍵字統計邏輯] ---
    print("🔍 正在進行全量關鍵字統計...")
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

    # 這裡就是妳漏掉的 candidates 定義！
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
    print(f"📊 統計完成：鎖定全量 {len(top_candidates)} 個高頻詞，進行一次性全局語義聚合...")

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
        print("🚀 正在發射全局語義請求 (Gemini 2.0 Flash)...")
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
            print("❌ AI 回傳格式不包含有效 JSON")
            return
            
        res_json = json.loads(json_match.group())
        updates = res_json.get("suggested_updates", [])

        # --- [3. 寫入儀表板] ---
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            f.write("# === 🚀 全局架構演進儀表板 (Global Mode) ===\n\n")
            for item in updates:
                tag_zh = item.get('tag_zh')
                code_snippet = item.get('code')
                if not tag_zh or not code_snippet: continue

                f.write(f"## 📂 聚合結果：{tag_zh}\n")
                f.write(f"'''\n包含原詞: {item.get('variants')}\n理由: {item.get('reason')}\n'''\n")
                f.write(f"{code_snippet}\n")
                f.write(f"FEATURE_DEFINITION.update({{ '{tag_zh}': ('{item.get('var_name')}', True) }})\n")
                f.write(f"{'='*50}\n\n")

        print(f"✅ 全局分析完成！產出 {len(updates)} 組聚合建議至 {OUTPUT_FILE}")

    except Exception as e:
        print(f"❌ 全局分析執行錯誤: {e}")
        
if __name__ == "__main__":
    run_strict_evolution()