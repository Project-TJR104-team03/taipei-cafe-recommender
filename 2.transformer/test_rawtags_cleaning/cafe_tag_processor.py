import pandas as pd
import re
import json
import tag_config
from collections import Counter
import gcsfs

# --- [核心處理引擎：保持邏輯完全對齊] ---
def normalize_tag(raw_tag_text):
    if raw_tag_text in tag_config.TAG_MAPPING:
        return tag_config.TAG_MAPPING[raw_tag_text][0]
    if raw_tag_text in tag_config.FEATURE_DEFINITION:
        return raw_tag_text
    raw_lower = raw_tag_text.lower()
    for std_name, keywords in tag_config.NORM_RULES.items():
        if any(k.lower() in raw_lower for k in keywords):
            return std_name
    return raw_tag_text

def process_cafe_engine(place_id, tag_series):
    doc = {
        "place_id": place_id,
        "official_tags": {},
        "features": {
            "has_wifi": False, "has_plug": False, "is_work_friendly": False,
            "is_quiet": False, "parking_easy": None, "can_reserve": False,
            "has_dessert": False, "is_vegetarian_friendly": False, "has_meal": False,
            "has_alcohol": False, "has_delivery": False, "has_outdoor_seating": False,
            "has_restroom": False, "is_lgbtq_friendly": False, "is_smoke_free": False,
            "has_smoking_area": False, "accessibility": False,
            "accept_credit_card": False, "accept_mobile_payment": False, "is_cash_only": False
        }
    }
    unmapped_in_this_doc = {}
    for row in tag_series:
        if pd.isna(row): continue
        parts = re.split(r'[：:]', str(row), maxsplit=1)
        if len(parts) < 2: continue
        raw_cat, raw_content = parts[0].strip(), parts[1].strip()
        mongo_key = tag_config.CAT_MAP.get(raw_cat, f"auto_{raw_cat}")
        if mongo_key not in doc["official_tags"]:
            doc["official_tags"][mongo_key] = set()
        tags = [t.strip() for t in raw_content.split('|')]
        for t in tags:
            norm_name = normalize_tag(t)
            doc["official_tags"][mongo_key].add(norm_name)
            feat_info = tag_config.FEATURE_DEFINITION.get(norm_name)
            if feat_info:
                f_key, f_val = feat_info
                if norm_name == "信用卡": doc["features"]["accept_credit_card"] = f_val
                elif norm_name == "電子支付": doc["features"]["accept_mobile_payment"] = f_val
                elif norm_name == "現金": doc["features"]["is_cash_only"] = f_val
                else: doc["features"][f_key] = f_val
            if norm_name == t and t not in tag_config.FEATURE_DEFINITION:
                unmapped_in_this_doc[t] = (t, mongo_key)
    
    # 邏輯校正
    if doc["features"]["accept_credit_card"] or doc["features"]["accept_mobile_payment"]:
        doc["features"]["is_cash_only"] = False
    elif doc["features"]["is_cash_only"]:
        doc["features"]["accept_credit_card"] = doc["features"]["accept_mobile_payment"] = False
    if doc["features"]["has_wifi"] and doc["features"]["has_plug"]:
        doc["features"]["is_work_friendly"] = True
        
    doc["official_tags"] = {k: list(v) for k, v in doc["official_tags"].items()}
    return doc, unmapped_in_this_doc

# --- [混合模式主程序] ---
if __name__ == "__main__":
    # ☁️ 雲端輸入路徑 (請依實際 Bucket 名稱修改)
    CLOUD_INPUT_PATH = "gs://tjr104-cafe-datalake/raw/tag/tags_total.csv"
    
    # 💻 地端輸出路徑
    LOCAL_OUTPUT_JSON = "cafe_data_final.json"
    LOCAL_REPORT_FILE = "needs_normalization.py"

    print(f"📖 正在從 GCS 雲端讀取資料...")

    try:
        # 直接讀取雲端 CSV (需要安裝 gcsfs)
        df = pd.read_csv(CLOUD_INPUT_PATH)
        all_docs = []
        global_unmapped = Counter()
        unmapped_meta = {}

        for pid, group in df.groupby('place_id'):
            doc, unmapped = process_cafe_engine(pid, group['Tag'])
            all_docs.append(doc)
            global_unmapped.update(unmapped.keys())
            unmapped_meta.update(unmapped)

        # 寫出到本地檔案進行觀察
        print(f"📥 正在將處理結果儲存至地端：{LOCAL_OUTPUT_JSON}")
        with open(LOCAL_OUTPUT_JSON, "w", encoding="utf-8") as f:
            json.dump(all_docs, f, ensure_ascii=False, indent=2)

        if global_unmapped:
            with open(LOCAL_REPORT_FILE, "w", encoding="utf-8") as f:
                f.write("# --- [審核報告] ---\n")
                f.write("PRIORITY_TAGS = {\n")
                for tag, count in global_unmapped.most_common(50):
                    meta = unmapped_meta[tag]
                    f.write(f"    '{tag}': ('{tag}', '{meta[1]}', None, None),  # 次數: {count}\n")
                f.write("}\n")
            print(f"📂 審核報告已產出：{LOCAL_REPORT_FILE}")

        print("\n✅ Hybrid 處理完成！現在你可以打開地端檔案進行檢查了。")

    except Exception as e:
        print(f"❌ 執行失敗: {e}")