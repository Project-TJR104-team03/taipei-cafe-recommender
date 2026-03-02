import pandas as pd
import re
import json
import os
from configs import tag_config
from collections import Counter
from google.cloud import storage

from dotenv import load_dotenv

load_dotenv()

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

def process_cafe_engine(place_id, name, tag_series):
    doc = {
        "place_id": place_id,
        "name": name,
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

    # 從環境變數讀取配置
    PROJECT_ID = os.getenv("PROJECT_ID", "project-tjr104-cafe")
    BUCKET_NAME = os.getenv("BUCKET_NAME", "tjr104-cafe-datalake")
    
    # 輸入路徑維持 pandas gs:// 格式 (需確保有安裝 gcsfs)
    RAW_TAGS_PATH = os.getenv("GCS_RAW_TAGS_PATH", "raw/tag/tags_official.csv")
    CLOUD_INPUT_PATH = f"gs://{BUCKET_NAME}/{RAW_TAGS_PATH}"
    
    # 輸出 GCS 路徑
    GCS_OUTPUT_JSON_PATH = os.getenv("GCS_CAFE_DATA_FINAL_PATH", "transform/stage0/cafe_data_final.json")
    GCS_REPORT_PATH = os.getenv("GCS_TAG_REPORT_PATH", "transform/stage0/needs_normalization.py")

    print(f"📖 正在從 GCS 雲端讀取資料...")

    try:
        # 直接讀取雲端 CSV (需要安裝 gcsfs)
        storage_client = storage.Client(project=PROJECT_ID)
        bucket = storage_client.bucket(BUCKET_NAME)

        df = pd.read_csv(CLOUD_INPUT_PATH)
        all_docs = []
        global_unmapped = Counter()
        unmapped_meta = {}

        for pid, group in df.groupby('place_id'):
            cafe_name = group['name'].iloc[0] if 'name' in group.columns else "Unknown"
            doc, unmapped = process_cafe_engine(pid, cafe_name, group['Tag'])
            all_docs.append(doc)
            global_unmapped.update(unmapped.keys())
            unmapped_meta.update(unmapped)
        
        # 輸出至雲端
        print(f"📥 正在將處理結果儲存至 GCS：gs://{BUCKET_NAME}/{GCS_OUTPUT_JSON_PATH}")
        json_data = json.dumps(all_docs, ensure_ascii=False, indent=2)
        json_blob = bucket.blob(GCS_OUTPUT_JSON_PATH)
        json_blob.upload_from_string(json_data, content_type='application/json')

        # 寫出審核報告到 GCS
        if global_unmapped:
            report_lines = ["# --- [審核報告] ---\n", "PRIORITY_TAGS = {\n"]
            for tag, count in global_unmapped.most_common(50):
                meta = unmapped_meta[tag]
                report_lines.append(f"    '{tag}': ('{tag}', '{meta[1]}', None, None),  # 次數: {count}\n")
            report_lines.append("}\n")
            
            report_blob = bucket.blob(GCS_REPORT_PATH)
            report_blob.upload_from_string("".join(report_lines), content_type='text/plain; charset=utf-8')
            print(f"📂 審核報告已產出至：gs://{BUCKET_NAME}/{GCS_REPORT_PATH}")

        print("\n✅ Hybrid 處理完成！資料已全面落地雲端。")

    except Exception as e:
        print(f"❌ 執行失敗: {e}")