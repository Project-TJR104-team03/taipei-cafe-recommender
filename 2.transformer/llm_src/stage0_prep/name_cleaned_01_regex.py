import pandas as pd
import re
import json
import os
import io
from google.cloud import storage

def stage1_ultimate_scrubber(name):
    """核心清洗邏輯"""
    if pd.isna(name): return "", "", []
    name = str(name).strip()
    raw_tags = []
    
    # 1. 斷點機制：處理括號
    break_match = re.search(r'[\(\（【\[《『]', name)
    if break_match:
        split_idx = break_match.start()
        regex_clean_name = name[:split_idx].strip()
        tail_content = name[split_idx:].strip()
        raw_tags.append(tail_content)
        inner_brackets = re.findall(r'[\(\（【\[《『「](.*?)[\)\）】\]》』」]', tail_content)
        raw_tags.extend(inner_brackets)
    else:
        regex_clean_name = name

    # 2. 感性斷點：處理表情符號或波浪號
    break_pattern = r'([\^_]{1,}[_]{0,}[\^_]{1,}|[～~])'
    parts = re.split(break_pattern, regex_clean_name)
    regex_clean_name = parts[0].strip()
    if len(parts) > 1:
        raw_tags.extend([p.strip() for p in parts[1:] if p.strip()])

    # 3. 末尾掃除
    regex_clean_name = re.sub(r'[^\w\u4e00-\u9fff]+$', '', regex_clean_name).strip()       
    return regex_clean_name, "", list(set(raw_tags))

# --- 正式執行程序 ---
def clean_name_by_py():

    # ================= 配置區  =================
    BUCKET_NAME = os.getenv("BUCKET_NAME", "tjr104-cafe-datalake")
    INPUT_FILE = os.getenv("GCS_RAW_STORE_PATH", "raw/store/base.csv")
    OUT_CSV = os.getenv("GCS_NAME_REGEX_CLEAND", "transform/stage0/cafes_name_regex_cleaned.csv")
    OUT_JSON = os.getenv("GCS_TAG_REGEX", "transform/stage0/cafes_tag_regex.csv")
    # ==========================================
    
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    try:
        print(f"🚀 正在讀取檔案: {INPUT_FILE}")
        blob_in = bucket.blob(INPUT_FILE)
        data = blob_in.download_as_text(encoding='utf-8-sig')
        df = pd.read_csv(io.StringIO(data))
        
        # 去重處理：防止 place_id 重複
        df = df.drop_duplicates(subset=['place_id'])
        print(f"📊 實際處理筆數: {len(df)}")

        csv_results = []
        json_map = {}
        
        for _, row in df.iterrows():
            p_id = str(row['place_id'])
            raw_n = str(row['name'])
            
            # 這裡統一呼叫 stage1_ultimate_scrubber
            c_name, br, tags = stage1_ultimate_scrubber(raw_n)
            
            csv_results.append({
                "place_id": p_id,
                "regex_clean_name": c_name,
                "branch": br,
                "original_name": raw_n
            })
            json_map[p_id] = {"clean_name": c_name, "raw_tags": tags, "original_name": raw_n}

        # 存檔
        print(f"📁 正在上傳清洗後的資料至: {OUT_CSV}")
        blob_csv = bucket.blob(OUT_CSV)
        blob_csv.upload_from_string(
            pd.DataFrame(csv_results).to_csv(index=False, encoding="utf-8-sig"),
            content_type='text/csv'
        )

        print(f"📁 正在上傳標籤資料至: {OUT_JSON}")
        blob_json = bucket.blob(OUT_JSON)
        blob_json.upload_from_string(
            json.dumps(json_map, ensure_ascii=False, indent=2),
            content_type='application/json'
        )
        print(f"✅ 第一階段初步篩選完成！")
        print(f"📁 產出 CSV: {OUT_CSV}")
        print(f"📁 產出 JSON: {OUT_JSON}")

    except FileNotFoundError:
        print(f"❌ 錯誤: 找不到檔案 '{INPUT_FILE}'。請檢查路徑是否正確。")
    except Exception as e:
        print(f"❌ 發生非預期錯誤: {e}")

if __name__ == "__main__":
    clean_name_by_py()