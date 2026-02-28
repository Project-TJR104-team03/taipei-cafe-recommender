import os
import io
import pandas as pd
from google.cloud import storage

# --- 合併設定檔 (MERGE_CONFIG) ---
MERGE_CONFIG = {
    #  標籤資料 (Tags)
    "tags": {
        "source_folder": "raw/tag/",               # 來源：所有分散的標籤檔
        "output_file": "raw/tag/tags_official.csv",# 目標：完整標籤總表
        "dedup_key": ["place_id", "Tag"]           # 複合鍵：確保一家店的多個標籤共存
    },

    #  Google 評論 (Reviews)
    "reviews": {
        "source_folder": "raw/comments/",          # 來源：所有分散的評論檔
        "output_file": "raw/comments/reviews_all.csv",
        "dedup_key": ["review_id"]                 # 唯一鍵：用評論 ID 去重
    },

    #  動態資料 (Dynamic Info)
    "store_dynamic": {
        "source_folder": "raw/store_dynamic/",     # 來源：動態資料夾
        "output_file": "raw/store_dynamic/store_dynamic.csv",
        "dedup_key": ["place_id"]                  # 只留最新：一家店只留一筆最新的狀態
    },
    #  儲存最新進度 (Checkpoints)
        "checkpoints": {
        "source_folder": "raw/checkpoint/",        # 來源：raw/checkpoint/ 下的所有分片
        "output_file": "raw/checkpoint/checkpoint_all.csv",
        "dedup_key": ["place_id"]                  # ★ 唯一鍵：一家店只留最新的進度
    },
    #  愛食記評論 (iFoodie)
    "ifoodie": {
        "source_folder": "raw/ifoodie/",               # 來源：愛食記分片檔所在的資料夾 (請確認是否與爬蟲存檔路徑一致)
        "output_file": "raw/ifoodie/ifoodie_all.csv",  # 目標：完整的愛食記總表
        "dedup_key": ["place_id", "reviewer_name"]            # 去重鍵：同一家店同一個作者只留最新的一筆評論 (可依實際欄位調整)
    }
}

def get_gcs_client():
    return storage.Client()

def list_csv_blobs(bucket_name, prefix, exclude_file=None):
    """ 列出指定資料夾下的 CSV，並排除掉「輸出檔本身」以免無限迴圈 """
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    
    clean_list = []
    for blob in blobs:
        if blob.name.endswith(".csv"):
            # 如果這個檔案就是我們要寫入的目標檔 (例如 reviews_all.csv)，就不要讀它
            if exclude_file and blob.name == exclude_file:
                continue
            clean_list.append(blob)
    return clean_list

def read_csv_from_gcs(bucket_name, blob_name):
    """ 讀取 GCS 上的 CSV """
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    try:
        content = blob.download_as_string()
        return pd.read_csv(io.BytesIO(content))
    except Exception as e:
        print(f"讀取失敗 {blob_name}: {e}")
        return pd.DataFrame()

def upload_df_to_gcs(df, bucket_name, blob_name):
    """ 上傳 DataFrame 到 GCS """
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
    blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
    print(f"存檔成功: gs://{bucket_name}/{blob_name} (共 {len(df)} 筆)")

# --- Store Base 更新的函式 ---
def update_store_base(bucket_name):
    print("\n任務: STORE BASE UPDATE (合併 Parts 並回填 Base)")
    
    parts_folder = "raw/store/parts/"
    base_file = "raw/store/base.csv"
    
    # 1. 讀取所有 Store Parts (分片)
    part_blobs = list_csv_blobs(bucket_name, parts_folder)
    if not part_blobs:
        print(f"   目錄 {parts_folder} 為空，無須更新。")
        return

    print(f"   發現 {len(part_blobs)} 個 Store 分片，讀取合併中...")
    df_parts_list = []
    for blob in part_blobs:
        df = read_csv_from_gcs(bucket_name, blob.name)
        if not df.empty:
            df_parts_list.append(df)
    
    if not df_parts_list:
        print("   分片皆為空，跳過。")
        return

    # 合併所有分片並去重 (只留最新抓到的資訊)
    df_updates = pd.concat(df_parts_list, ignore_index=True)
    
    # 確保只有需要的欄位，避免雜訊
    target_cols = ['place_id', 'google_maps_url', 'payment_options']
    # 過濾掉分片中不存在的欄位 (以防萬一)
    existing_cols = [c for c in target_cols if c in df_updates.columns]
    
    if 'place_id' not in existing_cols:
        print("   錯誤：分片資料中缺少 place_id，無法進行合併。")
        return

    df_updates = df_updates[existing_cols].drop_duplicates(subset=['place_id'], keep='last')
    print(f"   分片合併完成，準備更新資料筆數: {len(df_updates)}")

    # 2. 讀取原始 Base.csv
    print(f"   讀取主檔: {base_file}")
    df_base = read_csv_from_gcs(bucket_name, base_file)
    
    if df_base.empty:
        print("   錯誤：找不到 base.csv 或檔案為空，無法進行更新。")
        return

    # 3. 執行合併 (Update Logic)
    cols_to_update = [c for c in ['google_maps_url', 'payment_options'] if c in df_updates.columns]
    
    if cols_to_update:
        print(f"   正在更新欄位: {cols_to_update}")

        # 1. 把 place_id 設為 Index，這樣 Pandas 才知道誰要更新誰
        df_base.set_index('place_id', inplace=True)
        df_updates.set_index('place_id', inplace=True)
        
        # 2. 核心魔法：只把 df_updates 有的資料，覆蓋掉 df_base 對應的格子
        df_base.update(df_updates[cols_to_update])
        
        # 3. 把 Index 變回原本的欄位
        df_base.reset_index(inplace=True)
        
        # 4. 回存 Base.csv 
        upload_df_to_gcs(df_base, bucket_name, base_file)
    else:
        print("   分片中沒有 google_maps_url 或 payment_options 欄位，無需更新。")

def run(region=None):
    BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
    if not BUCKET_NAME:
        print("錯誤：未設定 GCS_BUCKET_NAME")
        return

    print(f"[Merger] 開始合併作業 (Bucket: {BUCKET_NAME})...")

    for task_name, config in MERGE_CONFIG.items():
        print(f"\n任務: {task_name.upper()}")
        
        # 1. 讀取現有的總表 (Old Data) - 實現增量更新
        old_df = pd.DataFrame()
        try:
            old_df = read_csv_from_gcs(BUCKET_NAME, config['output_file'])
            if not old_df.empty:
                print(f"   讀取舊總表: {len(old_df)} 筆")
        except: pass

        # 2. 抓取新分片 (New Parts)
        # 這裡會自動排除掉 output_file 本身，避免自己讀自己
        all_blobs = list_csv_blobs(BUCKET_NAME, config['source_folder'], exclude_file=config['output_file'])
        
        if not all_blobs:
            print(f"   目錄 {config['source_folder']} 為空，跳過。")
            continue
            
        print(f"   發現 {len(all_blobs)} 個新分片，讀取中...")
        
        new_df_list = []
        for blob in all_blobs:
            df_part = read_csv_from_gcs(BUCKET_NAME, blob.name)
            if not df_part.empty:
                new_df_list.append(df_part)
                
        if not new_df_list and old_df.empty:
            print("   沒有任何資料可合併，跳過。")
            continue

        # 3. 合併 (Old + New)
        full_df = pd.concat([old_df] + new_df_list, ignore_index=True)
        print(f"   合併後總筆數 (未去重): {len(full_df)}")

        # 4. 去重 (Deduplicate)
        if config['dedup_key']:
            before_len = len(full_df)
            # keep='last' 代表如果 ID 重複，保留「最後讀進來」的那一筆 (通常是新的)
            full_df = full_df.drop_duplicates(subset=config['dedup_key'], keep='last')
            print(f"   去重後剩餘: {len(full_df)} (刪除 {before_len - len(full_df)} 筆重複)")
        
        # 5. 上傳結果
        upload_df_to_gcs(full_df, BUCKET_NAME, config['output_file'])
    
    update_store_base(BUCKET_NAME)

    print("\n[Merger] 所有合併任務執行完畢！")

if __name__ == "__main__":
    run()