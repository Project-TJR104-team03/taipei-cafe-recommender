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

    print("\n[Merger] 所有合併任務執行完畢！")

if __name__ == "__main__":
    run()