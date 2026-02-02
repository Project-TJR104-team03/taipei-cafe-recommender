import sys
import os
import time
import io
import pandas as pd
import googlemaps
from google.cloud import storage, secretmanager
from src.config.regions import CAFE_REGIONS, MODE_HIGH, MODE_LOW

# --- 1. 路徑設定 (保留以確保模組引用正常) ---
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if root_path not in sys.path:
    sys.path.append(root_path)

# 假設 src.config.regions 存在於你的專案結構中
try:
    from src.config.regions import CAFE_REGIONS, MODE_HIGH
except ImportError:
    # 預防性錯誤處理，若在單檔測試時沒有 config 檔
    print("⚠️ 警告: 無法匯入 src.config.regions，請確保專案結構正確。")
    CAFE_REGIONS = {} 
    MODE_HIGH = "high"

# --- 2. 雲端工具函式 ---

def get_secret(secret_resource_name):
    """
    從 Google Secret Manager 獲取敏感資訊 (API Key)
    格式: projects/{project_id}/secrets/{secret_id}/versions/latest
    """
    client = secretmanager.SecretManagerServiceClient()
    try:
        response = client.access_secret_version(request={"name": secret_resource_name})
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        print(f"❌ 無法存取 Secret Manager ({secret_resource_name}): {e}")
        # 在 Cloud Run 中，這會導致容器崩潰並重新啟動 (CrashLoopBackOff)，這是預期行為
        sys.exit(1)

def upload_to_gcs(df, bucket_name, destination_blob_name):
    """
    將 DataFrame 直接轉為 CSV 並上傳至 GCS (不落地)
    """
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        # 使用記憶體緩衝區，避免寫入容器硬碟
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
        
        blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
        print(f"✅ 上傳成功: gs://{bucket_name}/{destination_blob_name}")
    except Exception as e:
        print(f"❌ 上傳 GCS 失敗: {e}")


def download_from_gcs_to_df(bucket_name, blob_name):
    """從 GCS 讀取現有總表，若不存在則回傳空 DataFrame"""
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        if blob.exists():
            content = blob.download_as_text()
            return pd.read_csv(io.StringIO(content))
        return pd.DataFrame()
    except Exception as e:
        print(f"⚠️ 讀取 GCS 總表失敗 (可能是第一次跑): {e}")
        return pd.DataFrame()




# --- 3. 核心邏輯：網格搜尋 (保留原始邏輯) ---
def get_cafes_with_grid(gmaps_client, lat, lng, rad, offset, mode, limit=None):
    if mode == MODE_HIGH:
        points = [(lat + i * offset, lng + j * offset) for i in [-1, 0, 1] for j in [-1, 0, 1]]
    else:
        points = [(lat, lng), (lat+offset, lng), (lat-offset, lng), (lat, lng+offset), (lat, lng-offset)]

    all_basic_results = []
    print(f"🚀 啟動網格掃描...")

    for i, (p_lat, p_lng) in enumerate(points):
        next_page_token = None
        while True:
            try:
                res = gmaps_client.places(
                    query='咖啡廳 OR 咖啡店',
                    location=(p_lat, p_lng),
                    radius=rad,
                    language='zh-TW',
                    page_token=next_page_token
                )
                all_basic_results.extend(res.get('results', []))
                next_page_token = res.get('next_page_token')
                if not next_page_token: break
                time.sleep(2)
            except Exception as e:
                print(f"⚠️ Places API 請求錯誤: {e}")
                break

    unique_places = list({p['place_id']: p for p in all_basic_results}.values())
    if limit:
        unique_places = unique_places[:limit]
        print(f" 已套用數量限制：{len(unique_places)} 筆")
    return unique_places

# --- 4. 核心邏輯：詳細資料抓取 (保留原始邏輯) ---
def fetch_details(gmaps_client, unique_places):
    store_list = []
    dynamic_list = []
    print(f"\n 開始詳細欄位採集 (共 {len(unique_places)} 筆)...")

    for idx, place in enumerate(unique_places):
        p_id = place['place_id']
        name = place['name']
        loc = place.get('geometry', {}).get('location', {})
        print(f"[{idx+1}/{len(unique_places)}] 正在採集: {name}")

        try:
            details = gmaps_client.place(
                place_id=p_id,
                fields=[
                    'formatted_phone_number', 
                    'website', 
                    'rating', 
                    'opening_hours', 
                    'price_level', 
                    'business_status', 
                    'type', 
                    'user_ratings_total'
                ],
                language='zh-TW'
            ).get('result', {})
        except Exception as e:
            print(f"    {name} 採集失敗: {e}")
            details = {}

        # 資料清洗與轉換
        weekday_text = details.get('opening_hours', {}).get('weekday_text', [])
        f_opening = " | ".join(weekday_text) if weekday_text else None
        
        raw_type = details.get('type') or place.get('types', [])
        f_types = ",".join(raw_type) if isinstance(raw_type, list) else str(raw_type)

        # --- A. Store Table ---
        store_list.append({
            'name': name,
            'place_id': p_id,
            'formatted_phone_number': details.get('formatted_phone_number'),
            'formatted_address': place.get('formatted_address'),
            'website': details.get('website'),
            'location': f"POINT({loc.get('lng')} {loc.get('lat')})" if loc else None,
            'opening_hours': f_opening,
            'price_level': details.get('price_level'),
            'business_status': details.get('business_status'),
            'types': f_types,
            'payment_options': "" 
        })

        # --- B. Store_Dynamic_Feedback Table ---
        dynamic_list.append({
            'place_id': p_id,
            'name': name,
            'rating': details.get('rating'),
            'user_ratings_total': details.get('user_ratings_total'),
            'data_source': 'Google_Maps_API',
            'processed_at': time.strftime('%Y-%m-%d %H:%M:%S')
        })
        time.sleep(0.5)

    return store_list, dynamic_list

# --- 5. 大腦控制中心 (Cloud Run 入口) ---
if __name__ == "__main__":
    # ... (前面的讀取環境變數、初始化 gmaps 邏輯不變) ...

    # 🌟 插入點 A：在任務開始前，讀取 GCS 現有總表 (這就是你的「黑名單」)
    print(f"🔍 正在檢查 GCS 現有資料庫...")
    # 使用我們之前寫的讀取函式 (假設你已定義 download_from_gcs_to_df)
    SECRET_RESOURCE_NAME = os.getenv("SECRET_RESOURCE_NAME")
    BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "tjr104-cafe-datalake")
    
    SCAN_ALL = os.getenv("SCAN_ALL", "false").lower() == "true"
    SCAN_REGION = os.getenv("SCAN_REGION", "A-2")
    SCAN_LIMIT_RAW = os.getenv("SCAN_LIMIT")
    SCAN_LIMIT = int(SCAN_LIMIT_RAW) if (SCAN_LIMIT_RAW and SCAN_LIMIT_RAW.isdigit()) else None

    api_key = get_secret(SECRET_RESOURCE_NAME)
    gmaps = googlemaps.Client(key=api_key)

    # 檢查變數是否存在
    if not BUCKET_NAME:
        print("❌ 錯誤: 缺少環境變數 GCS_BUCKET_NAME")
        sys.exit(1)
    df_existing_base = download_from_gcs_to_df(BUCKET_NAME, "raw/store/base.csv")
    df_existing_dynamic = download_from_gcs_to_df(BUCKET_NAME, "raw/store_dynamic/store_dynamic.csv")
    
    # 建立一個 ID 集合，用來快速比對
    existing_ids = set(df_existing_base['place_id']) if not df_existing_base.empty else set()
    print(f"📊 目前資料庫已存有 {len(existing_ids)} 筆店家。")

    all_stores_new = []   # 這次任務新抓到的基本資料
    all_dynamic_new = []  # 這次任務新抓到的動態資料

    # 3. 決定執行範圍
    run_list = list(CAFE_REGIONS.keys()) if SCAN_ALL else [SCAN_REGION]

    # 4. 執行任務循環
    for r_id in run_list:
        cfg = CAFE_REGIONS.get(r_id)
        if not cfg: continue
        
        print(f"\n📍 正在處理區域: {r_id} ...")
        
        # (A) 搜尋：網格抓回一堆 ID
        basic_list = get_cafes_with_grid(
            gmaps, cfg['lat'], cfg['lng'], cfg['radius'], cfg['offset'], cfg['mode'], limit=SCAN_LIMIT
        )
        
        if not basic_list: continue

        # 🌟 插入點 B：過濾重複店家
        # 只留下「不存在於 existing_ids」的店家才去跑 fetch_details
        new_to_crawl = [p for p in basic_list if p['place_id'] not in existing_ids]
        print(f"✨ 網格掃到 {len(basic_list)} 筆，其中 {len(new_to_crawl)} 筆是新發現，準備採集...")

        if not new_to_crawl:
            print(f"⏩ 區域 {r_id} 無新店家，跳過 API 詳細採集。")
            continue

        # (B) 抓細節：只對「新面孔」花錢呼叫 API
        store_data, dynamic_data = fetch_details(gmaps, new_to_crawl)

        # 將這次新抓到的放進「新資料容器」
        if store_data:
            all_stores_new.extend(store_data)
            # 同時更新 existing_ids，避免同一次任務中跨區重疊重複抓
            for item in store_data:
                existing_ids.add(item['place_id'])
                
        if dynamic_data:
            all_dynamic_new.extend(dynamic_data)
            
        print(f"✅ 區域 {r_id} 新數據已暫存。")

    # --- 🌟 5. 插入點 C：統一「舊 + 新」合併並覆寫上傳 ---
    print(f"\n📦 正在執行全量整合與上傳...")

    # A. 處理 Base Table (靜態大表)
    if all_stores_new:
        df_new_base = pd.DataFrame(all_stores_new)
        # 合併：舊的資料 + 這次新抓的資料
        df_total_base = pd.concat([df_existing_base, df_new_base], ignore_index=True)
        # 去重
        df_total_base = df_total_base.drop_duplicates(subset=['place_id'], keep='first')
        upload_to_gcs(df_total_base, BUCKET_NAME, "raw/store/base.csv")
        print(f"💾 總表更新完成！目前共 {len(df_total_base)} 筆店家。")
    else:
        print("ℹ️ 本次任務無新店家存入 Base Table。")

    # B. 處理 Dynamic Table (動態大表)
    if all_dynamic_new:
        df_new_dynamic = pd.DataFrame(all_dynamic_new)
        # 合併：舊的動態資料 + 這次新抓的動態資料
        df_total_dynamic = pd.concat([df_existing_dynamic, df_new_dynamic], ignore_index=True)
        # 如果你希望每家店只留「最新評分」，這裡 keep='last'
        # 如果你想留存歷史，就不要去重，直接存
        upload_to_gcs(df_total_dynamic, BUCKET_NAME, "raw/store_dynamic/store_dynamic.csv")
    
    print("\n🎉 增量更新任務已順利結束！")