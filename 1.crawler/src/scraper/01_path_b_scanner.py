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
    print(f"\n" + "="*40)
    print(f"☁️ [TJR104 Cloud Run 爬蟲系統啟動]")
    
    # 1. 讀取 Cloud Run 環境變數
    # 注意：這些變數必須在 Cloud Run 的「變數與祕密」頁面設定
    SECRET_RESOURCE_NAME = os.getenv("SECRET_RESOURCE_NAME")
    BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
    
    SCAN_ALL = os.getenv("SCAN_ALL", "false").lower() == "true"
    SCAN_REGION = os.getenv("SCAN_REGION", "A-2")
    SCAN_LIMIT_RAW = os.getenv("SCAN_LIMIT")
    SCAN_LIMIT = int(SCAN_LIMIT_RAW) if (SCAN_LIMIT_RAW and SCAN_LIMIT_RAW.isdigit()) else None
    
    # 檢查必要變數
    if not SECRET_RESOURCE_NAME or not BUCKET_NAME:
        print("❌ 錯誤: 缺少必要環境變數 (SECRET_RESOURCE_NAME 或 GCS_BUCKET_NAME)")
        sys.exit(1)

    print(f"   - Target Bucket: {BUCKET_NAME}")
    print(f"   - Scan Mode: {'ALL Regions' if SCAN_ALL else f'Region {SCAN_REGION}'}")
    print(f"   - Limit: {SCAN_LIMIT if SCAN_LIMIT else 'No Limit'}")
    print("="*40 + "\n")

    # 2. 初始化 Google Maps Client (從 Secret Manager 拿 Key)
    api_key = get_secret(SECRET_RESOURCE_NAME)
    gmaps = googlemaps.Client(key=api_key)

    # 3. 決定執行範圍
    run_list = list(CAFE_REGIONS.keys()) if SCAN_ALL else [SCAN_REGION]

    # 4. 執行任務循環
    for r_id in run_list:
        cfg = CAFE_REGIONS.get(r_id)
        if not cfg: 
            print(f"⚠️ 找不到區域設定: {r_id}，跳過。")
            continue
        
        print(f"\n📍 正在處理區域: {r_id} ...")
        
        # (A) 搜尋
        basic_list = get_cafes_with_grid(
            gmaps, cfg['lat'], cfg['lng'], cfg['radius'], cfg['offset'], cfg['mode'], limit=SCAN_LIMIT
        )
        
        if not basic_list:
            print(f"   區域 {r_id} 未找到任何店家。")
            continue

        # (B) 抓細節
        store_data, dynamic_data = fetch_details(gmaps, basic_list)

        # (C) 準備上傳 GCS
        # 加入時間戳記以利 Airflow 辨識新檔案
        timestamp = time.strftime('%Y%m%d_%H%M')

        # 上傳 Store Base Data
        if store_data:
            upload_to_gcs(
                pd.DataFrame(store_data), 
                BUCKET_NAME, 
                f"raw/store/{r_id}_{timestamp}_base.csv"
            )
        
        # 上傳 Dynamic Data
        if dynamic_data:
            upload_to_gcs(
                pd.DataFrame(dynamic_data), 
                BUCKET_NAME, 
                f"raw/store_dynamic/{r_id}_{timestamp}_dynamic.csv"
            )
            
        print(f"✨ 區域 {r_id} 處理完成。")

    print("\n✅ 所有採集任務與雲端同步已結束！")