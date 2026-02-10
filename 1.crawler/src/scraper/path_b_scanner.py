import sys
import os
import time
import io
import pandas as pd
import googlemaps
from google.cloud import storage, secretmanager

# --- 1. 路徑與模組設定 ---
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if root_path not in sys.path:
    sys.path.append(root_path)

try:
    from src.config.regions import CAFE_REGIONS, MODE_HIGH
except ImportError:
    print(" 警告: 無法匯入 src.config.regions，使用預設設定。")
    CAFE_REGIONS = {} 
    MODE_HIGH = "high"

# --- 2. 雲端工具函式 ---

def get_secret(secret_resource_name):
    """從 Secret Manager 獲取 API Key"""
    client = secretmanager.SecretManagerServiceClient()
    try:
        response = client.access_secret_version(request={"name": secret_resource_name})
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        print(f" 無法存取 Secret Manager: {e}")
        sys.exit(1)

def upload_to_gcs(df, bucket_name, destination_blob_name):
    """DataFrame 轉 CSV 上傳 GCS"""
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
        blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
        print(f" 上傳成功: gs://{bucket_name}/{destination_blob_name}")
    except Exception as e:
        print(f" 上傳 GCS 失敗: {e}")

def download_from_gcs_to_df(bucket_name, blob_name):
    """從 GCS 下載 CSV"""
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        if blob.exists():
            content = blob.download_as_text()
            return pd.read_csv(io.StringIO(content))
        return pd.DataFrame()
    except Exception as e:
        print(f" 讀取 GCS 失敗 (可能是第一次跑): {e}")
        return pd.DataFrame()

# --- 3. 核心邏輯：網格搜尋 ---
def get_cafes_with_grid(gmaps_client, lat, lng, rad, offset, mode, limit=None):
    if mode == MODE_HIGH:
        points = [(lat + i * offset, lng + j * offset) for i in [-1, 0, 1] for j in [-1, 0, 1]]
    else:
        points = [(lat, lng), (lat+offset, lng), (lat-offset, lng), (lat, lng+offset), (lat, lng-offset)]

    all_basic_results = []
    print(f" 啟動網格掃描...")

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
                print(f" Places API 請求錯誤: {e}")
                break

    unique_places = list({p['place_id']: p for p in all_basic_results}.values())
    
    # 初步過濾：如果有地址且包含新北市，先踢掉 (省一點 token)
    filtered_places = []
    for p in unique_places:
        addr = p.get('formatted_address', '')
        if '新北市' not in addr:
            filtered_places.append(p)
            
    if limit:
        filtered_places = filtered_places[:limit]
        print(f" 已套用數量限制：{len(filtered_places)} 筆")
        
    return filtered_places

# --- 4. 核心邏輯：詳細資料抓取 ---
def fetch_details(gmaps_client, unique_places):
    store_list = []
    dynamic_list = []
    print(f"\n 開始詳細欄位採集 (共 {len(unique_places)} 筆)...")

    for idx, place in enumerate(unique_places):
        p_id = place['place_id']
        name = place['name']
        address = place.get('formatted_address', '') # Basic search 的地址
        
        # [過濾器 Level 1]
        if '新北市' in address:
            print(f"[{idx+1}]  跳過 (位於新北市): {name}")
            continue

        print(f"[{idx+1}/{len(unique_places)}] 正在採集: {name}")

        try:
            details = gmaps_client.place(
                place_id=p_id,
                fields=[
                    'formatted_phone_number', 
                    'formatted_address', 
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
            print(f"     {name} 採集失敗: {e}")
            details = {}

        # [過濾器 Level 2] 詳細地址確認
        full_address = details.get('formatted_address', address)
        if '新北市' in full_address:
            print(f"     詳細地址確認為新北市，剔除。")
            continue

        # 資料清洗與轉換
        loc = place.get('geometry', {}).get('location', {})
        weekday_text = details.get('opening_hours', {}).get('weekday_text', [])
        f_opening = " | ".join(weekday_text) if weekday_text else None
        
        raw_type = details.get('type') or place.get('types', [])
        f_types = ",".join(raw_type) if isinstance(raw_type, list) else str(raw_type)

        # --- A. Store Table ---
        store_list.append({
            'name': name,
            'place_id': p_id,
            'formatted_phone_number': details.get('formatted_phone_number'),
            'formatted_address': full_address,
            'website': details.get('website'),
            'location': f"POINT({loc.get('lng')} {loc.get('lat')})" if loc else None,
            'opening_hours': f_opening,
            'price_level': details.get('price_level'),
            'business_status': details.get('business_status'),
            'types': f_types,
            'payment_options': "",
            'google_maps_url': "" 
        })

        # --- B. Dynamic Table ---
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

# --- 5. 模組化入口 (被 main.py 呼叫) ---
def run(region="A-2"):
    """
    執行網格掃描任務
    :param region: 指定要掃描的區域代碼 (例如 "A-2")
    """
    print(f" [Scanner] 模組啟動，目標區域: {region}")

    SECRET_RESOURCE_NAME = os.getenv("SECRET_RESOURCE_NAME")
    BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "tjr104-cafe-datalake")
    
    # [修改點] 
    # 強制只跑指定的 region，不使用 SCAN_ALL
    SCAN_REGION = region 
    
    SCAN_LIMIT_RAW = os.getenv("SCAN_LIMIT")
    SCAN_LIMIT = int(SCAN_LIMIT_RAW) if (SCAN_LIMIT_RAW and SCAN_LIMIT_RAW.isdigit()) else None

    if not BUCKET_NAME or not SECRET_RESOURCE_NAME:
        print(" 錯誤: 缺少環境變數 GCS_BUCKET_NAME 或 SECRET_RESOURCE_NAME")
        # 在模組中，不建議直接 sys.exit()，但如果缺 key 也只能停了
        return

    api_key = get_secret(SECRET_RESOURCE_NAME)
    gmaps = googlemaps.Client(key=api_key)

    # 1. 讀取 GCS 現有名單 (黑名單機制)
    print(f" 正在檢查 GCS 現有資料庫...")
    df_existing_base = download_from_gcs_to_df(BUCKET_NAME, "raw/store/base.csv")
    df_existing_dynamic = download_from_gcs_to_df(BUCKET_NAME, "raw/store_dynamic/store_dynamic.csv")
    
    existing_ids = set(df_existing_base['place_id']) if not df_existing_base.empty else set()
    print(f" 目前資料庫已存有 {len(existing_ids)} 筆店家。")

    all_stores_new = []
    all_dynamic_new = []

    # 2. 準備執行清單 (單區模式)
    run_list = [SCAN_REGION]

    # 3. 執行任務循環
    for r_id in run_list:
        cfg = CAFE_REGIONS.get(r_id)
        if not cfg: 
            print(f" 找不到區域設定: {r_id}，跳過。")
            continue
        
        print(f"\n 正在處理區域: {r_id} ...")
        
        # (A) 搜尋：網格抓回一堆 ID
        basic_list = get_cafes_with_grid(
            gmaps, cfg['lat'], cfg['lng'], cfg['radius'], cfg['offset'], cfg['mode'], limit=SCAN_LIMIT
        )
        
        if not basic_list: continue

        # (B) 過濾重複 (只抓 DB 沒有的)
        new_to_crawl = [p for p in basic_list if p['place_id'] not in existing_ids]
        print(f" 網格掃到 {len(basic_list)} 筆，其中 {len(new_to_crawl)} 筆是新發現，準備採集...")

        if not new_to_crawl:
            print(f" 區域 {r_id} 無新店家，跳過 API 詳細採集。")
            continue

        # (C) 抓細節
        store_data, dynamic_data = fetch_details(gmaps, new_to_crawl)

        if store_data:
            all_stores_new.extend(store_data)
            for item in store_data:
                existing_ids.add(item['place_id'])
                
        if dynamic_data:
            all_dynamic_new.extend(dynamic_data)
            
        print(f" 區域 {r_id} 新數據已暫存。")

    # --- 4. 統一存檔 ---
    print(f"\n 正在執行全量整合與上傳...")

    if all_stores_new:
        df_new_base = pd.DataFrame(all_stores_new)
        df_total_base = pd.concat([df_existing_base, df_new_base], ignore_index=True)
        df_total_base = df_total_base.drop_duplicates(subset=['place_id'], keep='first')
        upload_to_gcs(df_total_base, BUCKET_NAME, "raw/store/base.csv")
        print(f" 總表更新完成！目前共 {len(df_total_base)} 筆店家。")
    else:
        print(" 本次任務無新店家存入 Base Table。")

    if all_dynamic_new:
        df_new_dynamic = pd.DataFrame(all_dynamic_new)
        df_total_dynamic = pd.concat([df_existing_dynamic, df_new_dynamic], ignore_index=True)
        upload_to_gcs(df_total_dynamic, BUCKET_NAME, "raw/store_dynamic/store_dynamic.csv")
        print(f"💾 動態表更新完成。")
    
    print("\n Scanner 任務結束！")