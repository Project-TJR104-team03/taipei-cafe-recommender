import sys
import os
import time
import pandas as pd
from dotenv import load_dotenv
import googlemaps

# --- 1. 路徑與環境變數  ---
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if root_path not in sys.path:
    sys.path.append(root_path)

env_path = os.path.join(root_path, ".env")
load_dotenv(dotenv_path=env_path)

from src.config.regions import CAFE_REGIONS, MODE_HIGH

api_key = os.getenv("GOOGLE_MAPS_API_KEY")
gmaps = googlemaps.Client(key=api_key)

# --- 2. 核心邏輯：網格搜尋 ---
def get_cafes_with_grid(lat, lng, rad, offset, mode, limit=None):
    if mode == MODE_HIGH:
        points = [(lat + i * offset, lng + j * offset) for i in [-1, 0, 1] for j in [-1, 0, 1]]
    else:
        points = [(lat, lng), (lat+offset, lng), (lat-offset, lng), (lat, lng+offset), (lat, lng-offset)]

    all_basic_results = []
    print(f"🚀 啟動網格掃描...")

    for i, (p_lat, p_lng) in enumerate(points):
        next_page_token = None
        while True:
            res = gmaps.places(
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

    unique_places = list({p['place_id']: p for p in all_basic_results}.values())
    if limit:
        unique_places = unique_places[:limit]
        print(f" 已套用數量限制：{len(unique_places)} 筆")
    return unique_places

# --- 3. 核心邏輯：詳細資料抓取 ---
def fetch_details(unique_places):
    store_list = []
    dynamic_list = []
    print(f"\n 開始詳細欄位採集 (共 {len(unique_places)} 筆)...")

    for idx, place in enumerate(unique_places):
        p_id = place['place_id']
        name = place['name']
        loc = place.get('geometry', {}).get('location', {})
        print(f"[{idx+1}/{len(unique_places)}] 正在採集: {name}")

        try:
            # 移除 'payment_methods'，保留 'opening_hours', 'types' (或 'type')
            details = gmaps.place(
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
        
        # 處理類別
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
            'payment_options': "" #  保留欄位回填，API 抓不到沒關係，Schema 不能亂！
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

# --- 4. 大腦控制中心 ---
if __name__ == "__main__":
    # 讀取 .env
    s_all = os.getenv("SCAN_ALL", "false").lower() == "true"
    s_region = os.getenv("SCAN_REGION", "A-2")
    s_limit_raw = os.getenv("SCAN_LIMIT")
    s_limit = int(s_limit_raw) if (s_limit_raw and s_limit_raw.isdigit()) else None
    
    # 無頭模式！
    s_headless = os.getenv("HEADLESS", "false").lower() == "true"

    print(f"\n" + "="*40)
    print(f"📡 [系統狀態確認]")
    print(f"   - SCAN_ALL   : {s_all}")
    print(f"   - SCAN_REGION: {s_region}")
    print(f"   - SCAN_LIMIT : {s_limit}")
    print(f"   - HEADLESS   : {s_headless} ")
    print(f"   - 路徑定位   : {env_path}")
    print("="*40 + "\n")

    run_list = list(CAFE_REGIONS.keys()) if s_all else [s_region]

    for r_id in run_list:
        cfg = CAFE_REGIONS.get(r_id)
        if not cfg: continue
        
        # 1. 搜尋
        basic_list = get_cafes_with_grid(cfg['lat'], cfg['lng'], cfg['radius'], cfg['offset'], cfg['mode'], limit=s_limit)
        
        # 2. 抓細節並拆分
        store_csv, dynamic_csv = fetch_details(basic_list)

        # 3. 存檔
        path_base = "data/raw/Store"
        path_dyn = "data/raw/Store_Dynamic_Feedback"
        os.makedirs(path_base, exist_ok=True); os.makedirs(path_dyn, exist_ok=True)

        pd.DataFrame(store_csv).to_csv(f"{path_base}/{r_id}_base.csv", index=False, encoding='utf-8-sig')
        pd.DataFrame(dynamic_csv).to_csv(f"{path_dyn}/{r_id}_dynamic.csv", index=False, encoding='utf-8-sig')
        print(f" {r_id} 任務完成。")

    print("\n 採集任務結束！")