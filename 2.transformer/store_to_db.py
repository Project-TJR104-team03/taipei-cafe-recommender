import pandas as pd
import json
import os
import re
import io
from datetime import datetime, timezone
from google.cloud import storage
from pymongo import MongoClient, UpdateOne

# --- 1. 設定環境變數讀取 ---
def get_config():
    config = {
        "MONGO_URI": os.getenv("MONGO_URI"),
        "BUCKET_NAME": os.getenv("BUCKET_NAME"),
        "DB_NAME": os.getenv("DB_NAME"),
        "COLLECTION_NAME": os.getenv("COLLECTION_NAME"),
        "FILE_PATH": os.getenv("FILE_PATH")
    }
    missing = [k for k, v in config.items() if not v]
    if missing:
        raise ValueError(f"❌ 缺少必要的環境變數: {', '.join(missing)}")
    return config

# --- 2. 核心解析邏輯 (已整合 v1.2) ---

def extract_area_info(address):
    """提取地區資訊，對應實測的 formatted_address"""
    if pd.isna(address):
        return {"city": "臺北市", "district": None}
    clean_addr = re.sub(r'^\d+', '', str(address).strip())
    clean_addr = re.sub(r'^(?:台灣|臺灣)', '', clean_addr.strip())
    match = re.search(r'([^\d\s]{2,3}[市縣])([^\d\s]{2,3}[區市鎮鄉])', clean_addr)
    if match:
        city = match.group(1).replace("台北市", "臺北市")
        return {"city": city, "district": match.group(2)}
    return {"city": "臺北市", "district": "中山區" if "中山區" in clean_addr else None}

def parse_wkt_point(wkt_str):
    if pd.isna(wkt_str) or not isinstance(wkt_str, str):
        return [None, None]
    match = re.search(r'POINT\s*\(([-\d.]+)\s+([-\d.]+)\)', wkt_str)
    return [float(match.group(1)), float(match.group(2))] if match else [None, None]

def parse_opening_hours_to_periods(hours_string):
    """v1.2 新增：解析營業時間為結構化分鐘數"""
    if pd.isna(hours_string) or not isinstance(hours_string, str):
        return []
    day_map = {"星期日": 0, "星期一": 1, "星期二": 2, "星期三": 3, "星期四": 4, "星期五": 5, "星期六": 6}
    periods = []
    days_data = re.split(r'[|\|\n]', hours_string)
    for day_data in days_data:
        day_match = re.search(r'(星期[一二三四五六日])', day_data)
        if not day_match or "休息" in day_data: continue
        day_idx = day_map[day_match.group(1)]
        time_pairs = re.findall(r'(\d{1,2}:\d{2})\s*[–\-~]\s*(\d{1,2}:\d{2})', day_data)
        for start_str, end_str in time_pairs:
            def to_min(s):
                h, m = map(int, s.split(':'))
                return h * 60 + m
            try:
                open_min, close_min = to_min(start_str), to_min(end_str)
                if close_min < open_min: # 跨午夜
                    periods.append({"day": day_idx, "open": open_min, "close": 1439, "is_overnight": True})
                    periods.append({"day": (day_idx + 1) % 7, "open": 0, "close": close_min, "is_overnight": True})
                else:
                    periods.append({"day": day_idx, "open": open_min, "close": close_min, "is_overnight": False})
            except: continue
    return sorted(periods, key=lambda x: (x['day'], x['open']))

# --- 3. 主執行程序 ---

def run_full_process():
    try:
        cfg = get_config()
    except ValueError as e:
        print(e); return

    print(f"📂 正在從 GCS 下載: gs://{cfg['BUCKET_NAME']}/{cfg['FILE_PATH']}")
    storage_client = storage.Client()
    bucket = storage_client.bucket(cfg['BUCKET_NAME'])
    blob = bucket.blob(cfg['FILE_PATH'])
    
    try:
        content = blob.download_as_bytes()
    except Exception as e:
        print(f"❌ GCS 下載失敗: {e}"); return

    # 💡 欄位校準：對齊 12 個欄位名稱
    cols = ['name', 'place_id', 'formatted_phone_number', 'formatted_address', 'website', 'location', 
            'opening_hours', 'price_level', 'business_status', 'types', 'payment_options', 'google_maps_url']
    
    # 讀取數據 (跳過第一行 header，手動指定標題以防錯位)
    df = pd.read_csv(io.BytesIO(content), names=cols, header=0, quotechar='"', encoding='utf-8-sig')
    print(f"開始 v1.2 轉檔同步，總計處理 {len(df)} 筆資料...")
    
    final_data = []

    for _, row in df.iterrows():
        # 處理價格 (price_level)
        raw_price = row.get('price_level')
        price_level = None if pd.isna(raw_price) else float(raw_price)

        # 處理類型
        raw_types = row.get('types')
        if pd.notna(raw_types):
            all_types = [t.strip() for t in str(raw_types).split(',')]
            kick_tags = {'point_of_interest', 'establishment', 'store'}
            types_list = [t for t in all_types if t not in kick_tags]
            if 'cafe' not in types_list: types_list.append('cafe')
        else:
            types_list = ['cafe']

        area = extract_area_info(row.get('formatted_address'))
        
        # 建構 MongoDB Schema 物件 (v1.2 結構)
        store_node = {
            "place_id": row['place_id'],
            "original_name": row['name'],
            "location": {
                "type": "Point",
                "coordinates": parse_wkt_point(row['location'])
            },
            "area_info": area,
            "attributes": {
                "price_level": price_level,
                "business_status": row.get('business_status') if pd.notna(row.get('business_status')) else "OPERATIONAL",
                "types": types_list 
            },
            "contact": {
                "phone": str(row['formatted_phone_number']) if pd.notna(row.get('formatted_phone_number')) else None,
                "website": str(row['website']) if pd.notna(row.get('website')) else None,
                "google_maps_url": row.get('google_maps_url') if pd.notna(row.get('google_maps_url')) else None
            },
            # 💡 新增營業時間區塊
            "opening_hours": {
                "periods": parse_opening_hours_to_periods(row.get('opening_hours')),
                "is_24_hours": True if (pd.notna(row.get('opening_hours')) and "24 小時" in str(row.get('opening_hours'))) else False
            },
            "embedding_config": {
                "model_name": "text-embedding-004",
                "dimensions": 1536,
                "vector": [] 
            },
            "metadata": {
                "crawler_source": "google_maps",
                "data_version": "1.2",
                "is_processed": False
            },
            "last_updated": datetime.now(timezone.utc)
        }

        if row.get('place_id'):
            final_data.append(
                UpdateOne(
                    {"place_id": row['place_id']},
                    {"$set": store_node},
                    upsert=True
                )
            )

    # 執行 MongoDB 寫入
    if final_data:
        print(f"🚀 正在批次寫入 MongoDB (Total: {len(final_data)})...")
        try:
            client = MongoClient(cfg['MONGO_URI'])
            db = client[cfg['DB_NAME']]
            collection = db[cfg['COLLECTION_NAME']]
            
            result = collection.bulk_write(final_data)
            print(f"🎉 同步完成！新增: {result.upserted_count}, 更新: {result.modified_count}")
            client.close()
        except Exception as e:
            print(f"🔥 MongoDB 錯誤: {e}")
    else:
        print("⚠️ 無有效資料")

if __name__ == "__main__":
    run_full_process()