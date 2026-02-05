import pandas as pd
import json
import os
import re
from datetime import datetime
from google.cloud import storage
import io
from pymongo import MongoClient, UpdateOne

## --- 1. 設定給cloud run看的環境變數

MONGO_URI = os.getenv("MONGO_URI")
BUCKET_NAME = os.getenv("BUCKET_NAME")
DB_NAME = os.getenv("DB_NAME")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")
FILE_PATH = os.getenv("FILE_PATH")

def get_config():
    """從環境變數讀取配置，若缺少關鍵變數則報錯"""
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

## --- 2. 讀取 GCP 的 CSV 檔並轉成 Pandas DataFrame ---
def read_gcs_csv(bucket_name, file_path):
    """連線 GCS 下載指定 CSV 並回傳 DataFrame"""
    print(f"📂 正在從 GCS 下載: {file_path}...")
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_path)
    
    content = blob.download_as_bytes()
    ## 使用 utf-8-sig 處理可能包含 BOM 的繁體中文 CSV
    df = pd.read_csv(io.BytesIO(content), encoding='utf-8-sig')
    return df


def extract_area_info(address):
    if pd.isna(address):
        return {"city": "臺北市", "district": None}
    
    # 移除開頭數字、台灣等
    clean_addr = re.sub(r'^\d+', '', str(address).strip())
    clean_addr = re.sub(r'^(?:台灣|臺灣)', '', clean_addr.strip())

    # 使用Regex提取縣市、區域
    match = re.search(r'([^\d\s]{2,3}[市縣])([^\d\s]{2,3}[區市鎮鄉])', clean_addr)
    
    if match:
        city = match.group(1).replace("台北市", "臺北市")
        return {"city": city, "district": match.group(2)}
 
    return {"city": "臺北市", "district": "中山區" if "中山區" in clean_addr else None}


def parse_wkt_point(wkt_str):
    # 解析座標
    if pd.isna(wkt_str) or not isinstance(wkt_str, str):
        return [None, None]
    match = re.search(r'POINT\s*\(([-\d.]+)\s+([-\d.]+)\)', wkt_str)
    return [float(match.group(1)), float(match.group(2))] if match else [None, None]


def run_full_process():
    ## A. 取得設定
    try:
        cfg = get_config()
    except ValueError as e:
        print(e)
        return

    ## B. 從 GCS 讀取檔案 (取代原本的 INPUT_FILE)
    print(f"📂 正在從 GCS 下載: gs://{cfg['BUCKET_NAME']}/{cfg['FILE_PATH']}")
    storage_client = storage.Client()
    bucket = storage_client.bucket(cfg['BUCKET_NAME'])
    blob = bucket.blob(cfg['FILE_PATH'])
    
    try:
        content = blob.download_as_bytes()
    except Exception as e:
        print(f"❌ GCS 下載失敗: {e}")
        return

    # 定義CSV欄位名稱
    cols = ['name', 'place_id', 'phone', 'address', 'website', 'location', 
            'hours', 'price', 'status', 'types', 'payment']
    
    # C. 讀取玄量數據
    df = pd.read_csv(io.BytesIO(content), names=cols, header=0, quotechar='"', encoding='utf-8-sig')
    total_count = len(df)
    print(f"開始全量轉檔程序，總計處理 {total_count} 筆店家資料...")
    
    final_data = []

    # D. 資料清洗與轉換
    for _, row in df.iterrows():
        # 處理價格
        raw_price = row.get('price')
        price_level = None if pd.isna(raw_price) else float(raw_price)

        # 處理Tags
        raw_types = row.get('types')
        if pd.notna(raw_types):
            all_types = [t.strip() for t in str(raw_types).split(',')]
            kick_tags = {'point_of_interest', 'establishment', 'store'}
            types_list = [t for t in all_types if t not in kick_tags]
            
            if 'cafe' not in types_list:
                types_list.append('cafe')
        else:
            types_list = ['cafe']

        # 處理電話與網站
        raw_phone = row.get('phone')
        phone = None if pd.isna(raw_phone) else str(raw_phone)
        
        raw_website = row.get('website')
        website = None if pd.isna(raw_website) else str(raw_website)

        area = extract_area_info(row['address'])
        
        # 建構 MongoDB Schema 物件
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
                "business_status": row.get('status', 'OPERATIONAL'),
                "types": types_list 
            },
            "contact": {
                "phone": phone,
                "website": website
            },
            "embedding_config": {
                "model_name": "text-embedding-004",
                "dimensions": 1536,
                "vector": [] 
            },
            "metadata": {
                "crawler_source": "google_maps",
                "data_version": "1.1",
                "is_processed": False
            },
            "last_updated": {"$date": datetime.utcnow().isoformat() + "Z"}
        }
        final_data.append(store_node)
        
    # E. 寫入 MongoDB (取代原本的 json.dump)
    if final_data:
        print(f"🚀 正在連線至 MongoDB ({cfg['DB_NAME']} - {cfg['COLLECTION_NAME']})...")
        try:
            client = MongoClient(cfg['MONGO_URI'])
            db = client[cfg['DB_NAME']]
            collection = db[cfg['COLLECTION_NAME']]
            
            result = collection.bulk_write(final_data)
            
            print(f"🎉 資料庫同步成功！")
            print(f"   - 總處理: {len(final_data)} 筆")
            print(f"   - 新增: {result.upserted_count} 筆")
            print(f"   - 更新: {result.modified_count} 筆")
            
            client.close()
        except Exception as e:
            print(f"🔥 MongoDB 寫入錯誤: {e}")
    else:
        print("⚠️ 無有效資料可寫入")

if __name__ == "__main__":
    run_full_process()
