import pandas as pd
import json
import os
import re
from datetime import datetime

current_file_path = os.path.abspath(__file__)
BASE_DIR = os.path.dirname(os.path.dirname(current_file_path))
INPUT_FILE = os.path.join(BASE_DIR, "data", "raw", "base.csv")
OUTPUT_FILE = os.path.join(BASE_DIR, "data", "processed", "full_stores_1536_v1.json")

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
    if not os.path.exists(INPUT_FILE):
        print(f"錯誤：找不到來源檔案 {INPUT_FILE}")
        return

    # 定義CSV欄位名稱
    cols = ['name', 'place_id', 'phone', 'address', 'website', 'location', 
            'hours', 'price', 'status', 'types', 'payment']
    
    # 讀取玄量數據
    df = pd.read_csv(INPUT_FILE, names=cols, header=0, quotechar='"')
    total_count = len(df)
    print(f"開始全量轉檔程序，總計處理 {total_count} 筆店家資料...")
    
    final_data = []
    for _, row in df.iterrows():
        raw_price = row.get('price')
        price_level = None if pd.isna(raw_price) else float(raw_price)

        # 處理價格
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
        
    # 輸出存成Json
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(final_data, f, ensure_ascii=False, indent=2)
    
    print(f"全量轉檔成功！已完成 {len(final_data)} 筆資料清洗與轉換。")
    print(f"最終 JSON 路徑：{OUTPUT_FILE}")

if __name__ == "__main__":
    run_full_process()