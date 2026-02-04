import json
import os
from pymongo import UpdateOne, MongoClient
from datetime import datetime
import certifi
from dotenv import load_dotenv

# 1. 載入 .env 檔案中的環境變數
load_dotenv()

# 2. 從環境變數中讀取連線字串
CONNECTION_STRING = os.getenv("MONGODB_URL")

# 3. 設定連線
client = MongoClient(CONNECTION_STRING, tlsCAFile=certifi.where())

# 測試是否連線成功
db = client['coffee_db']
print(f"成功連線至資料庫：{db.name}")


def run_import():
    client = MongoClient(CONNECTION_STRING)
    db = client['coffee_db']
    col = db['cafes']

    # 設定檔名
    file_path = 'full_stores_1536_v1.json'
    
    if not os.path.exists(file_path):
        print(f"找不到檔案: {file_path}")
        return

    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    bulk_ops = []
    for item in data:
        # 1. 處理日期轉換
        if isinstance(item.get('last_updated'), dict) and '$date' in item['last_updated']:
            # 轉換為 Python datetime 物件，MongoDB 才能正確存成 Date 格式
            dt_str = item['last_updated']['$date'].replace('Z', '+00:00')
            item['last_updated'] = datetime.fromisoformat(dt_str)

        # 2. 建立 Upsert 操作：以 place_id 為基準
        op = UpdateOne(
            {'place_id': item['place_id']},
            {'$set': item},
            upsert=True
        )
        bulk_ops.append(op)

    # 3. 執行批次寫入
    if bulk_ops:
        result = col.bulk_write(bulk_ops)
        print(f"--- 匯入報告 ---")
        print(f"成功處理: {len(data)} 筆資料")
        print(f"新店入庫: {result.upserted_count} 筆")
        print(f"舊店更新: {result.modified_count} 筆")

if __name__ == "__main__":
    run_import()