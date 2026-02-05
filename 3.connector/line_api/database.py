### 連線管理中心
### 以後任何檔案要用資料庫，只要 import 這個檔案就好


# database.py
from pymongo import MongoClient
import certifi
import os
from dotenv import load_dotenv

# 1. 載入環境變數
load_dotenv()

class Database:
    client: MongoClient = None

    def connect(self):
        # 這裡直接讀取環境變數，安全又方便
        mongo_url = os.getenv("MONGODB_URL")
        
        if not mongo_url:
            print("❌ 錯誤：找不到 MONGODB_URL 環境變數！請檢查 .env 檔案。")
            return

        self.client = MongoClient(mongo_url, tlsCAFile=certifi.where())
        print("✅ MongoDB 連線成功 (使用安全連線)")

    def close(self):
        if self.client:
            self.client.close()
            print("🛑 MongoDB 連線已關閉")

    def get_db(self):
        # 回傳你的資料庫名稱
        return self.client['coffee_db']

# 建立實體
db_client = Database()


# if __name__ == "__main__":
#     print("--- 開始執行 database.py 自我測試 ---")
#     db_client.connect()
    
#     # 順便測試一下拿資料庫
#     if db_client.client:
#         db = db_client.get_db()
#         print(f"目前連線的資料庫名稱: {db.name}")
#         db_client.close()
    
#     print("--- 測試結束 ---")