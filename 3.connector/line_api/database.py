### 連線管理中心
### 以後任何檔案要用資料庫，只要 import 這個檔案就好


# coffee_api/database.py
import os
from pymongo import MongoClient

# 優先讀取雲端環境變數，本機測試則預設為 localhost
# 部署到 Cloud Run 時，我們會設定這個 MONGO_URL
MONGO_URL = os.getenv("MONGO_URL", "mongodb://localhost:27017")

class DatabaseClient:
    def __init__(self):
        self.client = None
    
    def connect(self):
        self.client = MongoClient(MONGO_URL)
    
    def get_db(self):
        return self.client['coffee_db'] # 資料庫名稱
    
    def close(self):
        if self.client:
            self.client.close()

db_client = DatabaseClient()