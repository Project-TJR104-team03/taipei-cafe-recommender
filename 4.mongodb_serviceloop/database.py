from pymongo import MongoClient
import certifi
import os
from dotenv import load_dotenv

load_dotenv()

class Database:
    client: MongoClient = None

    def connect(self):
        mongo_url = os.getenv("MONGODB_URL")
        if not mongo_url:
            print("❌ 錯誤：找不到 MONGODB_URL 環境變數！")
            return
        # 使用安全連線
        self.client = MongoClient(mongo_url, tlsCAFile=certifi.where())
        print("✅ MongoDB 連線成功 (使用安全連線)")

    def close(self):
        if self.client:
            self.client.close()
            print("🛑 MongoDB 連線已關閉")

    def get_db(self):
        return self.client['coffee_db']

db_client = Database()