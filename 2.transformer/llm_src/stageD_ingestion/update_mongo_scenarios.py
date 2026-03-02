import pandas as pd
import ast
import logging
from pymongo import MongoClient, UpdateOne, ASCENDING, DESCENDING

# ==========================================
# 參數配置區
# ==========================================
CSV_FILE = "cafes_with_scenarios_final.csv"  # 剛剛算出來的檔案
MONGO_URI = "mongodb+srv://a84682579_db_user:1zWbKmt1jR9emhHx@projectcoffee.ipknpgr.mongodb.net/"
DB_NAME = "coffee_db"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MongoScenarioUpdater:
    def __init__(self, mongo_uri, db_name):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.cafes_col = self.db["cafes"]

    def create_scenario_indexes(self):
        """
        為懶人按鈕建立降冪索引，確保前端查詢時的極致效能
        """
        logger.info("⚙️ 正在為場景分數建立資料庫索引 (Indexes)...")
        # 建立降冪索引 (-1 代表 DESCENDING)，因為前端通常是找最高分的
        self.cafes_col.create_index([("score_workspace", DESCENDING)])
        self.cafes_col.create_index([("score_dating", DESCENDING)])
        self.cafes_col.create_index([("score_pet_friendly", DESCENDING)])
        self.cafes_col.create_index([("score_relax", DESCENDING)])
        logger.info("✅ 索引建立完成！")

    def update_scenarios(self, csv_path):
        logger.info(f"📦 正在讀取場景運算結果: {csv_path}")
        try:
            df = pd.read_csv(csv_path)
        except FileNotFoundError:
            logger.error(f"❌ 找不到 CSV 檔案: {csv_path}")
            return

        ops = []
        batch_size = 500
        updated_count = 0

        # 安全的字串轉陣列函數 (因為 CSV 存 List 會變成字串 "['a', 'b']")
        def safe_eval_list(val):
            try:
                # 把字串 "['店貓', '甜點']" 轉回真正的 Python List
                return ast.literal_eval(str(val)) if pd.notna(val) else []
            except:
                return []

        logger.info("🚀 開始執行局部更新 (Partial Update)...")
        
        for _, row in df.iterrows():
            place_id = row.get("place_id")
            if not place_id: continue

            # 構建只要更新的欄位 ($set)
            update_fields = {
                "score_workspace": float(row.get("score_workspace", 0.0)),
                "tags_workspace": safe_eval_list(row.get("tags_score_workspace")),
                
                "score_dating": float(row.get("score_dating", 0.0)),
                "tags_dating": safe_eval_list(row.get("tags_score_dating")),
                
                "score_pet_friendly": float(row.get("score_pet_friendly", 0.0)),
                "tags_pet_friendly": safe_eval_list(row.get("tags_score_pet_friendly")),
                
                "score_relax": float(row.get("score_relax", 0.0)),
                "tags_relax": safe_eval_list(row.get("tags_score_relax"))
            }

            # 使用 $set 確保只覆蓋或新增這些特定欄位，不動其他資料
            update_operation = {"$set": update_fields}
            
            # 這裡不開 upsert=True，因為我們只更新「已經存在」的店家
            ops.append(UpdateOne({"place_id": place_id}, update_operation))
            
            # 批次提交
            if len(ops) >= batch_size:
                self.cafes_col.bulk_write(ops)
                updated_count += len(ops)
                ops = []

        # 提交剩餘資料
        if ops:
            self.cafes_col.bulk_write(ops)
            updated_count += len(ops)

        logger.info(f"🎉 任務達成！成功更新了 {updated_count} 筆店家的懶人按鈕分數與驚喜標籤。")

if __name__ == "__main__":
    updater = MongoScenarioUpdater(MONGO_URI, DB_NAME)
    # 1. 先建索引
    updater.create_scenario_indexes()
    # 2. 執行更新
    updater.update_scenarios(CSV_FILE)