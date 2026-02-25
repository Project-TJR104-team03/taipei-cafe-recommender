import json
import os
import logging
from pymongo import MongoClient, UpdateOne

# ==========================================
# 參數配置區
# ==========================================
VECTOR_FILE = "slim_1536_vectors_for_mongo.jsonl"    # 包含向量的檔案
SCORED_FILE = "final_scored_data.json"               # 包含完整鐵三角的 Ground Truth
MONGO_URI = "mongodb+srv://a84682579_db_user:1zWbKmt1jR9emhHx@projectcoffee.ipknpgr.mongodb.net/"
DB_NAME = "coffee_db"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MongoFinalIngestor:
    def __init__(self, mongo_uri, db_name):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.cafes_col = self.db["cafes"]
        self.review_col = self.db["AI_embedding"]
        
        # 建立索引以優化 Upsert 效能
        self.cafes_col.create_index("place_id", unique=True)
        self.review_col.create_index("doc_id", unique=True)
        self.review_col.create_index("parent_place_id")

    def process_and_upload(self, vector_path, scored_path):
        if not os.path.exists(vector_path) or not os.path.exists(scored_path):
            logger.error("❌ 找不到來源檔案，請確認 JSONL 與 JSON 檔案路徑。")
            return

        # 1. 載入 Ground Truth (打分結果) 作為記憶體對照表
        logger.info("📦 正在載入 Scored Data 對照表...")
        with open(scored_path, 'r', encoding='utf-8') as f:
            scored_data_map = json.load(f)

        cafes_ops = []
        review_ops = []
        batch_size = 500
        counts = {"store": 0, "review": 0}

        logger.info(f"🚀 開始執行向量與 Metadata 合併寫入: {DB_NAME}")

        with open(vector_path, 'r', encoding='utf-8') as f:
            for line in f:
                if not line.strip(): continue
                try:
                    data = json.loads(line)
                    vector = data.get("embedding_1536")
                    if not vector: continue

                    doc_type = data.get("doc_type")
                    
                    # ==========================================
                    # 邏輯 A：店家總表 (Cafes) -> 執行記憶體 Join
                    # ==========================================
                    if doc_type == "store_level":
                        place_id = data.get("custom_id")
                        
                        # [關鍵操作]：直接從 Ground Truth 提取完整資料，放棄有缺失的 safe_metadata
                        store_truth = scored_data_map.get(place_id, {})
                        meta_filter = store_truth.get("metadata_for_filtering", {})
                        
                        # 強制數值轉型防禦
                        raw_scores = meta_filter.get("feature_scores", {})
                        float_scores = {k: float(v) for k, v in raw_scores.items() if v is not None}

                        # 構建完美的 $set 更新內容
                        update_operation = {
                            "$set": {
                                "tags": meta_filter.get("tags", []),           # 合併標籤 (Array)
                                "features": meta_filter.get("features", {}),   # 特徵布林值 (Dict)
                                "scores": float_scores,                        # 權重分數 (Dict)
                                "vector": vector,                              # 1536d 總向量
                                "summary": data.get("content", ""),            # 推薦總結
                                "embedding_config": {
                                    "model": "text-embedding-004",
                                    "dimension": 1536,
                                    "stage": "Final_Merged"
                                },
                                "last_updated": "2026-02-25"
                            }
                        }
                        
                        cafes_ops.append(UpdateOne({"place_id": place_id}, update_operation, upsert=True))
                        counts["store"] += 1

                    # ==========================================
                    # 邏輯 B：評論佐證表 (AI_embedding) -> 全部重寫
                    # ==========================================
                    else:
                        doc_id = data.get("custom_id")
                        review_doc = {
                            "doc_id": doc_id,
                            "parent_place_id": data.get("parent_place_id", ""),
                            "content": data.get("content", ""),
                            "embedding": vector,
                            "doc_type": "review_level"
                        }
                        
                        # 評論部分因為你已經刪掉舊資料，這裡 Upsert 相當於全新的 Insert
                        review_ops.append(UpdateOne({"doc_id": doc_id}, {"$set": review_doc}, upsert=True))
                        counts["review"] += 1

                    # 批次提交
                    if len(cafes_ops) >= batch_size:
                        self.cafes_col.bulk_write(cafes_ops)
                        cafes_ops = []
                    if len(review_ops) >= batch_size:
                        self.review_col.bulk_write(review_ops)
                        review_ops = []

                except Exception as e:
                    logger.error(f"❌ 解析錯誤: {e}")

        # 提交剩餘資料
        if cafes_ops: self.cafes_col.bulk_write(cafes_ops)
        if review_ops: self.review_col.bulk_write(review_ops)
            
        logger.info(f"🎉 任務達成！成功更新主表 {counts['store']} 筆，寫入評論表 {counts['review']} 筆。")

if __name__ == "__main__":
    ingestor = MongoFinalIngestor(MONGO_URI, DB_NAME)
    # 請確保這兩個檔案都在同一個資料夾
    ingestor.process_and_upload(VECTOR_FILE, SCORED_FILE)