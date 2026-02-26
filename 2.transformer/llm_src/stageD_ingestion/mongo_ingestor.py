import pandas as pd
import json
import os
import re
import logging
import io
from datetime import datetime, timezone
from google.cloud import storage
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv

load_dotenv()
# ==========================================
# 參數配置區
# ==========================================
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME", "coffee_db")
PROJECT_ID = os.getenv("PROJECT_ID")
BUCKET_NAME = os.getenv("BUCKET_NAME")

# GCS 路徑配置
GCS_RAW_STORE_PATH = os.getenv("GCS_RAW_STORE_PATH")
GCS_EMBEDDING_RESULTS_FOLDER = os.getenv("GCS_EMBEDDING_RESULTS_FOLDER") # 指向 Vertex AI 產出的母目錄
GCS_SCORED_FILE_PATH = os.getenv("GCS_FINAL_SCORED_PATH") # 指向 Stage B 的產出

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ==========================================
# 輔助函數 (來自原 store_to_db.py)
# ==========================================
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
    """解析營業時間為結構化分鐘數"""
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

class MongoFinalIngestor:
    def __init__(self, mongo_uri, db_name, PROJECT_ID, BUCKET_NAME):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.cafes_col = self.db["cafes"]
        self.review_col = self.db["AI_embedding"]
        
        self.gcs_client = storage.Client(project=PROJECT_ID)
        self.bucket = self.gcs_client.bucket(BUCKET_NAME)

        # 建立索引以優化 Upsert 效能
        self.cafes_col.create_index("place_id", unique=True)
        self.review_col.create_index("doc_id", unique=True)
        self.review_col.create_index("parent_place_id")

    def _get_latest_prediction_blob(self, folder_path):
        """
        [架構師優化]：自動在輸出資料夾中找尋包含 'predictions' 的最新 JSONL
        """
        blobs = list(self.bucket.list_blobs(prefix=folder_path))
        # 過濾出有效的預測檔案
        prediction_blobs = [b for b in blobs if b.name.endswith(".jsonl") and "predictions" in b.name]
        
        if not prediction_blobs:
            return None
            
        # 依更新時間排序取最新
        prediction_blobs.sort(key=lambda x: x.updated, reverse=True)
        return prediction_blobs[0]
    
    def _load_base_csv_to_map(self, gcs_base_csv_path):
        """讀取 GCS 上的基礎 CSV，並轉為以 place_id 為 Key 的字典"""
        logger.info(f"📂 正在載入基礎物理資料: {gcs_base_csv_path}")
        blob = self.bucket.blob(gcs_base_csv_path)
        content = blob.download_as_bytes()
        
        cols = ['name', 'place_id', 'formatted_phone_number', 'formatted_address', 'website', 'location', 
                'opening_hours', 'price_level', 'business_status', 'types', 'payment_options', 'google_maps_url']
        df = pd.read_csv(io.BytesIO(content), names=cols, header=0, quotechar='"', encoding='utf-8-sig')
        
        # 轉換為 dict，方便 O(1) 尋找
        raw_store_map = {str(row['place_id']): row for _, row in df.iterrows() if pd.notna(row['place_id'])}
        return raw_store_map
    
    def process_and_upload(self, gcs_base_csv_path, gcs_vector_folder, gcs_scored_path):
        """
        從 GCS 讀取資料並匯入 MongoDB
        """
        # 1. 載入 Ground Truth (Stage B 的打分結果)
        logger.info(f"📦 正在從 GCS 載入 Scored Data: {gcs_scored_path}")
        try:
            scored_blob = self.bucket.blob(gcs_scored_path)
            scored_data_map = json.loads(scored_blob.download_as_text(encoding='utf-8'))
        except Exception as e:
            logger.error(f"❌ 讀取 Scored Data 失敗: {e}")
            return

        # 2. 尋找 Vertex AI 產出的向量檔案
        vector_blob = self._get_latest_prediction_blob(gcs_vector_folder)
        if not vector_blob:
            logger.error(f"❌ 在 GCS 路徑 {gcs_vector_folder} 找不到任何向量預測檔案。")
            return
        
        logger.info(f"🔍 鎖定向量來源檔案: gs://{self.bucket.name}/{vector_blob.name}")
        
        # 3. 載入原始物理資料 (Base CSV)
        try:
            raw_store_map = self._load_base_csv_to_map(gcs_base_csv_path)
        except Exception as e:
            logger.error(f"❌ 讀取基礎 CSV 失敗: {e}")
            return

        cafes_ops = []
        review_ops = []
        batch_size = 500
        counts = {"store": 0, "review": 0}

        vector_content = vector_blob.download_as_text(encoding='utf-8')

        logger.info("🚀 開始執行【三方資料大融合】與寫入作業...")
        
        for line in vector_content.splitlines():
            if not line.strip(): continue
            try:
                data = json.loads(line)
                # Vertex AI Batch 輸出通常封裝在 'instance' 或直接在層級下，視設定而定
                # 這裡假設你的 Stage C 封裝格式
                vector = data.get("embedding")
                if not vector:
                    # 如果是從 Vertex AI 產出的原始 JSONL，向量可能在 response.predictions[0].embeddings.values
                    # 這裡根據你解析後的內容調整
                    vector = data.get("response", {}).get("predictions", [{}])[0].get("embeddings", {}).get("values")
                
                if not vector: continue

                doc_type = data.get("doc_type")
                    
                # ==========================================
                # 邏輯 A：店家總表 (Cafes) -> 執行記憶體 Join
                # ==========================================
                if doc_type == "store_level":
                    place_id = data.get("custom_id")
                    
                    # --- [三方資料 Join] ---
                    # [關鍵操作]：直接從 Ground Truth 提取完整資料，放棄有缺失的 safe_metadata
                    ai_data = scored_data_map.get(place_id, {})
                    meta_filter = ai_data.get("metadata_for_filtering", {})
                    phys_data = raw_store_map.get(place_id, pd.Series())

                    # 解析 Types 邏輯 (來自 store_to_db)
                    raw_types = phys_data.get('types')
                    if pd.notna(raw_types):
                        all_types = [t.strip() for t in str(raw_types).split(',')]
                        kick_tags = {'point_of_interest', 'establishment', 'store'}
                        types_list = [t for t in all_types if t not in kick_tags]
                        if 'cafe' not in types_list: types_list.append('cafe')
                    else:
                        types_list = ['cafe']

                    # 強制數值轉型防禦
                    raw_scores = meta_filter.get("feature_scores", {})
                    float_scores = {k: float(v) for k, v in raw_scores.items() if v is not None}
                    
                    # --- 組裝終極版 Schema (對齊 v1.2) ---
                    store_node = {
                        "place_id": place_id,
                        "original_name": str(phys_data.get('name', ai_data.get('place_name'))),
                        "location": {
                            "type": "Point",
                            "coordinates": parse_wkt_point(phys_data.get('location'))
                        },
                        "area_info": extract_area_info(phys_data.get('formatted_address')),
                        "attributes": {
                            "price_level": float(phys_data['price_level']) if pd.notna(phys_data.get('price_level')) else None,
                            "business_status": str(phys_data.get('business_status')) if pd.notna(phys_data.get('business_status')) else "OPERATIONAL",
                            "types": types_list
                        },
                        "contact": {
                            "phone": str(phys_data['formatted_phone_number']) if pd.notna(phys_data.get('formatted_phone_number')) else None,
                            "website": str(phys_data['website']) if pd.notna(phys_data.get('website')) else None,
                            "google_maps_url": str(phys_data['google_maps_url']) if pd.notna(phys_data.get('google_maps_url')) else None
                        },
                        "opening_hours": {
                            "periods": parse_opening_hours_to_periods(phys_data.get('opening_hours')),
                            "is_24_hours": True if (pd.notna(phys_data.get('opening_hours')) and "24 小時" in str(phys_data.get('opening_hours'))) else False
                        },
                        "tags": meta_filter.get("tags", []),          
                        "features": meta_filter.get("features", {}),   
                        "scores": float_scores,                       
                        "vector": vector,                              
                        "summary": data.get("content", ""),            
                        "embedding_config": {
                                "model": "gemini-embedding-001",
                                "dimension": 1536,
                                "stage": "Final_Merged"},
                        "last_updated": datetime.now(timezone.utc)
                    }

                    cafes_ops.append(UpdateOne({"place_id": place_id}, {"$set": store_node}, upsert=True))
                    counts["store"] += 1
                    

                # ==========================================
                # 邏輯 B：評論佐證表 (AI_embedding)
                # ==========================================
                elif doc_type == "review_level":
                    doc_id = data.get("custom_id")
                    review_doc = {
                        "doc_id": doc_id,
                        "place_id": data.get("parent_place_id", ""),
                        "content": data.get("content", ""),
                        "embedding": vector,
                        "doc_type": "review_level"
                    }
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
    ingestor = MongoFinalIngestor(MONGO_URI, DB_NAME, PROJECT_ID, BUCKET_NAME)
    ingestor.process_and_upload(GCS_RAW_STORE_PATH, GCS_EMBEDDING_RESULTS_FOLDER, GCS_SCORED_FILE_PATH)