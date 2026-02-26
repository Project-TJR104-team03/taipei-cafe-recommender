import pandas as pd
import json
import os
import logging
from io import BytesIO
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class StageC_Embedding_Processor:
    def __init__(self, project_id, bucket_name, gcs_scored_data_path, gcs_raw_reviews_path, gcs_output_path):
        self.client = storage.Client(project=project_id)
        self.bucket = self.client.bucket(bucket_name)
        self.gcs_scored_data_path = gcs_scored_data_path
        self.gcs_raw_reviews_path = gcs_raw_reviews_path
        self.gcs_output_path = gcs_output_path
        self.max_reviews_per_store = 30
        self.min_review_length = 15

    def _load_raw_reviews(self):
        """讀取第一階段純化出來的 Top 50 評論 CSV，並嚴格保留品質排序"""
        logger.info(f"📥 正在從 GCS 讀取原始評論: gs://{self.bucket.name}/{self.gcs_raw_reviews_path}")
        try:
            blob = self.bucket.blob(self.gcs_raw_reviews_path)
            df = pd.read_csv(BytesIO(blob.download_as_bytes()))
        # [DE 嚴謹防線]：確保資料確實是依照 quality_score 降冪排列
        # 以防 CSV 在傳遞過程中順序被打亂
            if 'quality_score' in df.columns:
                df = df.sort_values(['place_id', 'quality_score'], ascending=[True, False])
        
        # 轉為 dict 時，list 內的順序已經是「品質最高 (Top 1)」在最前面
            reviews_map = df.groupby('place_id')['content'].apply(list).to_dict()
            return reviews_map
        except Exception as e:
            logger.error(f"❌ 讀取原始評論失敗: {e}")
            return {}

    def _filter_and_select_reviews(self, raw_reviews: list) -> list:
        """
        [修正] 尊重 A 階段的 4D 演算法 (權威/語意/深度/時效)
        依照傳入的順序 (已按 quality_score 排序)，直接篩選出 Top 30
        """
        valid_reviews = []
        
        for rev in raw_reviews:
            rev_str = str(rev).strip()
            
            # 基本衛生檢查：過濾掉極短無意義的雜訊 (深度過低)
            if len(rev_str) >= self.min_review_length:
                valid_reviews.append(rev_str)
            
            # 取到 30 筆就停止，完美保留品質分數最高的 Top 30
            if len(valid_reviews) == self.max_reviews_per_store:
                break
                
        return valid_reviews

    def generate_jsonl(self):
        """產出 Vertex AI Embedding 專用的 Batch JSONL (包含嚴格 Schema 防護)"""
        logger.info(f"📥 正在從 GCS 讀取 Scored Data: gs://{self.bucket.name}/{self.gcs_scored_data_path}")
        try:
            blob = self.bucket.blob(self.gcs_scored_data_path)
            scored_map = json.loads(blob.download_as_text(encoding='utf-8'))
        except Exception as e:
            logger.error(f"❌ 讀取 Scored Data 失敗: {e}")
            return
               
        reviews_map = self._load_raw_reviews()
        
        store_count = 0
        review_count = 0
        output_lines = []

        logger.info(f"🚀 開始生成雙層向量任務封包 (啟動 JSON Stringification 防護)...")

        
        for place_id, store_data in scored_map.items():
            place_name = store_data.get("place_name", "Unknown Store")
            
            # ==========================================
            # [Layer 1] 店家總結向量 (Store-Level)
            # ==========================================
            store_metadata = store_data.get("metadata_for_filtering", {})
            store_embedding_content = store_data.get("content_for_embedding", "")
            
            # 將動態的 dict 轉成純字串 (JSON String)，完美閃避 Vertex AI 的 Schema 解析錯誤
            safe_metadata_str = json.dumps({
                "tags": store_metadata.get("tags", []),
                "feature_scores": store_metadata.get("feature_scores", {})
            }, ensure_ascii=False)

            store_instance = {
                "content": store_embedding_content,
                "task_type": "RETRIEVAL_DOCUMENT",
                "title": place_name,
                # 使用自訂義欄位，避開多層次巢狀結構
                "custom_id": str(place_id),
                "doc_type": "store_level",
                "safe_metadata": safe_metadata_str  # <--- 這裡變成純字串了！
            }
            output_lines.append(json.dumps(store_instance, ensure_ascii=False))
            store_count += 1

            # ==========================================
            # [Layer 2] 獨立評論向量 (Review-Level)
            # ==========================================
            raw_reviews = reviews_map.get(place_id, [])
            selected_reviews = self._filter_and_select_reviews(raw_reviews)
            
            for idx, review_text in enumerate(selected_reviews):
                review_instance = {
                    "content": review_text,
                    "task_type": "RETRIEVAL_DOCUMENT",
                    "custom_id": f"{place_id}_rev_{idx}", 
                    "doc_type": "review_level",
                    "parent_place_id": str(place_id) # 攤平為單一欄位，不使用 nested dict
                }
                output_lines.append(json.dumps(review_instance, ensure_ascii=False))
                review_count += 1
        
        #結果上傳至GCS     
        final_jsonl_content = "\n".join(output_lines)
        output_blob = self.bucket.blob(self.gcs_output_path)
        output_blob.upload_from_string(final_jsonl_content, content_type='application/jsonl')

        logger.info("================ Stage C Pipeline Summary ================")
        logger.info(f"✅ Store-Level Vectors 準備數: {store_count} 筆")
        logger.info(f"✅ Review-Level Vectors 準備數: {review_count} 筆")
        logger.info(f"✅ 封裝完成並上傳至: gs://{self.bucket.name}/{self.gcs_output_path}")
        logger.info("==========================================================")

if __name__ == "__main__":
    CONFIG = {
        "project_id": os.getenv("PROJECT_ID"),
        "bucket_name": os.getenv("BUCKET_NAME"),
        # 讀取 Stage 0 的產出
        "gcs_raw_reviews_path": os.getenv("GCS_DISTILLED_CSV_PATH", "transform/stage0/reviews_top50_distilled.csv"),
        # 讀取 Stage B 的產出
        "gcs_scored_data_path": os.getenv("GCS_FINAL_SCORED_PATH", "transform/stageB/final_scored_data.json"),
        # 輸出給 Stage C 的 JSONL
        "gcs_output_path": os.getenv("GCS_STAGE_C_EMBEDDING_JSONL_PATH", "transform/stageC/vertex_job_stage_c_embedding.jsonl")
    }
    # 檔案路徑配置
    
    processor = StageC_Embedding_Processor(**CONFIG)
    processor.generate_jsonl()