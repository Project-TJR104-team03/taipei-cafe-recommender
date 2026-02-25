import os
import json
import time
import logging
from google.cloud import storage
from google.cloud import aiplatform_v1
from google import genai
from google.genai import types

# ==========================================
# [全局配置] 專案基礎設施
# ==========================================
PROJECT_ID = "tjr104-485403" 
BUCKET_NAME = "tjr104-cafe-datalake1"
LOCATION = "us-central1"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ==========================================
# 引擎 1：GCP 雲端批次發射器 (適用於 Stage A)
# ==========================================
class BatchJobLauncher:
    def __init__(self, project_id, location, bucket_name):
        self.project_id = project_id
        self.location = location
        self.bucket_name = bucket_name

    def upload_to_gcs(self, local_file, gcs_path):
        client = storage.Client(project=self.project_id)
        bucket = client.bucket(self.bucket_name)
        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(local_file)
        return f"gs://{self.bucket_name}/{gcs_path}"

    def submit(self, local_input_file, stage_name, model_id):
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        gcs_input_path = f"batch_input/{stage_name}/{timestamp}/input.jsonl"
        gcs_output_uri_prefix = f"gs://{self.bucket_name}/batch_output/{stage_name}/{timestamp}/"

        # 資料上傳
        gcs_input_uri = self.upload_to_gcs(local_input_file, gcs_input_path)

        # 建立 Job Service Client
        client_options = {"api_endpoint": f"{self.location}-aiplatform.googleapis.com"}
        client = aiplatform_v1.JobServiceClient(client_options=client_options)

        model_path = f"projects/{self.project_id}/locations/{self.location}/publishers/google/models/{model_id}"

        batch_prediction_job = {
            "display_name": f"cafe-{stage_name}-{timestamp}",
            "model": model_path,
            "input_config": {
                "instances_format": "jsonl",
                "gcs_source": {"uris": [gcs_input_uri]},
            },
            "output_config": {
                "predictions_format": "jsonl",
                "gcs_destination": {"output_uri_prefix": gcs_output_uri_prefix},
            },
        }

        try:
            logger.info(f"🔥 [Batch 引擎] 正在發射全量審計任務: {model_path}")
            parent = f"projects/{self.project_id}/locations/{self.location}"
            response = client.create_batch_prediction_job(parent=parent, batch_prediction_job=batch_prediction_job)
            
            job_id = response.name.split('/')[-1]
            logger.info(f"✅ 全量任務提交成功！Job ID: {job_id}")
            logger.info(f"🔗 追蹤連結: https://console.cloud.google.com/vertex-ai/locations/{self.location}/batch-predictions/{job_id}?project={self.project_id}")
            return response
        except Exception as e:
            logger.error(f"❌ 全量提交失敗: {e}")
            raise e

# ==========================================
# 引擎 2：本地微批次在線發射器 (適用於 Stage B - 1536d)
# ==========================================
class OnlineMicroBatchLauncher:
    def __init__(self, project_id, location):
        self.client = genai.Client(vertexai=True, project=project_id, location=location)
        self.batch_size = 100

    def submit(self, input_path, output_path, model_id):
        if not os.path.exists(input_path):
            logger.error(f"❌ 找不到來源檔案: {input_path}")
            return

        with open(input_path, 'r', encoding='utf-8') as f:
            lines = [json.loads(line) for line in f if line.strip()]
        
        total_records = len(lines)
        logger.info(f"📊 [Online 引擎] 開始處理 {total_records} 筆向量資料...")

        # 斷點續傳機制
        processed_count = 0
        if os.path.exists(output_path):
            with open(output_path, 'r', encoding='utf-8') as f:
                processed_count = sum(1 for _ in f)
            logger.info(f"♻️ 發現既有進度，從第 {processed_count} 筆開始接續執行...")

        with open(output_path, 'a', encoding='utf-8') as f_out:
            for i in range(processed_count, total_records, self.batch_size):
                batch = lines[i : i + self.batch_size]
                texts = [item["content"] for item in batch]
                
                try:
                    # 🔥 調用 1536 維度
                    response = self.client.models.embed_content(
                        model=model_id,
                        contents=texts,
                        config=types.EmbedContentConfig(
                            task_type="RETRIEVAL_DOCUMENT",
                            output_dimensionality=1536 
                        )
                    )

                    for j, embedding_obj in enumerate(response.embeddings):
                        result_record = batch[j]
                        result_record["embedding_1536"] = embedding_obj.values
                        f_out.write(json.dumps(result_record, ensure_ascii=False) + '\n')
                    
                    logger.info(f"✅ 進度: {min(i + self.batch_size, total_records)} / {total_records}")
                    time.sleep(1) # 速率控制

                except Exception as e:
                    logger.error(f"❌ 批次 {i} 到 {i+self.batch_size} 發生錯誤: {e}")
                    logger.info("暫停 10 秒後重試...")
                    time.sleep(10)

        logger.info(f"🎉 1536d 向量全部處理完成！已輸出至: {output_path}")

# ==========================================
# [總司令部] 任務路由控制中心
# ==========================================
if __name__ == "__main__":
    # ==========================
    # 🎯 策略切換開關
    # ==========================
    TARGET_TASK = "STAGE_B" # 切換 "STAGE_A" 或 "STAGE_B"

    if TARGET_TASK == "STAGE_A":
        SOURCE_FILE = "vertex_job_stage_a_final.jsonl" 
        STAGE_NAME = "stage_a_full_audit"
        MODEL_ID = "gemini-2.0-flash-001" 
        
        # 啟動 Batch 引擎
        launcher = BatchJobLauncher(PROJECT_ID, LOCATION, BUCKET_NAME)
        launcher.submit(SOURCE_FILE, STAGE_NAME, MODEL_ID)
        
    elif TARGET_TASK == "STAGE_B":
        SOURCE_FILE = "vertex_job_stage_b_embedding.jsonl"
        OUTPUT_FILE = "final_1536_vectors_for_mongo.jsonl" # Stage B 專屬落地檔
        MODEL_ID = "gemini-embedding-001" 
        
        # 啟動 Online 微批次引擎
        launcher = OnlineMicroBatchLauncher(PROJECT_ID, LOCATION)
        launcher.submit(SOURCE_FILE, OUTPUT_FILE, MODEL_ID)

    else:
        logger.error("❌ 未知的任務類型設定")