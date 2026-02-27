import os
import json
import time
import logging
from google.cloud import storage
from google.cloud import aiplatform_v1
from google import genai
from google.genai import types
from dotenv import load_dotenv
import vertexai
from vertexai.language_models import TextEmbeddingModel
load_dotenv()

# ==========================================
# [全局配置] 專案基礎設施
# ==========================================
PROJECT_ID = os.getenv("PROJECT_ID")
BUCKET_NAME = os.getenv("BUCKET_NAME")
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

    def submit(self, gcs_source_path, TASK_NAME, model_id):
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        gcs_output_uri_prefix = f"gs://{self.bucket_name}/batch_output/{TASK_NAME}/{timestamp}/"

        # 資料上傳
        gcs_input_uri = f"gs://{self.bucket_name}/{gcs_source_path}"
        
        # 建立 Job Service Client
        client_options = {"api_endpoint": f"{self.location}-aiplatform.googleapis.com"}
        client = aiplatform_v1.JobServiceClient(client_options=client_options)

        model_path = f"projects/{self.project_id}/locations/{self.location}/publishers/google/models/{model_id}"

        batch_prediction_job = {
            "display_name": f"cafe-{TASK_NAME}-{timestamp}",
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
            logger.info(f"🤖 使用模型: {model_id}")
            logger.info(f"📂 讀取來源: {gcs_input_uri}")
            parent = f"projects/{self.project_id}/locations/{self.location}"
            response = client.create_batch_prediction_job(parent=parent, batch_prediction_job=batch_prediction_job)
            
            job_name = response.name
            job_id = job_name.split('/')[-1]
            logger.info(f"✅ 全量任務提交成功！Job ID: {job_id}")
            logger.info(f"🔗 追蹤連結: https://console.cloud.google.com/vertex-ai/locations/{self.location}/batch-predictions/{job_id}?project={self.project_id}")
            
            while True:
                # 重新抓取任務最新狀態
                current_job = client.get_batch_prediction_job(name=job_name)
                state = current_job.state

                # 成功狀態：退出迴圈，讓程式正常結束
                if state == aiplatform_v1.JobState.JOB_STATE_SUCCEEDED:
                    logger.info(f"🎉 Vertex AI 任務 {job_id} 成功完成！")
                    break
                
                # 失敗狀態：主動報錯，讓 Airflow 抓到失敗 (Red Light)
                elif state in [
                    aiplatform_v1.JobState.JOB_STATE_FAILED, 
                    aiplatform_v1.JobState.JOB_STATE_CANCELLED, 
                    aiplatform_v1.JobState.JOB_STATE_EXPIRED
                ]:
                    error_detail = current_job.error.message if current_job.error else "未知錯誤"
                    logger.error(f"❌ Vertex AI 任務失敗 (狀態: {state}): {error_detail}")
                    raise Exception(f"Vertex AI Job Failed: {error_detail}")

                # 進行中狀態：睡一分鐘再問一次
                else:
                    logger.info(f"⏳ 任務處理中 (目前狀態: {state})... 60 秒後再次檢查")
                    time.sleep(60)
            
            return response
        except Exception as e:
            logger.error(f"❌ 全量提交失敗: {e}")
            raise e

# ==========================================
# 引擎 2：微批次在線發射器 (適用於 Stage B - 1536d)
# ==========================================
class OnlineMicroBatchLauncher:
    def __init__(self, project_id, location):
        vertexai.init(project=project_id, location=location)
        self.batch_size = 100
        self.max_retries = 3  # 🌟 設定每批次最大重試次數

    def submit(self, input_path, output_path, model_id):
        if not os.path.exists(input_path):
            error_msg = f"❌ 找不到來源檔案: {input_path}"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)
        
        with open(input_path, 'r', encoding='utf-8') as f:
            lines = [json.loads(line) for line in f if line.strip()]
        
        total_records = len(lines)
        logger.info(f"📊 [Vertex 引擎] 開始處理 {total_records} 筆向量資料...")

        model = TextEmbeddingModel.from_pretrained(model_id)

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
                
                success = False
                for attempt in range(self.max_retries):
                    try:
                        embeddings = model.get_embeddings(
                            texts,
                            output_dimensionality=1536,
                            task_type="RETRIEVAL_DOCUMENT")
                        
                        for j, embedding in enumerate(embeddings):
                            result_record = batch[j]
                            result_record["embedding_1536"] = embedding.values
                            f_out.write(json.dumps(result_record, ensure_ascii=False) + '\n')
                        
                        logger.info(f"✅ 進度: {min(i + self.batch_size, total_records)} / {total_records}")
                        time.sleep(1) # 速率控制
                        success = True
                        break # 本批次成功，跳出重試迴圈

                    except Exception as e:
                        logger.warning(f"⚠️ 批次 {i} 到 {i+len(batch)} 發生錯誤 (第 {attempt+1}/{self.max_retries} 次): {e}")
                        time.sleep(10 * (attempt + 1)) # 遞增等待時間 (10s, 20s, 30s)
            
             # 🌟 修正 3：如果重試 3 次都失敗，強制中斷任務，讓 Airflow 亮紅燈
                if not success:
                    fatal_msg = f"❌ 批次 {i} 處理失敗已達上限，終止任務以保護資料完整性！"
                    logger.error(fatal_msg)
                    raise Exception(fatal_msg)

        logger.info(f"🎉 1536d 向量全部處理完成！已輸出至: {output_path}")

# ==========================================
# [總司令部] 任務路由控制中心
# ==========================================
if __name__ == "__main__":
    # ==========================
    # 🎯 策略切換開關
    # ==========================
    TARGET_TASK = os.getenv("TARGET_TASK", "AUDIT")
    logger.info(f"🚀 接收到 Router 任務指示: TARGET_TASK={TARGET_TASK}")

    if TARGET_TASK == "AUDIT":
        SOURCE_FILE = os.getenv("GCS_STAGE_A_JSONL_PATH", "transform/stageA/vertex_job_stage_a.jsonl")
        TASK_NAME = "stage_a_full_audit"
        MODEL_ID = "gemini-2.0-flash-001" 
       
        # 啟動 Batch 引擎
        launcher = BatchJobLauncher(PROJECT_ID, LOCATION, BUCKET_NAME)
        launcher.submit(SOURCE_FILE, TASK_NAME, MODEL_ID)
        
    elif TARGET_TASK == "EMBEDDING":
        SOURCE_FILE = os.getenv("GCS_STAGE_C_EMBEDDING_JSONL_PATH", "transform/stageC/vertex_job_stage_c_embedding.jsonl")
        TASK_NAME = "embedding_generation"
        MODEL_ID = "gemini-embedding-001" 
        
        launcher = BatchJobLauncher(PROJECT_ID, LOCATION, BUCKET_NAME)
        launcher.submit(SOURCE_FILE, TASK_NAME, MODEL_ID)

    else:
        logger.error("❌ 未知的任務類型設定")