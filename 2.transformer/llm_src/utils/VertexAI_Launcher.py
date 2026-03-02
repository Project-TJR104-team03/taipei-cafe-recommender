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
from vertexai.language_models import TextEmbeddingInput, TextEmbeddingModel
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
            logger.info(f"🔥 [Batch 引擎] 正在發射任務 [{TASK_NAME}]: {model_path}")
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
    def __init__(self, project_id, location, bucket_name):
        vertexai.init(project=project_id, location=location)
        self.storage_client = storage.Client(project=project_id)
        self.bucket_name = bucket_name
        self.batch_size = 100
        self.max_retries = 3  # 🌟 設定每批次最大重試次數


    def submit(self, input_path, output_folder, model_id):

        if not output_folder.endswith('/'):
            output_folder += '/'
        bucket = self.storage_client.bucket(self.bucket_name)
        in_blob = bucket.blob(input_path)

        if not in_blob.exists():
            error_msg = f"❌ GCS 找不到來源檔案: {input_path}"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)

        model = TextEmbeddingModel.from_pretrained(model_id)

        # 斷點續傳機制
        existing_blobs = list(bucket.list_blobs(prefix=output_folder))
        existing_batches = [b.name for b in existing_blobs if "batch_" in b.name and b.name.endswith(".jsonl")]
        
        current_batch_index = len(existing_batches)
        processed_count = current_batch_index * self.batch_size

        logger.info(f"♻️ 發現 {current_batch_index} 個已完成的分片。將從第 {processed_count} 筆資料開始接續執行...")
        # ==========================================
        # 2. 流式讀取來源檔 (避免 OOM)
        # ==========================================
        logger.info(f"🚀 [Online 引擎] 開始以流式讀取處理資料...")
        
        # 只需要一個很小的暫存檔來存放當下的 100 筆
        import tempfile
        temp_dir = tempfile.gettempdir()

        with in_blob.open("r", encoding="utf-8") as f_in:
            for _ in range(processed_count):
                next(f_in, None)

            batch = []
            
            for i, line in enumerate(f_in, start=processed_count):
                if not line.strip(): continue
                batch.append(json.loads(line))

                # 當湊滿 100 筆，或是已經讀到檔案的最後一筆時，開始執行 AI 任務
                if len(batch) == self.batch_size:
                    self._process_and_upload_batch(batch, current_batch_index, output_folder, bucket, model, temp_dir)
                    current_batch_index += 1
                    batch = []
            if batch:
                self._process_and_upload_batch(batch, current_batch_index, output_folder, bucket, model, temp_dir)

        logger.info(f"🎉 所有向量資料已成功分片寫入至 GCS 資料夾: gs://{self.bucket_name}/{output_folder}")
    
    def _process_and_upload_batch(self, batch, batch_index, output_folder, bucket, model, temp_dir):
        texts = [item["content"] for item in batch]
        success = False
        
        for attempt in range(self.max_retries):
            try:
                inputs = [TextEmbeddingInput(text=t, task_type="RETRIEVAL_DOCUMENT") for t in texts]
                embeddings = model.get_embeddings(
                    inputs,
                    output_dimensionality=1536,
                )
                
                # 將這 100 筆資料寫入本地暫存檔
                # 檔名補零，例如 batch_00000.jsonl, batch_00001.jsonl
                batch_filename = f"batch_{batch_index:05d}.jsonl"
                local_batch_path = os.path.join(temp_dir, batch_filename)
                
                with open(local_batch_path, 'w', encoding='utf-8') as f_out:
                    for j, embedding in enumerate(embeddings):
                        result_record = batch[j]
                        result_record["embedding_1536"] = embedding.values
                        f_out.write(json.dumps(result_record, ensure_ascii=False) + '\n')
                
                # 上傳這個小分片到 GCS
                gcs_destination = f"{output_folder}{batch_filename}"
                out_blob = bucket.blob(gcs_destination)
                out_blob.upload_from_filename(local_batch_path)
                
                # 上傳完畢立即刪除本地暫存檔，釋放空間
                os.remove(local_batch_path)
                
                logger.info(f"✅ 完成分片上傳: {gcs_destination} (累積處理約 {(batch_index + 1) * self.batch_size} 筆)")
                success = True
                time.sleep(1) # API 速率控制
                break
                
            except Exception as e:
                logger.warning(f"⚠️ 分片 {batch_index} 發生錯誤 (第 {attempt+1}/{self.max_retries} 次): {e}")
                time.sleep(5 * (attempt + 1))
        
        if not success:
            raise Exception(f"❌ 分片 {batch_index} 重試失敗達上限，任務終止！")

# ==========================================
# [總司令部] 任務路由控制中心
# ==========================================
if __name__ == "__main__":
    # ==========================
    # 🎯 策略切換開關
    # ==========================
    TARGET_TASK = os.getenv("TARGET_TASK", None)
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
        OUTPUT_FOLDER = os.getenv("GCS_EMBEDDING_RESULTS_OUTPUT", "batch_output/embedding_generation/")

        launcher = OnlineMicroBatchLauncher(PROJECT_ID, LOCATION, BUCKET_NAME)
        launcher.submit(SOURCE_FILE, OUTPUT_FOLDER, MODEL_ID)

    else:
        logger.error("❌ 未知的任務類型設定")