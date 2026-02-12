import os
import time
import logging
from google.cloud import storage
from google.cloud import aiplatform_v1

# ==========================================
# [策略定位] 使用最新 Gemini 2.0 進行 DE 實務
# ==========================================
PROJECT_ID = "project-tjr104-cafe" 
BUCKET_NAME = "tjr104-cafe-datalake"
LOCATION = "us-central1"

# 2.0 版本在 2026 年是 Batch 預測的主力
MODEL_A = "gemini-2.0-flash-001" 
MODEL_B = "gemini-2.0-pro-001"

# ==========================================
# [核心發射器] 
# ==========================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class VertexLauncher:
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

    def submit_batch_job(self, local_input_file, stage_name, model_id):
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        gcs_input_path = f"batch_input/{stage_name}/{timestamp}/input.jsonl"
        gcs_output_uri_prefix = f"gs://{self.bucket_name}/batch_output/{stage_name}/{timestamp}/"

        # 1. 資料上傳
        gcs_input_uri = self.upload_to_gcs(local_input_file, gcs_input_path)

        # 2. 建立 Job Service Client
        client_options = {"api_endpoint": f"{self.location}-aiplatform.googleapis.com"}
        client = aiplatform_v1.JobServiceClient(client_options=client_options)

        # 3. [關鍵修正] 建立完整的 Publisher Model 路徑
        # 這是 Gemini 2.0/2.5 專用的 Batch 路徑格式
        model_path = f"publishers/google/models/{model_id}"

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
            logger.info(f"🚀 [DE Strategy] 正在使用最新模型: {model_path}")
            parent = f"projects/{self.project_id}/locations/{self.location}"
            
            response = client.create_batch_prediction_job(
                parent=parent, 
                batch_prediction_job=batch_prediction_job
            )
            
            logger.info(f"✅ 任務提交成功！")
            logger.info(f"🆔 Job ID: {response.name.split('/')[-1]}")
            logger.info(f"🔗 追蹤: https://console.cloud.google.com/vertex-ai/locations/{self.location}/batch-predictions/{response.name.split('/')[-1]}?project={self.project_id}")
            return response
        except Exception as e:
            logger.error(f"❌ 提交失敗: {e}")
            raise e

if __name__ == "__main__":
    launcher = VertexLauncher(PROJECT_ID, LOCATION, BUCKET_NAME)
    # 執行 Stage A
    launcher.submit_batch_job("vertex_job_stage_a.jsonl", "stage_a", MODEL_A)