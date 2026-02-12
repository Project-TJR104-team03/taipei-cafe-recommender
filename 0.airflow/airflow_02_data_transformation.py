from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from datetime import datetime, timedelta
import pendulum
local_tz = pendulum.timezone("Asia/Taipei") #改台北時區

# ================= 設定區 =================
PROJECT_ID = "project-tjr104-cafe"
REGION = "asia-east1"
JOB_NAME = "cafe-store-to-db"  

default_args = {
    'owner': 'Carter',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 1),
    'email_on_failure': False,
    'retries': 0,
}

with DAG(
    'cafe_transform_pipeline',
    default_args=default_args,
    description='觸發 Cloud Run Job 進行咖啡廳資料清洗',
    start_date=datetime(2026, 2, 1, tzinfo=local_tz),
    schedule_interval='0 10 * * 4',  # 每周四早上10點執行
    catchup=False,
    tags=['vertex-ai', 'cloud-run'],
) as dag:

    # --- Task 1: 執行 Regex 清洗 ---
    # 透過 override 改寫 container 的執行指令，只跑 stage01
    stage1_regex = CloudRunExecuteJobOperator(
        task_id='trigger_stage1_regex',
        project_id=PROJECT_ID,
        region=REGION,
        job_name=JOB_NAME,
        trigger_rule="all_success",
        overrides={
            "container_overrides": [
                {
                    # 如果只有一個 container，通常留空或填 container 名稱
                    "args": ["python", "stage01_regex_cleaned.py"], 
                }
            ]
        }
    )

    # --- Task 2: 執行 Vertex AI 清洗 ---
    # 透過 override 改寫指令，只跑 stage02
    stage2_ai = CloudRunExecuteJobOperator(
        task_id='trigger_stage2_vertex_ai',
        project_id=PROJECT_ID,
        region=REGION,
        job_name=JOB_NAME,
        trigger_rule="all_success",
        overrides={
            "container_overrides": [
                {
                    "args": ["python", "stage02_cleaning_name.py"],
                }
            ]
        }
    )

    # 定義順序
    stage1_regex >> stage2_ai