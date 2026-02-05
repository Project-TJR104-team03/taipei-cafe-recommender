##此py檔用於GCP上的VM內，透過airflow決定執行順序與cloud run的task數

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator

# ==========================================
# 1. 基礎設定 (請根據你的 GCP 環境修改)
# ==========================================
PROJECT_ID = "你的-GCP-PROJECT-ID"
REGION = "asia-east1"

# DAG 預設參數
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ==========================================
# 2. 定義 DAG 流程
# ==========================================
with DAG(
    'cafe_scrape_and_merge_pipeline',
    default_args=default_args,
    description='順序：取得店家 -> 平行爬取評論 -> 合併數據',
    schedule_interval=None,          # 手動啟動，或設定如 '0 3 * * *' (每天凌晨3點)
    start_date=datetime(2024, 2, 5),
    catchup=False,
    tags=['cafe_project', 'cloud_run'],
) as dag:

    # --- 步驟 1: 取得店家資訊 (Store Scrape) ---
    # 設定 1 個 Task
    task_store_scrape = CloudRunExecuteJobOperator(
        task_id='step1_store_scrape',
        project_id=PROJECT_ID,
        region=REGION,
        job_name='store_scrape',
        overrides={
            "task_count": 1
        },
        deferrable=True,
    )

    # --- 步驟 2: 爬取評論與標籤 (Comment & Tag Scrape) ---
    # 設定 5 個 Task 平行處理
    # 這裡會自動觸發 5 個實例，每個實例會拿到不同的 CLOUD_RUN_TASK_INDEX
    task_comment_tag_scrape = CloudRunExecuteJobOperator(
        task_id='step2_comment_tag_scrape',
        project_id=PROJECT_ID,
        region=REGION,
        job_name='comment_tag_scrape',
        overrides={
            "task_count": 5
        },
        deferrable=True,
    )

    # --- 步驟 3: 資料合併 (Data Merge) ---
    # 設定 1 個 Task
    task_data_merge = CloudRunExecuteJobOperator(
        task_id='step3_data_merge',
        project_id=PROJECT_ID,
        region=REGION,
        job_name='data_merge',
        overrides={
            "task_count": 1
        },
        deferrable=True,
    )

    # ==========================================
    # 3. 設定執行順序 (Dependency)
    # ==========================================
    task_store_scrape >> task_comment_tag_scrape >> task_data_merge