##此py檔用於GCP上的VM內，透過airflow決定執行順序與cloud run的task數

import requests
import json
import pendulum
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator

# 處理 Airflow 版本相容性
try:
    from airflow.operators.empty import EmptyOperator
except ImportError:
    from airflow.operators.dummy import DummyOperator as EmptyOperator

# 設定台北時區
local_tz = pendulum.timezone("Asia/Taipei")

# ==========================================
# 1. 基礎設定 (GCP 環境)
# ==========================================
PROJECT_ID = "project-tjr104-cafe"
REGION = "asia-east1"
JOB_NAME = "cafe-scraper"
REGIONS_TO_SCAN = [
    "A-1", "A-2", "A-3", "B-1", "B-2", "B-3",
    "C-1", "C-2", "C-3", "D-1", "D-2", "D-3",
    "E-1", "E-2", "E-3"
]

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
}

# ==========================================
# 2. 2026 LINE Messaging API 發送函式
# ==========================================
def send_line_notification(message):
    """使用 LINE Messaging API 發送 Push Message"""
    # 從 Airflow Variables 抓取新版憑證
    try:
        token = Variable.get("line_bot_token")
        user_id = Variable.get("line_user_id")
    except KeyError:
        print("錯誤：尚未在 Airflow Variables 設定 line_bot_token 或 line_user_id")
        return 404
    
    url = "https://api.line.me/v2/bot/message/push"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }
    payload = {
        "to": user_id,
        "messages": [
            {
                "type": "text",
                "text": message
            }
        ]
    }
    
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    print(f"LINE 傳送狀態碼: {response.status_code}, 回傳: {response.text}")
    return response.status_code

# ==========================================
# 3. 定義 DAG 流程
# ==========================================
with DAG(
    'cafe_crawler_pipeline',
    default_args=default_args,
    description='2026 TJR104 咖啡廳系統：Messaging API 整合版',
    schedule_interval='0 10 * * 1', # 每周一早上10點
    start_date=datetime(2026, 2, 1, tzinfo=local_tz),
    catchup=False,
    tags=['cafe', 'production', 'v2', 'line_bot'],
) as dag:

    # --- Phase 1: 掃描與建置 ---
    scan_tasks = []
    for region_code in REGIONS_TO_SCAN:
        safe_id = region_code.lower().replace("-", "_")
        task = CloudRunExecuteJobOperator(
            task_id=f'scan_{safe_id}',
            project_id=PROJECT_ID,
            region=REGION,
            job_name=JOB_NAME,
            overrides={"container_overrides": [{"args": ["--task", "scan", "--region", region_code]}]}
        )
        scan_tasks.append(task)

    for i in range(len(scan_tasks) - 1):
        scan_tasks[i] >> scan_tasks[i+1]

    task_supertaste = CloudRunExecuteJobOperator(
        task_id='p1_supertaste',
        project_id=PROJECT_ID,
        region=REGION,
        job_name=JOB_NAME,
        overrides={"container_overrides": [{"args": ["--task", "supertaste"]}]}
    )

    phase1_done = EmptyOperator(task_id='phase1_completed')
    
    notify_phase1 = PythonOperator(
        task_id='notify_phase1_done',
        python_callable=send_line_notification,
        op_kwargs={'message': '☕ [資料爬蟲 Phase 1 完成]\n基礎店家名單已建立，開始啟動平行挖掘任務！'}
    )

    # --- Phase 2: 深度挖掘 ---
    task_tags = CloudRunExecuteJobOperator(
        task_id='p2_tags',
        project_id=PROJECT_ID,
        region=REGION,
        job_name=JOB_NAME,
        overrides={"task_count": 3, "container_overrides": [{"args": ["--task", "tags", "--region", "ALL"]}]}
    )

    task_reviews = CloudRunExecuteJobOperator(
        task_id='p2_reviews',
        project_id=PROJECT_ID,
        region=REGION,
        job_name=JOB_NAME,
        overrides={"task_count": 5, "container_overrides": [{"args": ["--task", "reviews", "--region", "ALL"]}]}
    )

    task_ifoodie = CloudRunExecuteJobOperator(
        task_id='p2_ifoodie',
        project_id=PROJECT_ID,
        region=REGION,
        job_name=JOB_NAME,
        overrides={"task_count": 3, "container_overrides": [{"args": ["--task", "ifoodie", "--region", "ALL"]}]}
    )

    task_reviews_original = CloudRunExecuteJobOperator(
        task_id='p2_reviews_original',
        project_id=PROJECT_ID,
        region=REGION,
        job_name=JOB_NAME,
        trigger_rule='all_done',
        overrides={"container_overrides": [{"args": ["--task", "reviews_original"]}]}
    )

    phase2_done = EmptyOperator(task_id='phase2_completed')
    
    notify_phase2 = PythonOperator(
        task_id='notify_phase2_done',
        python_callable=send_line_notification,
        op_kwargs={'message': '☕ [資料爬蟲 Phase 2 完成]\n所有評論與標籤抓取完畢，準備進入資料合併。'}
    )

    # --- Phase 3: 資料合併 ---
    task_merge = CloudRunExecuteJobOperator(
        task_id='p3_merge',
        project_id=PROJECT_ID,
        region=REGION,
        job_name=JOB_NAME,
        overrides={"container_overrides": [{"args": ["--task", "merge"]}]}
    )

    notify_all_done = PythonOperator(
        task_id='notify_all_done',
        python_callable=send_line_notification,
        op_kwargs={'message': '🎉 [資料爬蟲 咖啡廳管線完工]\n本週資料更新已存入資料庫，請至後端查看最新推薦名單。'}
    )

    # ==========================================
    # 流程串接
    # ==========================================
    scan_tasks[-1] >> task_supertaste >> phase1_done >> notify_phase1
    notify_phase1 >> [task_tags, task_reviews, task_ifoodie] >> task_reviews_original >> phase2_done >> notify_phase2
    notify_phase2 >> task_merge >> notify_all_done