##此py檔用於GCP上的VM內，透過airflow決定執行順序與cloud run的task數

import requests
import json
import pendulum
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator

# ==========================================
# 參數配置區 (請依據你的 GCP 環境修改)
# ==========================================
PROJECT_ID = "project-tjr104-cafe"       # 你的 GCP Project ID
REGION = "asia-east1"             # Cloud Run 所在的區域
JOB_NAME = "cafe-transformer"     # 你在 Cloud Run 建立的 Job 名稱

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
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


# 建立 DAG
with DAG(
    'cafe_end_to_end_pipeline',
    default_args=default_args,
    description='咖啡廳資料清洗、AI 審計與向量化管線',
    schedule_interval=None, # 設定排程，例如 '@daily'，目前設為手動觸發
    start_date=datetime(2026, 2, 26),
    catchup=False,
    tags=['cafe_project', 'etl', 'vertex_ai'],
) as dag:

    # 封裝一個產生 Cloud Run Task 的 Helper Function
    def create_cloud_run_task(task_id, cmd_task_name):
        return CloudRunExecuteJobOperator(
            task_id=task_id,
            project_id=PROJECT_ID,
            region=REGION,
            job_name=JOB_NAME,
            overrides={
                "container_overrides": [
                    {
                        "args": ["--task", cmd_task_name]
                    }
                ]
            }
        )

    # ==========================================
    # 建立所有任務節點 (Task Nodes)
    # ==========================================
    
    # --- Stage 0: 資料預處理 ---
    stage0_regex = create_cloud_run_task("stage0_regex", "stage0_regex")
    stage0_name_AI = create_cloud_run_task("stage0_name_AI", "stage0_name_AI")
    stage0_review = create_cloud_run_task("stage0_review_prefilter", "stage0_review_prefilter")
    stage0_tag = create_cloud_run_task("stage0_tag_processor", "stage0_tag_processor")

    notify_stage0 = PythonOperator(
    task_id='notify_stage0_done',
    python_callable=send_line_notification,
    op_kwargs={'message': '☕ [資料清洗 stage 0 已完成]\n已成功完成名稱清洗、篩選50則推薦評論與處理店家標籤'})

    # --- Stage A: AI 審計 ---
    stageA_processor = create_cloud_run_task("stageA_processor", "stageA_processor")
    stageA_launcher = create_cloud_run_task("stageA_launcher", "stageA_launcher")
    stageA_parser = create_cloud_run_task("stageA_parser", "stageA_parser")

    notify_stageA = PythonOperator(
    task_id='notify_stageA_done',
    python_callable=send_line_notification,
    op_kwargs={'message': '☕ [資料清洗 stage A 已完成]\n已成功完成店家與評論的AI審計'})

    # --- Stage B: 特徵融合與評分 ---
    stageB_merger = create_cloud_run_task("stageB_merger", "stageB_merger")
    stageB_scorer = create_cloud_run_task("stageB_scorer", "stageB_scorer")

    notify_stageB = PythonOperator(
    task_id='notify_stageB_done',
    python_callable=send_line_notification,
    op_kwargs={'message': '☕ [資料清洗 stage B 已完成]\n已成功完成TAG特徵融合與評分'})


    # --- Stage C: 向量生成 ---
    stageC_builder = create_cloud_run_task("stageC_builder", "stageC_builder")
    stageC_launcher = create_cloud_run_task("stageC_launcher", "stageC_launcher")

    notify_stageC = PythonOperator(
    task_id='notify_stageC_done',
    python_callable=send_line_notification,
    op_kwargs={'message': '☕ [資料清洗 stage C 已完成]\n已成功為店家與評論生成向量'})

    # --- Stage D: 終極資料庫寫入 ---
    stageD_ingestor = create_cloud_run_task("stageD_ingestor", "stageD_ingestor")

    notify_stageD = PythonOperator(
    task_id='notify_stageD_done',
    python_callable=send_line_notification,
    op_kwargs={'message': '☕ [資料清洗 stage D 已完成]\n大功告成～已成功將data存入MongoDB'})

    # ==========================================
    # 定義任務相依性 (Dependencies & Workflow)
    # ==========================================
    
    # 1. 基礎店名清洗必須有先後順序
    stage0_regex >> stage0_name_AI

    # 2. 當所有 Stage 0 的前置作業都完成後，才進入 Stage A 封裝
    # (店名清洗完成、評論篩選完成、標籤初步處理完成)
    [stage0_name_AI, stage0_review, stage0_tag] >> notify_stage0 >> stageA_processor

    # 3. Stage A 核心流程
    stageA_processor >> stageA_launcher >> stageA_parser

    # 4. Stage B 核心流程
    stageA_parser >> notify_stageA >> stageB_merger >> stageB_scorer

    # 5. Stage C 核心流程
    stageB_scorer >> notify_stageB >>stageC_builder >> stageC_launcher

    # 6. Stage D 寫入 MongoDB
    stageC_launcher >> notify_stageC >> stageD_ingestor >> notify_stageD