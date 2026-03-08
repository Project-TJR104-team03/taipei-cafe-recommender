from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from datetime import datetime, timedelta
import random

# 基本參數設定
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 1),
    'retries': 0,
}

# 設定 DAG：每 5 分鐘執行一次
with DAG(
    'cafe_dynamic_traffic_simulator',
    default_args=default_args,
    schedule_interval='*/5 * * * *',
    catchup=False,
    tags=['simulation', 'cloud_run'],
    # 🌟 神級參數：允許 Airflow 把變數渲染為原本的型態 (整數/字典/布林值)，而不是字串
    render_template_as_native_obj=True 
) as dag:

    @task
    def calculate_concurrent_tasks():
        """根據當前小時，計算這 5 分鐘內要『同時』開幾台 Cloud Run"""
        current_hour = datetime.now().hour
        
        # 依照你的需求，分配每個時段的併發數量
        if 0 <= current_hour < 6:
            tasks = 1 if random.random() < 0.7 else 0
        elif 6 <= current_hour < 12:
            tasks = 3 if random.random() < 0.8 else 2
        elif 12 <= current_hour < 18:
            tasks = 4 if random.random() < 0.8 else 5
        else: # 18 to 24
            tasks = 3 if random.random() < 0.8 else 2
            
        print(f"⏰ 現在時間: {current_hour} 點，決定同時啟動 【{tasks}】 個 Cloud Run 實例！")
        return tasks

    # 取得要併發的整數數量
    task_count_val = calculate_concurrent_tasks()

    # 執行 Cloud Run Job
    execute_job = CloudRunExecuteJobOperator(
        task_id='trigger_cloud_run_job',
        project_id='project-tjr104-cafe',
        region='asia-east1',
        job_name='ai-stress-test-job',
        # 🌟 修正寫法：直接傳入 TaskFlow 的變數，Airflow 會自動解析為整數
        overrides={
            "task_count": task_count_val
        }
    )

    # 任務順序不需要另外寫 >>，因為變數傳遞 (task_count_val) 已經隱含了依賴關係