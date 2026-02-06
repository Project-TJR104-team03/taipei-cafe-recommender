##此py檔用於GCP上的VM內，透過airflow決定執行順序與cloud run的task數

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator

# 處理 Airflow 版本相容性
try:
    from airflow.operators.empty import EmptyOperator
except ImportError:
    from airflow.operators.dummy import DummyOperator as EmptyOperator

# ==========================================
# 1. 基礎設定 (請根據你的 GCP 環境修改)
# ==========================================
PROJECT_ID = "project-tjr104-cafe"
REGION = "asia-east1"
JOB_NAME = "cafe-scraper" # 部署在 Cloud Run 的 Job 名稱 (對應 main.py)
REGIONS_TO_SCAN = [
    "A-1", "A-2", "A-3",  # 核心商務區
    "B-1", "B-2", "B-3",  # 文青精華區
    "C-1", "C-2", "C-3",  # 東區與內湖
    "D-1", "D-2", "D-3",  # 北區觀光區
    "E-1", "E-2", "E-3"   # 西南與南港
]

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
    'cafe_crawler_pipeline',
    default_args=default_args,
    description='TJR104 完整爬蟲流程：建置 -> 挖掘 -> 合併',
    schedule_interval=None, # 手動觸發
    start_date=datetime(2026, 2, 6),
    catchup=False,
    tags=['cafe', 'production', 'v2'],
) as dag:

    # ==========================================
    # Phase 1: 建置期 (Discovery)
    # 目標：找出店家，建立基礎名單
    # ==========================================
    scan_tasks = []
    # 1. Google Maps 掃描 (單機)
    # 迴圈建立 15 個 Task
    for region_code in REGIONS_TO_SCAN:
        # 轉換 task_id 格式 (例如: A-1 -> scan_a_1)
        safe_id = region_code.lower().replace("-", "_")
        
        task = CloudRunExecuteJobOperator(
            task_id=f'scan_{safe_id}', 
            project_id=PROJECT_ID,
            region=REGION,
            job_name=JOB_NAME,
            overrides={
                "task_count": 1, 
                "container_overrides": [{
                    # 這裡傳入具體的區域代碼，Python 程式就會只掃這一區
                    "args": ["--task", "scan", "--region", region_code]
                }]
            }
        )
        scan_tasks.append(task)

    # 鏈接順序: scan_a_1 >> scan_a_2 >> ... >> scan_e_3
    for i in range(len(scan_tasks) - 1):
        scan_tasks[i] >> scan_tasks[i+1]

    # 抓出最後一棒 (E-3)，準備交接給下一階段
    last_scan_task = scan_tasks[-1]

    # 2. 食尚玩家爬蟲 (單機)
    task_supertaste = CloudRunExecuteJobOperator(
        task_id='p1_supertaste',
        project_id=PROJECT_ID,
        region=REGION,
        job_name=JOB_NAME,
        overrides={
            "task_count": 1,
            "container_overrides": [{
                "args": ["--task", "supertaste"] # 食尚玩家可能不需要地區參數
            }]
        }
    )

    # 同步點：確保 Phase 1 全部跑完才進 Phase 2
    phase1_done = EmptyOperator(task_id='phase1_completed')

    # ==========================================
    # Phase 2: 挖掘期 (Mining)
    # 目標：針對名單進行深度資料抓取 (支援分片)
    # ==========================================

    # 官方標籤 (3台機器)
    task_tags = CloudRunExecuteJobOperator(
        task_id='p2_tags',
        project_id=PROJECT_ID,
        region=REGION,
        job_name=JOB_NAME,
        overrides={
            "task_count": 3, 
            "container_overrides": [{"args": ["--task", "tags", "--region", "ALL"]}]
        }
    )

    # Google 評論 (5台機器 - 最繁重)
    task_reviews = CloudRunExecuteJobOperator(
        task_id='p2_reviews',
        project_id=PROJECT_ID,
        region=REGION,
        job_name=JOB_NAME,
        overrides={
            "task_count": 5, 
            "container_overrides": [{"args": ["--task", "reviews", "--region", "ALL"]}]
        }
    )

    # 愛食記 (3台機器)
    task_ifoodie = CloudRunExecuteJobOperator(
        task_id='p2_ifoodie',
        project_id=PROJECT_ID,
        region=REGION,
        job_name=JOB_NAME,
        overrides={
            "task_count": 3, 
            "container_overrides": [{"args": ["--task", "ifoodie", "--region", "ALL"]}]
        }
    )

    phase2_done = EmptyOperator(task_id='phase2_completed')

    # ==========================================================================
    # Phase 3: 資料合併
    # ==========================================================================
    task_merge = CloudRunExecuteJobOperator(
        task_id='p3_merge',
        project_id=PROJECT_ID,
        region=REGION,
        job_name=JOB_NAME,
        overrides={
            "task_count": 1,
            "container_overrides": [{"args": ["--task", "merge"]}]
        }
    )

    # ==========================================================================
    # 流程串接 (Dependency)
    # ==========================================================================
    
    # 1. 15個區跑完 -> 食尚玩家 -> Phase 1 結束
    last_scan_task >> task_supertaste >> phase1_done

    # 2. Phase 1 結束 -> 開啟平行挖掘
    phase1_done >> [task_tags, task_reviews, task_ifoodie] >> phase2_done

    # 3. Phase 2 結束 -> 合併資料
    phase2_done >> task_merge