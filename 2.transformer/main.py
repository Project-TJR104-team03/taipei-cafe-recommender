import argparse
import subprocess
import sys
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [ROUTER] - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 定義所有可執行的任務與其對應的模組路徑
TASK_ROUTING_TABLE = {
    # --- Stage 0: 資料預處理 ---
    "stage0_regex": "llm_src.stage0_prep.name_cleaned_01_regex",
    "stage0_name_AI": "llm_src.stage0_prep.name_cleaned_02_AI",
    "stage0_review_prefilter": "llm_src.stage0_prep.review_prefilter_top50",
    "stage0_tag_processor" : "llm_src.stage0_prep.tag_processor",

    # --- Stage A: AI 審計 ---
    "stageA_processor": "llm_src.stageA_extraction.A_StageA_Processor",
    "stageA_launcher": "llm_src.utils.VertexAI_Launcher",  # 發射 Audit 任務
    "stageA_parser": "llm_src.stageA_extraction.audit_result_parser",
    
    # --- Stage B: 特徵融合與評分 ---
    "stageB_merger": "llm_src.stageB_convergence.tags_Merger",
    "stageB_scorer": "llm_src.stageB_convergence.tag_scorer",
    
    # --- Stage C: 向量生成 ---
    "stageC_builder": "llm_src.stageC_embeddin.embed_builder",
    "stageC_launcher": "llm_src.utils.VertexAI_Launcher",  # 發射 Embedding 任務
    
    # --- Stage D: 終極資料庫寫入 ---
    "stageD_ingestor": "llm_src.stageD_ingestion.mongo_ingestor"
}

def main():
    parser = argparse.ArgumentParser(description="Cafe Data Pipeline Router for Cloud Run")
    parser.add_argument(
        "--task", 
        type=str, 
        required=True, 
        help=f"指定要執行的任務。可選值: {', '.join(TASK_ROUTING_TABLE.keys())}"
    )
    
    args = parser.parse_args()
    task_name = args.task

    if task_name not in TASK_ROUTING_TABLE:
        logger.error(f"❌ 找不到指定的任務: {task_name}")
        logger.info(f"💡 可用的任務清單: {list(TASK_ROUTING_TABLE.keys())}")
        sys.exit(1)

    module_to_run = TASK_ROUTING_TABLE[task_name]
    logger.info(f"🚀 準備啟動任務: [{task_name}] -> 執行模組: {module_to_run}")

    # 複製當前的環境變數 (這樣 .env 或 Cloud Run 的變數才會帶入)
    env = os.environ.copy()

    # 💡 特殊處理：根據 Launcher 任務動態注入 TARGET_TASK
    if task_name == "stageA_launcher":
        env["TARGET_TASK"] = "AUDIT"
        logger.info("⚙️ 已動態注入環境變數: TARGET_TASK=AUDIT")
    elif task_name == "stageC_launcher":
        env["TARGET_TASK"] = "EMBEDDING"
        logger.info("⚙️ 已動態注入環境變數: TARGET_TASK=EMBEDDING")

    # 使用 subprocess 執行，等同於在終端機輸入 python -m ...
    try:
        result = subprocess.run(
            ["python", "-m", module_to_run],
            env=env,
            check=True # 如果 returncode 不是 0，會拋出 CalledProcessError
        )
        logger.info(f"✅ 任務 [{task_name}] 執行成功！")
        
    except subprocess.CalledProcessError as e:
        logger.error(f"💥 任務 [{task_name}] 執行失敗，退出碼 (Exit Code): {e.returncode}")
        sys.exit(e.returncode)
    except Exception as e:
        logger.error(f"💥 發生未知的嚴重錯誤: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()