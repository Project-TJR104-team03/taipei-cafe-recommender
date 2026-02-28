import argparse
import sys
import os
import logging

# --- 設定 Logging (讓 Log 顯示時間與發話者) ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [Controller] - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("MainController")

# --- 匯入爬蟲模組 (Worker Modules) ---
# 這裡對應你的 src/scraper/ 資料夾
# 如果你的資料夾內沒有 __init__.py，請記得新增一個空的 __init__.py 檔案
try:
    from src.scraper import (
        path_b_scanner,          # 對應 path_b_scanner.py
        supertaste_store,        # 對應 supertaste_store.py
        official_tag_scraper,    # 對應 official_tag_scraper.py
        review_dynamic_scraper,  # 對應 review_dynamic_scraper.py
        ifoodie_review_scraper,  # 對應 ifoodie_review_scraper.py
        review_scraper_original, # 對應 review_scraper_original.py
        official_tag_retry,      # 對應 official_tag_retry.py
        merger                   # 對應 merger.py
    )
except ImportError as e:
    # 🌟 修改：直接中止，這樣 Log 會立刻顯示到底缺哪個檔案
    logger.critical(f"❌ 模組匯入嚴重失敗！訊息: {e}")
    sys.exit(1)

def main():
    # 1. 定義指令參數 (Arguments)
    parser = argparse.ArgumentParser(description="TJR104 Cafe Data Pipeline Controller")
    
    # [必填] 任務名稱
    parser.add_argument("--task", type=str, required=True, 
                        choices=["scan", "supertaste", "tags", "reviews", "ifoodie", "merge", "reviews_original", "tags_retry"],
                        help="指定要執行的任務階段")
    
    # [選填] 平行處理參數 (預設為 1，即單機模式)
    parser.add_argument("--total_shards", type=int, default=1, help="總分片數 (機器總數)")
    parser.add_argument("--shard_index", type=int, default=0, help="當前分片編號 (0 ~ N-1)")
    
    # [選填] 區域參數 (預設 A-2)
    parser.add_argument("--region", type=str, default="A-2", help="掃描區域代碼")

    args = parser.parse_args()

    # Cloud Run Job 會自動注入這兩個變數
    env_task_index = os.environ.get('CLOUD_RUN_TASK_INDEX')
    env_task_count = os.environ.get('CLOUD_RUN_TASK_COUNT')

    if env_task_index is not None and env_task_count is not None:
        # 如果在 Cloud Run 上跑，使用環境變數覆蓋
        current_shard = int(env_task_index)
        total_shards = int(env_task_count)
        source_mode = "Cloud Run Autoscaling"
    else:
        # 如果在本地跑，使用指令參數
        current_shard = args.shard_index
        total_shards = args.total_shards
        source_mode = "Manual CLI Args"

    # 顯示當前任務資訊
    logger.info("="*50)
    logger.info(f" 啟動任務: {args.task.upper()}")
    logger.info(f" 參數配置: Region={args.region} | Shard={current_shard + 1}/{total_shards}")
    logger.info("="*50)

    try:
        # ==========================================
        # Phase 1: 建置期 (通常單機跑)
        # ==========================================
        if args.task == "scan":
            logger.info("呼叫 [Google Maps Scanner]...")
            path_b_scanner.run(region=args.region)

        elif args.task == "supertaste":
            logger.info("呼叫 [SuperTaste Scraper]...")
            # 食尚玩家通常不需要分區或分片，掃全台或特定邏輯
            supertaste_store.run()

        # ==========================================
        # Phase 2: 挖掘期 (支援分片平行處理)
        # ==========================================
        elif args.task == "tags":
            logger.info(f"呼叫 [Official Tags] (Shard {current_shard})...")
            official_tag_scraper.run(
                region=args.region,
                total_shards=total_shards,   # 使用計算後的變數
                shard_index=current_shard    # 使用計算後的變數
            )

        elif args.task == "reviews":
            logger.info(f"呼叫 [Google Reviews] (Shard {current_shard})...")
            review_dynamic_scraper.run(
                region=args.region,
                total_shards=total_shards,   # 使用計算後的變數
                shard_index=current_shard    # 使用計算後的變數
            )
            
        elif args.task == "ifoodie":
            logger.info(f"呼叫 [iFoodie Reviews] (Shard {current_shard})...")
            ifoodie_review_scraper.run(
                region=args.region,
                total_shards=total_shards,   # 使用計算後的變數
                shard_index=current_shard    # 使用計算後的變數
            )
        
        elif args.task == "reviews_original":
            logger.info("呼叫 [Google Reviews Original]...")
            review_scraper_original.run()

        elif args.task == "tags_retry":
            logger.info(f"呼叫 [Official Tags Retry] (救援補漏模式)...")
            # 救援模式通常單機跑完全部失敗者，所以只傳 region
            official_tag_retry.run(region=args.region)

        # Phase 3: 合併期
        elif args.task == "merge":
            logger.info("呼叫 [Data Merger]...")
            merger.run()

        logger.info(f" 任務 {args.task} 執行完畢！")

    except AttributeError as e:
        logger.error(f" 模組內找不到 run() 函式！請確認您已將主程式封裝進 def run(): 中。\n錯誤: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f" 任務執行發生未預期錯誤: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()