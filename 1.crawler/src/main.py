import sys
import os
import time
import runpy

# --- 設定區域 ---
# 定義腳本執行清單 (顯示名稱, 相對路徑)
SCRIPTS = [
    ("1. Store & Dynamic Data (API)", "src/scraper/01_path_b_scanner.py"),
    ("2. Official Tags (Selenium)",   "src/scraper/02_official_tag_scraper.py"),
    ("3. Reviews & User Tags (Selenium)", "src/scraper/03_review_dynamic_scraper.py")
]

def run_step(step_name, script_path):
    """
    執行單一腳本，並處理錯誤與計時
    """
    print(f"\n" + "="*50)
    print(f"🎬 [TJR104 總流程] 啟動步驟: {step_name}")
    print(f"📂 執行檔案: {script_path}")
    print("="*50 + "\n")
    
    start_time = time.time()
    
    # 檢查檔案是否存在
    if not os.path.exists(script_path):
        print(f"❌ 嚴重錯誤: 找不到檔案 {script_path}")
        print(f"   請確認 Dockerfile 是否有正確 COPY src 資料夾")
        sys.exit(1)

    try:
        # 使用 runpy 執行目標檔案的 __main__ 區塊
        #這等同於在終端機輸入 python script_path
        runpy.run_path(script_path, run_name="__main__")
        
        elapsed = time.time() - start_time
        print(f"\n✅ {step_name} 執行成功！ (耗時: {elapsed:.2f} 秒)")
        
    except SystemExit as e:
        # 處理子腳本內的 sys.exit()
        if e.code != 0:
            print(f"❌ {step_name} 回報錯誤退出 (Code: {e.code})")
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ {step_name} 發生未預期錯誤: {e}")
        # 在 Cloud Run Job 中，exit(1) 會觸發重試機制 (如果有的話) 或標記為失敗
        sys.exit(1)

if __name__ == "__main__":
    # 設定工作目錄為專案根目錄 (Docker 預設 WORKDIR)
    BASE_DIR = os.getenv("PYTHONPATH", os.getcwd())
    
    print(f"☁️ [TJR104 Cloud Run Job] 整合爬蟲任務啟動")
    print(f"📍 工作目錄: {BASE_DIR}")
    print(f"🎯 目標區域: {os.getenv('SCAN_REGION', '未設定 (使用預設)')}")
    
    total_start = time.time()
    
    for name, relative_path in SCRIPTS:
        full_path = os.path.join(BASE_DIR, relative_path)
        run_step(name, full_path)
        
        # 步驟間稍作休息，讓 log 緩衝寫入
        time.sleep(2)

    total_time = (time.time() - total_start) / 60
    print(f"\n🎉🎉🎉 所有爬蟲任務圓滿完成！總耗時: {total_time:.2f} 分鐘")