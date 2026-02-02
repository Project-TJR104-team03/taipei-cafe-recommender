import sys
import os
import time
import runpy
from src.config.regions import CAFE_REGIONS  # 匯入你的區域設定檔

# --- 設定區域 ---
# 定義腳本執行清單 (顯示名稱, 相對路徑)
SCRIPTS = [
    ("1. Store & Dynamic Data (API)", "src/scraper/01_path_b_scanner.py"),
    ("2. Official Tags (Selenium)",   "src/scraper/02_official_tag_scraper.py"),
    ("3. Reviews & User Tags (Selenium)", "src/scraper/03_review_dynamic_scraper.py")
]

def run_step(step_name, script_path, current_region):
    """執行單一步驟"""
    print(f"   👉 [步驟] {step_name} | 區域: {current_region}")
    
    # 檢查檔案
    if not os.path.exists(script_path):
        print(f"❌ 找不到檔案: {script_path}")
        sys.exit(1)

    try:
        # 使用 runpy 執行
        runpy.run_path(script_path, run_name="__main__")
    except SystemExit as e:
        if e.code != 0:
            print(f"❌ {step_name} 失敗退出 (Code: {e.code})")
            sys.exit(1) # 遇到錯誤直接停止整個 Job，方便除錯
    except Exception as e:
        print(f"❌ {step_name} 發生例外錯誤: {e}")
        sys.exit(1)

if __name__ == "__main__":
    BASE_DIR = os.getenv("PYTHONPATH", os.getcwd())
    
    # --- 1. 決定要跑哪些區域 ---
    # 如果 SCAN_ALL = true，就跑 regions.py 裡的所有 Key
    # 否則，只跑 SCAN_REGION 設定的那一區
    is_scan_all = os.getenv("SCAN_ALL", "false").lower() == "true"
    
    if is_scan_all:
        target_regions = list(CAFE_REGIONS.keys()) # ['A-1', 'A-2', ... 'E-3']
        print(f"🔥 [全域模式] 準備掃描所有區域: {target_regions}")
    else:
        single_region = os.getenv("SCAN_REGION", "A-2")
        target_regions = [single_region]
        print(f"🎯 [單點模式] 鎖定掃描區域: {single_region}")

    total_start = time.time()

    # --- 2. 大迴圈：遍歷區域 ---
    for r_idx, region_code in enumerate(target_regions):
        region_info = CAFE_REGIONS.get(region_code, {})
        r_name = region_info.get('name', 'Unknown')
        
        print(f"\n" + "="*60)
        print(f"🌍 [進度 {r_idx+1}/{len(target_regions)}] 開始處理區域: {region_code} ({r_name})")
        print(f"="*60)

        # 🌟【關鍵技術】🌟
        # 動態修改環境變數！這樣 step1, step2, step3 讀取 os.getenv('SCAN_REGION') 時
        # 抓到的就會是現在迴圈跑到這一個，而不是寫死的全域變數
        os.environ["SCAN_REGION"] = region_code

        # --- 3. 小迴圈：執行三步驟 ---
        for step_name, relative_path in SCRIPTS:
            full_path = os.path.join(BASE_DIR, relative_path)
            run_step(step_name, full_path, region_code)
            
            # 步驟間緩衝，讓 Log 寫入 Cloud Logging
            time.sleep(2)
        
        print(f"✅ 區域 {region_code} 處理完成。")
        # 區域間稍微休息，避免過度頻繁請求被擋
        time.sleep(5)

    total_time = (time.time() - total_start) / 60
    print(f"\n🎉🎉🎉 任務全數完成！總共處理 {len(target_regions)} 個區域，耗時: {total_time:.2f} 分鐘")