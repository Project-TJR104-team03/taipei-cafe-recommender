# main.py
import subprocess
import os
import sys

def run_script(script_name):
    # 💡 關鍵修正：使用 sys.executable 確保子腳本使用同一個虛擬環境
    python_path = sys.executable 
    
    print(f" 正在啟動腳本: {script_name}...")
    
    # 修正路徑處理，確保在 Windows/Linux 都能正確找到檔案
    script_full_path = os.path.join(os.getcwd(), script_name)
    
    result = subprocess.run([python_path, script_full_path], capture_output=False)
    
    if result.returncode == 0:
        print(f" {script_name} 執行成功！")
    else:
        print(f"❌ {script_name} 執行失敗，終止後續任務。")
        sys.exit(1)

if __name__ == "__main__":
    # 這裡的路徑請根據你的資料夾結構微調
    scripts = [
        "src/scraper/01_path_b_scanner.py",
        "src/scraper/02_official_tag_scraper.py",
        "src/scraper/03_review_dynamic_scraper.py"
    ]

    print(" 啟動 TJR104 咖啡廳資料採集全流程 🌟")
    for script in scripts:
        run_script(script)
    
    print(" 所有爬蟲任務已圓滿完成！")