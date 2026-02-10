import os
import time
from dotenv import load_dotenv
from stage01_regex_cleaned import clean_name_by_py
from stage02_cleaning_name import clean_name_by_gemini

# 載入設定
load_dotenv()

def main():
    start_time = time.time()
    
    # 1. 取得環境變數
    bucket_name = os.getenv("GCS_BUCKET_NAME", "tjr104-cafe-datalake")
    api_key = os.getenv("GEMINI_API_KEY")
    
    if not api_key:
        print("❌ 錯誤：找不到 GEMINI_API_KEY")
        return

    print("=== 🚀 TJR104 咖啡廳資料清洗管線開始 ===")

    # --- 階段一：Regex 初步清洗 ---
    print("\n--- [Step 1] 執行 Regex 初步清洗 ---")
    try:
        input_raw = "raw/store/base.csv"
        clean_name_by_py(bucket_name, input_raw)
    except Exception as e:
        print(f"❌ 階段一失敗: {e}")
        return

    # --- 階段二：Gemini AI 進階校對 ---
    print("\n--- [Step 2] 執行 Gemini AI 進階校對 ---")
    try:
        # 使用你在 stage2 定義的邏輯與路徑
        clean_name_by_gemini(bucket_name, api_key)
    except Exception as e:
        print(f"❌ 階段二失敗: {e}")
        return

    end_time = time.time()
    duration = round((end_time - start_time) / 60, 2)
    print(f"\n=== ✨ 所有任務完成！總耗時: {duration} 分鐘 ===")

if __name__ == "__main__":
    main()