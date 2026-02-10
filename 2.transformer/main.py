import os
import time
from dotenv import load_dotenv
from stage01_regex_cleaned import clean_name_by_py
from stage02_cleaning_name import clean_name_by_gemini

# 載入設定
load_dotenv()

def main():
    start_time = time.time()
    
    print("=== 🚀 TJR104 咖啡廳資料清洗管線開始 ===")

    # --- 階段一：Regex 初步清洗 ---
    print("\n--- [Step 1] 執行 Regex 初步清洗 ---")
    try:
        clean_name_by_py()
    except Exception as e:
        print(f"❌ 階段一失敗: {e}")
        return

    # --- 階段二：Gemini AI 進階校對 ---
    print("\n--- [Step 2] 執行 Gemini AI 進階校對 ---")
    try:
        # 使用你在 stage2 定義的邏輯與路徑
        clean_name_by_gemini()
    except Exception as e:
        print(f"❌ 階段二失敗: {e}")
        return

    end_time = time.time()
    duration = round((end_time - start_time) / 60, 2)
    print(f"\n=== ✨ 所有任務完成！總耗時: {duration} 分鐘 ===")

if __name__ == "__main__":
    main()