import google.generativeai as genai
import os
from dotenv import load_dotenv

# 1. 載入環境變數
load_dotenv()

api_key = os.getenv("GOOGLE_API_KEY")
if not api_key:
    print("❌ 錯誤：找不到 GOOGLE_API_KEY，請檢查 .env 檔案")
    exit()

genai.configure(api_key=api_key)

print("🔍 正在查詢您的 API Key 可用的所有模型...\n")
print(f"{'模型名稱 (請複製這個)':<30} | {'功能 (Embedding/Generate)'}")
print("-" * 70)

try:
    found_embedding = False
    for m in genai.list_models():
        methods = m.supported_generation_methods
        
        # 標記功能類型
        func_type = ""
        if 'embedContent' in methods:
            func_type += "[轉向量 Embedding] "
            found_embedding = True
        if 'generateContent' in methods:
            func_type += "[對話生成 Chat] "
            
        print(f"{m.name:<30} | {func_type}")

    print("-" * 70)
    


except Exception as e:
    print(f"❌ 查詢失敗: {e}")