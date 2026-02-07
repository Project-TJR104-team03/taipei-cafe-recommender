import os
import json
import time
import gc
from google import genai
from google.genai import types
from dotenv import load_dotenv
from tqdm import tqdm

# 1. 載入環境變數
load_dotenv()

# 取得 API Key
api_key = os.getenv("GOOGLE_API_KEY")
if not api_key:
    print("❌ 錯誤：找不到 GOOGLE_API_KEY")
    exit()

# 2. 初始化新版 Client (注意：這裡不使用 genai.configure)
client = genai.Client(api_key=api_key)

# 設定檔案路徑
INPUT_FILE = "raw_data\cafes_raw_1600.json"
OUTPUT_FILE = "processed_data/cafes_vectors_google_1536.json"

# 🔥 設定批次大小 (每幾筆存一次)
BATCH_SIZE = 40


def get_embedding_new_sdk(text):
    """
    使用 google-genai (新版 SDK) 嘗試請求 1536 維度
    """
    try:
        # 使用 models.embed_content
        response = client.models.embed_content(
            model="models/gemini-embedding-001",
            contents=text,
            config=types.EmbedContentConfig(
                task_type="RETRIEVAL_DOCUMENT",
                # 🔥 關鍵：在這裡嘗試請求 1536 維度
                # 注意：如果模型不支援放大，這裡可能會報錯或被忽略
                output_dimensionality=1536 
            )
        )
        return response.embeddings[0].values
    except Exception as e:
        print(f"\n⚠️ API 請求失敗: {e}")
        return None


def main():
    # 建立資料夾
    output_dir = os.path.dirname(OUTPUT_FILE)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    if not os.path.exists(INPUT_FILE):
        print(f"❌ 找不到輸入檔案: {INPUT_FILE}")
        return

    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        cafes_data = json.load(f)

    print(f"🚀 準備處理 {len(cafes_data)} 筆資料...")
    print(f"💾 設定每 {BATCH_SIZE} 筆儲存一次...")

    # --- 步驟 A: 初始化輸出檔案 ---
    # 先以 'w' 模式開啟，寫入陣列的開頭 '['
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write('[\n')

    batch_buffer = []  # 暫存區
    is_first_batch = True # 用來判斷是否需要加逗號
    total_processed = 0

    # 使用 tqdm 顯示進度
    for i, cafe in enumerate(tqdm(cafes_data, desc="Embedding")):
        
        # 1. 資料提取
        cafe_id = cafe.get('place_id') or cafe.get('id') or "unknown"
        name = cafe.get('name', '未知')
        tags = cafe.get('tags', [])
        reviews = cafe.get('reviews', [])

        tags_str = ", ".join(tags) if isinstance(tags, list) else str(tags)
        reviews_str = " ".join([str(r) for r in reviews[:3]]) if isinstance(reviews, list) else str(reviews)
        
        text_to_embed = f"店名: {name}。 特色: {tags_str}。 評論: {reviews_str}"

        # 2. 呼叫 API
        vector = get_embedding_new_sdk(text_to_embed)

        if vector:
            item = {
                "place_id": cafe_id,
                "name": name,
                "embedding": vector,
                "metadata": text_to_embed[:50]
            }
            batch_buffer.append(item)

        # --- 步驟 B: 檢查是否達到批次大小 ---
        if len(batch_buffer) >= BATCH_SIZE:
            save_batch(batch_buffer, is_first_batch)
            
            # 重置狀態
            batch_buffer = []      # 清空 Python list
            is_first_batch = False # 之後都不是第一批了
            
            # 🔥 強制釋放記憶體
            gc.collect() 
            
        # 避免 API Rate Limit
        time.sleep(1.0) # 稍微睡一下比較安全

    # --- 步驟 C: 處理剩下的尾數 ---
    if batch_buffer:
        save_batch(batch_buffer, is_first_batch)
        batch_buffer = [] # 清空

    # --- 步驟 D: 寫入陣列結尾 ']' ---
    with open(OUTPUT_FILE, 'a', encoding='utf-8') as f:
        f.write('\n]')

    print(f"\n✅ 全部完成！已儲存至 {OUTPUT_FILE}")

def save_batch(data, is_first_batch):
    """
    負責將資料「附加 (Append)」到檔案中
    """
    if not data:
        return

    # 使用 'a' (append) 模式開啟檔案
    with open(OUTPUT_FILE, 'a', encoding='utf-8') as f:
        # 如果不是第一批，要在前面補上逗號和換行，保持 JSON 格式正確
        if not is_first_batch:
            f.write(',\n')
        
        # 將每個物件轉成 JSON 字串並寫入
        # 注意：我們不使用 json.dump(data)，因為那會多出 [] 括號
        # 我們要的是物件本身，並用逗號隔開
        json_strings = [json.dumps(item, ensure_ascii=False) for item in data]
        f.write(',\n'.join(json_strings))
        
        # 立即將資料從緩衝區寫入硬碟
        f.flush() 

if __name__ == "__main__":
    main()