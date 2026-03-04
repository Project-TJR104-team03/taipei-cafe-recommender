from google.cloud import storage
import pandas as pd
import json
import time
import os
import io
import vertexai
from vertexai.generative_models import GenerativeModel, GenerationConfig
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv()

# 模型與 API 配置
PROJECT_ID = os.getenv("PROJECT_ID", "project-tjr104-cafe") 
LOCATION = "us-central1"  # 建議使用 us-central1，模型支援度最高
MODEL_NAME = "gemini-2.5-pro"

# 效能與速率限制 (10 RPM 安全設定)
BATCH_SIZE = 30  
SLEEP_TIME = 8   

# 連接到vertexai
vertexai.init(project=PROJECT_ID, location=LOCATION)

# --- 3. 初始化模型 (注意 GenerationConfig 的寫法) ---
model = GenerativeModel(MODEL_NAME)
generation_config = GenerationConfig(
    response_mime_type="application/json",
    temperature=0.0,
    top_p=0.95
)
def ai_cleaner_batch(model, batch_data):
    """呼叫 AI 進行批次清洗"""
    prompt = f"""
    你是一位台灣咖啡廳資料專家。請根據提供資訊，拆分「品牌主體」與「分店名」。
    規則：
    1. final_name：品牌主體。
        👉 **【重要：連鎖品牌正規化】若為知名連鎖品牌，請一律轉換為最簡潔的中文官方名稱（例如：將「STARBUCKS 星巴克」或「Starbucks」統一輸出為「星巴克」；將「LOUISA COFFEE」統一為「路易莎咖啡」）。**
        若 regex_name 誤切，請參考 original 找回完整名稱。
    2. branch：識別地理位置或編號（如：南京、2、二店）。若 tags 中有分店資訊請提取。
    3. 絕對禁止將分店資訊(如地區、路段、編號)保留在final_name中，必須嚴格拆分至branch
    4. 雜訊：移除廣告詞、SEO關鍵字、表情符號及括號。
    待處理資料：{json.dumps(batch_data, ensure_ascii=False)}
    輸出格式：JSON List [{{ "place_id": "...", "final_name": "...", "branch": "..." }}]
    """
    try:
        response = model.generate_content(prompt, generation_config=generation_config)
        # 🟢 加入字串清洗，防止 AI 噴出 ```json 框框
        clean_text = response.text.replace("```json", "").replace("```", "").strip()
        return json.loads(clean_text)
    except Exception as e:
        print(f"⚠️ 批次處理出錯 (API 可能達到限制): {e}")
        return []

def clean_name_by_gemini():
    # ================= 配置區 =================
    BUCKET_NAME = os.getenv("BUCKET_NAME", "tjr104-cafe-datalake")
    INPUT_CSV = os.getenv("GCS_NAME_REGEX_CLEAND")
    INPUT_JSON = os.getenv("GCS_TAG_REGEX")
    PROCESS_FILE = os.getenv("GCS_NAME_CLEAN_JSON_PROCESS", "transform/stage0/name_clean_process/cleaning_process.json")
    TEMP_CSV = os.getenv("GCS_NAME_CLEAN_CSV_PROCESS", "transform/stage0/name_clean_process/temp_results.csv")
    OUTPUT_FINAL = os.getenv("GCS_NAME_CLEAN_FINISH","transform/stage0/name_clean_finished.csv")
    
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    # 1. 從 GCS 讀取原始資料
    print(f"📡 正在從 GCS 讀取資料: {BUCKET_NAME}...")
    try:
        csv_blob = bucket.blob(INPUT_CSV)
        df_stage1 = pd.read_csv(io.BytesIO(csv_blob.download_as_bytes()))
        
        # 讀取 JSON
        json_blob = bucket.blob(INPUT_JSON)
        tags_data = json.loads(json_blob.download_as_text())
    except Exception as e:
        print(f"❌ 讀取失敗: {e}")
        return

    # 2. 讀取雲端已完成進度 (修正點)
    processed_ids = []
    try:
        process_blob = bucket.blob(PROCESS_FILE)
        if process_blob.exists():
            processed_ids = json.loads(process_blob.download_as_text())
            print(f"🔄 發現進度檔，已完成 {len(processed_ids)} 筆")
        else:
            print("💡 找不到進度檔，將從頭開始處理。")
    except Exception as e:
        print(f"💡 讀取進度檔發生狀況，將從頭開始處理。({e})")
    
    # 3. 過濾任務
    tasks = []
    for _, row in df_stage1.iterrows():
        pid = str(row['place_id'])
        if pid not in processed_ids:
            tasks.append({
                "place_id": pid,
                "regex_name": row['regex_clean_name'],
                "tags": tags_data.get(pid, {}).get('raw_tags', []),
                "original": row['original_name']
            })

    print(f"📊 總筆數: {len(df_stage1)} | ✅ 已完成: {len(processed_ids)} | 📝 待處理: {len(tasks)}")

    if not tasks:
        print("🎉 所有資料皆已處理完畢！")
        return

    # 4. 讀取暫存結果
    all_results = []
    try:
        temp_blob = bucket.blob(TEMP_CSV)
        if temp_blob.exists():
            temp_df = pd.read_csv(io.BytesIO(temp_blob.download_as_bytes()))
            all_results = temp_df.to_dict('records')
    except:
        pass

    # 5. 分批處理
    print(f"🚀 開始處理任務，共 {len(tasks)} 筆待處理...")

    initial_processed_count = len(processed_ids) 
    total_records = len(df_stage1)

    for i in range(0, len(tasks), BATCH_SIZE):
        batch = tasks[i : i + BATCH_SIZE]
        current_processed_total = initial_processed_count + i
        remaining_count = total_records - current_processed_total
        
        print(f"📦 正在處理批次: {i // BATCH_SIZE + 1} | ✅ 進度: {current_processed_total} / {total_records} | ⏳ 剩餘: {remaining_count} 筆...")        
        
        # 呼叫 Vertex AI
        cleaned = ai_cleaner_batch(model, batch)
        
        if cleaned:
            all_results.extend(cleaned)
            new_ids = [d['place_id'] for d in cleaned]
            processed_ids.extend(new_ids)

            # --- 立即同步至 GCS (確保 Cloud Run 中斷時進度不遺失) ---
            try:
                # 儲存進度 ID 清單
                bucket.blob(PROCESS_FILE.replace(f"gs://{BUCKET_NAME}/", "")).upload_from_string(
                    json.dumps(processed_ids), content_type='application/json'
                )
                
                # 儲存暫存結果 CSV
                temp_df = pd.DataFrame(all_results)
                bucket.blob(TEMP_CSV.replace(f"gs://{BUCKET_NAME}/", "")).upload_from_string(
                    temp_df.to_csv(index=False, encoding="utf-8-sig"), content_type='text/csv'
                )
                print(f"✅ 批次完成並已同步至 GCS")
            except Exception as e:
                print(f"⚠️ 雲端同步失敗 (但程式繼續): {e}")
        else:
            # 如果失敗，通常是觸發了 RPM (每分鐘限制)
            print(f"❌ 批次失敗，可能是達到 RPM 限制，冷卻 30 秒後重試...")
            time.sleep(30) # 遇到錯誤時加長冷卻時間
            continue 

        # 每個批次間的固定冷卻 (預防觸發 Vertex AI 預設 RPM 限制)
        time.sleep(SLEEP_TIME)

    # 6. 生成最終檔案
    print("\n💾 正在生成最終合併檔案...")
    result_df = pd.DataFrame(all_results)
    final_df = pd.merge(df_stage1, result_df[['place_id', 'final_name', 'branch']], on="place_id", how="left")
    final_csv_string = final_df.to_csv(index=False, encoding="utf-8-sig")
    bucket.blob(OUTPUT_FINAL).upload_from_string(final_csv_string, content_type='text/csv')
    
    print(f"✨ 全量任務完成！檔案已上傳至 GCS：gs://{BUCKET_NAME}/{OUTPUT_FINAL}")

if __name__ == "__main__":
    clean_name_by_gemini()