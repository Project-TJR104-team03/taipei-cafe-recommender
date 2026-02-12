from google.cloud import storage
import pandas as pd
import json
import time
import os
import vertexai
from vertexai.generative_models import GenerativeModel, GenerationConfig
from google.cloud import storage


# 模型與 API 配置
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "project-tjr104-cafe") 
LOCATION = "us-central1"  # 建議使用 us-central1，模型支援度最高
MODEL_NAME = os.getenv("AI_MODEL", "gemini-2.5-flash") 

# 效能與速率限制 (10 RPM 安全設定)
BATCH_SIZE = 30  
SLEEP_TIME = 8   

# 連接到vertexai
vertexai.init(project=PROJECT_ID, location=LOCATION)

# --- 3. 初始化模型 (注意 GenerationConfig 的寫法) ---
model = GenerativeModel(MODEL_NAME)
generation_config = GenerationConfig(
    response_mime_type="application/json"
)
def ai_cleaner_batch(model, batch_data):
    """呼叫 AI 進行批次清洗"""
    prompt = f"""
    你是一位台灣咖啡廳資料專家。請根據提供資訊，拆分「品牌主體」與「分店名」。
    規則：
    1. final_name：品牌主體。若 regex_name 誤切，請參考 original 找回完整名稱。
    2. branch：識別地理位置或編號（如：南京、2、二店）。若 tags 中有分店資訊請提取。
    3. 雜訊：移除廣告詞、SEO關鍵字、表情符號及括號。
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
    BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "tjr104-cafe-datalake")
    PROJECT_FOLDER = os.getenv("PROJECT_FOLDER", "cafe_cleaning_project")

    # --- 自動生成的路徑 ---
    PROJECT_ROOT = f"gs://{BUCKET_NAME}/{PROJECT_FOLDER}"
    INPUT_CSV = f"{PROJECT_ROOT}/processed/cafes_stage1_cleaned.csv"
    INPUT_JSON = f"{PROJECT_ROOT}/processed/cafes_raw_tags.json"
    PROGRESS_FILE = f"{PROJECT_ROOT}/staging/cleaning_progress.json"
    TEMP_CSV = f"{PROJECT_ROOT}/staging/temp_results.csv"
    OUTPUT_FINAL = f"{PROJECT_ROOT}/output/cafes_stage2_final_all.csv"
    
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    # 1. 從 GCS 讀取原始資料
    print(f"📡 正在從 GCS 讀取資料: {BUCKET_NAME}...")
    try:
        df_stage1 = pd.read_csv(INPUT_CSV)
        json_blob_path = INPUT_JSON.replace(f"gs://{BUCKET_NAME}/", "")
        json_data = bucket.blob(json_blob_path).download_as_text()
        tags_data = json.loads(json_data)
    except Exception as e:
        print(f"❌ 讀取失敗: {e}")
        return

    # 2. 讀取雲端已完成進度 (修正點)
    processed_ids = []
    try:
        with pd.io.common.get_handle(PROGRESS_FILE, "r") as handles:
            processed_ids = json.load(handles.handle)
        print(f"🔄 發現進度檔，已完成 {len(processed_ids)} 筆")
    except:
        print("💡 找不到進度檔，將從頭開始處理。")
    
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
        all_results = pd.read_csv(TEMP_CSV).to_dict('records')
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
                bucket.blob(PROGRESS_FILE.replace(f"gs://{BUCKET_NAME}/", "")).upload_from_string(
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
    final_df.to_csv(OUTPUT_FINAL, index=False, encoding="utf-8-sig")
    print(f"✨ 全量任務完成！檔案：{OUTPUT_FINAL}")

if __name__ == "__main__":
    clean_name_by_gemini()