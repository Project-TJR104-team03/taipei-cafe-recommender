import google.generativeai as genai
import pandas as pd
import json
import time
import os

def ai_cleaner_batch(batch_data):
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
        response = model.generate_content(prompt)
        return json.loads(response.text)
    except Exception as e:
        print(f"⚠️ 批次處理出錯 (API 可能達到限制): {e}")
        return []

def clean_name_by_gemini():

    # ================= 配置區 (請確保 GCS 名稱與網頁一致) =================

    # 1. 雲端路徑設定
    BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "tjr104-cafe-datalake")
    PROJECT_FOLDER = os.getenv("PROJECT_FOLDER", "cafe_cleaning_project")

    # --- 自動生成的路徑 ---
    PROJECT_ROOT = f"gs://{BUCKET_NAME}/{PROJECT_FOLDER}"
    INPUT_CSV = f"{PROJECT_ROOT}/processed/cafes_stage1_cleaned.csv"
    INPUT_JSON = f"{PROJECT_ROOT}/processed/cafes_raw_tags.json"
    PROGRESS_FILE = f"{PROJECT_ROOT}/staging/cleaning_progress.json"
    TEMP_CSV = f"{PROJECT_ROOT}/staging/temp_results.csv"
    OUTPUT_FINAL = f"{PROJECT_ROOT}/output/cafes_stage2_final_all.csv"

    # 2. 模型與 API 配置
    API_KEY = os.getenv("GEMINI_API_KEY")
    MODEL_NAME = 'gemini-2.5-flash'  # 採用你指定的最新 2.5 模型

    # 3. 效能與速率限制 (10 RPM 安全設定)
    BATCH_SIZE = 30  
    SLEEP_TIME = 8   
    # =====================================================================

    if not API_KEY:
        raise ValueError("❌ 找不到 API_KEY，請檢查 .env 檔案")

    # 初始化 Gemini
    genai.configure(api_key=API_KEY)
    model = genai.GenerativeModel(
        model_name=MODEL_NAME,
        generation_config={"response_mime_type": "application/json"}
    )
    
    # 1. 從 GCS 讀取原始資料
    print(f"📡 正在從 GCS 讀取資料: {BUCKET_NAME}...")
    try:
        df_stage1 = pd.read_csv(INPUT_CSV)
        # 讀取 JSON 需要特殊處理 gcsfs
        with pd.io.common.get_handle(INPUT_JSON, "r")[0] as f:
            tags_data = json.load(f)
    except Exception as e:
        print(f"❌ 讀取失敗，請確認路徑或權限: {e}")
        return

    # 2. 讀取雲端已完成進度
    processed_ids = []
    try:
        with pd.io.common.get_handle(PROGRESS_FILE, "r")[0] as f:
            processed_ids = json.load(f)
    except:
        print("💡 找不到進度檔，將從頭開始處理。")
    
    # 3. 過濾出尚未處理的任務
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

    # 4. 讀取暫存結果 (若有)
    all_results = []
    try:
        all_results = pd.read_csv(TEMP_CSV).to_dict('records')
    except:
        pass

    # 5. 分批處理
    for i in range(0, len(tasks), BATCH_SIZE):
        batch = tasks[i : i + BATCH_SIZE]
        print(f"📦 正在處理: {i + len(processed_ids)} / {len(df_stage1)}...")
        
        cleaned = ai_cleaner_batch(batch)
        
        if cleaned:
            all_results.extend(cleaned)
            new_ids = [d['place_id'] for d in cleaned]
            processed_ids.extend(new_ids)
            
            # --- 關鍵：將結果與進度同步回 GCS ---
            try:
                # 寫入進度 JSON
                with pd.io.common.get_handle(PROGRESS_FILE, "w")[0] as f:
                    json.dump(processed_ids, f)
                # 寫入暫存 CSV
                pd.DataFrame(all_results).to_csv(TEMP_CSV, index=False, encoding="utf-8-sig")
                print(f"✅ 成功同步至雲端 ({len(cleaned)} 筆)")
            except Exception as e:
                print(f"⚠️ 雲端寫入失敗 (請檢查權限): {e}")
        else:
            print(f"❌ 批次失敗，等待 {SLEEP_TIME*2} 秒後重試...")
            time.sleep(SLEEP_TIME)

        time.sleep(SLEEP_TIME)

    # 6. 合併產出最終檔案至 Output 區
    print("\n💾 正在生成最終合併檔案...")
    result_df = pd.DataFrame(all_results)
    final_df = pd.merge(df_stage1, result_df[['place_id', 'final_name', 'branch']], on="place_id", how="left")
    
    final_df.to_csv(OUTPUT_FINAL, index=False, encoding="utf-8-sig")
    print(f"✨ 第二階段清洗任務完成！最終檔案：{OUTPUT_FINAL}")

if __name__ == "__main__":
    clean_name_by_gemini()