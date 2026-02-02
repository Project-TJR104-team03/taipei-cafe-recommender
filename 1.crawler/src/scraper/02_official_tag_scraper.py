import sys
import os
import time
import random
import io
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from google.cloud import storage
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# --- 1. 雲端工具函數 ---
def get_gcs_client():
    return storage.Client()

def load_csv_from_gcs(bucket_name, blob_name):
    """從 GCS 讀取 CSV 轉為 DataFrame"""
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    
    if not blob.exists():
        print(f"⚠️ GCS 檔案不存在: gs://{bucket_name}/{blob_name}")
        return None
        
    content = blob.download_as_string()
    return pd.read_csv(io.BytesIO(content))

def upload_df_to_gcs(df, bucket_name, blob_name):
    """將 DataFrame 上傳回 GCS"""
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
    blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
    print(f"✅ 已儲存至: gs://{bucket_name}/{blob_name}")

# --- 2. 結構化清洗函數 (保留原本邏輯) ---
def clean_google_tags_final(raw_content):
    if not raw_content: return "", ""

    lines = [l.strip() for l in raw_content.split('\n') if l.strip()]
    unique_lines = []
    [unique_lines.append(x) for x in lines if x not in unique_lines]

    formatted_sections = []
    payment_methods = []
    
    for section in unique_lines:
        # 過濾特殊符號
        if "" in section or "[無]" in section:
            continue

        if '' in section:
            parts = section.split('')
            category = parts[0].strip()
            items_list = [p.strip() for p in parts[1:] if p.strip()]
            
            items_str = " | ".join(items_list)
            formatted_sections.append(f"{category}：{items_str}")
            
            if "付款" in category:
                payment_methods.extend(items_list)

    full_tags_text = " || ".join(formatted_sections)
    payment_options_str = ",".join(payment_methods) if payment_methods else ""
    
    return full_tags_text, payment_options_str

# --- 3. 核心執行邏輯 ---
if __name__ == "__main__":
    # 1. 環境變數設定
    BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "tjr104-cafe-datalake")
    REGION = os.getenv("SCAN_REGION", "A-2")
    ENV_LIMIT = os.getenv("SCAN_LIMIT")
    SCAN_LIMIT = int(ENV_LIMIT) if (ENV_LIMIT and ENV_LIMIT.isdigit()) else None

    # 檢查是否拿到了，沒拿到就報錯（預防雲端沒設好）
    if not BUCKET_NAME:
        print("❌ 錯誤: 階段二找不到環境變數 GCS_BUCKET_NAME")
        sys.exit(1)

    # 定義路徑：讀取全台總表，並將標籤存入標籤總表
    BASE_CSV_PATH = "raw/store/base.csv"
    TAGS_TOTAL_PATH = "raw/tag/tags_total.csv"

    print(f"\n" + "="*50)
    print(f"🚀 [Tag Scraper] 增量模式啟動")
    print(f"📍 目標區域: {REGION} | 限制筆數: {SCAN_LIMIT if SCAN_LIMIT else '無'}")
    print(f"="*50)

    # --- 步驟 1: 讀取名單與排重 ---
    # 讀取 Step 1 產出的全台店家總表
    full_df = load_csv_from_gcs(BUCKET_NAME, BASE_CSV_PATH)
    if full_df is None or full_df.empty:
        print("❌ 找不到店家總表 (base.csv)，請先執行 Step 1")
        sys.exit(1)

    # 讀取已經爬過的標籤總表
    df_existing_tags = load_csv_from_gcs(BUCKET_NAME, TAGS_TOTAL_PATH)
    
    # 計算「尚未爬取」的名單
    if df_existing_tags is not None and not df_existing_tags.empty:
        done_ids = set(df_existing_tags['place_id'].unique())
        # 排除掉已經在標籤總表裡的店家
        df_to_process = full_df[~full_df['place_id'].isin(done_ids)]
        print(f"📊 總表共有 {len(full_df)} 筆，已完成 {len(done_ids)} 筆。")
    else:
        df_to_process = full_df
        df_existing_tags = pd.DataFrame()
        print("📊 標籤總表尚未建立，將從頭開始爬取。")

    # 套用掃描數量限制
    if SCAN_LIMIT:
        df_to_process = df_to_process.head(SCAN_LIMIT)

    if df_to_process.empty:
        print("✅ 所有店家皆已爬取完畢，無需執行。")
        sys.exit(0)

    print(f"📝 本次準備爬取 {len(df_to_process)} 筆新店家...\n")

    # --- 步驟 2: 初始化 Selenium ---
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--lang=zh-TW") # 強制中文，確保解析正確
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    
    new_tag_records = []
    payment_patch = {} # 用於回填支付方式

    # --- 步驟 3: 執行爬取 ---
    try:
        for index, row in df_to_process.iterrows():
            place_id = row.get('place_id')
            name = row.get('name')
            address = row.get('formatted_address', '')
            
            # 🌟 關鍵 1：變數初始化 (放在搜尋前，確保出錯也不會報 NameError)
            beautiful_text, payment_options = "", ""
            raw_content = ""
            
            query = f"{name} {str(address)[:10]}"
            print(f"🔍 [{index+1}/{len(df_to_process)}] 搜尋: {name}")

            try:
                # 🌟 關鍵 2：回到打字搜尋流程
                # A. 先前往主頁面
                driver.get("https://www.google.com/maps?hl=zh-TW")
                time.sleep(random.uniform(2, 3)) 

                # --- B. 找到搜尋框、輸入並 Enter (穩定版) ---
                try:
                    # 建立一個最多等 15 秒的「監視器」
                    wait = WebDriverWait(driver, 15)
                    
                    # 🌟 關鍵：等到搜尋框「真的出現在 DOM」且「可以被看到」
                    search_box = wait.until(
                        EC.visibility_of_element_located((By.ID, "searchboxinput"))
                    )
                    
                    search_box.clear()
                    search_box.send_keys(query)
                    search_box.send_keys(Keys.ENTER)
                    
                    # 這裡可以保留一點點 time.sleep，讓頁面有時間開始跳轉
                    time.sleep(random.uniform(2, 3)) 

                except Exception as e:
                    print(f"❌ 搜尋框等太久沒出現，目前網址: {driver.current_url}")
                    # 這裡可以選擇報錯或是截圖偵錯
                    raise e

                # C. 如果搜尋結果是列表，點擊第一個
                list_items = driver.find_elements(By.CLASS_NAME, "hfpxzc")
                if list_items:
                    list_items[0].click()
                    time.sleep(2)

                # D. 點擊「關於 (About)」
                try:
                    # 使用多重條件 XPATH 以提高穩定性
                    about_btn = driver.find_element(By.XPATH, "//button[contains(@aria-label, '關於') or contains(@aria-label, '簡介') or .//div[text()='關於']]")
                    driver.execute_script("arguments[0].click();", about_btn) # 使用 JS 點擊較不受遮擋影響
                    time.sleep(2)
                except Exception:
                    print(f" ℹ️  {name} 無法點擊「關於」分頁，可能直接顯示在主頁或無簡介。")

                # E. 解析標籤
                soup = BeautifulSoup(driver.page_source, "html.parser")
                info_blocks = soup.select('div[role="region"].m6QErb div.iP2t7d')
                for b in info_blocks:
                    raw_content += b.get_text(separator="\n") + "\n"

                # 🌟 關鍵 3：將解析出的 raw_content 丟進清洗函式
                if raw_content.strip():
                    beautiful_text, payment_options = clean_google_tags_final(raw_content)

                # F. 收集結果至容器
                if payment_patch is not None and payment_options:
                    payment_patch[place_id] = payment_options
                    print(f"    💰 支付方式: {payment_options}")

                if beautiful_text:
                    for section in beautiful_text.split(" || "):
                        new_tag_records.append({
                            'name': name,
                            'place_id': place_id,
                            'Tag': section,
                            'data_source': 'google_about_tab',
                            'crawled_at': time.strftime('%Y-%m-%d %H:%M:%S')
                        })
                    print(f"    ✅ 標籤採集成功")
                else:
                    print(f"    ⚠️ 未能解析到有效標籤")

            except Exception as e:
                print(f"    ❌ {name} 搜尋過程出錯: {e}")
            
            # 每跑完一家店休息一下
            time.sleep(random.uniform(2, 4))
    except Exception as global_e:
        print(f"🚨 執行過程發生嚴重錯誤: {global_e}")

    # --- 步驟 4: 合併新舊資料並儲存 ---
    if new_tag_records:
        print(f"\n📦 正在整合資料並回寫 GCS...")
        df_new_tags = pd.DataFrame(new_tag_records)
        
        # 合併舊有的標籤與本次新抓的標籤
        df_final_tags = pd.concat([df_existing_tags, df_new_tags], ignore_index=True)
        # 去重：確保同一個 place_id 下沒有重複的 Tag 內容
        df_final_tags = df_final_tags.drop_duplicates(subset=['place_id', 'Tag'], keep='first')
        
        # 覆寫回 GCS 總表 (讓下次執行能辨識已完成)
        upload_df_to_gcs(df_final_tags, BUCKET_NAME, TAGS_TOTAL_PATH)
        
        # 另存一份當次的備份檔案 (Archive)
        timestamp = time.strftime('%Y%m%d_%H%M')
        archive_path = f"raw/tag/archive/tags_{REGION}_{timestamp}.csv"
        upload_df_to_gcs(df_new_tags, BUCKET_NAME, archive_path)
        
        print(f"✅ 標籤總表更新成功，目前共 {len(df_final_tags)} 筆記錄。")
    else:
        print("ℹ️ 本次未採集到新標籤。")

    if payment_patch:
        print("\n🔄 正在將支付方式更新回店家總表...")
        # 將新抓到的支付方式對應回原本的 full_df
        full_df['payment_options'] = full_df['place_id'].map(payment_patch).fillna(full_df.get('payment_options', ''))
        
        # 覆寫回 GCS 上的 base.csv
        upload_df_to_gcs(full_df, BUCKET_NAME, BASE_CSV_PATH)
        print("✅ 店家總表 (base.csv) 支付方式更新完成。")

    print(f"🎉 區域 {REGION} 處理結束！")