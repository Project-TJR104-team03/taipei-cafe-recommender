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
from selenium.common.exceptions import TimeoutException, WebDriverException

# --- 1. 雲端工具函數 ---
def get_gcs_client():
    return storage.Client()

def load_csv_from_gcs(bucket_name, blob_name):
    """從 GCS 讀取 CSV 轉為 DataFrame """
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    
    if not blob.exists():
        print(f"⚠️ GCS 檔案不存在: gs://{bucket_name}/{blob_name}")
        return None
        
    content = blob.download_as_string()
    return pd.read_csv(io.BytesIO(content))

def upload_df_to_gcs(df, bucket_name, blob_name):
    """將 DataFrame 上傳回 GCS """
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
    blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
    print(f"✅ 已儲存至: gs://{bucket_name}/{blob_name}")

# --- 2. 結構化清洗函數 ---
def clean_google_tags_final(raw_content):
    if not raw_content: return "", ""

    lines = [l.strip() for l in raw_content.split('\n') if l.strip()]
    unique_lines = []
    [unique_lines.append(x) for x in lines if x not in unique_lines]

    formatted_sections = []
    payment_methods = []
    
    for section in unique_lines:
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

    if not BUCKET_NAME:
        print("❌ 錯誤: 找不到環境變數 GCS_BUCKET_NAME")
        sys.exit(1)

    BASE_CSV_PATH = "raw/store/base.csv"
    TAGS_TOTAL_PATH = "raw/tag/tags_total.csv"

    print(f"\n" + "="*50)
    print(f"🚀 [Tag Scraper] 穩定版啟動")
    print(f"📍 目標區域: {REGION} | 限制筆數: {SCAN_LIMIT if SCAN_LIMIT else '無'}")
    print(f"="*50)

    # 讀取名單
    full_df = load_csv_from_gcs(BUCKET_NAME, BASE_CSV_PATH)
    if full_df is None or full_df.empty:
        print("❌ 找不到店家總表 (base.csv)")
        sys.exit(1)

    df_existing_tags = load_csv_from_gcs(BUCKET_NAME, TAGS_TOTAL_PATH)
    
    if df_existing_tags is not None and not df_existing_tags.empty:
        done_ids = set(df_existing_tags['place_id'].unique())
        df_to_process = full_df[~full_df['place_id'].isin(done_ids)]
    else:
        df_to_process = full_df
        df_existing_tags = pd.DataFrame()

    if SCAN_LIMIT:
        df_to_process = df_to_process.head(SCAN_LIMIT)

    if df_to_process.empty:
        print("✅ 所有店家皆已爬取完畢。")
        sys.exit(0)

    # --- 初始化 Selenium ---
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage") # 解決 Tab Crashed 關鍵 
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--lang=zh-TW")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    wait = WebDriverWait(driver, 15)
    
    batch_size = 3  # 設定每 3 筆為一個批次
    new_tag_records = []
    payment_patch = {}

    try:
        for i, (index, row) in enumerate(df_to_process.iterrows(), 1):
            place_id = row.get('place_id')
            name = row.get('name')
            address = row.get('formatted_address', '')
            
            if (i - 1) % batch_size == 0:
                if 'driver' in locals(): driver.quit() # 如果已有 driver 則先關閉
                print(f"🔄 啟動全新瀏覽器實例 (處理第 {i} 筆起)...")
                driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
                wait = WebDriverWait(driver, 15)

            beautiful_text, payment_options, raw_content = "", "", ""
            query = f"{name} {str(address)[:10]}"
            print(f"🔍 [{index+1}/{len(df_to_process)}] 搜尋: {name}")

            try:
                # A. 前往主頁 (使用標準 Google Maps 網址提高穩定性)
                driver.get("https://www.google.com.tw/maps?hl=zh-TW")
                
                # B. 處理 Cookie 同意彈窗
                try:
                    consent_btn = WebDriverWait(driver, 5).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, "button[aria-label*='全部接受'], button[aria-label*='Accept all']"))
                    )
                    consent_btn.click()
                    time.sleep(1)
                except:
                    pass

                # C. 顯式等待搜尋框出現
                search_box = driver.find_element(By.NAME, "q")
                search_box.clear()
                search_box.send_keys(query)
                search_box.send_keys(Keys.ENTER)
                time.sleep(random.uniform(2, 4))

                # D. 處理列表或直接進入
                list_items = driver.find_elements(By.CLASS_NAME, "hfpxzc")
                if list_items:
                    list_items[0].click()
                    time.sleep(2)

                # E. 點擊「關於」分頁 (增加等待)
                try:
                    about_btn = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(@aria-label, '關於') or contains(@aria-label, '簡介') or .//div[text()='關於']]")))
                    driver.execute_script("arguments[0].click();", about_btn)
                    time.sleep(2)
                except:
                    print(f" ℹ️  {name} 無法點擊「關於」分頁")

                # F. 解析標籤
                soup = BeautifulSoup(driver.page_source, "html.parser")
                info_blocks = soup.select('div[role="region"].m6QErb div.iP2t7d')
                for b in info_blocks:
                    raw_content += b.get_text(separator="\n") + "\n"

                if raw_content.strip():
                    beautiful_text, payment_options = clean_google_tags_final(raw_content)

                if payment_options:
                    payment_patch[place_id] = payment_options

                if beautiful_text:
                    for section in beautiful_text.split(" || "):
                        new_tag_records.append({
                            'name': name, 'place_id': place_id, 'Tag': section,
                            'data_source': 'google_about_tab', 'crawled_at': time.strftime('%Y-%m-%d %H:%M:%S')
                        })
                    print(f"    ✅ 標籤採集成功")

            except (TimeoutException, WebDriverException) as e:
                page_title = driver.title
                print(f"    ❌ {name} 過程出錯 (跳過): {type(e).__name__}")
                print(f"    ℹ️ 當時網頁標題為: {page_title} | 網址: {driver.current_url}")

                continue # 跳過這間，繼續下一間
            
            time.sleep(random.uniform(1, 2))

            # --- 🌟 關鍵點：每 3 筆執行一次「中途存檔」 ---
            if i % batch_size == 0 or i == len(df_to_process):
                if new_tag_records:
                    print(f"💾 達到 {batch_size} 筆，執行中途存檔至 GCS...")
                    
                    # 重新讀取最新的總表 (避免多個 Job 同時寫入衝突，雖然 Job 通常是單一的)
                    df_latest_existing = load_csv_from_gcs(BUCKET_NAME, TAGS_TOTAL_PATH)
                    df_new_batch = pd.DataFrame(new_tag_records)
                    
                    # 合併並去重
                    df_updated_tags = pd.concat([df_latest_existing, df_new_batch], ignore_index=True)
                    df_updated_tags = df_updated_tags.drop_duplicates(subset=['place_id', 'Tag'])
                    
                    # 存回 GCS
                    upload_df_to_gcs(df_updated_tags, BUCKET_NAME, TAGS_TOTAL_PATH)
                    
                    # 存完後清空暫存容器，避免下次重複存入
                    new_tag_records = []
                    print(f"✅ 中途存檔完成，已釋放暫存清單。")

    finally:
        # 🌟 釋放資源與記憶體
        if 'driver' in locals():
            driver.quit()
            print("🧹 任務結束，瀏覽器已關閉。")

    # --- 儲存資料 ---
    if new_tag_records:
        df_new_tags = pd.DataFrame(new_tag_records)
        df_final_tags = pd.concat([df_existing_tags, df_new_tags], ignore_index=True).drop_duplicates(subset=['place_id', 'Tag'])
        upload_df_to_gcs(df_final_tags, BUCKET_NAME, TAGS_TOTAL_PATH)
        
        if payment_patch:
            full_df['payment_options'] = full_df['place_id'].map(payment_patch).fillna(full_df.get('payment_options', ''))
            upload_df_to_gcs(full_df, BUCKET_NAME, BASE_CSV_PATH)

    print(f"🎉 區域 {REGION} 任務完成！")