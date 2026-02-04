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
    """從 GCS 讀取 CSV"""
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    if not blob.exists():
        return None
    content = blob.download_as_string()
    return pd.read_csv(io.BytesIO(content))

def upload_df_to_gcs(df, bucket_name, blob_name):
    """上傳 DataFrame 到 GCS"""
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
    blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
    print(f" 已儲存至: gs://{bucket_name}/{blob_name}")

# --- 2. 標籤清洗函式 ---
def clean_google_tags_final(raw_content):
    if not raw_content: return "", ""
    lines = [l.strip() for l in raw_content.split('\n') if l.strip()]
    unique_lines = []
    [unique_lines.append(x) for x in lines if x not in unique_lines]

    formatted_sections = []
    payment_methods = []
    
    for section in unique_lines:
        if "" in section or "[無]" in section: continue
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
    BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "tjr104-cafe-datalake")
    REGION = os.getenv("SCAN_REGION", "A-2")
    ENV_LIMIT = os.getenv("SCAN_LIMIT")
    SCAN_LIMIT = int(ENV_LIMIT) if (ENV_LIMIT and ENV_LIMIT.isdigit()) else None

    if not BUCKET_NAME:
        print(" 錯誤: 找不到環境變數 GCS_BUCKET_NAME")
        sys.exit(1)

    BASE_CSV_PATH = "raw/store/base.csv"
    # 架構師建議：將官方標籤獨立存放，避免與評論標籤混淆，或者依據你的需求決定是否合併
    # 這裡示範獨立檔名，若你要合併，請改為 "raw/tag/tags_total.csv"
    TAGS_TOTAL_PATH = "raw/tag/tags_official.csv"

    print(f"🚀 [02 Cloud Tag & URL Scraper] 啟動 | 區域: {REGION}")

    full_df = load_csv_from_gcs(BUCKET_NAME, BASE_CSV_PATH)
    if full_df is None or full_df.empty:
        print(" 找不到 base.csv")
        sys.exit(1)

    # 簡單過濾：只跑還沒有 URL 的，或者根據 SCAN_LIMIT 跑
    # 這裡先假設照 SCAN_LIMIT 跑
    df_to_process = full_df.head(SCAN_LIMIT) if SCAN_LIMIT else full_df

    # 初始化 Selenium (雲端配置)
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=900,1000")
    chrome_options.add_argument("--lang=zh-TW")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    wait = WebDriverWait(driver, 15)
    
    batch_size = 3
    payment_patch = {}
    url_patch = {} #  新增：網址收集器
    new_tag_records = []

    try:
        for i, (index, row) in enumerate(df_to_process.iterrows(), 1):
            place_id = row.get('place_id')
            name = row.get('name')
            address = row.get('formatted_address', '')
            
            # 批次重啟 (記憶體管理)
            if (i - 1) % batch_size == 0 and i > 1:
                driver.quit()
                driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
                wait = WebDriverWait(driver, 15)

            query = f"{name} {str(address)[:10]}"
            print(f"[{i}/{len(df_to_process)}]  搜尋: {name}")

            try:
                driver.get("https://www.google.com.tw/maps")
                time.sleep(1)
                
                # Cookie 處理
                try:
                    btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button[aria-label*='全部接受'], button[aria-label*='Accept all']")))
                    btn.click()
                except: pass

                box = driver.find_element(By.NAME, "q")
                box.clear()
                box.send_keys(query + Keys.ENTER)
                time.sleep(3)

                items = driver.find_elements(By.CLASS_NAME, "hfpxzc")
                if items:
                    items[0].click()
                    time.sleep(2)

                # 🌟 [關鍵新增]：抓取當前 Google Maps 網址
                current_url = driver.current_url
                url_patch[place_id] = current_url
                print(f"     取得網址")

                # 點擊關於
                try:
                    about_btn = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(@aria-label, '關於') or contains(@aria-label, '簡介')]")))
                    driver.execute_script("arguments[0].click();", about_btn)
                    wait.until(EC.text_to_be_present_in_element((By.CSS_SELECTOR, 'div[role="region"]'), ""))
                    time.sleep(1)
                except:
                    print(f"     無法進入簡介頁")

                # 解析
                soup = BeautifulSoup(driver.page_source, "html.parser")
                info_blocks = soup.select('div[role="region"].m6QErb div.iP2t7d')
                raw_content = "\n".join([b.text for b in info_blocks])

                beautiful_text, payment_options = clean_google_tags_final(raw_content)

                if payment_options:
                    payment_patch[place_id] = payment_options
                
                if beautiful_text:
                    for section in beautiful_text.split(" || "):
                        new_tag_records.append({
                            'name': name,
                            'place_id': place_id,
                            'Tag': section,
                            'Tag_id': "PENDING",
                            'data_source': 'google簡介標籤'
                        })
                    print(f"     標籤已抓取")

            except Exception as e:
                print(f"     錯誤: {e}")
                continue
            
            time.sleep(random.uniform(1, 2))

            # --- 中途存檔 Checkpoint ---
            if i % batch_size == 0:
                print(f" 中途存檔...")
                # 1. 存 Tags
                if new_tag_records:
                    df_new_tags = pd.DataFrame(new_tag_records)
                    df_existing_tags = load_csv_from_gcs(BUCKET_NAME, TAGS_TOTAL_PATH)
                    if df_existing_tags is not None:
                        df_updated_tags = pd.concat([df_existing_tags, df_new_tags], ignore_index=True)
                    else:
                        df_updated_tags = df_new_tags
                    
                    df_updated_tags.drop_duplicates(subset=['place_id', 'Tag'], inplace=True)
                    upload_df_to_gcs(df_updated_tags, BUCKET_NAME, TAGS_TOTAL_PATH)
                    new_tag_records = [] # 清空暫存

                # 2. 存 Base (回填 URL) - 這裡需要重新讀取最新的 base，以免覆蓋別人的修改
                if payment_patch or url_patch:
                    current_base = load_csv_from_gcs(BUCKET_NAME, BASE_CSV_PATH)
                    if current_base is not None:
                        # 使用 map 更新
                        current_base['google_maps_url'] = current_base['place_id'].map(url_patch).fillna(current_base.get('google_maps_url', ''))
                        current_base['payment_options'] = current_base['place_id'].map(payment_patch).fillna(current_base.get('payment_options', ''))
                        upload_df_to_gcs(current_base, BUCKET_NAME, BASE_CSV_PATH)
    finally:
        driver.quit()

    # --- 最終存檔 ---
    print("\n 執行最終存檔...")
    if new_tag_records:
        df_new_tags = pd.DataFrame(new_tag_records)
        df_existing_tags = load_csv_from_gcs(BUCKET_NAME, TAGS_TOTAL_PATH)
        df_final_tags = pd.concat([df_existing_tags, df_new_tags], ignore_index=True) if df_existing_tags is not None else df_new_tags
        df_final_tags.drop_duplicates(subset=['place_id', 'Tag'], inplace=True)
        upload_df_to_gcs(df_final_tags, BUCKET_NAME, TAGS_TOTAL_PATH)

    if payment_patch or url_patch:
        current_base = load_csv_from_gcs(BUCKET_NAME, BASE_CSV_PATH)
        if current_base is not None:
            current_base['google_maps_url'] = current_base['place_id'].map(url_patch).fillna(current_base.get('google_maps_url', ''))
            current_base['payment_options'] = current_base['place_id'].map(payment_patch).fillna(current_base.get('payment_options', ''))
            upload_df_to_gcs(current_base, BUCKET_NAME, BASE_CSV_PATH)

    print(" 02 任務結束！")