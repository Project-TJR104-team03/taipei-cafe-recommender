import sys
import os
import time
import random
import io
import pandas as pd
import urllib.parse
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

def list_gcs_parts(bucket_name, prefix):
    """找出所有分片檔案"""
    client = get_gcs_client()
    blobs = client.list_blobs(bucket_name, prefix=prefix)
    return [blob.name for blob in blobs if blob.name.endswith('.csv')]

def load_csv_from_gcs(bucket_name, blob_name):
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    if not blob.exists(): return None
    content = blob.download_as_string()
    return pd.read_csv(io.BytesIO(content))

def upload_df_to_gcs(df, bucket_name, blob_name):
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
    blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
    print(f" ✅ 救援結果已存至: gs://{bucket_name}/{blob_name}")

# --- 2. 救援核心主程式 ---
def run(region="A-2"):
    BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "tjr104-cafe-datalake-test")
    
    PART_FOLDER_PREFIX = f"raw/store/parts/base_update_{region}_part_"
    RETRY_TAGS_PATH = f"raw/tag/parts/tags_official_{region}_retry.csv"
    RETRY_UPDATE_PATH = f"raw/store/parts/base_update_{region}_retry.csv"

    print(f"🕵️ 啟動 [Official Tags] 救援補漏模式 | 區域: {region}")

    # --- Step 1: 建立「有記憶力」的名單 ---
    all_part_files = list_gcs_parts(BUCKET_NAME, PART_FOLDER_PREFIX)
    if not all_part_files: return

    # 1. 讀取所有原始分片
    part_dfs = []
    for f in all_part_files:
        df = load_csv_from_gcs(BUCKET_NAME, f)
        if df is not None: part_dfs.append(df)
    combined_updates = pd.concat(part_dfs, ignore_index=True)

    # 🌟 2. [防重複核心] 讀取之前的救援帳本，把救成功的劃掉
    existing_retry = load_csv_from_gcs(BUCKET_NAME, RETRY_UPDATE_PATH)
    if existing_retry is not None:
        print(f" 📜 讀取現有救援進度，過濾已成功的店家...")
        combined_updates = pd.concat([combined_updates, existing_retry], ignore_index=True)
        # 關鍵：place_id 重複時保留最後一筆（救援後的 True）
        combined_updates = combined_updates.drop_duplicates(subset=['place_id'], keep='last')

    # 3. 找出「真正還沒成功」的店家
    retry_list = combined_updates[combined_updates['is_scanned'] == False].copy()

    # 補回店名
    base_df = load_csv_from_gcs(BUCKET_NAME, "raw/store/base.csv")
    retry_list = pd.merge(retry_list[['place_id']], base_df[['place_id', 'name']], on='place_id', how='left')

    print(f"📊 總待救名單: {len(combined_updates[combined_updates['is_scanned']==False]) + len(combined_updates[combined_updates['is_scanned']==True])}")
    print(f"✅ 先前已救回: {len(combined_updates[combined_updates['is_scanned']==True])}")
    print(f"🚀 本次目標補救: {len(retry_list)} 筆")

    if retry_list.empty:
        print(" 🎉 恭喜！所有店家都已救回成功。")
        return

    # --- Step 2: 設定瀏覽器 ---
    def create_driver():
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=900,1000")
        chrome_options.add_argument("--lang=zh-TW")
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

    driver = create_driver()
    wait = WebDriverWait(driver, 20)

    new_tag_records = []
    base_updates = []

    try:
        for i, (idx, row) in enumerate(retry_list.iterrows(), 1):
            place_id = row['place_id']
            name = row['name']
            is_scanned = False
            payment_options = ""
            current_url = f"https://www.google.com/maps/search/?api=1&query={urllib.parse.quote(name)}&query_place_id={place_id}"

            # --- 爬取邏輯 ---
            for attempt in range(2):
                print(f"[{i}/{len(retry_list)}] 🆘 救援嘗試 {attempt+1}: {name}")
                try:
                    driver.get(current_url)
                    time.sleep(6) 
                    driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.ESCAPE)
                    
                    about_xpath = "//button[contains(@aria-label, '關於') or contains(@aria-label, '簡介') or contains(@aria-label, 'About') or .//div[contains(text(), '簡介') or contains(text(), '關於')]]"
                    about_btn = wait.until(EC.presence_of_element_located((By.XPATH, about_xpath)))
                    driver.execute_script("arguments[0].click();", about_btn)
                    time.sleep(5) 
                    
                    soup = BeautifulSoup(driver.page_source, "html.parser")
                    info_blocks = soup.select('div[role="region"].m6QErb div.iP2t7d')
                    
                    formatted_sections = []
                    payment_methods = []
                    for block in info_blocks:
                        title_tag = block.find('h2')
                        if not title_tag: continue
                        category = title_tag.text.strip()
                        items = block.find_all('li')
                        valid_items = []
                        for li in items:
                            if "" in li.text: continue
                            text_span = li.find('span', attrs={'aria-label': True})
                            if text_span and "不提供" in text_span.get('aria-label', ''): continue
                            icon_span = li.find('span', class_=lambda c: c and 'google-symbols' in c)
                            if icon_span: icon_span.decompose()
                            it = li.text.strip()
                            if it: valid_items.append(it)
                        if valid_items:
                            formatted_sections.append(f"{category}：{' | '.join(valid_items)}")
                            if "付款" in category: payment_methods.extend(valid_items)

                    beautiful_text = " || ".join(formatted_sections)
                    payment_options = ",".join(payment_methods) if payment_methods else ""
                    
                    if len(beautiful_text) > 0:
                        is_scanned = True 
                        print(f"    ✅ 救回成功！")
                        for s in beautiful_text.split(" || "):
                            new_tag_records.append({'name': name, 'place_id': place_id, 'Tag': s, 'Tag_id': "PENDING", 'data_source': 'google簡介標籤'})
                        break 
                    else:
                        is_scanned = False 
                except Exception as e:
                    print(f"    ❌ 錯誤: {str(e).splitlines()[0]}")
            
            base_updates.append({'place_id': place_id, 'google_maps_url': current_url, 'payment_options': payment_options, 'is_scanned': is_scanned})

            # 🌟 [每 5 筆自動存檔與重啟]
            if i % 5 == 0:
                print(f" 🧹 存檔中並重啟瀏覽器 (進度: {i})...")
                # 存檔邏輯 (Tags & Updates)
                if new_tag_records:
                    df_t = pd.DataFrame(new_tag_records)
                    old_t = load_csv_from_gcs(BUCKET_NAME, RETRY_TAGS_PATH)
                    if old_t is not None: df_t = pd.concat([old_t, df_t], ignore_index=True).drop_duplicates()
                    upload_df_to_gcs(df_t, BUCKET_NAME, RETRY_TAGS_PATH)
                    new_tag_records = []
                if base_updates:
                    df_u = pd.DataFrame(base_updates)
                    old_u = load_csv_from_gcs(BUCKET_NAME, RETRY_UPDATE_PATH)
                    if old_u is not None:
                        final_u = pd.concat([old_u, df_u], ignore_index=True).drop_duplicates(subset=['place_id'], keep='last')
                    else: final_u = df_u
                    upload_df_to_gcs(final_u, BUCKET_NAME, RETRY_UPDATE_PATH)
                    base_updates = []
                
                driver.quit()
                driver = create_driver()
                wait = WebDriverWait(driver, 20)

    finally:
        driver.quit()
        # 最後存檔
        if new_tag_records:
            df_t = pd.DataFrame(new_tag_records)
            old_t = load_csv_from_gcs(BUCKET_NAME, RETRY_TAGS_PATH)
            if old_t is not None: df_t = pd.concat([old_t, df_t], ignore_index=True).drop_duplicates()
            upload_df_to_gcs(df_t, BUCKET_NAME, RETRY_TAGS_PATH)
        if base_updates:
            df_u = pd.DataFrame(base_updates)
            old_u = load_csv_from_gcs(BUCKET_NAME, RETRY_UPDATE_PATH)
            if old_u is not None:
                final_u = pd.concat([old_u, df_u], ignore_index=True).drop_duplicates(subset=['place_id'], keep='last')
            else: final_u = df_u
            upload_df_to_gcs(final_u, BUCKET_NAME, RETRY_UPDATE_PATH)

    print("🏁 救援任務結束！")