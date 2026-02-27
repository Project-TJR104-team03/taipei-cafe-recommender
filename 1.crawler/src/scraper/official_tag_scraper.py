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

# --- 3. 模組化入口 (被 main.py 呼叫) ---
def run(region="A-2", total_shards=1, shard_index=0):
    """
    執行官方標籤與網址採集任務 (支援分片)
    """
    BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "tjr104-cafe-datalake")
    ENV_LIMIT = os.getenv("SCAN_LIMIT")
    SCAN_LIMIT = int(ENV_LIMIT) if (ENV_LIMIT and ENV_LIMIT.isdigit()) else None

    if not BUCKET_NAME:
        print(" 錯誤: 找不到環境變數 GCS_BUCKET_NAME")
        sys.exit(1)

    # 路徑設定
    BASE_CSV_PATH = "raw/store/base.csv"
    
    # [修改點 1] 輸出檔名改為分片格式，避免衝突
    # 標籤檔
    TAGS_PART_PATH = f"raw/tag/parts/tags_official_{region}_part_{shard_index}.csv"
    # Base 更新檔 (URL/Payment)
    BASE_UPDATE_PATH = f"raw/store/parts/base_update_{region}_part_{shard_index}.csv"

    print(f"🚀 [Official Tags] 模組啟動 | 分片 {shard_index+1}/{total_shards} | 區域: {region}")

    full_df = load_csv_from_gcs(BUCKET_NAME, BASE_CSV_PATH)
    if full_df is None or full_df.empty:
        print(" 找不到 base.csv")
        sys.exit(1)

    # [修改點 2] 執行分片切分 (Sharding)
    # 只保留餘數等於當前 shard_index 的資料
    df_to_process = full_df[full_df.index % total_shards == shard_index].copy()
    print(f"📊 本分片分配到 {len(df_to_process)} 筆任務 (總數 {len(full_df)})")

    # 簡單過濾：根據 SCAN_LIMIT 跑 (如果是測試模式)
    if SCAN_LIMIT:
        df_to_process = df_to_process.head(SCAN_LIMIT)
        print(f" 測試模式: 僅執行前 {SCAN_LIMIT} 筆")

    # 初始化 Selenium
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=900,1000")
    chrome_options.add_argument("--lang=zh-TW")
    # 禁止圖片 (加速)
    prefs = {"profile.managed_default_content_settings.images": 2}
    chrome_options.add_experimental_option("prefs", prefs)
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    wait = WebDriverWait(driver, 15)
    
    batch_size = 10
    
    # 暫存容器
    new_tag_records = []
    base_updates = [] # 存 place_id, url, payment_options

    try:
        for i, (idx, row) in enumerate(df_to_process.iterrows(), 1):
            place_id = row.get('place_id')
            name = row.get('name')
            
            # --- 🌟 1. 唯一初始化 (關鍵：移除原本下方的重複定義) ---
            is_scanned = False 
            payment_options = ""
            current_url = f"https://www.google.com/maps/search/?api=1&query={urllib.parse.quote(name)}&query_place_id={place_id}"

            # 批次重啟邏輯 (維持原樣)
            if (i - 1) % batch_size == 0 and i > 1:
                print(f" 📦 批次重啟中...")
                driver.quit()
                driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
                wait = WebDriverWait(driver, 15)

            print(f"[{i}/{len(df_to_process)}] 🔗 處理: {name}")

            # --- 🌟 2. 爬取嘗試區塊 ---
            try:
                driver.get(current_url)
                time.sleep(4)
                driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.ESCAPE)
                
                # A. 點擊簡介按鈕
                about_xpath = "//button[contains(@aria-label, '關於') or contains(@aria-label, '簡介') or contains(@aria-label, 'About') or .//div[contains(text(), '簡介') or contains(text(), '關於')]]"
                about_btn = wait.until(EC.presence_of_element_located((By.XPATH, about_xpath)))
                driver.execute_script("arguments[0].click();", about_btn)
                
                # B. 驗證面板是否真的開啟 (真相檢測點)
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div[role="region"]')))
                time.sleep(4)
                
                # C. 滾動面板 (確保載入)
                try:
                    pane = driver.find_element(By.CSS_SELECTOR, 'div[role="region"][aria-label*="關於"], div[role="region"][aria-label*="簡介"], div.m6QErb.DxyBCb')
                    driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", pane)
                    time.sleep(2)
                except:
                    driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.PAGE_DOWN)
                    time.sleep(2)

                # D. 抓取內容塊
                soup = BeautifulSoup(driver.page_source, "html.parser")
                info_blocks = soup.select('div[role="region"].m6QErb div.iP2t7d')

                # --- 🌟 3. 終極成功判定 ---
                if info_blocks and len(info_blocks) > 0:
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
                    
                    # 🌟 只有真的有解析到內容，才翻轉為 True
                    if len(beautiful_text) > 0:
                        is_scanned = True 
                        print(f"    ✅ 真正掃描成功: 抓到 {len(beautiful_text)} 個字")

                    # 收集標籤資料
                    if beautiful_text:
                        for s in beautiful_text.split(" || "):
                            new_tag_records.append({'name': name, 'place_id': place_id, 'Tag': s, 'Tag_id': "PENDING", 'data_source': 'google簡介標籤'})
                else:
                    print(f"    ⚠️ 雖然面板開了，但抓不到任何標籤文字，標記為 False")
                    is_scanned = False

            except Exception as e:
                print(f"    ❌ 過程出錯: {str(e).splitlines()[0]}")
                is_scanned = False

            # --- 🌟 4. [關鍵位置] 不論成功或失敗，這筆一定會存入 base_updates ---
            # 請確保此 append 與外層的 try 對齊 (即迴圈內的最後一行)
            base_updates.append({
                'place_id': place_id,
                'google_maps_url': current_url,
                'payment_options': payment_options,
                'is_scanned': is_scanned
            })
            
            time.sleep(random.uniform(1, 2))

            # --- [修改點 3] 中途存檔 (存成分片檔，不讀取舊檔，直接 append 或 overwrite) ---
            # 為了簡化邏輯，我們這裡採用「累積一定量後存檔」
            if i % batch_size == 0:
                print(f" 中途寫入分片檔...")
                if new_tag_records:
                    df_tags = pd.DataFrame(new_tag_records)
                    # 讀取自己已經存過的 part file (append mode)
                    existing_part = load_csv_from_gcs(BUCKET_NAME, TAGS_PART_PATH)
                    if existing_part is not None:
                        df_tags = pd.concat([existing_part, df_tags], ignore_index=True)
                    
                    upload_df_to_gcs(df_tags.drop_duplicates(), BUCKET_NAME, TAGS_PART_PATH)
                    new_tag_records = [] # 清空

                if base_updates:
                    df_updates = pd.DataFrame(base_updates)
                    existing_part = load_csv_from_gcs(BUCKET_NAME, BASE_UPDATE_PATH)
                    if existing_part is not None:
                        df_updates = pd.concat([existing_part, df_updates], ignore_index=True)

                    upload_df_to_gcs(df_updates.drop_duplicates(subset=['place_id']), BUCKET_NAME, BASE_UPDATE_PATH)
                    base_updates = [] # 清空

    finally:
        driver.quit()

    # --- 最終存檔 ---
    print("\n 執行最終存檔...")
    if new_tag_records:
        df_tags = pd.DataFrame(new_tag_records)
        existing_part = load_csv_from_gcs(BUCKET_NAME, TAGS_PART_PATH)
        if existing_part is not None:
            df_tags = pd.concat([existing_part, df_tags], ignore_index=True)
        upload_df_to_gcs(df_tags.drop_duplicates(), BUCKET_NAME, TAGS_PART_PATH)

    if base_updates:
        df_updates = pd.DataFrame(base_updates)
        existing_part = load_csv_from_gcs(BUCKET_NAME, BASE_UPDATE_PATH)
        if existing_part is not None:
            df_updates = pd.concat([existing_part, df_updates], ignore_index=True)
        upload_df_to_gcs(df_updates.drop_duplicates(subset=['place_id']), BUCKET_NAME, BASE_UPDATE_PATH)

    print(" Official Tags 分片任務結束！")