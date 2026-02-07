import sys
import os
import time
import random
import io
import pandas as pd
import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

# Google Cloud
from google.cloud import storage

# 設定 Logger
logger = logging.getLogger("ReviewScraper")
logger.setLevel(logging.INFO)

# --- 1. GCS I/O 工具函式 ---
def get_gcs_client():
    return storage.Client()

def upload_screenshot_to_gcs(driver, bucket_name, place_id, step_name):
    """  [除錯神器] 截圖並上傳到 GCS """
    try:
        screenshot_bytes = driver.get_screenshot_as_png()
        blob_name = f"debug_screenshots/{place_id}_{step_name}.png"
        
        client = get_gcs_client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(screenshot_bytes, content_type='image/png')
        logger.info(f" 已截圖: gs://{bucket_name}/{blob_name}")
    except Exception as e:
        logger.error(f" 截圖上傳失敗: {e}")

def load_all_csvs_from_gcs(bucket_name, prefix):
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=prefix))
    csv_blobs = [b for b in blobs if b.name.endswith('.csv')]
    
    if not csv_blobs:
        logger.error(f" 在 gs://{bucket_name}/{prefix} 找不到任何 CSV 檔案")
        return None

    logger.info(f" 發現 {len(csv_blobs)} 個分片檔，開始下載合併...")
    df_list = []
    for blob in csv_blobs:
        try:
            content = blob.download_as_string()
            df = pd.read_csv(io.BytesIO(content))
            df.columns = df.columns.str.strip()
            df_list.append(df)
        except Exception as e:
            logger.warning(f" 無法讀取 {blob.name}: {e}")
            
    if df_list:
        full_df = pd.concat(df_list, ignore_index=True)
        return full_df
    return None

def save_csv_to_gcs(df, bucket_name, blob_name):
    try:
        client = get_gcs_client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        
        if blob.exists():
            old_content = blob.download_as_string()
            old_df = pd.read_csv(io.BytesIO(old_content))
            combined_df = pd.concat([old_df, df], ignore_index=True)
            combined_df = combined_df.drop_duplicates()
        else:
            combined_df = df
            
        csv_buffer = io.StringIO()
        combined_df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
        blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
        logger.info(f" 已上傳至: gs://{bucket_name}/{blob_name}")
    except Exception as e:
        logger.error(f" GCS 存檔失敗 {blob_name}: {e}")

def load_checkpoint_from_gcs(bucket_name, blob_name):
    try:
        client = get_gcs_client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        if blob.exists():
            content = blob.download_as_string()
            return pd.read_csv(io.BytesIO(content))
    except Exception as e:
        pass
    return pd.DataFrame(columns=['place_id', 'latest_review_id', 'last_sync_at'])

# --- 2. 輔助解析函式 ---
def parse_google_date(relative_date_text):
    now = datetime.now()
    try:
        clean_text = relative_date_text.replace("上次編輯：", "").replace("已編輯", "").strip()
        num = int(''.join(filter(str.isdigit, clean_text))) if any(char.isdigit() for char in clean_text) else 0
        if '天' in clean_text: return now - relativedelta(days=num)
        elif '週' in clean_text: return now - relativedelta(weeks=num)
        elif '個月' in clean_text: return now - relativedelta(months=num)
        elif '年' in clean_text: return now - relativedelta(years=num)
        return now
    except: return None

def split_reviewer_info(level_text):
    if not level_text: return "一般評論者", "0 則評論"
    parts = [p.strip() for p in level_text.split('·')]
    identity = "在地嚮導" if any("在地嚮導" in p for p in parts) else "一般評論者"
    review_count = next((p for p in parts if "則評論" in p), "0 則評論")
    return identity, review_count

# --- 3. 核心抓取邏輯 ---
def scrape_reviews_by_url(driver, url, p_name_placeholder, p_id, batch_id, last_seen_id=None, bucket_name=None):
    wait = WebDriverWait(driver, 15) # 縮短等待時間，加快除錯
    target_cutoff = datetime.now() - relativedelta(years=3) 
    review_results = []
    tag_records = []
    new_top_id = None
    real_store_name = p_name_placeholder 

    try:
        if not url or str(url) == 'nan':
            return [], [], None

        driver.get(url)
        time.sleep(random.uniform(3.0, 5.0)) # 等久一點，讓 Cloud Run 渲染

        # 登入偵測
        if "accounts.google.com" in driver.current_url or "signin" in driver.current_url:
            logger.warning(f"  {real_store_name} 觸發強制登入")
            # 截圖確認
            if bucket_name: upload_screenshot_to_gcs(driver, bucket_name, p_id, "login_blocked")
            return [], [], None

        # 嘗試關閉彈窗
        try:
            dismiss_btns = driver.find_elements(By.XPATH, "//span[contains(text(), '不用了') or contains(text(), 'Not now')]")
            if dismiss_btns:
                driver.execute_script("arguments[0].click();", dismiss_btns[0])
                time.sleep(1)
        except: pass

        # 抓取真實店名
        try:
            h1_element = wait.until(EC.presence_of_element_located((By.TAG_NAME, "h1")))
            if h1_element.text.strip():
                real_store_name = h1_element.text.strip()
        except: pass

        # --- 尋找評論按鈕 (最容易失敗的地方) ---
        found_btn = None
        try:
            # 擴充 XPATH，增加相容性
            xpath = "//*[self::button or self::div or self::span or self::a][contains(text(), '評論') or contains(text(), 'Reviews') or contains(@aria-label, '評論') or contains(@aria-label, 'Reviews')]"
            candidates = driver.find_elements(By.XPATH, xpath)
            for elm in candidates:
                if elm.is_displayed() and len(elm.text.strip()) < 15:
                    found_btn = elm
                    break
            
            if found_btn:
                driver.execute_script("arguments[0].click();", found_btn)
                time.sleep(random.uniform(3.0, 4.0))
            else:
                # 如果找不到按鈕，且沒在評論頁面，那就是失敗了 -> 截圖！
                if "reviews" not in driver.current_url:
                    logger.warning(f" {real_store_name} 找不到評論按鈕，截圖存證...")
                    if bucket_name: upload_screenshot_to_gcs(driver, bucket_name, p_id, "no_review_button")
                    return [], [], None
        except Exception as e:
            logger.error(f"按鈕點擊異常: {e}")
            if bucket_name: upload_screenshot_to_gcs(driver, bucket_name, p_id, "button_error")
            return [], [], None

        # --- A. 抓取標籤 ---
        try:
            tag_elements = driver.find_elements(By.CLASS_NAME, "e2moi")
            blacklist_keywords = ["所有評論", "查看另外", "個主題"]
            for tag in tag_elements:
                label = tag.get_attribute("aria-label")
                if label and not any(k in label for k in blacklist_keywords):
                    tag_records.append({
                        "name": real_store_name, 
                        "place_id": p_id,
                        "Tag": label.split('(')[0].strip(), 
                        "Tag_id": "PENDING",
                        "data_source": "google評論標籤"
                    })
        except: pass

        # --- B. 排序 ---
        try:
            sort_btn = wait.until(EC.presence_of_element_located((By.XPATH, "//button[.//span[text()='排序' or text()='Sort']]")))
            driver.execute_script("arguments[0].click();", sort_btn)
            time.sleep(1.0)
            latest_opt = wait.until(EC.presence_of_element_located((By.XPATH, "//div[contains(text(), '最新') or contains(text(), 'Newest')]")))
            driver.execute_script("arguments[0].click();", latest_opt)
            time.sleep(3.0)
        except: pass

        # --- C. 智慧滾動 ---
        try:
            scrollable_div = driver.find_element(By.XPATH, "//div[contains(@class, 'm6QErb') and contains(@class, 'DxyBCb')]")
        except:
            # 找不到滾動條 -> 截圖
            logger.warning(f" {real_store_name} 找不到滾動區域")
            if bucket_name: upload_screenshot_to_gcs(driver, bucket_name, p_id, "no_scroll")
            return [], tag_records, None

        last_height = driver.execute_script("return arguments[0].scrollHeight", scrollable_div)
        retry_count = 0
        
        while True:
            driver.execute_script('arguments[0].scrollTop = arguments[0].scrollHeight', scrollable_div)
            time.sleep(random.uniform(1.5, 2.5))
            
            soup = BeautifulSoup(driver.page_source, "html.parser")
            blocks = soup.select('div.jftiEf')
            if not blocks: break
            
            if not new_top_id: new_top_id = blocks[0].get('data-review-id')

            last_date_text = blocks[-1].select_one('span.rsqaWe').text if blocks[-1].select_one('span.rsqaWe') else ""
            last_date_obj = parse_google_date(last_date_text)
            
            if last_date_obj and last_date_obj < target_cutoff: break
            if last_seen_id and any(b.get('data-review-id') == last_seen_id for b in blocks): break

            new_height = driver.execute_script("return arguments[0].scrollHeight", scrollable_div)
            if new_height == last_height:
                retry_count += 1
                if retry_count >= 3: break
            else:
                retry_count = 0
                last_height = new_height

        # --- D. 解析 ---
        final_soup = BeautifulSoup(driver.page_source, "html.parser")
        blocks = final_soup.select('div.jftiEf')
        
        # 如果最後連一個 block 都沒抓到，也截圖看看發生什麼事
        if not blocks:
            logger.warning(f" {real_store_name} 滾動後未發現任何評論區塊")
            if bucket_name: upload_screenshot_to_gcs(driver, bucket_name, p_id, "empty_reviews")

        for block in blocks:
            rid = block.get('data-review-id')
            if last_seen_id and rid == last_seen_id: break
            
            content_text = block.select_one('span.wiI7pd').text.strip() if block.select_one('span.wiI7pd') else ""
            if not content_text: continue
            
            rel_date_text = block.select_one('span.rsqaWe').text if block.select_one('span.rsqaWe') else ""
            date_obj = parse_google_date(rel_date_text)
            reviewer_name = block.select_one('div.d4r55').text.strip() if block.select_one('div.d4r55') else "Unknown"
            
            if date_obj and date_obj >= target_cutoff:
                identity, amount = split_reviewer_info(block.select_one('div.RfnDt').text if block.select_one('div.RfnDt') else "")
                
                review_results.append({
                    "place_name": real_store_name,
                    "place_id": p_id,
                    "review_id": rid,
                    "reviewer_name": reviewer_name,
                    "content": content_text,
                    "relative_date": rel_date_text,
                    "full_date": date_obj.strftime('%Y-%m-%d'),
                    "is_edited": True if "編輯" in rel_date_text else False,
                    "reviewer_level": identity,
                    "reviewer_amount": amount,
                    "processed_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    "batch_id": batch_id,
                    "data_source": "Google_Maps"
                })

        return review_results, tag_records, new_top_id 
    except Exception as e:
        logger.error(f" 抓取異常: {e}")
        return [], [], None

# --- 4. 模組入口 ---
def run(region="A-2", total_shards=1, shard_index=0):
    BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME")
    if not BUCKET_NAME:
        logger.error(" 未設定 GCS_BUCKET_NAME")
        return

    SCAN_LIMIT_ENV = os.environ.get("SCAN_LIMIT")
    SCAN_LIMIT = int(SCAN_LIMIT_ENV) if SCAN_LIMIT_ENV and SCAN_LIMIT_ENV.isdigit() else None

    logger.info(f" [Reviews Scraper] 啟動 | Shard: {shard_index+1}/{total_shards}")
    
    # GCS 路徑 (小寫)
    INPUT_PREFIX = "raw/store/parts/"
    REVIEW_OUTPUT_BLOB = f"raw/comments/reviews_{region}_part_{shard_index}.csv"
    TAG_OUTPUT_BLOB = f"raw/tag/tags_{region}_part_{shard_index}.csv"
    CHECKPOINT_BLOB = f"raw/checkpoint/checkpoint_{region}_part_{shard_index}.csv"
    
    MY_BATCH_ID = f"BATCH_{datetime.now().strftime('%m%d_%H%M')}"

    full_df = load_all_csvs_from_gcs(BUCKET_NAME, INPUT_PREFIX)
    if full_df is None or full_df.empty: return

    column_mapping = {
        'URL': 'google_maps_url', 'url': 'google_maps_url', 'Google Maps URL': 'google_maps_url',
        'Place ID': 'place_id', 'Place Id': 'place_id', 'Name': 'name'
    }
    full_df.rename(columns=column_mapping, inplace=True)
    if 'name' not in full_df.columns:
        full_df['name'] = full_df['place_id'].astype(str)

    stores_df = full_df[full_df.index % total_shards == shard_index].copy()
    if SCAN_LIMIT: stores_df = stores_df.head(SCAN_LIMIT)
    
    logger.info(f" 任務數: {len(stores_df)} 筆")

    checkpoint_df = load_checkpoint_from_gcs(BUCKET_NAME, CHECKPOINT_BLOB)

    #  [關鍵修正]：Headless 模式的 Options 設定
    chrome_options = Options()
    chrome_options.add_argument("--headless=new") # 使用新版 headless
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080") # 加大視窗，避免 RWD 隱藏按鈕
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled") # 隱藏自動化特徵
    # 偽裝 User-Agent (這是繞過 Google 封鎖的關鍵)
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    
    temp_reviews, temp_tags = [], []
    batch_size = 2

    try:
        for step, (idx, row) in enumerate(stores_df.iterrows(), 1):
            if (step - 1) % batch_size == 0 and step > 1:
                driver.quit()
                driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

            logger.info(f"[{step}/{len(stores_df)}] {row['name']}")
            
            last_id = None
            if not checkpoint_df.empty and row['place_id'] in checkpoint_df['place_id'].values:
                last_id = checkpoint_df.loc[checkpoint_df['place_id'] == row['place_id'], 'latest_review_id'].values[0]

            # 呼叫時傳入 BUCKET_NAME 以便截圖上傳
            reviews, tags, new_top_id = scrape_reviews_by_url(
                driver, row['google_maps_url'], row['name'], row['place_id'], MY_BATCH_ID, last_id, bucket_name=BUCKET_NAME
            )
            
            if reviews: temp_reviews.extend(reviews)
            if tags: temp_tags.extend(tags)

            if new_top_id:
                new_cp = pd.DataFrame([{
                    'place_id': row['place_id'], 
                    'latest_review_id': new_top_id,
                    'last_sync_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }])
                checkpoint_df = checkpoint_df[checkpoint_df['place_id'] != row['place_id']]
                checkpoint_df = pd.concat([checkpoint_df, new_cp], ignore_index=True)

            if step % batch_size == 0:
                if temp_reviews:
                    save_csv_to_gcs(pd.DataFrame(temp_reviews), BUCKET_NAME, REVIEW_OUTPUT_BLOB)
                    temp_reviews = []
                if temp_tags:
                    save_csv_to_gcs(pd.DataFrame(temp_tags), BUCKET_NAME, TAG_OUTPUT_BLOB)
                    temp_tags = []
                save_csv_to_gcs(checkpoint_df, BUCKET_NAME, CHECKPOINT_BLOB)

    finally:
        driver.quit()
        if temp_reviews: save_csv_to_gcs(pd.DataFrame(temp_reviews), BUCKET_NAME, REVIEW_OUTPUT_BLOB)
        if temp_tags: save_csv_to_gcs(pd.DataFrame(temp_tags), BUCKET_NAME, TAG_OUTPUT_BLOB)
        save_csv_to_gcs(checkpoint_df, BUCKET_NAME, CHECKPOINT_BLOB)
        
        logger.info(f" 分片 {shard_index} 完成！")