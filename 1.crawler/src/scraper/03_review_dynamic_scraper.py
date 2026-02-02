import sys
import os
import time
import random
import io
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from google.cloud import storage

# --- 1. 雲端 IO 工具函式 ---
def get_gcs_client():
    return storage.Client()

def load_csv_from_gcs(bucket_name, blob_name):
    """從 GCS 下載並讀取 CSV"""
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    
    if not blob.exists():
        return None
    
    content = blob.download_as_string()
    return pd.read_csv(io.BytesIO(content))

def upload_df_to_gcs(df, bucket_name, blob_name):
    """將 DataFrame 上傳至 GCS"""
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
    blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
    print(f"☁️ 已同步至 GCS: {blob_name}")

# --- 2. 資料解析工具 (保留原始邏輯) ---
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

# --- 3. 核心抓取函式 (針對 Cloud Run 優化) ---
def scrape_reviews_production(driver, p_name, p_addr, p_id, batch_id, last_seen_id=None):
    wait = WebDriverWait(driver, 20) # 稍微縮短 timeout
    target_cutoff = datetime.now() - relativedelta(years=3)
    review_results = []
    tag_records = []
    new_top_id = None

    try:
        query = f"{p_name} {str(p_addr)[:10]}"
        driver.get("https://www.google.com/maps")
        
        # 搜尋框處理
        search_box = wait.until(EC.element_to_be_clickable((By.NAME, "q")))
        search_box.clear()
        search_box.send_keys(query + Keys.ENTER)
        time.sleep(3) # 雲端網路可能稍慢，保留緩衝

        # 點擊地標 (HFpxzc)
        try:
            list_item = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "hfpxzc")))
            driver.execute_script("arguments[0].click();", list_item)
            time.sleep(3)
        except:
            print(f"    ⚠️ 找不到店家: {p_name}")
            return [], [], None

        # 點擊評論分頁
        try:
            review_tab = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(., '評論')]")))
            review_tab.click()
            time.sleep(2)
        except:
            print(f"    ⚠️ 無法切換至評論頁")
            return [], [], None

        # A. 抓取評論標籤 (Tag)
        try:
            tag_elements = driver.find_elements(By.CLASS_NAME, "e2moi")
            blacklist = ["所有評論", "查看另外", "個主題"]
            for tag in tag_elements:
                label = tag.get_attribute("aria-label")
                if label and not any(item in label for item in blacklist):
                    clean_tag = label.split('(')[0].strip()
                    tag_records.append({
                        "name": p_name, "place_id": p_id,
                        "Tag": clean_tag, "Tag_id": "PENDING",
                        "data_source": "google評論標籤"
                    })
        except: pass

        # B. 排序：最新
        try:
            sort_btn = wait.until(EC.presence_of_element_located((By.XPATH, "//button[.//span[text()='排序']]")))
            driver.execute_script("arguments[0].click();", sort_btn)
            time.sleep(1)
            latest_opt = wait.until(EC.presence_of_element_located((By.XPATH, "//div[contains(text(), '最新')]")))
            driver.execute_script("arguments[0].click();", latest_opt)
            time.sleep(3)
        except:
            print("    ⚠️ 無法切換排序，使用預設排序")

        # C. 滾動加載
        scrollable_div = driver.find_element(By.XPATH, "//div[contains(@class, 'm6QErb') and contains(@class, 'DxyBCb')]")
        last_height = driver.execute_script("return arguments[0].scrollHeight", scrollable_div)
        retry_count = 0
        
        while True:
            driver.execute_script('arguments[0].scrollTop = arguments[0].scrollHeight', scrollable_div)
            time.sleep(random.uniform(2, 3))
            
            soup = BeautifulSoup(driver.page_source, "html.parser")
            blocks = soup.select('div.jftiEf')
            if not blocks: break
            
            # 記錄最新的一則 ID (用於 Checkpoint)
            if not new_top_id: 
                new_top_id = blocks[0].get('data-review-id')

            # 檢查是否達到時間截止點
            last_date_text = blocks[-1].select_one('span.rsqaWe').text if blocks[-1].select_one('span.rsqaWe') else ""
            last_date_obj = parse_google_date(last_date_text)
            if last_date_obj and last_date_obj < target_cutoff:
                break
            
            # 檢查是否遇到上次爬過的 ID (增量更新關鍵)
            if last_seen_id and any(b.get('data-review-id') == last_seen_id for b in blocks):
                print(f"    ✅ 銜接至上次進度 (ID: {last_seen_id})")
                break

            new_height = driver.execute_script("return arguments[0].scrollHeight", scrollable_div)
            if new_height == last_height:
                retry_count += 1
                if retry_count >= 3: break # 增加重試容忍度
            else: 
                retry_count = 0
                last_height = new_height

        # D. 展開全文與解析
        expand_buttons = driver.find_elements(By.XPATH, "//button[contains(@aria-label, '顯示更多') or text()='更多']")
        for btn in expand_buttons:
            try: driver.execute_script("arguments[0].click();", btn)
            except: continue

        final_soup = BeautifulSoup(driver.page_source, "html.parser")
        for block in final_soup.select('div.jftiEf'):
            rid = block.get('data-review-id')
            if last_seen_id and rid == last_seen_id: break
            
            content_text = block.select_one('span.wiI7pd').text.strip() if block.select_one('span.wiI7pd') else ""
            if not content_text: continue # 略過無文字評論
            
            rel_date_text = block.select_one('span.rsqaWe').text if block.select_one('span.rsqaWe') else ""
            date_obj = parse_google_date(rel_date_text)
            
            if date_obj and date_obj >= target_cutoff:
                identity, amount = split_reviewer_info(block.select_one('div.RfnDt').text if block.select_one('div.RfnDt') else "")
                review_results.append({
                    "place_name": p_name, "place_id": p_id, "review_id": rid,
                    "reviewer_name": block.select_one('div.d4r55').text if block.select_one('div.d4r55') else "Unknown",
                    "content": content_text,
                    "relative_date": rel_date_text, "full_date": date_obj.strftime('%Y-%m-%d'),
                    "is_edited": True if "編輯" in rel_date_text else False,
                    "reviewer_level": identity, 
                    "reviewer_amount": amount,
                    "processed_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    "batch_id": batch_id, "data_source": "Google_Maps"
                })
        
        print(f"    --> 抓取 {len(review_results)} 則評論, {len(tag_records)} 個標籤")
        return review_results, tag_records, new_top_id 

    except Exception as e:
        print(f"    ❌ 抓取異常: {e}")
        return [], [], None

# --- 4. 執行主流程 ---
if __name__ == "__main__":
    # 環境變數
    BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "tjr104-cafe-datalake")
    REGION = os.getenv("SCAN_REGION", "A-2")
    SCAN_LIMIT = int(os.getenv("SCAN_LIMIT")) if os.getenv("SCAN_LIMIT") else None
    
    # GCS 路徑配置
    # 假設 Base Table 固定放在這裡 (或透過 Airflow 傳入)
    INPUT_BLOB = f"raw/store/{REGION}_base.csv" 
    # Checkpoint 檔案 (記錄每個店家上次爬到的 ID)
    CHECKPOINT_BLOB = f"raw/checkpoint/sync_checkpoint_{REGION}.csv"
    
    print(f"🚀 [Review Scraper] 啟動 - 區域: {REGION}")
    
    # 1. 讀取店家名單
    stores_df = load_csv_from_gcs(BUCKET_NAME, INPUT_BLOB)
    if stores_df is None:
        print(f"❌ 找不到輸入檔案: gs://{BUCKET_NAME}/{INPUT_BLOB}")
        sys.exit(1)
        
    if SCAN_LIMIT:
        stores_df = stores_df.head(SCAN_LIMIT)
        print(f"⚠️ 測試模式: 限制處理前 {SCAN_LIMIT} 筆")

    # 2. 讀取 Checkpoint (若無則建立空的)
    checkpoint_df = load_csv_from_gcs(BUCKET_NAME, CHECKPOINT_BLOB)
    if checkpoint_df is None:
        print("ℹ️ 無 Checkpoint 紀錄，將進行全量抓取。")
        checkpoint_df = pd.DataFrame(columns=['place_id', 'latest_review_id', 'last_sync_at'])

    # 3. 初始化 Selenium
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless") # 必備
    chrome_options.add_argument("--no-sandbox") # 必備
    chrome_options.add_argument("--disable-dev-shm-usage") # 必備
    chrome_options.add_argument("--window-size=900,1000")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    
    # 產生本次批次 ID (所有檔案都用這個 ID，方便追蹤)
    BATCH_ID = f"BATCH_{datetime.now().strftime('%Y%m%d_%H%M')}"
    
    # 暫存容器 (用於累積寫入，避免頻繁 IO)
    all_reviews = []
    all_tags = []
    checkpoint_updates = {} # 暫存 Checkpoint 更新

    try:
        for idx, row in stores_df.iterrows():
            p_id = row['place_id']
            p_name = row['name']
            p_addr = row.get('formatted_address', '')
            
            print(f"[{idx+1}/{len(stores_df)}] 處理: {p_name}")

            # 取得上次爬取的 Review ID
            last_id = None
            if p_id in checkpoint_df['place_id'].values:
                last_id = checkpoint_df.loc[checkpoint_df['place_id'] == p_id, 'latest_review_id'].values[0]

            # 執行爬蟲
            reviews, tags, new_top_id = scrape_reviews_production(
                driver, p_name, p_addr, p_id, BATCH_ID, last_id
            )

            if reviews: all_reviews.extend(reviews)
            if tags: all_tags.extend(tags)

            # 更新 Checkpoint 暫存 (只有當真的有抓到新 ID 時才更新)
            if new_top_id:
                checkpoint_updates[p_id] = {
                    'place_id': p_id,
                    'latest_review_id': new_top_id,
                    'last_sync_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
            
            time.sleep(random.uniform(1.5, 3))

    finally:
        driver.quit()
        print("\n💾 正在保存資料至 GCS...")

        # 4. 存檔 (採增量儲存，不覆蓋舊檔)
        if all_reviews:
            review_df = pd.DataFrame(all_reviews)
            # 檔名範例: raw/comments/A-2_reviews_BATCH_20231027.csv
            review_path = f"raw/comments/{REGION}_reviews_{BATCH_ID}.csv"
            upload_df_to_gcs(review_df, BUCKET_NAME, review_path)
        
        if all_tags:
            tag_df = pd.DataFrame(all_tags)
            tag_path = f"raw/tag/{REGION}_tags_{BATCH_ID}.csv"
            upload_df_to_gcs(tag_df, BUCKET_NAME, tag_path)

        # 5. 更新並覆寫 Checkpoint (這是唯一需要覆寫的檔案)
        if checkpoint_updates:
            # 將新的更新合併回原本的 DF
            for pid, data in checkpoint_updates.items():
                # 如果已存在，先刪除舊列
                checkpoint_df = checkpoint_df[checkpoint_df['place_id'] != pid]
                # 加入新列 (使用 pd.concat)
                new_row = pd.DataFrame([data])
                checkpoint_df = pd.concat([checkpoint_df, new_row], ignore_index=True)
            
            upload_df_to_gcs(checkpoint_df, BUCKET_NAME, CHECKPOINT_BLOB)
            print("✅ Checkpoint 已更新。")
        else:
            print("ℹ️ Checkpoint 無需更新。")

    print("🎉 任務圓滿結束！")