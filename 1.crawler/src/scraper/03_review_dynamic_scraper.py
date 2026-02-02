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
    # 1. 環境變數與路徑配置
    BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "tjr104-cafe-datalake")
    REGION = os.getenv("SCAN_REGION", "A-2")
    SCAN_LIMIT = int(os.getenv("SCAN_LIMIT")) if os.getenv("SCAN_LIMIT") else None
    
    # 指向統一的總表路徑
    INPUT_BLOB = "raw/store/base.csv"  # 讀取 Step 1 整合後的總名單
    REVIEWS_TOTAL_PATH = "raw/comments/reviews_total.csv" # 評論總表
    TAGS_TOTAL_PATH = "raw/tag/tags_total.csv" # 標籤總表
    CHECKPOINT_BLOB = f"raw/checkpoint/sync_checkpoint_{REGION}.csv"
    
    print(f"\n" + "="*50)
    print(f"🚀 [Review Scraper] 啟動 - 區域模式: {REGION}")
    print(f"="*50)
    
    # 2. 讀取店家名單 (從總表讀取)
    full_stores_df = load_csv_from_gcs(BUCKET_NAME, INPUT_BLOB)
    if full_stores_df is None:
        print(f"❌ 找不到店家總表: {INPUT_BLOB}")
        sys.exit(1)
        
    # 這裡可以根據 REGION 篩選，或者如果是 SCAN_ALL 則全跑
    # 建議：即便跑全域，Step 3 也可以根據 Checkpoint 自動跳過不需要更新的店
    stores_to_process = full_stores_df
    if SCAN_LIMIT:
        stores_to_process = stores_to_process.head(SCAN_LIMIT)

    # 3. 讀取現有總表 (準備後續合併)
    df_existing_reviews = load_csv_from_gcs(BUCKET_NAME, REVIEWS_TOTAL_PATH) or pd.DataFrame()
    df_existing_tags = load_csv_from_gcs(BUCKET_NAME, TAGS_TOTAL_PATH) or pd.DataFrame()
    checkpoint_df = load_csv_from_gcs(BUCKET_NAME, CHECKPOINT_BLOB) or pd.DataFrame(columns=['place_id', 'latest_review_id', 'last_sync_at'])

    # 4. 初始化 Selenium
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--lang=zh-TW") # 確保抓到中文標籤
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    BATCH_ID = f"BATCH_{datetime.now().strftime('%Y%m%d_%H%M')}"
    
    new_reviews_accumulated = []
    new_tags_accumulated = []
    checkpoint_updates = {}

    try:
        for idx, row in stores_to_process.iterrows():
            p_id = row['place_id']
            p_name = row['name']
            p_addr = row.get('formatted_address', '')
            
            print(f"🔍 [{idx+1}/{len(stores_to_process)}] 同步評論: {p_name}")

            # 取得 Checkpoint 進度
            last_id = None
            if not checkpoint_df.empty and p_id in checkpoint_df['place_id'].values:
                last_id = checkpoint_df.loc[checkpoint_df['place_id'] == p_id, 'latest_review_id'].values[0]

            # 執行爬蟲 (使用你原本強大的 scrape_reviews_production)
            reviews, tags, new_top_id = scrape_reviews_production(
                driver, p_name, p_addr, p_id, BATCH_ID, last_id
            )

            if reviews: new_reviews_accumulated.extend(reviews)
            if tags: new_tags_accumulated.extend(tags)
            if new_top_id:
                checkpoint_updates[p_id] = {
                    'place_id': p_id,
                    'latest_review_id': new_top_id,
                    'last_sync_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
            
            time.sleep(random.uniform(1, 2))

    finally:
        driver.quit()
        print("\n📦 正在執行全量資料整合...")

        # --- 5. 整合與上傳 (增量模式) ---
        
        # A. 評論總表更新
        if new_reviews_accumulated:
            df_new_reviews = pd.DataFrame(new_reviews_accumulated)
            df_total_reviews = pd.concat([df_existing_reviews, df_new_reviews], ignore_index=True)
            # 評論通常不需過度去重(因為有 review_id)，但可防萬一
            df_total_reviews = df_total_reviews.drop_duplicates(subset=['review_id'])
            upload_df_to_gcs(df_total_reviews, BUCKET_NAME, REVIEWS_TOTAL_PATH)
        
        # B. 標籤總表更新 (與 Step 2 共用同一個標籤池)
        if new_tags_accumulated:
            df_new_tags = pd.DataFrame(new_tags_accumulated)
            df_total_tags = pd.concat([df_existing_tags, df_new_tags], ignore_index=True)
            df_total_tags = df_total_tags.drop_duplicates(subset=['place_id', 'Tag'])
            upload_df_to_gcs(df_total_tags, BUCKET_NAME, TAGS_TOTAL_PATH)

        # C. Checkpoint 更新 (保持原有的覆寫邏輯)
        if checkpoint_updates:
            for pid, data in checkpoint_updates.items():
                checkpoint_df = checkpoint_df[checkpoint_df['place_id'] != pid]
                checkpoint_df = pd.concat([checkpoint_df, pd.DataFrame([data])], ignore_index=True)
            upload_df_to_gcs(checkpoint_df, BUCKET_NAME, CHECKPOINT_BLOB)

    print(f"🎉 階段三同步完成！批次 ID: {BATCH_ID}")