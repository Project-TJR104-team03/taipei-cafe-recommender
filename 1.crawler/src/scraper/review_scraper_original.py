import sys
import os
import time
import random
import pandas as pd
import io
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
from selenium.common.exceptions import TimeoutException, WebDriverException

# --- 1. 雲端工具函式 (新增) ---
def get_gcs_client():
    return storage.Client()

def load_csv_from_gcs(bucket_name, blob_name):
    """從 GCS 讀取 CSV 轉為 DataFrame，若無則回傳 None"""
    try:
        client = get_gcs_client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        if not blob.exists():
            print(f" GCS 檔案不存在: gs://{bucket_name}/{blob_name}")
            return None
        content = blob.download_as_string()
        return pd.read_csv(io.BytesIO(content))
    except Exception as e:
        print(f" 讀取 GCS 失敗: {e}")
        return None

def upload_df_to_gcs(df, bucket_name, blob_name):
    """將 DataFrame 上傳回 GCS"""
    try:
        client = get_gcs_client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
        blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
        print(f" 已儲存至: gs://{bucket_name}/{blob_name}")
    except Exception as e:
        print(f" 上傳 GCS 失敗: {e}")

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

# --- 3. 核心抓取邏輯 (Web Scraper) ---
def scrape_reviews_production(driver, p_name, p_addr, p_id, batch_id, last_seen_id=None):
    wait = WebDriverWait(driver, 25)
    target_cutoff = datetime.now() - relativedelta(years=3) # 只抓最近 3 年
    review_results = []
    tag_records = []
    new_top_id = None

    try:
        query = f"{p_name} {str(p_addr)[:10]}"
        driver.get("https://www.google.com/maps")
        time.sleep(1.5)

        # 搜尋輸入
        search_box = wait.until(EC.element_to_be_clickable((By.NAME, "q")))
        search_box.clear()
        search_box.send_keys(query + Keys.ENTER)
        time.sleep(5)

        # 列表點擊補救 (防止直接進入搜尋結果列表而非商家詳情)
        list_items = driver.find_elements(By.CLASS_NAME, "hfpxzc")
        if list_items:
            driver.execute_script("arguments[0].click();", list_items[0])
            time.sleep(4)

        # 點擊「評論」頁籤
        try:
            review_tab = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(., '評論')]")))
            review_tab.click()
            time.sleep(3)
        except:
            print(f"  {p_name} 找不到評論按鈕，可能無評論。")
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
            if tag_records:
                print(f"    找到 {len(tag_records)} 個評論標籤")
        except: 
            pass

        # B. 排序：切換至「最新」
        try:
            sort_btn = wait.until(EC.presence_of_element_located((By.XPATH, "//button[.//span[text()='排序']]")))
            driver.execute_script("arguments[0].click();", sort_btn)
            time.sleep(1.5)
            latest_opt = wait.until(EC.presence_of_element_located((By.XPATH, "//div[contains(text(), '最新')]")))
            driver.execute_script("arguments[0].click();", latest_opt)
            time.sleep(3)
        except:
            print("  無法切換至最新排序，使用預設排序。")

        # C. 智慧滾動 (Smart Scroll)
        try:
            scrollable_div = driver.find_element(By.XPATH, "//div[contains(@class, 'm6QErb') and contains(@class, 'DxyBCb')]")
        except:
            print("  找不到滾動區塊，可能評論數極少。")
            return [], tag_records, None

        last_height = driver.execute_script("return arguments[0].scrollHeight", scrollable_div)
        retry_count = 0
        
        print(f"    開始滾動抓取評論...")

        while True:
            driver.execute_script('arguments[0].scrollTop = arguments[0].scrollHeight', scrollable_div)
            time.sleep(random.uniform(2.5, 3.5))
            
            soup = BeautifulSoup(driver.page_source, "html.parser")
            blocks = soup.select('div.jftiEf')
            if not blocks: continue
            
            # 記錄最新的一則 ID (用於更新 checkpoint)
            if not new_top_id: 
                new_top_id = blocks[0].get('data-review-id')

            # 檢查停止條件 1: 時間超過 3 年
            last_date_text = blocks[-1].select_one('span.rsqaWe').text if blocks[-1].select_one('span.rsqaWe') else ""
            last_date_obj = parse_google_date(last_date_text)
            if last_date_obj and last_date_obj < target_cutoff:
                break
            
            # 檢查停止條件 2: 遇到上次抓過的 ID (增量更新關鍵)
            if last_seen_id and any(b.get('data-review-id') == last_seen_id for b in blocks):
                print(f"     銜接至上次同步點 (增量更新)。")
                break

            # 檢查停止條件 3: 滾不動了
            new_height = driver.execute_script("return arguments[0].scrollHeight", scrollable_div)
            if new_height == last_height:
                retry_count += 1
                if retry_count >= 3: break # 嘗試 3 次都沒變就停
            else: 
                retry_count = 0
                last_height = new_height

        # D. 展開全文 & 解析內容
        expand_buttons = driver.find_elements(By.XPATH, "//button[contains(@aria-label, '顯示更多') or text()='更多']")
        for btn in expand_buttons:
            try: driver.execute_script("arguments[0].click();", btn)
            except: continue

        final_soup = BeautifulSoup(driver.page_source, "html.parser")
        for block in final_soup.select('div.jftiEf'):
            rid = block.get('data-review-id')
            if last_seen_id and rid == last_seen_id: break # 再次確認不重複抓
            
            content_text = block.select_one('span.wiI7pd').text.strip() if block.select_one('span.wiI7pd') else ""
            if not content_text: continue # 略過無文字的純評分
            
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

        return review_results, tag_records, new_top_id 
    except Exception as e:
        print(f"     抓取異常: {e}")
        return [], [], None
    
    
# --- 4. 模組入口 ---
def run():
    # 環境變數設定
    BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "tjr104-cafe-datalake")
    REGION = os.getenv("SCAN_REGION", "A-2")
    ENV_LIMIT = os.getenv("SCAN_LIMIT")
    SCAN_LIMIT = int(ENV_LIMIT) if (ENV_LIMIT and ENV_LIMIT.isdigit()) else None
    
    MY_BATCH_ID = f"BATCH_{datetime.now().strftime('%m%d_%H%M')}"
    
    # GCS 路徑定義
    INPUT_PATH = f"raw/store/base.csv" # 讀取店家總表
    REVIEW_OUTPUT = f"raw/comments/reviews_all.csv" # 評論大表
    TAG_OUTPUT = f"raw/tag/tags_reviews_all.csv" # 評論標籤表
    CHECKPOINT_FILE = f"raw/checkpoint/checkpoint_all.csv"

    print(f" [Review Scraper] 啟動 | 區域: {REGION} | 限制: {SCAN_LIMIT}")

    # 1. 讀取店家名單
    full_df = load_csv_from_gcs(BUCKET_NAME, INPUT_PATH)
    if full_df is None or full_df.empty:
        print(" 找不到店家總表 (base.csv)")
        sys.exit(1)

    # 簡單過濾 (之後可加入更複雜的區域篩選邏輯)
    # 這裡假設 base.csv 有 location 或其他欄位可篩選，目前先全跑或用 LIMIT
    stores_df = full_df 
    if SCAN_LIMIT: stores_df = stores_df.head(SCAN_LIMIT)

    # 2. 讀取 Checkpoint
    checkpoint_df = load_csv_from_gcs(BUCKET_NAME, CHECKPOINT_FILE)
    if checkpoint_df is None:
        checkpoint_df = pd.DataFrame(columns=['place_id', 'latest_review_id', 'last_sync_at'])

    # 3. 初始化 Selenium 選項
    chrome_options = Options()
    chrome_options.add_argument("--headless") # 雲端強制無頭
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage") # 關鍵: 防止記憶體崩潰
    chrome_options.add_argument("--window-size=900,1000")
    chrome_options.add_argument("--lang=zh-TW")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    
    batch_size = 3 # 批次重啟頻率
    temp_reviews = []
    temp_tags = []

    try:
        for i, (idx, row) in enumerate(stores_df.iterrows(), 1):
            
            # --- 資源管控：定期重啟瀏覽器 ---
            if (i - 1) % batch_size == 0 and i > 1:
                print(f" 釋放記憶體，重啟瀏覽器 (第 {i} 筆)...")
                driver.quit()
                driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

            print(f"[{i}/{len(stores_df)}] {row['name']}")
            
            # 取得上次的 review_id (增量更新用)
            last_id = None
            if not checkpoint_df.empty and row['place_id'] in checkpoint_df['place_id'].values:
                last_id = checkpoint_df.loc[checkpoint_df['place_id'] == row['place_id'], 'latest_review_id'].values[0]

            # 執行抓取
            reviews, tags, new_top_id = scrape_reviews_production(driver, row['name'], row.get('formatted_address', ''), row['place_id'], MY_BATCH_ID, last_id)
            
            if reviews: temp_reviews.extend(reviews)
            if tags: temp_tags.extend(tags)

            # 更新 Checkpoint (記憶體中)
            if new_top_id:
                new_cp = pd.DataFrame([{
                    'place_id': row['place_id'], 
                    'latest_review_id': new_top_id, 
                    'last_sync_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }])
                # 移除舊紀錄，加入新紀錄
                checkpoint_df = checkpoint_df[checkpoint_df['place_id'] != row['place_id']]
                checkpoint_df = pd.concat([checkpoint_df, new_cp], ignore_index=True)

            time.sleep(random.uniform(2, 4))

            # --- 中途存檔 (Checkpointing) ---
            # 每 3 筆存一次，避免全部跑完才存風險太高
            if i % batch_size == 0:
                print(f" 執行中途存檔...")
                
                # A. 存評論
                if temp_reviews:
                    current_reviews_df = load_csv_from_gcs(BUCKET_NAME, REVIEW_OUTPUT)
                    new_reviews_df = pd.DataFrame(temp_reviews)
                    if current_reviews_df is not None:
                        final_reviews = pd.concat([current_reviews_df, new_reviews_df], ignore_index=True)
                    else:
                        final_reviews = new_reviews_df
                    # 去重
                    final_reviews = final_reviews.drop_duplicates(subset=['place_id', 'review_id'])
                    upload_df_to_gcs(final_reviews, BUCKET_NAME, REVIEW_OUTPUT)
                    temp_reviews = [] # 清空暫存

                # B. 存標籤
                if temp_tags:
                    current_tags_df = load_csv_from_gcs(BUCKET_NAME, TAG_OUTPUT)
                    new_tags_df = pd.DataFrame(temp_tags)
                    if current_tags_df is not None:
                        final_tags = pd.concat([current_tags_df, new_tags_df], ignore_index=True)
                    else:
                        final_tags = new_tags_df
                    final_tags = final_tags.drop_duplicates(subset=['place_id', 'Tag'])
                    upload_df_to_gcs(final_tags, BUCKET_NAME, TAG_OUTPUT)
                    temp_tags = [] # 清空暫存

                # C. 存 Checkpoint
                upload_df_to_gcs(checkpoint_df, BUCKET_NAME, CHECKPOINT_FILE)

    finally:
        driver.quit()
        print(" 任務結束，瀏覽器已關閉。")
        
        # 最後一次存檔 (防止最後幾筆沒存到)
        if temp_reviews or temp_tags:
            print(f" 執行最終存檔...")
            if temp_reviews:
                current_reviews_df = load_csv_from_gcs(BUCKET_NAME, REVIEW_OUTPUT)
                new_reviews_df = pd.DataFrame(temp_reviews)
                final_reviews = pd.concat([current_reviews_df, new_reviews_df], ignore_index=True) if current_reviews_df is not None else new_reviews_df
                upload_df_to_gcs(final_reviews.drop_duplicates(subset=['place_id', 'review_id']), BUCKET_NAME, REVIEW_OUTPUT)
            
            if temp_tags:
                current_tags_df = load_csv_from_gcs(BUCKET_NAME, TAG_OUTPUT)
                new_tags_df = pd.DataFrame(temp_tags)
                final_tags = pd.concat([current_tags_df, new_tags_df], ignore_index=True) if current_tags_df is not None else new_tags_df
                upload_df_to_gcs(final_tags.drop_duplicates(subset=['place_id', 'Tag']), BUCKET_NAME, TAG_OUTPUT)

            upload_df_to_gcs(checkpoint_df, BUCKET_NAME, CHECKPOINT_FILE)

    print(" 所有作業完成！")