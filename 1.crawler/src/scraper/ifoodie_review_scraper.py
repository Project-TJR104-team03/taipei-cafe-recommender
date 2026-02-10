import sys
import os
import time
import random
import re
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
from google.cloud import storage

# --- 1. 雲端工具函式 ---
def get_gcs_client():
    return storage.Client()

def load_csv_from_gcs(bucket_name, blob_name):
    """從 GCS 讀取 CSV 轉為 DataFrame，若無則回傳 None"""
    try:
        client = get_gcs_client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        if not blob.exists():
            return None
        content = blob.download_as_string()
        return pd.read_csv(io.BytesIO(content))
    except Exception as e:
        print(f"讀取 GCS ({blob_name}) 異常: {e}")
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
        print(f"已儲存至: gs://{bucket_name}/{blob_name}")
    except Exception as e:
        print(f"上傳 GCS 失敗: {e}")

# --- 2. 輔助解析函式 ---
def clean_shop_name(name):
    if pd.isna(name): return ""
    name = str(name).replace('\n', '').replace('\r', '').strip()
    name = re.sub(r'[\(\（\[\{][^)\）\]\}]*[\)\）\]\}]', '', name)
    delimiters = r'[｜\|\-\–\—\:\：\/]'
    name = re.split(delimiters, name)[0]
    return re.sub(r'\s+', ' ', name).strip()

def parse_ifoodie_date(date_text):
    try:
        match = re.search(r'\d{4}/\d{1,2}/\d{1,2}', date_text)
        if match:
            return datetime.strptime(match.group(), "%Y/%m/%d")
        return None
    except: return None

# --- 3. 核心抓取函式 ---
def scrape_ifoodie_reviews(driver, p_name, p_id, batch_id):
    wait = WebDriverWait(driver, 15)
    target_cutoff = datetime.now() - relativedelta(years=3)
    review_results = []
    
    search_query = clean_shop_name(p_name)
    # print(f"   搜尋關鍵字：{search_query}")

    try:
        driver.get("https://ifoodie.tw/")
        
        # 搜尋設定
        try:
            loc_input = wait.until(EC.element_to_be_clickable((By.XPATH, "//input[contains(@placeholder, '搜尋地點')]")))
            loc_input.send_keys(Keys.CONTROL + "a", Keys.BACKSPACE)
            loc_input.send_keys("台北市")
        except:
            pass # 有時候地點會自動定位，不一定需要輸入

        search_input = driver.find_element(By.XPATH, "//input[contains(@placeholder, '美食分類、餐廳')]")
        search_input.send_keys(search_query, Keys.ENTER)

        # 點擊搜尋結果第一筆
        try:
            first_result = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "div.restaurant-item a.title-text")))
            driver.get(first_result.get_attribute('href'))
        except:
            print(f"   找不到店家: {search_query}")
            return []
        
        time.sleep(3) 
        driver.execute_script("window.scrollTo(0, 500);") 

        # 切換到評論分頁
        try:
            review_tab = wait.until(EC.element_to_be_clickable((By.XPATH, "//a[contains(@href, '#reviews') or contains(text(), '所有評論')]")))
            driver.execute_script("arguments[0].click();", review_tab)
            time.sleep(2)
        except: pass

        # 捲動展開 (愛食記是動態載入)
        # 這裡設定捲動 3 次，可根據需求增加
        for _ in range(3):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2) 
            try:
                more_btns = driver.find_elements(By.CLASS_NAME, "btn-more-checkin")
                if more_btns and more_btns[0].is_displayed():
                    driver.execute_script("arguments[0].click();", more_btns[0])
            except: pass

        # 展開全文
        expand_btns = driver.find_elements(By.XPATH, "//button[contains(text(), '顯示更多')]")
        for btn in expand_btns:
            try: driver.execute_script("arguments[0].click();", btn)
            except: continue
        time.sleep(1)

        # 抓取評論並對齊 Schema
        review_elements = driver.find_elements(By.XPATH, "//div[./div/div/a[contains(@class, 'username')]]")
        
        for elem in review_elements:
            try:
                date_text = elem.find_element(By.XPATH, ".//div[contains(@class, 'date')]").text
                date_obj = parse_ifoodie_date(date_text)

                if date_obj and date_obj >= target_cutoff:
                    content = elem.find_element(By.XPATH, ".//div[contains(@class, 'message')]").text
                    content = content.replace("... 顯示更多", "").replace("收起", "").strip()
                    if len(content) < 5: continue 

                    name = elem.find_element(By.XPATH, ".//a[contains(@class, 'username')]").text
                    
                    try:
                        stat_text = elem.find_element(By.XPATH, ".//span[contains(@class, 'stat')]").text
                        reviewer_level = stat_text.split('(')[0].strip() if '(' in stat_text else stat_text
                        reviewer_amount = re.search(r'\(.*?\)', stat_text).group().strip('()') if '(' in stat_text else "0"
                    except:
                        reviewer_level, reviewer_amount = "一般用戶", "0"

                    review_results.append({
                        "place_name": p_name,
                        "place_id": p_id,
                        "review_id": None, # iFoodie 無法取得穩定 review_id
                        "reviewer_name": name,
                        "content": content,
                        "relative_date": None,
                        "full_date": date_obj.strftime('%Y-%m-%d'),
                        "is_edited": False,
                        "reviewer_level": reviewer_level,
                        "reviewer_amount": reviewer_amount,
                        "processed_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        "batch_id": batch_id,
                        "data_source": "ifoodie"
                    })
            except: continue

        return review_results
    except Exception as e:
        print(f"   執行錯誤: {str(e)[:50]}...")
        return []

# --- 4. 模組化入口 (被 main.py 呼叫) ---
def run(region="A-2", total_shards=1, shard_index=0):
    """
    執行愛食記爬蟲任務 (支援分片)
    """
    BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "tjr104-cafe-datalake")
    ENV_LIMIT = os.getenv("SCAN_LIMIT")
    SCAN_LIMIT = int(ENV_LIMIT) if (ENV_LIMIT and ENV_LIMIT.isdigit()) else None

    # 路徑定義
    INPUT_PATH = "raw/store/base.csv" 
    # [修改點 1] 輸出檔名加入分片後綴
    COMMENTS_PART_OUTPUT = f"raw/iFoodie/parts/reviews_ifoodie_{region}_part_{shard_index}.csv"

    print(f" [iFoodie Scraper] 模組啟動 | 分片 {shard_index + 1}/{total_shards} | 區域: {region}")

    # 1. 讀取名單
    full_df = load_csv_from_gcs(BUCKET_NAME, INPUT_PATH)
    if full_df is None or full_df.empty:
        print(" 找不到店家總表 (base.csv)，請先執行 01 爬蟲。")
        sys.exit(1)

    # [修改點 2] 執行分片切分
    stores_df = full_df[full_df.index % total_shards == shard_index].copy()
    print(f" 本分片分配到 {len(stores_df)} 筆任務 (總數 {len(full_df)})")

    if SCAN_LIMIT: 
        stores_df = stores_df.head(SCAN_LIMIT)
        print(f" 測試模式: 僅執行前 {SCAN_LIMIT} 筆")

    # 2. 初始化 Selenium
    options = Options()
    options.add_argument("--headless") 
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    MY_BATCH_ID = f"BATCH_IFO_{datetime.now().strftime('%m%d_%H%M')}"
    
    batch_size = 5
    temp_reviews = []

    try:
        # 使用 enumerate 重新計數
        for i, (orig_idx, row) in enumerate(stores_df.iterrows(), 1):
            
            # --- 資源管控 ---
            if (i - 1) % batch_size == 0 and i > 1:
                # print(f"    釋放記憶體，重啟瀏覽器...")
                driver.quit()
                driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

            print(f"[{i}/{len(stores_df)}] {row['name']}")
            
            # 執行爬取
            reviews = scrape_ifoodie_reviews(driver, row['name'], row['place_id'], MY_BATCH_ID)
            
            if reviews:
                temp_reviews.extend(reviews)
                print(f"    抓取到 {len(reviews)} 筆評論")
            else:
                pass
                # print(f"    無資料或未找到店家")
            
            time.sleep(random.uniform(2, 4))

            # --- 中途存檔 ---
            if i % batch_size == 0 and temp_reviews:
                print(f" 中途寫入分片檔...")
                # 讀取現有分片檔 (Append Mode)
                current_df = load_csv_from_gcs(BUCKET_NAME, COMMENTS_PART_OUTPUT)
                new_df = pd.DataFrame(temp_reviews)
                
                if current_df is not None:
                    final_df = pd.concat([current_df, new_df], ignore_index=True)
                else:
                    final_df = new_df
                
                # 去重
                final_df = final_df.drop_duplicates(subset=['place_id', 'reviewer_name', 'content'], keep='last')
                
                upload_df_to_gcs(final_df, BUCKET_NAME, COMMENTS_PART_OUTPUT)
                temp_reviews = [] 

    finally:
        if 'driver' in locals():
            driver.quit()
        
        # --- 最終存檔 ---
        if temp_reviews:
            print(f" 執行最終存檔...")
            current_df = load_csv_from_gcs(BUCKET_NAME, COMMENTS_PART_OUTPUT)
            new_df = pd.DataFrame(temp_reviews)
            final_df = pd.concat([current_df, new_df], ignore_index=True) if current_df is not None else new_df
            final_df = final_df.drop_duplicates(subset=['place_id', 'reviewer_name', 'content'], keep='last')
            upload_df_to_gcs(final_df, BUCKET_NAME, COMMENTS_PART_OUTPUT)

    print(" iFoodie 分片任務完成！")