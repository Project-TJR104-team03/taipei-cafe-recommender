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
    # 環境變數設定
    BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "tjr104-cafe-datalake")
    REGION = os.getenv("SCAN_REGION", "A-2")
    
    # 為了能找到剛剛上一支程式產出的檔案，這裡需要對應上一支程式的輸出路徑
    # 假設上一支程式輸出檔名格式為：raw/store/{REGION}_base.csv (我們會去抓最新的一份，或是固定名稱)
    # *注意*：為了簡化流程，這裡假設我們讀取該區域最新的 Base 檔，或者你有固定命名的檔案
    # 這裡示範讀取一個固定路徑，實際應用可搭配 Airflow 傳入具體檔名
    TARGET_BASE_PATH = f"raw/store/{REGION}_latest_base.csv" 
    
    ENV_LIMIT = os.getenv("SCAN_LIMIT")
    SCAN_LIMIT = int(ENV_LIMIT) if (ENV_LIMIT and ENV_LIMIT.isdigit()) else None

    print(f"🚀 [Tag Scraper] 啟動 - 區域: {REGION}, Bucket: {BUCKET_NAME}")

    # 1. 從 GCS 下載名單
    full_df = load_csv_from_gcs(BUCKET_NAME, TARGET_BASE_PATH)
    
    if full_df is None or full_df.empty:
        print("❌ 無法讀取資料或資料為空，程式終止。")
        sys.exit(1)

    df_to_process = full_df.head(SCAN_LIMIT) if SCAN_LIMIT else full_df
    print(f"📋 預計處理 {len(df_to_process)} 筆店家資料...")

    # 2. 設定 Selenium (Cloud Run 專用配置)
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless") # 必選
    chrome_options.add_argument("--no-sandbox") # Docker 內必選
    chrome_options.add_argument("--disable-dev-shm-usage") # 記憶體優化
    chrome_options.add_argument("--window-size=900,1000") # 你的特定優化
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    
    # 暫存容器
    payment_patch = {}
    all_tag_records = []

    try:
        for index, row in df_to_process.iterrows():
            place_id = row.get('place_id')
            name = row.get('name')
            address = row.get('formatted_address', '')
            
            query = f"{name} {str(address)[:10]}"
            print(f"🔍 [{index+1}/{len(df_to_process)}] 搜尋: {name}")

            try:
                driver.get("https://www.google.com/maps")
                time.sleep(1.5)

                search_box = driver.find_element(By.NAME, "q")
                search_box.clear()
                search_box.send_keys(query + Keys.ENTER)
                time.sleep(3) # 稍微縮短等待時間，視 Cloud Run 網路狀況調整

                # 列表點擊補救機制
                list_items = driver.find_elements(By.CLASS_NAME, "hfpxzc")
                if list_items:
                    list_items[0].click()
                    time.sleep(3)

                # 點擊「關於」
                try:
                    about_btn = driver.find_element(By.XPATH, "//button[contains(@aria-label, '關於') or contains(@aria-label, '簡介')]")
                    about_btn.click()
                    time.sleep(2)
                except:
                    # 如果找不到關於，可能直接就在該頁面或該店沒有關於頁
                    pass

                # 解析 HTML
                soup = BeautifulSoup(driver.page_source, "html.parser")
                raw_content = ""
                info_blocks = soup.select('div[role="region"].m6QErb div.iP2t7d')
                for b in info_blocks:
                    raw_content += b.text + "\n"

                # 清洗資料
                beautiful_text, payment_options = clean_google_tags_final(raw_content)

                # A. 收集支付方式 (稍後回填)
                if payment_options:
                    payment_patch[place_id] = payment_options
                    print(f"    💰 支付方式: {payment_options}")

                # B. 收集 Tags (稍後存成新檔)
                if beautiful_text:
                    for section in beautiful_text.split(" || "):
                        all_tag_records.append({
                            'name': name,
                            'place_id': place_id,
                            'Tag': section,
                            'Tag_id': "PENDING",
                            'data_source': 'google_about_tab',
                            'crawled_at': time.strftime('%Y-%m-%d %H:%M:%S')
                        })

            except Exception as e:
                print(f"    ⚠️ {name} 處理失敗: {e}")
            
            time.sleep(random.uniform(1, 2))

    finally:
        driver.quit()

    # --- 4. 資料回寫與儲存 ---
    timestamp = time.strftime('%Y%m%d_%H%M')

    # A. 儲存 Tag 表 (Raw Data)
    if all_tag_records:
        tag_df = pd.DataFrame(all_tag_records)
        tag_blob_path = f"raw/tag/{REGION}_{timestamp}_tags.csv"
        upload_df_to_gcs(tag_df, BUCKET_NAME, tag_blob_path)
    else:
        print("⚠️ 本次未擷取到任何 Tag 資料。")

    # B. 更新原始 Base 表 (回填支付方式)
    if payment_patch:
        print("\n🔄 正在更新 Base Table 的支付方式...")
        # 使用 map 更新，並保留原值 (若無新資料)
        full_df['payment_options'] = full_df['place_id'].map(payment_patch).fillna(full_df.get('payment_options', ''))
        
        # 覆蓋回寫 GCS (或另存新檔，視策略而定，這裡示範更新原檔)
        # 建議：實務上 Data Lake 盡量只增不改，所以這裡我存成一個 _enriched 版本
        enriched_path = f"raw/store/{REGION}_{timestamp}_enriched.csv"
        upload_df_to_gcs(full_df, BUCKET_NAME, enriched_path)
        print(f"✨ 流程結束！已產出 Enriched Table: {enriched_path}")
    else:
        print("⚠️ 未發現新的支付資訊，跳過 Base Table 更新。")