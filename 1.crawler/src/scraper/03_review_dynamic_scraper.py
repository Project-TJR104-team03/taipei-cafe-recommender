import os
import time
import random
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
from dotenv import load_dotenv

load_dotenv()

# --- 1. 工具函式 ---
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

def save_with_deduplication(file_path, new_df, key_cols):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    if os.path.exists(file_path):
        old_df = pd.read_csv(file_path)
        combined_df = pd.concat([old_df, new_df], ignore_index=True)
    else:
        combined_df = new_df
    final_df = combined_df.drop_duplicates(subset=key_cols, keep='last')
    final_df.to_csv(file_path, index=False, encoding='utf-8-sig')

# --- 2. 核心抓取函式 ---
def scrape_reviews_production(driver, p_name, p_addr, p_id, batch_id, last_seen_id=None):
    wait = WebDriverWait(driver, 25)
    target_cutoff = datetime.now() - relativedelta(years=3)
    review_results = []
    tag_records = []
    new_top_id = None

    try:
        # 同步的搜尋入口
        query = f"{p_name} {str(p_addr)[:10]}"
        driver.get("https://www.google.com/maps")
        time.sleep(1.5)

        search_box = wait.until(EC.element_to_be_clickable((By.NAME, "q")))
        search_box.clear()
        search_box.send_keys(query + Keys.ENTER)
        time.sleep(5)

        # 列表點擊補救
        list_items = driver.find_elements(By.CLASS_NAME, "hfpxzc")
        if list_items:
            driver.execute_script("arguments[0].click();", list_items[0])
            time.sleep(4)

        # 點擊評論分頁
        review_tab = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(., '評論')]")))
        review_tab.click()
        time.sleep(3)

        # 標籤抓取邏輯 (原本消失的地方)
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
                print(f"     找到 {len(tag_records)} 個評論標籤")
        except: 
            pass

        # 排序：最新
        sort_btn = wait.until(EC.presence_of_element_located((By.XPATH, "//button[.//span[text()='排序']]")))
        driver.execute_script("arguments[0].click();", sort_btn)
        time.sleep(1.5)
        latest_opt = wait.until(EC.presence_of_element_located((By.XPATH, "//div[contains(text(), '最新')]")))
        driver.execute_script("arguments[0].click();", latest_opt)
        time.sleep(3)

        # 智慧滾動邏輯
        scrollable_div = driver.find_element(By.XPATH, "//div[contains(@class, 'm6QErb') and contains(@class, 'DxyBCb')]")
        last_height = driver.execute_script("return arguments[0].scrollHeight", scrollable_div)
        retry_count = 0
        
        print(f"     視窗優化完成，開始智慧滾動...")

        while True:
            driver.execute_script('arguments[0].scrollTop = arguments[0].scrollHeight', scrollable_div)
            time.sleep(random.uniform(2.5, 3.5))
            
            soup = BeautifulSoup(driver.page_source, "html.parser")
            blocks = soup.select('div.jftiEf')
            if not blocks: continue
            if not new_top_id: new_top_id = blocks[0].get('data-review-id')

            last_date_text = blocks[-1].select_one('span.rsqaWe').text if blocks[-1].select_one('span.rsqaWe') else ""
            last_date_obj = parse_google_date(last_date_text)
            
            if last_date_obj and last_date_obj < target_cutoff:
                break
            if last_seen_id and any(b.get('data-review-id') == last_seen_id for b in blocks):
                print(f"     銜接至同步點。")
                break

            new_height = driver.execute_script("return arguments[0].scrollHeight", scrollable_div)
            if new_height == last_height:
                retry_count += 1
                if retry_count >= 2: break
            else: 
                retry_count = 0
                last_height = new_height

        # 展開全文
        expand_buttons = driver.find_elements(By.XPATH, "//button[contains(@aria-label, '顯示更多') or text()='更多']")
        for btn in expand_buttons:
            try: driver.execute_script("arguments[0].click();", btn)
            except: continue

        final_soup = BeautifulSoup(driver.page_source, "html.parser")
        for block in final_soup.select('div.jftiEf'):
            rid = block.get('data-review-id')
            if last_seen_id and rid == last_seen_id: break
            content_text = block.select_one('span.wiI7pd').text.strip() if block.select_one('span.wiI7pd') else ""
            if not content_text: continue
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
        # 回傳抓到的 tag_records
        return review_results, tag_records, new_top_id 
    except Exception as e:
        print(f"     抓取異常: {e}")
        return [], [], None

# --- 3. 執行主流程 ---
if __name__ == "__main__":
    region = os.getenv("SCAN_REGION", "A-2")
    limit = int(os.getenv("SCAN_LIMIT")) if os.getenv("SCAN_LIMIT") else None
    MY_BATCH_ID = f"BATCH_{datetime.now().strftime('%m%d_%H%M')}"
    
    INPUT_PATH = f"data/raw/Store/{region}_base.csv"
    REVIEW_OUTPUT = f"data/raw/Comments/{region}_reviews.csv"
    TAG_OUTPUT = f"data/raw/Tag_column/{region}_tags.csv" # 定義標籤輸出路徑
    CHECKPOINT_FILE = f"data/raw/sync_checkpoint_{region}.csv"

    if os.path.exists(INPUT_PATH):
        stores_df = pd.read_csv(INPUT_PATH)
        if limit: stores_df = stores_df.head(limit)
        
        options = Options()
        options.add_argument("--window-size=900,1000") 
        if os.getenv("HEADLESS", "false").lower() == "true":
            options.add_argument("--headless")
        
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        checkpoint_df = pd.read_csv(CHECKPOINT_FILE) if os.path.exists(CHECKPOINT_FILE) else pd.DataFrame(columns=['place_id', 'latest_review_id'])

        try:
            for idx, row in stores_df.iterrows():
                print(f" [{idx+1}/{len(stores_df)}] {row['name']}")
                last_id = checkpoint_df.loc[checkpoint_df['place_id'] == row['place_id'], 'latest_review_id'].values[0] if row['place_id'] in checkpoint_df['place_id'].values else None
                
                # 執行抓取
                reviews, tags, new_top_id = scrape_reviews_production(driver, row['name'], row['formatted_address'], row['place_id'], MY_BATCH_ID, last_id)

                # 存檔標籤邏輯
                if reviews: save_with_deduplication(REVIEW_OUTPUT, pd.DataFrame(reviews), ['place_id', 'review_id'])
                if tags: save_with_deduplication(TAG_OUTPUT, pd.DataFrame(tags), ['place_id', 'Tag'])

                if new_top_id:
                    new_cp = pd.DataFrame([{'place_id': row['place_id'], 'latest_review_id': new_top_id, 'last_sync_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}])
                    checkpoint_df = pd.concat([checkpoint_df[checkpoint_df['place_id'] != row['place_id']], new_cp], ignore_index=True)
                    checkpoint_df.to_csv(CHECKPOINT_FILE, index=False)
                
                time.sleep(random.uniform(2, 4))
        finally:
            driver.quit()
            print(f" 任務圓滿結束！")