import os
import time
import random
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# --- 1. 初始化環境變數 ---
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
load_dotenv(dotenv_path=os.path.join(root_path, ".env"))

# --- 2. 結構化清洗函數 (移除符號、過濾斜線、抓支付方式) ---
def clean_google_tags_final(raw_content):
    if not raw_content: return "", ""

    lines = [l.strip() for l in raw_content.split('\n') if l.strip()]
    unique_lines = []
    [unique_lines.append(x) for x in lines if x not in unique_lines]

    formatted_sections = []
    payment_methods = []
    
    for section in unique_lines:
        # 過濾：有斜線 () 或 [無] 代表沒有提供，直接跳過不抓
        if "" in section or "[無]" in section:
            continue

        if '' in section:
            parts = section.split('')
            category = parts[0].strip()
            # 移除✔：只抓取文字項目
            items_list = [p.strip() for p in parts[1:] if p.strip()]
            
            # 格式：類別：項目1 | 項目2
            items_str = " | ".join(items_list)
            formatted_sections.append(f"{category}：{items_str}")
            
            # 提取支付方式供後續回填
            if "付款" in category:
                payment_methods.extend(items_list)

    full_tags_text = " || ".join(formatted_sections)
    # 支付方式合併為逗號字串
    payment_options_str = ",".join(payment_methods) if payment_methods else ""
    
    return full_tags_text, payment_options_str

# --- 3. 設定區 ---
REGION = os.getenv("SCAN_REGION", "A-2")
STATIC_TABLE = f"data/raw/Store/{REGION}_base.csv"
TAG_COLUMN_FILE = f"data/raw/Tag_column/{REGION}_tags.csv"

HEADLESS = os.getenv("HEADLESS", "false").lower() == "true"
ENV_LIMIT = os.getenv("SCAN_LIMIT")
SCAN_LIMIT = int(ENV_LIMIT) if (ENV_LIMIT and ENV_LIMIT.isdigit()) else None

# --- 4. 執行邏輯 ---
if not os.path.exists(STATIC_TABLE):
    print(f" 找不到靜態 Table 檔案: {STATIC_TABLE}")
else:
    full_df = pd.read_csv(STATIC_TABLE)
    payment_patch = {}
    df_to_process = full_df.head(SCAN_LIMIT) if SCAN_LIMIT else full_df

    options = webdriver.ChromeOptions()
    if HEADLESS:
        options.add_argument("--headless")
    
    # 🌟 關鍵修正：強制設定為窄長視窗，避免 Google 跳出地圖側邊欄
    options.add_argument("--window-size=900,1000")
    options.add_argument("--disable-blink-features=AutomationControlled")
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    try:
        for index, row in df_to_process.iterrows():
            query = f"{row['name']} {str(row['formatted_address'])[:10]}"
            print(f" 處理中 [{index+1}/{len(df_to_process)}]: {row['name']}")

            driver.get("https://www.google.com/maps")
            time.sleep(1.5)

            try:
                search_box = driver.find_element(By.NAME, "q")
                search_box.clear()
                search_box.send_keys(query + Keys.ENTER)
                time.sleep(5)

                # 列表點擊補救機制
                list_items = driver.find_elements(By.CLASS_NAME, "hfpxzc")
                if list_items:
                    list_items[0].click()
                    time.sleep(4)

                # 點擊「關於」分頁
                try:
                    about_btn = driver.find_element(By.XPATH, "//button[contains(@aria-label, '關於') or contains(@aria-label, '簡介')]")
                    about_btn.click()
                    time.sleep(2)
                except:
                    pass

                soup = BeautifulSoup(driver.page_source, "html.parser")
                raw_content = ""
                # 採用最穩定的簡介區塊選擇器
                info_blocks = soup.select('div[role="region"].m6QErb div.iP2t7d')
                for b in info_blocks:
                    raw_content += b.text + "\n"

                # 解析標籤與支付方式
                beautiful_text, payment_options = clean_google_tags_final(raw_content)

                # 暫存結果
                if payment_options:
                    payment_patch[row['place_id']] = payment_options
                    print(f"    找到支付方式: {payment_options}")

                # 存入 Tag_column
                if beautiful_text:
                    tag_records = []
                    for section in beautiful_text.split(" || "):
                        tag_records.append({
                            'name': row['name'],
                            'place_id': row['place_id'],
                            'Tag': section,
                            'Tag_id': "PENDING",
                            'data_source': 'google簡介標籤'
                        })
                    os.makedirs(os.path.dirname(TAG_COLUMN_FILE), exist_ok=True)
                    pd.DataFrame(tag_records).to_csv(TAG_COLUMN_FILE, mode='a', index=False, header=not os.path.exists(TAG_COLUMN_FILE), encoding='utf-8-sig')

            except Exception as e:
                print(f"    {row['name']} 遭遇錯誤，跳過。")

            time.sleep(random.uniform(1, 2))

        # --- 5. 最終回填 ---
        if payment_patch:
            print(f"\n正在將支付方式回填至 {STATIC_TABLE}...")
            full_df['payment_options'] = full_df['place_id'].map(payment_patch).fillna(full_df['payment_options'])
            full_df.to_csv(STATIC_TABLE, index=False, encoding='utf-8-sig')
            print(f"靜態 Table 回填更新成功！")

    finally:
        driver.quit()
        print(f"任務圓滿結束！")