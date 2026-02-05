import sys
import os
import time
import json
import re
import random
import logging
import pandas as pd
import io
import unicodedata
import googlemaps
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from google.cloud import storage
from bs4 import BeautifulSoup 

# --- 0. 雲端工具與設定 ---
PROJECT_NAME = "TJR104_SuperTaste_Cloud"
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(PROJECT_NAME)

def get_gcs_client():
    return storage.Client()

def load_csv_from_gcs(bucket_name, blob_name):
    """從 GCS 讀取 CSV 轉 DataFrame"""
    try:
        client = get_gcs_client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        if not blob.exists():
            return None
        content = blob.download_as_string()
        return pd.read_csv(io.BytesIO(content))
    except Exception as e:
        logger.warning(f" GCS 讀取異常 ({blob_name}): {e}")
        return None

def upload_df_to_gcs(df, bucket_name, blob_name):
    """DataFrame 上傳回 GCS"""
    try:
        client = get_gcs_client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
        blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
        logger.info(f" 資料已更新至: gs://{bucket_name}/{blob_name}")
    except Exception as e:
        logger.error(f" GCS 上傳失敗: {e}")

# --- 1. 食尚玩家爬蟲類別 (維持不變) ---
class SuperTasteCrawler:
    def __init__(self):
        self.driver = self._setup_driver()
        self.wait = WebDriverWait(self.driver, 20)

    def _setup_driver(self):
        options = Options()
        options.add_argument("--start-maximized")
        options.add_argument("--disable-notifications")
        # 雲端設定
        options.add_argument("--headless") 
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        service = Service(ChromeDriverManager().install())
        return webdriver.Chrome(service=service, options=options)
    
    def restart_driver(self):
        logger.info(" 重啟瀏覽器釋放資源...")
        try: self.driver.quit()
        except: pass
        self.driver = self._setup_driver()
        self.wait = WebDriverWait(self.driver, 20)

    @staticmethod
    def clean_seed_name(raw_name):
        if not raw_name: return ""
        name = re.sub(r'^\d+\.\s*', '', raw_name.strip()) 
        name = name.replace('\n', '').replace('\r', '')
        delimiters = r'[｜\|\-\–\—\:\：\/]'
        name = re.split(delimiters, name)[0].strip()
        blacklist = ["總整理", "懶人包", "攻略", "精選", "必吃", "推薦", "名單"]
        if any(bad in name for bad in blacklist): return ""
        if "Top" in name and any(c.isdigit() for c in name): return ""
        return name

    def scroll_down_slowly(self):
        try:
            last_height = self.driver.execute_script("return document.body.scrollHeight")
            curr = 0
            while curr < last_height:
                curr += 800
                self.driver.execute_script(f"window.scrollTo(0, {curr});")
                time.sleep(0.5)
                new_h = self.driver.execute_script("return document.body.scrollHeight")
                if new_h > last_height: last_height = new_h
            time.sleep(2)
        except: pass

    def _extract_cards_from_current_view(self):
        captured = []
        try:
            self.wait.until(EC.presence_of_all_elements_located((By.XPATH, "//div[contains(@class, 'group/card')]")))
            card_xpath = "//a[.//div[contains(@class, 'group/card')]]"
            cards = self.driver.find_elements(By.XPATH, card_xpath)
            valid_pattern = r'Top\s*\d+|\d+\s*[家選間]'
            for card in cards:
                try:
                    url = card.get_attribute('href')
                    title = card.find_element(By.XPATH, ".//h3").text.strip()
                    allowed = ["/article/", "/food/", "/travel/"]
                    if url and title and any(p in url for p in allowed):
                        if re.search(valid_pattern, title, re.IGNORECASE) and "總整理" not in title:
                            captured.append({"title": title, "url": url})
                except: continue
            return captured
        except: return []

    def step_1_harvest_article_links(self, keyword, max_pages=3):
        logger.info(f" [Step 1] 搜尋: {keyword}")
        self.driver.get("https://supertaste.tvbs.com.tw/")
        try:
            try:
                agree = self.wait.until(EC.element_to_be_clickable((By.CLASS_NAME, "policy__agree")))
                self.driver.execute_script("arguments[0].click();", agree)
                time.sleep(1)
            except: pass
            
            icon = self.wait.until(EC.presence_of_element_located((By.ID, "search_m")))
            self.driver.execute_script("arguments[0].click();", icon)
            
            inp = self.wait.until(EC.visibility_of_element_located((By.CLASS_NAME, "lightbox__search-input")))
            inp.clear()
            inp.send_keys(keyword)
            time.sleep(0.5)
            
            btn = self.driver.find_element(By.CLASS_NAME, "lightbox__search-btn")
            self.driver.execute_script("arguments[0].click();", btn)
            time.sleep(3)

            try:
                more = self.wait.until(EC.element_to_be_clickable((By.XPATH, "//a[contains(text(), 'More') and contains(@class, 'bg-black')]")))
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", more)
                time.sleep(1)
                self.driver.execute_script("arguments[0].click();", more)
                time.sleep(3)
            except: pass

            all_res = {}
            page = 1
            while page <= max_pages:
                logger.info(f"📄 抓取第 {page} 頁...")
                self.scroll_down_slowly()
                items = self._extract_cards_from_current_view()
                new_cnt = 0
                for i in items: 
                    if i['url'] not in all_res:
                        all_res[i['url']] = i['title']
                        new_cnt += 1
                logger.info(f"   本頁新增 {new_cnt} 篇，總計 {len(all_res)}")
                try:
                    nxt = self.driver.find_element(By.XPATH, f"//a[contains(@href, 'page={page+1}')]")
                    self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", nxt)
                    time.sleep(1)
                    self.driver.execute_script("arguments[0].click();", nxt)
                    page += 1
                    time.sleep(3)
                except: break
            return [{"url": k, "title": v} for k, v in all_res.items()]
        except Exception as e:
            logger.error(f"❌ Step 1 Error: {e}")
            return []

    def extract_content_with_bs4(self, html):
        soup = BeautifulSoup(html, 'html.parser')
        container = None
        for cand in [{"id": "article_content"}, {"class": "article_detail"}, {"itemprop": "articleBody"}]:
            if "id" in cand: container = soup.find(id=cand["id"])
            elif "class" in cand: container = soup.find("div", class_=cand["class"])
            elif "itemprop" in cand: container = soup.find("div", attrs={"itemprop": cand["itemprop"]})
            if container: break
        
        if not container: return []
        data = []
        headers = container.find_all(['h2', 'h3'])
        for h in headers:
            raw = h.get_text(strip=True)
            clean = self.clean_seed_name(raw)
            if len(clean) <= 1: continue
            desc = []
            for sib in h.next_siblings:
                if sib.name in ['h2', 'h3']: break
                if sib.name == 'p':
                    txt = sib.get_text(strip=True)
                    if txt and "Advertisement" not in txt and len(txt) > 5: desc.append(txt)
            full_desc = "\n".join(desc)
            if full_desc: data.append({"raw_title": raw, "cleaned_name": clean, "description": full_desc})
        return data

    def step_2_extract_cafes(self, articles):
        logger.info(" [Step 2] 提取內容...")
        results = []
        target = articles 
        for idx, art in enumerate(target):
            if idx > 0 and idx % 5 == 0: self.restart_driver()
            try:
                self.driver.get(art['url'])
                WebDriverWait(self.driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                time.sleep(random.uniform(2, 4))
                try: art_title = self.driver.find_element(By.TAG_NAME, "h1").text.strip()
                except: art_title = art['title']
                logger.info(f"📖 [{idx+1}] {art_title[:15]}...")
                shops = self.extract_content_with_bs4(self.driver.page_source)
                for s in shops:
                    results.append({
                        "place_name": s['cleaned_name'], 
                        "raw_title": s['raw_title'],
                        "description": s['description'],
                        "article_title": art_title,
                        "source_url": art['url'],
                        "processed_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    })
                logger.info(f"   --> 抓到 {len(shops)} 筆")
            except: continue
        return results

    def close(self):
        if self.driver: self.driver.quit()

# --- 2. 核心：資料治理 (Matching, Enrichment & Split) ---

def normalize_text(text):
    if pd.isna(text): return ""
    text = str(text)
    normalized = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8')
    return normalized.lower().replace(" ", "")

def is_valid_cafe_type(types_list):
    """類別過濾器"""
    if not types_list: return False
    allow = ['cafe', 'bakery', 'food', 'restaurant', 'meal_takeaway', 'store']
    block = ['department_store', 'shopping_mall', 'bar', 'night_club', 'lodging', 'gym']
    
    has_allow = any(k in types_list for k in allow)
    has_block = any(k in types_list for k in block)
    
    if 'cafe' in types_list or 'bakery' in types_list: return True
    if has_block and not has_allow: return False
    return True

def fetch_and_format_new_store(place_name):
    """
    呼叫 API 並將回傳資料格式化為 (static_dict, dynamic_dict)
    """
    api_key = os.getenv("GOOGLE_MAPS_API_KEY")
    if not api_key: return None, None
    
    try:
        gmaps = googlemaps.Client(key=api_key)
        # 為了符合 01.py 的 Schema，我們需要 opening_hours, website 等，所以用 find_place 找 ID 後，最好能拿到足夠資訊
        # 為了節省請求次數，我們這裡使用 find_place 的 fields，雖然它沒有 price_level，但夠用了
        # 如果需要更完整，可以用 place_details (但較貴)
        
        fields_req = ["place_id", "name", "formatted_address", "geometry", "types", "rating", "user_ratings_total", "opening_hours", "business_status"]
        
        find_res = gmaps.find_place(
            input=f"{place_name} 台北", 
            input_type="textquery", 
            fields=fields_req
        )
        
        if not find_res['status'] == 'OK' or not find_res['candidates']:
            return None, None
            
        cand = find_res['candidates'][0]
        types = cand.get('types', [])
        
        # 1. 類別過濾
        if not is_valid_cafe_type(types):
            logger.warning(f"      攔截非咖啡廳: {cand['name']} ({types})")
            return None, None

        # 2. 資料清洗 (對齊 01.py 的格式)
        loc = cand.get('geometry', {}).get('location', {})
        loc_str = f"POINT({loc.get('lng')} {loc.get('lat')})" if loc else None
        
        weekday_text = cand.get('opening_hours', {}).get('weekday_text', [])
        f_opening = " | ".join(weekday_text) if weekday_text else None
        f_types = ",".join(types)
        
        # 3. 建構 Static Data (base.csv)
        static_data = {
            'name': cand['name'],
            'place_id': cand['place_id'],
            'formatted_phone_number': None, # find_place 可能拿不到，可留空
            'formatted_address': cand.get('formatted_address'),
            'website': None, 
            'location': loc_str,
            'opening_hours': f_opening,
            'price_level': None,
            'business_status': cand.get('business_status'),
            'types': f_types,
            'payment_options': "" 
        }
        
        # 4. 建構 Dynamic Data (store_dynamic.csv)
        dynamic_data = {
            'place_id': cand['place_id'],
            'name': cand['name'],
            'rating': cand.get('rating'),
            'user_ratings_total': cand.get('user_ratings_total'),
            'data_source': 'Supertaste_API_Fill', # 標記來源
            'processed_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        return static_data, dynamic_data

    except Exception as e:
        logger.error(f" API Error: {e}")
        return None, None

def process_and_merge_data(scraped_data, bucket_name):
    """
    整合邏輯：比對 -> API 補完 -> 拆分 -> 上傳
    """
    # 1. 讀取現有資料表
    base_path = "raw/store/base.csv"
    dyn_path = "raw/store_dynamic/store_dynamic.csv"
    
    df_base = load_csv_from_gcs(bucket_name, base_path)
    df_dyn = load_csv_from_gcs(bucket_name, dyn_path)
    
    # 初始化 (若 GCS 沒檔案)
    if df_base is None: df_base = pd.DataFrame(columns=['name', 'place_id'])
    if df_dyn is None: df_dyn = pd.DataFrame(columns=['place_id', 'rating'])
    
    # 建立快取
    df_base['norm_name'] = df_base['name'].apply(normalize_text)
    name_to_id = dict(zip(df_base['norm_name'], df_base['place_id']))
    existing_ids = set(df_base['place_id'].dropna().unique())
    
    new_static_rows = []
    new_dynamic_rows = []
    final_supertaste_reviews = []
    
    logger.info(f" 開始比對 (Base: {len(df_base)} 筆)...")

    for item in scraped_data:
        target_name = item['place_name']
        norm_target = normalize_text(target_name)
        p_id = None
        
        # A. 本地比對
        if norm_target in name_to_id:
            p_id = name_to_id[norm_target]
        
        # B. API 補完
        if not p_id:
            logger.info(f"    本地無 ({target_name}) -> 呼叫 API...")
            static_d, dynamic_d = fetch_and_format_new_store(target_name)
            
            if static_d:
                found_id = static_d['place_id']
                
                # Double Check: ID 是否已存在
                if found_id in existing_ids:
                    p_id = found_id
                    logger.info(f"     ↳  ID ({found_id}) 已存在，僅關聯。")
                else:
                    # 真正的 New Store!
                    p_id = found_id
                    logger.info(f"     ↳  發現新店家！加入佇列: {static_d['name']}")
                    
                    new_static_rows.append(static_d)
                    new_dynamic_rows.append(dynamic_d)
                    existing_ids.add(found_id) # 更新 Cache
                    
                    # 稍微 sleep 避免 API Rate Limit
                    time.sleep(0.5)

        item['place_id'] = p_id
        final_supertaste_reviews.append(item)
    
    # 2. 合併與上傳
    
    # A. 更新 Base Table
    if new_static_rows:
        df_new_static = pd.DataFrame(new_static_rows)
        # 移除 norm_name 以保持 schema 乾淨
        if 'norm_name' in df_base.columns: del df_base['norm_name']
        
        df_base = pd.concat([df_base, df_new_static], ignore_index=True)
        # 去重 (以防萬一)
        df_base = df_base.drop_duplicates(subset=['place_id'], keep='last')
        
        upload_df_to_gcs(df_base, bucket_name, base_path)
        logger.info(f" Base Table 已更新 (新增 {len(new_static_rows)} 筆)")
    else:
        logger.info(" Base Table 無需更新")

    # B. 更新 Dynamic Table
    if new_dynamic_rows:
        df_new_dyn = pd.DataFrame(new_dynamic_rows)
        df_dyn = pd.concat([df_dyn, df_new_dyn], ignore_index=True)
        upload_df_to_gcs(df_dyn, bucket_name, dyn_path)
        logger.info(f" Dynamic Table 已更新 (新增 {len(new_dynamic_rows)} 筆)")

    # C. 儲存食尚玩家評論
    df_review = pd.DataFrame(final_supertaste_reviews)
    # 整理欄位
    cols = ['place_id', 'place_name', 'description', 'article_title', 'source_url', 'processed_at', 'raw_title']
    for c in cols: 
        if c not in df_review.columns: df_review[c] = ""
    df_review = df_review[cols]
    
    review_path = f"raw/supertaste/reviews_supertaste_{datetime.now().strftime('%Y%m%d')}.csv"
    upload_df_to_gcs(df_review, bucket_name, review_path)
    
    logger.info(f" 食尚玩家評論表已儲存: {review_path}")
    logger.info(f"   比對成功率: {df_review['place_id'].notnull().sum()} / {len(df_review)}")

# --- 3. 模組化入口 (被 main.py 呼叫) ---
def run():
    """
    執行食尚玩家爬蟲與資料補完任務
    """
    BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "tjr104-cafe-datalake")
    ENV_LIMIT = os.getenv("SCAN_LIMIT")
    
    # 預設爬 3 頁，除非環境變數有指定
    SCAN_LIMIT_PAGES = int(ENV_LIMIT) if (ENV_LIMIT and ENV_LIMIT.isdigit()) else 3
    
    print(f" [SuperTaste] 模組啟動 | 目標頁數: {SCAN_LIMIT_PAGES}")

    crawler = SuperTasteCrawler()
    try:
        # 1. 爬蟲
        articles = crawler.step_1_harvest_article_links("台北咖啡廳", max_pages=SCAN_LIMIT_PAGES)
        if articles:
            raw_data = crawler.step_2_extract_cafes(articles)
            
            # 2. 資料治理 (比對 -> 補完 -> 拆分 -> 上傳)
            process_and_merge_data(raw_data, BUCKET_NAME)
            
        else:
            logger.warning(" Step 1 未能收集到任何文章連結")
            
    except Exception as e:
        logger.error(f" SuperTaste 執行發生錯誤: {e}", exc_info=True)
    finally:
        crawler.close()
        print(" SuperTaste 任務結束")