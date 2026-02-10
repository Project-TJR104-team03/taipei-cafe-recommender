import os
import time
import json
import re
import random
import logging
import io
import pandas as pd
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
from bs4 import BeautifulSoup 
from google.cloud import storage

# 設定 Logger
logger = logging.getLogger("SuperTaste")
logger.setLevel(logging.INFO)

# --- 1. GCS I/O 工具函式 ---
def get_gcs_client():
    return storage.Client()

def read_csv_from_gcs(bucket_name, blob_name):
    """ 從 GCS 讀取 CSV 轉為 DataFrame """
    try:
        client = get_gcs_client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        if blob.exists():
            content = blob.download_as_string()
            return pd.read_csv(io.BytesIO(content))
        else:
            return None
    except Exception as e:
        logger.error(f"無法讀取 GCS 檔案 {blob_name}: {e}")
        return None

def save_csv_to_gcs(df, bucket_name, blob_name):
    """ 將 DataFrame 存回 GCS """
    try:
        client = get_gcs_client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
        blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
        logger.info(f"已儲存至 gs://{bucket_name}/{blob_name}")
    except Exception as e:
        logger.error(f"GCS 存檔失敗 {blob_name}: {e}")

# --- 2. 爬蟲類別 (雲端版) ---
class SuperTasteCrawlerCloud:
    def __init__(self):
        self.driver = self._setup_driver()
        self.wait = WebDriverWait(self.driver, 20)

    def _setup_driver(self):
        options = Options()
        options.add_argument("--headless=new") # 雲端必備
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--disable-gpu")
        # 偽裝 User-Agent
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        
        service = Service(ChromeDriverManager().install())
        return webdriver.Chrome(service=service, options=options)
    
    def restart_driver(self):
        try: self.driver.quit()
        except: pass
        self.driver = self._setup_driver()
        self.wait = WebDriverWait(self.driver, 20)

    @staticmethod
    def clean_seed_name(raw_name):
        if not raw_name: return ""
        name = raw_name.strip()
        
        # [強化清洗]：移除 "台北咖啡廳必訪02." 這種前綴
        # 邏輯：找到 "數字."，並把前面的所有東西連同數字一起殺掉
        # 例如: "Test01. Name" -> "Name", "02. Name" -> "Name"
        name = re.sub(r'^.*?[0-9]+\.\s*', '', name)
        
        name = name.replace('\n', '').replace('\r', '')
        # 移除常見分隔符後的內容
        delimiters = r'[｜\|\-\–\—\:\：\/]'
        name = re.split(delimiters, name)[0].strip()
        
        # [強化黑名單]：過濾非店名的標題
        blacklist = [
            "總整理", "懶人包", "攻略", "精選", "必吃", "推薦", "名單", 
            "常見問題", "FAQ", "Q&A", "延伸閱讀", "看更多", "怎麼挑", "類型", "必訪", "介紹"
        ]
        if any(bad in name for bad in blacklist): return ""
        
        # 排除 Top X
        if "Top" in name and any(c.isdigit() for c in name): return ""
        
        # 排除太長像句子的標題 (通常店名不會超過 20 字)
        if len(name) > 20 and any(x in name for x in ["，", "！", "？", "。"]): return ""
        
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
                        if re.search(valid_pattern, title, re.IGNORECASE):
                            captured.append({"title": title, "url": url})
                except: continue
            return captured
        except: return []

    def step_1_harvest_article_links(self, keyword, max_pages=1):
        logger.info(f"[Step 1] 搜尋: {keyword}")
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
                logger.info(f"抓取第 {page} 頁...")
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
            logger.error(f"Step 1 Error: {e}")
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
            # 如果清洗完太短，或是被黑名單過濾掉變空字串，就跳過
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
        logger.info("[Step 2] 提取內容...")
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
                logger.info(f"[{idx+1}] {art_title[:15]}...")
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

# --- 3. 資料治理 (API 補完) ---

def normalize_text(text):
    if pd.isna(text): return ""
    text = str(text)
    normalized = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8')
    return normalized.lower().replace(" ", "")

def is_valid_cafe_type(types_list):
    if not types_list: return False
    allow = ['cafe', 'bakery', 'food', 'restaurant', 'meal_takeaway', 'store']
    block = ['department_store', 'shopping_mall', 'bar', 'night_club', 'lodging', 'gym']
    has_allow = any(k in types_list for k in allow)
    has_block = any(k in types_list for k in block)
    if 'cafe' in types_list or 'bakery' in types_list: return True
    if has_block and not has_allow: return False
    return True

def fetch_missing_place_id_detailed(place_name):
    api_key = os.getenv("GOOGLE_MAPS_API_KEY")
    if not api_key: 
        logger.error("未設定 GOOGLE_MAPS_API_KEY")
        return None
    
    try:
        gmaps = googlemaps.Client(key=api_key)
        
        # 1. Text Search
        find_res = gmaps.find_place(
            input=f"{place_name} 台北", 
            input_type="textquery", 
            fields=["place_id", "name", "formatted_address", "types"]
        )
        
        if not find_res['status'] == 'OK' or not find_res['candidates']:
            return None
            
        candidate_basic = find_res['candidates'][0]
        p_id = candidate_basic['place_id']
        
        types = candidate_basic.get('types', [])
        addr = candidate_basic.get('formatted_address', '')
        
        if not is_valid_cafe_type(types):
            logger.warning(f"     攔截非咖啡廳: {candidate_basic['name']}")
            return None
        if '新北市' in addr:
            logger.warning(f"     攔截新北市: {candidate_basic['name']}")
            return None

        # 2. Place Details
        details = gmaps.place(
            place_id=p_id,
            fields=[
                "name", "formatted_address", "geometry", "type", 
                "rating", "user_ratings_total", "business_status", 
                "opening_hours", "formatted_phone_number", "website"
            ],
            language='zh-TW'
        ).get('result', {})
        
        details['place_id'] = p_id
        
        # [關鍵修正]：過濾永久歇業的店家
        if details.get('business_status') == 'CLOSED_PERMANENTLY':
            logger.warning(f"     店家已永久歇業: {details['name']}")
            return None
        
        if '新北市' in details.get('formatted_address', ''):
            logger.warning(f"     詳細地址確認為新北市，剔除。")
            return None

        return details

    except Exception as e:
        logger.error(f"API Error: {e}")
        return None

def match_and_upsert_stores(scraped_data, bucket_name, base_blob, dyn_blob):
    """ 比對並更新 GCS 上的總表 """
    
    # 1. 從 GCS 讀取 Base Table
    df_base = read_csv_from_gcs(bucket_name, base_blob)
    if df_base is None:
        cols_base = ['name', 'place_id', 'formatted_phone_number', 'formatted_address', 
                     'website', 'location', 'opening_hours', 'price_level', 
                     'business_status', 'types', 'payment_options', 'google_maps_url']
        df_base = pd.DataFrame(columns=cols_base)
    
    # 2. 從 GCS 讀取 Dynamic Table
    df_dyn = read_csv_from_gcs(bucket_name, dyn_blob)
    if df_dyn is None:
        cols_dyn = ['place_id', 'name', 'rating', 'user_ratings_total', 'data_source', 'processed_at']
        df_dyn = pd.DataFrame(columns=cols_dyn)

    df_base['norm_name'] = df_base['name'].apply(normalize_text)
    name_to_id_map = dict(zip(df_base['norm_name'], df_base['place_id']))
    existing_id_set = set(df_base['place_id'].dropna().unique())
    
    matched_results = []
    new_stores_count = 0

    logger.info(f"開始比對 (Base: {len(df_base)} 筆)...")

    for item in scraped_data:
        target_name = item['place_name']
        norm_target = normalize_text(target_name)
        p_id = None
        
        # A. 本地名字比對
        if norm_target in name_to_id_map:
            p_id = name_to_id_map[norm_target]
        
        # B. API 補完
        if not p_id:
            logger.info(f"   清單無此店 ({target_name}) -> 呼叫 API...")
            api_res = fetch_missing_place_id_detailed(target_name)
            
            if api_res:
                found_id = api_res['place_id']
                
                if found_id in existing_id_set:
                    p_id = found_id
                    logger.info(f"     ↳ ID ({found_id}) 已存在，僅關聯。")
                else:
                    p_id = found_id
                    logger.info(f"     ↳ 發現新店家！新增: {api_res['name']}")
                    
                    loc = api_res.get('geometry', {}).get('location', {})
                    loc_str = f"POINT({loc.get('lng')} {loc.get('lat')})" if loc else None
                    
                    types_str = ",".join(api_res.get('types', []))
                    weekday_text = api_res.get('opening_hours', {}).get('weekday_text', [])
                    f_opening = " | ".join(weekday_text) if weekday_text else None
                    
                    # 1. Static Data
                    new_base_row = {
                        'name': api_res['name'],
                        'place_id': found_id,
                        'formatted_address': api_res.get('formatted_address'),
                        'formatted_phone_number': api_res.get('formatted_phone_number'),
                        'website': api_res.get('website'),
                        'location': loc_str,
                        'types': types_str, 
                        'opening_hours': f_opening,
                        'business_status': api_res.get('business_status'),
                        'payment_options': "",
                        'google_maps_url': "" 
                    }
                    
                    # 2. Dynamic Data
                    new_dyn_row = {
                        'place_id': found_id,
                        'name': api_res['name'],
                        'rating': api_res.get('rating'),
                        'user_ratings_total': api_res.get('user_ratings_total'),
                        'data_source': 'Google_Maps_API',
                        'processed_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                    
                    df_base = pd.concat([df_base, pd.DataFrame([new_base_row])], ignore_index=True)
                    df_dyn = pd.concat([df_dyn, pd.DataFrame([new_dyn_row])], ignore_index=True)
                    
                    existing_id_set.add(found_id)
                    new_stores_count += 1

        item['place_id'] = p_id
        matched_results.append(item)
    
    if new_stores_count > 0:
        if 'norm_name' in df_base.columns: del df_base['norm_name']
        save_csv_to_gcs(df_base, bucket_name, base_blob)
        save_csv_to_gcs(df_dyn, bucket_name, dyn_blob)
        logger.info(f"資料表已更新！新增了 {new_stores_count} 筆。")
    else:
        logger.info("資料表無需更新。")

    return matched_results

# --- 4. 模組入口 ---
def run(keyword="台北咖啡廳", max_pages=3):
    BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME")
    if not BUCKET_NAME:
        logger.error("未設定 GCS_BUCKET_NAME")
        return

    # GCS 檔案路徑 (依據需求設定)
    BASE_BLOB = "raw/store/base.csv"
    # [路徑更新] Dynamic Table 存到 store_dynamic 資料夾
    DYN_BLOB = "raw/store_dynamic/store_dynamic.csv"
    # [路徑更新] Supertaste Reviews 存到 supertaste 資料夾
    REVIEW_DIR = "raw/supertaste/"

    crawler = SuperTasteCrawlerCloud()
    try:
        articles = crawler.step_1_harvest_article_links(keyword, max_pages=max_pages)
        if articles:
            raw_data = crawler.step_2_extract_cafes(articles)
            
            # 比對並更新總表
            final_data = match_and_upsert_stores(raw_data, BUCKET_NAME, BASE_BLOB, DYN_BLOB)
            
            # 存取食尚玩家的評論 (文章內容)
            df_review = pd.DataFrame(final_data)
            
            # 剔除無 ID
            initial_len = len(df_review)
            df_review = df_review[df_review['place_id'].notna() & (df_review['place_id'] != "")]
            dropped_count = initial_len - len(df_review)
            
            if dropped_count > 0:
                logger.info(f"已剔除 {dropped_count} 筆無效資料 (無 Place ID)。")

            cols = ['place_id', 'place_name', 'description', 'article_title', 'source_url', 'processed_at', 'raw_title']
            for c in cols: 
                if c not in df_review.columns: df_review[c] = ""
            df_review = df_review[cols]
            
            out_blob = f"{REVIEW_DIR}supertaste_reviews_{datetime.now().strftime('%Y%m%d')}.csv"
            save_csv_to_gcs(df_review, BUCKET_NAME, out_blob)
            
            logger.info(f"任務完成！評論表已存: gs://{BUCKET_NAME}/{out_blob}")
            logger.info(f"   最終有效筆數: {len(df_review)}")

    finally:
        crawler.close()
        print("作業結束")