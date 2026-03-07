import uuid
import os
import json
import time
import certifi
import csv
from datetime import datetime
import pytz
import requests
from google.cloud import bigquery
import random
import vertexai
from vertexai.generative_models import GenerativeModel
from google.cloud import storage
import pymongo
import math
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential

load_dotenv()

# --- GCP 與 Vertex AI 初始化 ---
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "project-tjr104-cafe")
LOCATION = os.getenv("GCP_LOCATION", "asia-east1")
SEARCH_API_URL = os.getenv("SEARCH_API_URL")
MONGO_URI = os.getenv("MONGO_URL")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME","coffee_db")

BQ_TABLE_ID = os.getenv("BQ_TABLE_ID", f"{PROJECT_ID}.coffee_recommender.ai_interaction_logs")

VERTEX_AI_REGION = "us-central1" 
vertexai.init(project=PROJECT_ID, location=VERTEX_AI_REGION)
model = GenerativeModel("gemini-2.5-flash-lite")

# 設定台北時區
TPE_TZ = pytz.timezone('Asia/Taipei')

#本地輸出位置
LOG_DIR = "ai_interaction_log"

# 建立 DB 連線
try:
    if not MONGO_URI:
        print("❌ 錯誤：找不到 MONGODB_URL 環境變數，請檢查 .env 檔案！")
    else:
        # 🌟 加上 tlsCAFile=certifi.where() 確保能連上雲端 Atlas
        mongo_client = pymongo.MongoClient(MONGO_URI, tlsCAFile=certifi.where())
        db = mongo_client[MONGO_DB_NAME]
        cafes_collection = db["cafes"]
        reviews_collection = db["AI_embedding"]
        
        cafe_count = cafes_collection.count_documents({})
        print(f"✅ 成功連線至 MongoDB Atlas！目前【{MONGO_DB_NAME}】的 cafes 集合內有 {cafe_count} 筆店家資料。")
        
except Exception as e:
    print(f"❌ MongoDB 連線失敗: {e}")


def calculate_distance(lat1, lon1, lat2, lon2):
    """使用 Haversine 公式計算兩點實際距離 (公里)"""
    R = 6371.0 # 地球半徑 (公里)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return round(R * c, 2)

def format_opening_hours(opening_hours_data):
    """將 MongoDB 的分鐘數格式轉換為人類可讀的字串"""
    if not opening_hours_data or "periods" not in opening_hours_data:
        return "無營業時間資訊"

    # Google Places API 標準：0 是週日，1 是週一 ... 6 是週六
    day_map = {0: "週日", 1: "週一", 2: "週二", 3: "週三", 4: "週四", 5: "週五", 6: "週六"}
    formatted_hours = []

    for period in opening_hours_data.get("periods", []):
        day_num = period.get("day")
        open_min = period.get("open", 0)
        close_min = period.get("close", 0)

        day_str = day_map.get(day_num, f"未知({day_num})")

        # 轉換分鐘為 HH:MM 格式
        open_time = f"{open_min // 60:02d}:{open_min % 60:02d}"
        close_time = f"{close_min // 60:02d}:{close_min % 60:02d}"

        formatted_hours.append(f"{day_str}: {open_time}-{close_time}")

    return " | ".join(formatted_hours)

# 🌟 定義一個 Callback 函數：當重試達上限依然失敗時，執行這個函數來收尾
def handle_persona_failure(retry_state):
    print(f"❌ Persona 生成徹底失敗 (已達最大重試次數 {retry_state.attempt_number} 次): {retry_state.outcome.exception()}")
    return None

# 🌟 加上 Tenacity 重試裝飾器
# stop_after_attempt(4): 最多嘗試 4 次
# wait_exponential(multiplier=2, min=2, max=15): 失敗後等待 2秒 -> 4秒 -> 8秒 (最高等 15 秒)
@retry(
    stop=stop_after_attempt(4), 
    wait=wait_exponential(multiplier=2, min=2, max=15),
    retry_error_callback=handle_persona_failure
)

def generate_ai_persona():
        """生成虛擬使用者與查詢情境 (族群輪廓 x 對話情境 的矩陣組合)"""
        prompt = """
    請隨機生成一個在台北找咖啡廳的使用者 Persona。回傳純 JSON 格式，不含 Markdown。
    
    為了增加壓力測試資料的多樣性，請你先隨機挑選一個「族群輪廓」，再隨機挑選一個「對話情境」，並將兩者結合來產生使用者的真實發言 (query)。

    【族群輪廓池】(請隨機選一，這會決定該角色的 liked_tags 與 disliked_tags)：
    1. 數位遊牧族 (Digital Nomad)：核心需求「插座、Wi-Fi、不限時、安靜」。對咖啡好不好喝可能還好，但絕對不能沒有網路。
    2. IG 網美/探店達人：核心需求「採光好、裝潢美、有特色甜點 (如布丁、肉桂捲)、好拍照」。
    3. 寵物狂熱者：只關心「能不能帶毛小孩、有沒有店貓店狗」。
    4. 咖啡重度成癮者 (Coffee Snob)：關鍵字會是「手沖、淺焙、自家烘焙、藝伎、SOE」。
    5. 約會/聚餐客：需要「氣氛好、好聊天、有鹹食、最好能訂位」。
    
    【對話劇本池】(請隨機選一，這會決定 query 的複雜度與句型)：
    1. 全新搜尋：直接給出符合自己族群的具體需求（例：「幫我找半夜有開的安靜咖啡廳」）。
    2. 追加條件：基於某個地點，加上更嚴苛的限制（例：「在中山站、工作友善的咖啡廳裡，但晚上8點還要營業的」）。
    3. 純閒聊 / 模糊情境：表達心情、天氣或狀態，沒有具體硬體條件（例：「今天天氣好差心情不好，想找個地方躲雨喝杯熱的」）。
    4. 跨日反問：詢問特定日期或未來的狀態（例：「這禮拜天有營業、有賣特色甜點的店嗎？」）。
    5. 地點替換：表達現在的位置，但想換去另一個區域（例：「我現在在中山站，但想找松山附近適合看書的」）。
    6. 精準店名直達車：直接點名尋找某家特定店名，可加上族群口吻（例：「那家叫 dine in cafe 的店有提供插座嗎？」）。
    7. 中間點定位：尋找兩個地點中間的店（例：「想找北車跟中山中間，適合約會聊天的店」）。
    8. 保留地點的連鎖店/店名搜尋：尋找特定區域的知名品牌（例：「找比較安靜、靠近東門站的星巴克」）。
    9. 豁免條款：把知名店名當作形容詞來找店（例：「要找跟星巴克氛圍一樣的，可以坐很久用電腦的」）。
    10. 刁鑽邊界型：極端時間 + 極端條件，測試系統極限（例：「半夜三點，大安區，要能帶黃金獵犬進去的店」）。
    11. 惡意/無關型：與咖啡廳完全無關，測試系統防呆（例：「請推薦我台北好吃的滷肉飯」或「哪裡有修車廠」）。
                
    欄位要求 (回傳純 JSON)：
    - user_id: 隨機產生的 UUID 字串
    - role: 請用一句話標示這次的組合，格式為「[族群名稱] x [劇本名稱]」，例如 例如 "數位遊牧族 x 追加條件" 或 "約會客 x 跨日反問"
    - location: 台北市隨機座標 [longitude, latitude] (經度大約 121.5, 緯度大約 25.0)
    - query: 對 LINE Bot 說出的真實發言 (務必將抽中的族群特性與劇本情境自然融合！)
    - liked_tags: [陣列] (根據抽中的族群，列出 2~4 個關鍵字，例：["插座", "不限時"])
    - disliked_tags: [陣列] (根據抽中的族群，列出 1~3 個絕對不想踩雷的標籤，例網美討厭 "燈光昏暗"、遊牧族討厭 "限時")
    """

        response = model.generate_content(prompt)
        clean_text = response.text.replace("```json", "").replace("```", "").strip()
        return json.loads(clean_text)


def handle_eval_failure(retry_state):
    print(f"❌ AI 評估徹底失敗 (已達最大重試次數): {retry_state.outcome.exception()}")
    # 回傳預設的失敗資料結構，避免 BigQuery Schema Drift
    return {
        "decision": "FAIL",
        "semantic_score": 0, "review_score": 0, "distance_score": 0, "time_score": 0, "total_score": 0,
        "reason": f"API 超載或解析失敗",
        "user_lat": 0.0, "user_lng": 0.0, "cafe_lat": 0.0, "cafe_lng": 0.0,
        "api_dist": 0.0, "actual_dist": 0.0
    }

# 🌟 新增：加上 Tenacity 重試裝飾器 (最多試 3 次)
@retry(
    stop=stop_after_attempt(3), 
    wait=wait_exponential(multiplier=1.5, min=2, max=10),
    retry_error_callback=handle_eval_failure
)

def evaluate_recommendation(persona, cafe_data, rank):
        """🌟 結合 MongoDB 真實資料進行深度評分 (附帶 Debug 探測)"""

        # 1. 確認 API 傳來的 ID 到底長怎樣？
        place_id = cafe_data.get("_id")
        cafe_name = cafe_data.get("name", "未知店家")
        api_distance_km = cafe_data.get("distance_km", 0) 
        
        print(f"\n   🔍 [DB 探測] 準備撈取【{cafe_name}】的資料...")
        print(f"      -> API 提供的 place_id 為: '{place_id}'")
        
        if not place_id or place_id == "N/A":
            print("      ❌ 致命錯誤：place_id 無效，無法進行資料庫查詢！請檢查 FastAPI 的回傳邏輯。")

        # 2. 查詢 cafes 主檔
        db_cafe = cafes_collection.find_one({"place_id": place_id})
        
        # 🌟 診斷報告輸出
        if db_cafe:
            print("      ✅ cafes 主檔：成功找到資料！")
        else:
            print("      ⚠️ cafes 主檔：【找不到資料】！(請確認 DB 名稱與 ID 是否正確)")

        # 3. 查詢 AI_embedding 評論檔
        db_reviews_cursor = reviews_collection.find({"place_id": place_id, "doc_type": "review_level"}).limit(10)
        db_reviews = [doc.get("content", "") for doc in db_reviews_cursor]
        
        if db_reviews:
            print(f"      ✅ 評論檔：成功找到 {len(db_reviews)} 筆評論！")
        else:
            print("      ⚠️ 評論檔：【找不到任何評論】！")


        # --- 以下整理要餵給 Gemini 的數據 ---
        if db_cafe:
            db_tags = db_cafe.get("tags", [])
            db_summary = db_cafe.get("summary", "無總結")
            
            features_dict = db_cafe.get("features", {})
            true_features = [k for k, v in features_dict.items() if v is True]
            
            raw_hours = db_cafe.get("opening_hours", {})
            db_hours_str = format_opening_hours(raw_hours)

            # 撈取真實座標計算距離
            coords = db_cafe.get("location", {}).get("coordinates", [0, 0])
            if len(coords) == 2:
                cafe_lng, cafe_lat = coords[0], coords[1]
            else:
                cafe_lng, cafe_lat = 0.0, 0.0
        else:
            db_tags = cafe_data.get("tags", [])
            db_summary = "資料庫無詳細總結"
            true_features = []
            db_hours_str = "無營業時間資訊" #
            cafe_lng, cafe_lat = 0.0, 0.0
            
        reviews_text = "\n".join([f"- {r}" for r in db_reviews]) if db_reviews else "無具體評論紀錄"
                
        # 🌟 計算使用者與店家的「真實直線距離」
        p_lng, p_lat = persona["location"][0], persona["location"][1]
        actual_distance = calculate_distance(p_lat, p_lng, cafe_lat, cafe_lng) if cafe_lat != 0 else api_distance_km

        # 🌟 取得當前台北時間與「星期幾」，幫助 AI 快速對照
        now_tpe = datetime.now(TPE_TZ)
        current_time_str = now_tpe.strftime('%Y-%m-%d %H:%M')
        # isoweekday() 1-7 分別是週一到週日，轉換為中文
        weekday_map = {1: "週一", 2: "週二", 3: "週三", 4: "週四", 5: "週五", 6: "週六", 7: "週日"}
        current_weekday = weekday_map[now_tpe.isoweekday()]


        # 組合帶有「距離懲罰機制」的 Prompt
        prompt = f"""

        你是這名尋找咖啡廳的使用者：{json.dumps(persona, ensure_ascii=False)}。
        你對 LINE Bot 說出的真實需求是：「{persona.get('query')}」。
        
        系統為你推薦了店家：【{cafe_data.get('name')}】。
        以下是系統檢索出該店的真實背景資料 (RAG Context)：
        [店家絕對具備的特徵]: {true_features}
        [基本標籤]: {db_tags}
        [營業時間清單]: {db_hours_str}
        [AI 綜合摘要]: {db_summary}
        [真實顧客評論抽樣]: {reviews_text}

        🌟 [當前系統時間]：{current_time_str} ({current_weekday})
        
        請你以這名使用者的身分，評估這家店並給出四個維度的項目分數 (0~100) 以及加權總分。評分請嚴格依循以下四大支柱與權重：
        
        1. 語意擬合度分數 (Semantic/Vector Score) - 佔 40% 權重：
           - 評估 Query 背後的「核心意圖/氛圍」是否與店家的「AI 綜合摘要」高度吻合。
           - 這是模擬向量搜尋的指標。只要語意和氛圍契合，即使沒有完全對應的標籤，請給 60 分以上，否則則給 60 分以下。
           - 向量是最重要的評分項目，評分時請務必嚴謹

        2. 標籤與評論分數 (Tag & Review Score) - 佔 30% 權重：
           - 評估使用者的具體需求 (如：插座、安靜、甜點) 是否明確出現在「基本標籤」或「顧客評論」中。
           - 需求涵蓋度越高，分數越高。若評論中出現與需求相違背的負面描述（如想找安靜卻被評論說吵），請嚴格扣分。

        3. 距離分數 (Distance Score) - 佔 20% 權重：
           - 店家距離你的真實位置為：{actual_distance} 公里。
           - [地理位置例外]：若你的 query 中明確提到「想找其他地區」(與當下座標不同)，或「不限地點」，此項目直接給 100 分。
           - 否則按此標準給分：< 1.5 公里 (90~100分)；1.5~3 公里 (70~89分)；3~5 公里 (40~69分)；> 5 公里 (0~39分)。

        4. 營業時間分數 (Operating Hours Score) - 佔 10% 權重：
           - 判斷基準：依據 [當前系統時間] 或 query 中「明確指定的預計前往時間」，對比店家的 [營業時間資訊]。
           - 若距離打烊時間大於等於 3 小時，或店家是 24 小時營業 -> 給 90~100 分。
           - 若距離打烊時間不到 3 小時，按比例遞減 (例如剩 1 小時給 40 分)。
           - 若判斷抵達時已打烊，或店家當日公休 -> 直接給 0 分。
           - 若店家缺乏營業時間資料 -> 給予 60 分的中立分數。
           - 如果使用者query有指定時間，則應以該時間去做比照，若無就以現在為主

        總分 (Total Score) 計算公式：
        Total = (語意擬合 * 0.4) + (標籤評論 * 0.3) + (距離 * 0.2) + (營業時間 * 0.1)
           
        🌟 判斷規則 (Decision)：
        1. 總分 >= 80，且沒有踩到 disliked_tags，且未打烊 -> "YES"
        2. 總分 >= 80，但推薦店家命中 disliked_tags -> "KEEP"
        3. 總分 < 80，或已打烊，或完全踩雷 -> "NO"
        
        請以純 JSON 格式回傳 (嚴格遵守格式，不含 ```json 標記)：
        {{
            "semantic_score": 85,
            "review_score": 90,
            "distance_score": 100,
            "time_score": 95,
            "total_score": 90,
            "decision": "YES" | "NO" | "KEEP",
            "reason": "請用一句話總結：(1)語意/標籤契合度 (2)距離合理性 (3)是否有踩到 disliked_tags"
        }}
        """
        
        response = model.generate_content(prompt)
        clean_text = response.text.replace("```json", "").replace("```", "").strip()
        result = json.loads(clean_text)

        raw_decision_val = result.get("decision", "KEEP")
        raw_str = str(raw_decision_val).strip().upper()
        
        # 🌟 神級替換：把 YES/NO 換成 PASS/REJECT，徹底避開 BigQuery 的布林值地雷！
        if raw_decision_val is True or raw_str in ["TRUE", "YES"]:
            final_decision = "PASS"   # 代表推薦成功
        elif raw_decision_val is False or raw_str in ["FALSE", "NO"]:
            final_decision = "REJECT" # 代表推薦失敗
        elif raw_str == "KEEP":
            final_decision = "KEEP"   # 代表保留
        else:
            final_decision = "ERROR"  # 把原本的 Fail 改為 ERROR，更符合資料庫標準
        
        return {
            "decision": final_decision, # 這裡現在只會出現 PASS, REJECT, KEEP, ERROR
            "semantic_score": round(float(result.get("semantic_score", 0))),
            "review_score": round(float(result.get("review_score", 0))),
            "distance_score": round(float(result.get("distance_score", 0))),
            "time_score": round(float(result.get("time_score", 0))),
            "total_score": round(float(result.get("total_score", 0))),
            "reason": result.get("reason", "解析成功但無原因"),
            "user_lat": p_lat,     # 順便把座標打包回傳，讓主程式不用再算一次
            "user_lng": p_lng,
            "cafe_lat": cafe_lat,
            "cafe_lng": cafe_lng,
            "api_dist": api_distance_km,
            "actual_dist": actual_distance
        }


def single_search_cycle():
    """執行一次完整的搜尋、推薦、評估流程，並回傳結果清單"""
    persona = generate_ai_persona()
    if not persona: return []

    persona["user_id"] = str(uuid.uuid4())
    search_id = str(uuid.uuid4())[:8]
    request_time = datetime.now(TPE_TZ).strftime('%Y-%m-%d %H:%M:%S')
    start_time = time.time()
    
    cycle_logs = []
    try:
        payload = {"user_id": persona["user_id"], "location": persona["location"], "query": persona["query"]}
        # 增加 Timeout 至 180s 應對壓力測試時的延遲
        search_res = requests.post(SEARCH_API_URL, json=payload, timeout=180)
        search_res.raise_for_status()
        recommendations = search_res.json().get("data", [])
        latency = round(time.time() - start_time, 2)
        
        print(f"👤 [{persona['role']}] -> ⏱️ {latency}s")

        for rank, cafe in enumerate(recommendations[:3], start=1):
            eval_res = evaluate_recommendation(persona, cafe, rank)
            cycle_logs.append({
                "search_id": search_id,
                "request_timestamp": request_time,
                "user_id": persona.get("user_id"), 
                "role": persona.get("role"),
                "query": persona.get("query"),
                "user_lat": eval_res.get("user_lat"),
                "user_lng": eval_res.get("user_lng"),
                "cafe_lat": eval_res.get("cafe_lat"),
                "cafe_lng": eval_res.get("cafe_lng"),
                "api_distance_km": eval_res.get("api_dist"), 
                "actual_distance_km": eval_res.get("actual_dist"), 
                "latency_sec": latency,
                "rank": rank,
                "recommended_cafe_id": cafe.get("_id"),
                "recommended_cafe_name": cafe.get("name"),
                "decision": eval_res.get("decision"),
                "semantic_score": eval_res.get("semantic_score", 0),
                "review_score": eval_res.get("review_score", 0),
                "distance_score": eval_res.get("distance_score", 0),
                "time_score": eval_res.get("time_score", 0),
                "total_score": eval_res.get("total_score", 0),
                "ai_reason": eval_res.get("reason")
            })

    except Exception as e:
        print(f"❌ 搜尋流程失敗: {e}")
    
    return cycle_logs


def save_to_bigquery(data_batch):
    """將一批 Log (List of Dict) 直接載入 BigQuery，並自動建立/擴充 Schema"""
    if not data_batch: 
        return
    
    try:
        # 1. 初始化 BigQuery 客戶端
        client = bigquery.Client(project=PROJECT_ID)
        
        # 2. 設定寫入規則 (LoadJobConfig)
        job_config = bigquery.LoadJobConfig(
            autodetect=True, # 🌟 神奇魔法：讓 BQ 自動根據你的 JSON 決定是字串、整數還是浮點數  
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND, # 附加在現有資料後面
            schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION] # 允許未來動態新增欄位
        )
        
        print(f"☁️ 準備將 {len(data_batch)} 筆推薦紀錄載入 BigQuery...")
        
        # 3. 執行載入任務
        job = client.load_table_from_json(data_batch, BQ_TABLE_ID, job_config=job_config)
        
        # 4. 等待任務完成
        job.result() 
        print(f"✅ 成功寫入 BigQuery！(目標資料表: {BQ_TABLE_ID})")
        
    except Exception as e:
        print(f"❌ BigQuery 寫入失敗: {e}")


if __name__ == "__main__":
    print("🚀 Cloud Run Job 啟動：執行單次使用者模擬...")
    # 只跑 1 次，乾淨俐落！不到 10 秒就能收工關機。
    batch_results = single_search_cycle()
    save_to_bigquery(batch_results)
    print("✅ 模擬完成，資料寫入 BQ，準備關機。")