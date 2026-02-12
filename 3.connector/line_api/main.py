""" 
    判斷是否為自然語言 or Tag
        1. 自然語言:AI將自然語言轉向量(已成功embedding 1536搜尋功能)
                    -> 針對 reviews (評論) 集合進行向量相似度搜尋
                    -> 搜到評論後，用 $lookup 把對應的 cafes (店家資料) 抓進來
                    -> 過濾黑名單
                    -> Python Filter (過濾與評分階段)
                        剔除距離 > 3000 公尺 (3km) 的結果，取前10名
            例外處理：如果 AI 向量生成失敗，將 user_query 當作普通關鍵字，強制轉入 路徑 B
                   
                           
        2. Tag:路徑 B：傳統標籤/地理搜尋 (Geo + Keyword Match)
            -> 地理篩選 ($geoNear)：直接在資料庫層級找出方圓 3000 公尺內的店家（這是最優先條件）
            -> 過濾黑名單
            -> 模糊搜尋 (Regex Match)關鍵字
            -> 取前10筆
                               
    fast api 啟動
    uvicorn <檔名>:app --reload
    .\ngrok.exe http 8000
"""


import os
import logging
from typing import Optional, List
from contextlib import asynccontextmanager
from datetime import datetime

# 第三方套件
from fastapi import FastAPI, Query, HTTPException
from pydantic import BaseModel
import gc
from google import genai
from google.genai import types
from geopy.distance import geodesic
from dotenv import load_dotenv
# 自定義模組
from database import db_client


# --- 設定與初始化 ---
load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Coffee_Recommender")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

if GEMINI_API_KEY:
    client = genai.Client(api_key=GEMINI_API_KEY)
else:
    logger.warning("⚠️ 找不到 GEMINI_API_KEY")



# ---資料模型---
class UserLocation(BaseModel):
    lat: float
    lng: float


class UserLog(BaseModel):
    user_id: str
    action: str
    place_id: Optional[str] = None
    reason: Optional[str] = None

# ------------
# --- 生命週期管理 ---

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        db_client.connect()
        logger.info("✅ 資料庫連線已建立")
        yield
    except Exception as e:
        logger.error(f"❌ 資料庫連線失敗: {e}")
        raise e
    finally:
        db_client.close()

app = FastAPI(lifespan=lifespan)



# --- 輔助函式 ---
def get_embedding(text: str) -> Optional[List[float]]:
    """
    使用新版 google-genai SDK 取得向量
    模型: models/gemini-embedding-001
    維度: 強制輸出 1536
    """
    try:
        # 確保全域的 client 已經正確初始化
        if not client:
            logger.error("❌ Gemini Client 未初始化，請檢查 API Key 設定")
            return None
      

        # 新版語法呼叫
        response = client.models.embed_content(
            model='models/gemini-embedding-001',
            contents=text,
            config={
                'task_type': 'RETRIEVAL_QUERY',
                'output_dimensionality': 1536  # <--- 強制指定輸出維度為 1536
            }
        )     

        # 取得回傳的向量值 (List[float])
        vector = response.embeddings[0].values       

        # 額外檢查：確保回傳的維度確實是 1536
        if len(vector) != 1536:
            logger.warning(f"⚠️ 預期維度 1536，但回傳為 {len(vector)}")           
        return vector
      
    except Exception as e:
        logger.error(f"❌ [New SDK] Embedding Error: {e}")
        return None


# 1. 推薦咖啡廳 (Recommend)
@app.get("/recommend")
async def recommend_cafes(
    lat: float,
    lng: float,
    user_id: Optional[str] = None,
    cafe_tag: Optional[str] = Query(None, description="標籤/按鈕 (Database Match)"),
    user_query: Optional[str] = Query(None, description="自然語言 (Vector Search)")
):
    db = db_client.get_db()
    if db is None:
         raise HTTPException(status_code=503, detail="Database not available")

    user_loc = (lat, lng)
    final_data = []

    try:
        # ==========================================
        # 1. [共用邏輯] 取得黑名單
        # ==========================================
        blacklist_ids = []
        if user_id:
            blacklist_logs = list(db['interaction_logs'].find(
                {"user_id": user_id, "action": "NO"},
                {"place_id": 1}
            ))
            # 確保欄位對應 (假設 log 裡的 place_id 對應 cafes 的 place_id)
            blacklist_ids = [log['place_id'] for log in blacklist_logs]

        # ==========================================
        # 路徑 A: AI 語意搜尋 (Retrieve -> Python Filter)
        # ==========================================
        if user_query:
            query_vector = get_embedding(user_query)

                    # --- 加入這段驗證碼 ---
                     
            if query_vector:
            # 這裡印出來，代表使用者是用「打字」的，且 AI 成功運作
                logger.info(f"✅ [AI 語意分析成功]")
                logger.info(f"   - 用戶輸入: {user_query}")
                logger.info(f"   - 向量維度: {len(query_vector)}") # 監控是否為 1536
            else:
                logger.warning(f"❌ [AI 語意分析失敗] 輸入: {user_query}")
            # ---------------------
           
            if query_vector:
                # A-1. 向量搜尋 pipeline
                pipeline_vec = [
                    {
                        "$vectorSearch": {
                            "index": "vector_index",
                            "path": "embedding",
                            "queryVector": query_vector,
                            "numCandidates": 100,
                            "limit": 50
                        }
                    },
                    {
                        "$lookup": {
                            "from": "cafes",
                            "localField": "place_id",
                            "foreignField": "place_id",
                            "as": "cafe_info"
                        }
                    },
                    { "$unwind": "$cafe_info" },
                    # 投影回 cafes 的格式
                    {
                        "$project": {
                            "place_id": "$cafe_info.place_id",
                            "original_name": "$cafe_info.original_name",
                            "location": "$cafe_info.location",
                            "rating": "$cafe_info.total_ratings", # 或 rating
                            "attributes": "$cafe_info.attributes",
                            "ai_tags": "$cafe_info.ai_tags",
                            "vector_score": { "$meta": "vectorSearchScore" },
                            "matched_review": "$content"
                        }
                    }
                ]

                # A-2. 在 MongoDB 層級過濾黑名單 (如果有的話)
                if blacklist_ids:
                    pipeline_vec.append({
                        "$match": { "place_id": { "$nin": blacklist_ids } }
                    })

                raw_results = list(db['reviews'].aggregate(pipeline_vec))
               
                # A-3. [Python] 距離計算 + 評分公式復刻
                filtered_results = []
                for item in raw_results:
                    if not item.get('location') or 'coordinates' not in item['location']:
                        continue

                    # 座標轉換 GeoJSON [lng, lat] -> Geopy (lat, lng)
                    cafe_loc = (item['location']['coordinates'][1], item['location']['coordinates'][0])
                    dist_meters = geodesic(user_loc, cafe_loc).meters
                   
                    if dist_meters <= 3000:
                        item['dist_meters'] = int(dist_meters)
                       
                        # --- [核心] 重現你的評分公式 ---
                        # 原公式: rating / (dist/100 + 1)
                        # AI 版公式: (向量分數 * rating) / (dist/100 + 1)
                        # 這樣既考量了語意相似度(vector_score)，也保留了距離衰減邏輯
                        base_rating = item.get('rating', 0) or 0
                        vec_score = item.get('vector_score', 0.8)
                       
                        search_score = (base_rating * vec_score) / ((dist_meters / 100) + 1)
                       
                        item['search_score'] = search_score
                        filtered_results.append(item)
               
                filtered_results.sort(key=lambda x: x['search_score'], reverse=True)
                final_data = filtered_results[:10]
            else:
                # 向量失敗，降級為 Tag 搜尋
                cafe_tag = user_query

        # ==========================================
        # 路徑 B: 標籤/地理搜尋 (保留你的原始邏輯)
        # ==========================================
        if not final_data and (cafe_tag or not user_query):
            # 使用變數 `tag` 接住傳入的 cafe_tag，方便對照你的程式碼
            tag = cafe_tag
            if tag:
                logger.info(f"🏷️ [標籤搜尋] 使用者點擊按鈕: {tag}")           
            pipeline = []

            # (A) 地理位置搜尋 (基礎) - 3km內
            pipeline.append({
                "$geoNear": {
                    "near": { "type": "Point", "coordinates": [lng, lat] },
                    "distanceField": "dist_meters",
                    "maxDistance": 3000,
                    "spherical": True
                }
            })

            # (B) 過濾黑名單 - 排除 blacklist_ids
            if blacklist_ids:
                pipeline.append({
                    "$match": { "place_id": { "$nin": blacklist_ids } }
                })

            # (C) 多欄位模糊搜尋 - 檢查欄位，任一欄位包含關鍵字（不分大小寫）即符合
            if tag:
                pipeline.append({
                    "$match": {
                        "$or": [
                            { "original_name": { "$regex": tag, "$options": "i" } },
                            { "attributes.types": { "$regex": tag, "$options": "i" } },
                            { "ai_tags.tag": { "$regex": tag, "$options": "i" } },
                            # 也可以補上 seo_tags
                            { "seo_tags": { "$regex": tag, "$options": "i" } }
                        ]
                    }
                })

            # (D) 權重排序 
            pipeline.append({
                "$addFields": {
                    "search_score": {
                        "$divide": [
                            { "$ifNull": ["$rating", 0] },
                            { "$add": [{ "$divide": ["$dist_meters", 100] }, 1] }
                        ]
                    }
                }
            })
            pipeline.append({ "$sort": { "search_score": -1 } })
            pipeline.append({ "$limit": 10 })
            final_data = list(db['cafes'].aggregate(pipeline))

        # --- 最終格式化 ---
        formatted_response = []
        for r in final_data:
            formatted_response.append({
                "place_id": r.get("place_id", str(r.get("_id"))),
                "original_name": r.get("original_name", "未知店家"),
                "dist_meters": int(r.get("dist_meters", 0)),
                "rating": r.get("rating", 0),
                "ai_tags": r.get("ai_tags", [])[:3],
                "match_reason": r.get("matched_review", "符合條件")
            })
        return {"data": formatted_response}


    except Exception as e:
        logger.error(f"Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))



# 2. 儲存使用者位置 (Update Location)
@app.post("/users/{user_id}/location")
def update_user_location(user_id: str, loc: UserLocation):
    try:
        db = db_client.get_db()
        collection = db['users']
        
        collection.update_one(
            {"user_id": user_id},
            {"$set": {
                "lat": loc.lat,
                "lng": loc.lng,
                "updated_at": datetime.now()
            }},
            upsert=True # 若無資料則新增
        )
        return {"status": "success", "message": "Location updated"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# 3. 讀取使用者位置 (Get Location)
@app.get("/users/{user_id}/location")
def get_user_location(user_id: str):
    db = db_client.get_db()
    user_loc = db['users'].find_one({"user_id": user_id})
    
    if not user_loc:
        raise HTTPException(status_code=404, detail="Location not found")
        
    return {
        "lat": user_loc["lat"],
        "lng": user_loc["lng"]
    }

# 4. 記錄使用者回饋 (Log Action)
@app.post("/log_user_action")
def log_action(log_data: UserLog):
    try:
        db = db_client.get_db()
        collection = db['interaction_logs']
        
        doc = log_data.dict()
        doc['created_at_server'] = datetime.now()
        
        collection.insert_one(doc)
        return {"status": "success"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}

# 5. 檢查使用者是否存在 (Check Profile)
@app.get("/users/{user_id}/profile")
def check_user_profile(user_id: str):
    db = db_client.get_db()
    # 檢查是否有過任何互動紀錄或位置紀錄
    user_exists = db['interaction_logs'].find_one({"user_id": user_id})
    
    if user_exists:
        return {"status": "success", "message": "老手用戶"}
    else:
        # 回傳 404 代表新手
        raise HTTPException(status_code=404, detail="New User")

# 首頁測試
@app.get("/")
def read_root():
    return {"message": "API v2.0 運作正常"}