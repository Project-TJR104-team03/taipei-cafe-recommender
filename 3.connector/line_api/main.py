from fastapi import FastAPI, Query, HTTPException
from contextlib import asynccontextmanager
from database import db_client
from typing import List, Optional
from pydantic import BaseModel
from datetime import datetime

# --- 資料模型定義 ---

class UserLog(BaseModel):
    user_id: str
    action: str            # "YES", "NO", "KEEP", "INIT_PREF"
    cafe_id: Optional[str] = None 
    reason: Optional[str] = None
    timestamp: Optional[str] = None

class UserLocation(BaseModel):
    lat: float
    lng: float

# --- 生命週期管理 ---

@asynccontextmanager
async def lifespan(app: FastAPI):
    db_client.connect()
    yield
    db_client.close()

app = FastAPI(lifespan=lifespan)

# --- API 實作 ---

# 1. 推薦咖啡廳 (Recommend)
@app.get("/recommend")
def recommend_cafes(
    lat: float, 
    lng: float, 
    user_id: Optional[str] = None,
    tag: Optional[str] = Query(None)
):
    try:
        db = db_client.get_db()
        
        # 1. 取得該使用者的黑名單 (不喜歡的店)
        blacklist_ids = []
        if user_id:
            blacklist_logs = list(db['interaction_logs'].find(
                {"user_id": user_id, "action": "NO"},
                {"cafe_id": 1}
            ))
            blacklist_ids = [log['cafe_id'] for log in blacklist_logs]

        # 2. 建立 Pipeline
        pipeline = []

        # (A) 地理位置搜尋 (基礎)
        pipeline.append({
            "$geoNear": {
                "near": { "type": "Point", "coordinates": [lng, lat] },
                "distanceField": "dist_meters",
                "maxDistance": 3000, 
                "spherical": True
            }
        })

        # (B) 過濾黑名單
        if blacklist_ids:
            pipeline.append({
                "$match": { "place_id": { "$nin": blacklist_ids } }
            })

        # (C) 多欄位模糊搜尋 (強化搜尋深度)
        if tag:
            pipeline.append({
                "$match": {
                    "$or": [
                        { "original_name": { "$regex": tag, "$options": "i" } },
                        { "attributes.types": { "$regex": tag, "$options": "i" } },
                        { "ai_tags.tag": { "$regex": tag, "$options": "i" } }
                    ]
                }
            })

        # (D) 權重排序：評分 / (距離/100 + 1) -> 越高分排越前
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

        results = list(db['cafes'].aggregate(pipeline))
        
        # 3. 格式化回傳... (保持你原本的 formatted_data 邏輯)
        # 格式轉換，確保符合前端規格
        formatted_data = []
        for r in results:
            formatted_data.append({
                "place_id": r.get("place_id", str(r["_id"])),
                "original_name": r.get("original_name", "未知店家"),
                "dist_meters": round(r.get("dist_meters", 0)), # 取整數
                "attributes": r.get("attributes", {"types": [], "rating": 0}),
                "ai_tags": r.get("ai_tags", []),
                "rating": r.get("rating", r.get("attributes", {}).get("rating", 0)),
                "total_ratings": r.get("user_ratings_total", 0)
            })
        
        return {"data": formatted_data}

    except Exception as e:
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