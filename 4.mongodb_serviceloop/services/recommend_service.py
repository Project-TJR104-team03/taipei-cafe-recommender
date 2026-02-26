# app/services/recommend_service.py
import logging
import traceback
from datetime import datetime
from geopy.distance import geodesic
from typing import Any, Dict, List, Optional

from database import db_client
from utils import is_google_period_open
from locations import ALL_LOCATIONS
from agents.intent_agent import IntentAgent
from google import genai 
from services.scoring import calculate_comprehensive_score
from constants import TAG_EMOJI_MAP


logger = logging.getLogger("Coffee_Recommender")

class RecommendService:
    def __init__(self, api_key: str):
        self.intent_agent = IntentAgent()
        if api_key:
            self.client = genai.Client(api_key=api_key)
        else:
            self.client = None

    def get_embedding(self, text: str) -> Optional[List[float]]:
        try:
            if not self.client: 
                logger.error("❌ Gemini Client 未初始化")
                return None
            
            response = self.client.models.embed_content(
                model='models/gemini-embedding-001',
                contents=text,
                config={'task_type': 'RETRIEVAL_QUERY', 'output_dimensionality': 1536}
            )
            vector = response.embeddings[0].values
            
            if len(vector) == 1536:
                logger.info(f"✅ [AI 語意分析成功] 向量維度: 1536")
            else:
                logger.warning(f"⚠️ 預期維度 1536，但回傳為 {len(vector)}")
            return vector
        except Exception as e:
            logger.error(f"❌ Embedding Error: {e}")
            return None

    async def recommend(self, lat: float, lng: float, user_id: str = None, 
                        user_query: str = None, cafe_tag: str = None,
                        rejected_place_id: str = None,  # 🌟 新增：使用者剛剛拒絕的店家 ID
                        negative_reason: str = None     # 🌟 新增：使用者拒絕的原因
                        ) -> Dict[str, Any]:
        try:
            db = db_client.get_db()
            if db is None: return {"data": []}

            # 🔥檢查到底有沒有收到 user_query
            logger.info(f"🔥 DEBUG: 收到 user_query = '{user_query}'")

            # 🔥 [組員新增] === 1. 座標校正 (支援單點 & 多點中間值定位) ===
            current_search_lat, current_search_lng = lat, lng
            search_query = user_query # 複製一份，避免改到原始資料
            
            if search_query:
                found_coords = []
                
                # 掃描所有可能的關鍵字
                for loc_name, coords in ALL_LOCATIONS.items():
                    if loc_name in search_query:
                        found_coords.append(coords)
                        # 移除地名，避免干擾後續向量搜尋 (例如 "北車中山" -> "")
                        search_query = search_query.replace(loc_name, "").strip()
                
                # 如果有找到地點 (1個或多個)
                if found_coords:
                    # 算出平均經緯度 (中間點)
                    avg_lat = sum([c[0] for c in found_coords]) / len(found_coords)
                    avg_lng = sum([c[1] for c in found_coords]) / len(found_coords)
                    
                    current_search_lat, current_search_lng = avg_lat, avg_lng
                    
                    loc_count = len(found_coords)
                    if loc_count > 1:
                        logger.info(f"📍 [地點切換] 偵測到 {loc_count} 個地點，計算中間點 -> ({avg_lat}, {avg_lng})")
                    else:
                        logger.info(f"📍 [地點切換] 鎖定地點 -> ({avg_lat}, {avg_lng})")

            # 處理剩餘字串：如果地名拿掉後只剩空字串或無意義符號，視為 None
            # 這樣就會自動走 Path B (找附近的店)
            if not search_query or len(search_query) < 2: 
                search_query = None

            user_loc = (current_search_lat, current_search_lng)

            
            # === 2. AI 意圖分析 (時間過濾) ===
            filter_open_now = False
            target_datetime = None
            
            if user_query: # 注意：這裡依然傳入完整的 user_query 給 AI，讓 AI 知道完整情境
                ai_intent = self.intent_agent.analyze_user_intent(user_query)
                logger.info(f"🧠 AI 意圖分析結果: {ai_intent}")
                
                if ai_intent and "time_filter" in ai_intent:
                    tf = ai_intent["time_filter"]
                    filter_open_now = tf.get("filter_open_now", filter_open_now)
                    target_datetime = tf.get("target_iso_datetime", target_datetime)
                    logger.info(f"🕒 AI 判定時間條件 -> 現在營業: {filter_open_now}, 指定時間: {target_datetime}")

            # === 3. 決定檢查時間點 ===
            check_time = None
            if target_datetime:
            # 情況 A：有明確指定未來時間
                try: 
                    check_time = datetime.fromisoformat(target_datetime)
                    logger.info(f"🕒 [時間過濾] 依照 AI 指定時間: {check_time.strftime('%Y-%m-%d %H:%M')}")
                except: 
                    check_time = datetime.now()
            else:
            # 情況 B：沒有指定時間，或是 AI 斷線回傳 {}。一律強制設定為「現在」！
                check_time = datetime.now()
                filter_open_now = True  # 順手把狀態切為 True，維持邏輯一致性
                logger.info(f"🕒 [時間過濾] 未指定時間，預設尋找「現在」有營業的店家: {check_time.strftime('%Y-%m-%d %H:%M')}")
            
            # 定義內部過濾函式
            def filter_by_opening_hours(candidates):
                if not check_time: return candidates
                open_cafes = []
                for cafe in candidates:
                    opening_hours = cafe.get('opening_hours', {})
                    if not opening_hours: continue
                    if opening_hours.get('is_24_hours'):
                        open_cafes.append(cafe)
                        continue
                    if is_google_period_open(opening_hours.get('periods', []), check_time):
                        open_cafes.append(cafe)
                return open_cafes

            # === 4. 取得黑名單 ===
            blacklist_ids = []
            if user_id:
                logs = list(db['interaction_logs'].find({"user_id": user_id, "action": "NO"}, {"place_id": 1}))
                blacklist_ids = [l['place_id'] for l in logs]
            
            # 把它加進這次的黑名單裡，確保它絕對不會在下一秒又被推出來！
            if rejected_place_id and rejected_place_id not in blacklist_ids:
                blacklist_ids.append(rejected_place_id)

            # 把負面原因加入向量搜尋 (Path A 的 Prompt Injection)
            if search_query and negative_reason: # 注意這裡用 search_query
                search_query = f"{search_query}，但請絕對避開「{negative_reason}」的特徵"
                logger.info(f"🛡️ 觸發劇本一：加入避雷特徵的向量搜尋 -> {search_query}")
            
            # 如果沒有原因，但有拒絕的店家，去 DB 抓該店的標籤
            rejected_tags = []
            if rejected_place_id and not negative_reason:
                rejected_cafe = db['cafes'].find_one({"place_id": rejected_place_id}, {"ai_tags": 1})
                if rejected_cafe and 'ai_tags' in rejected_cafe:
                    # 確保拿到的是 list，避免錯誤
                    if isinstance(rejected_cafe['ai_tags'], list):
                        rejected_tags = [t.get('tag', '') for t in rejected_cafe['ai_tags'] if isinstance(t, dict)]
                logger.info(f"🛡️ 觸發劇本二：提取拒絕店家的隱性特徵 -> {rejected_tags}")

            final_data = []

            # === Path A: 向量搜尋 ===
            # 🔥 [組員新增邏輯] 只有在清洗後的 search_query 有值時才跑向量
            if search_query:
                logger.info(f"🔍 [Path A] 啟動向量搜尋: 關鍵字 '{search_query}'")
                query_vector = self.get_embedding(search_query)
                
                if query_vector:
                    logger.info(f"✅ [AI 語意分析成功] 向量維度: {len(query_vector)}")

                    pipeline_vec = [
                        {"$vectorSearch": {
                            "index": "vector_index", "path": "embedding", "queryVector": query_vector,
                            "numCandidates": 100, "limit": 50
                        }},
                        {"$lookup": {
                            "from": "cafes", "localField": "place_id", "foreignField": "place_id", "as": "cafe_info"
                        }},
                        {"$unwind": "$cafe_info"},
                        {"$project": {
                            "place_id": "$cafe_info.place_id",
                            "final_name": "$cafe_info.final_name",
                            "original_name": "$cafe_info.original_name",
                            "location": "$cafe_info.location",
                            "rating": "$cafe_info.total_ratings",
                            "attributes": "$cafe_info.attributes",
                            "ai_tags": "$cafe_info.ai_tags",
                            "tags": "$cafe_info.tags",
                            "vector_score": { "$meta": "vectorSearchScore" },
                            "matched_review": "$content",
                            "opening_hours": "$cafe_info.opening_hours",
                            "contact": "$cafe_info.contact"
                        }}
                    ]
                    
                    if blacklist_ids:
                        pipeline_vec.append({"$match": {"place_id": {"$nin": blacklist_ids}}})

                    raw_results = list(db['reviews'].aggregate(pipeline_vec))
                    logger.info(f"📦 [漏斗監控] 向量搜尋 & lookup 完，初始筆數: {len(raw_results)}")
                    
                    raw_results = filter_by_opening_hours(raw_results)
                    logger.info(f"⏳ [漏斗監控] 時間過濾後，剩餘筆數: {len(raw_results)}")

# ////////////////////////////////
# notion分隔
# ////////////////////////////////

                    filtered_results = []
                    for item in raw_results:
                        if not item.get('location') or 'coordinates' not in item['location']: continue
                        c_loc = (item['location']['coordinates'][1], item['location']['coordinates'][0])
                        dist_meters = geodesic(user_loc, c_loc).meters
                        
                        logger.info(f"📏 店名: {item.get('final_name')} | 距離: {int(dist_meters)}m")

                        if dist_meters <= 3000:
                            item['dist_meters'] = int(dist_meters)
                            hours_until_close = 3.0
                            clicks, keeps, dislikes = 0, 0, 0
                            has_disliked_features = False
                            if rejected_tags:
                                item_tags = [t['tag'] for t in item.get('ai_tags', [])]
                                # 如果這家店的標籤跟被拒絕的店有交集，觸發打 8 折懲罰
                                if set(rejected_tags) & set(item_tags):
                                    has_disliked_features = True
                            # 🌟 呼叫你的 8 維度大腦！
                            item['search_score'] = calculate_comprehensive_score(
                                vec_score=item.get('vector_score', 0.8),
                                rating=item.get('rating', 0) or 0,
                                total_reviews=item.get('total_ratings', 0),
                                dist_meters=dist_meters,
                                dist_to_nearest_mrt=500.0, # (先給預設值，後續階段四再補齊 DB 欄位)
                                hours_until_close=hours_until_close,
                                clicks=clicks, keeps=keeps, dislikes=dislikes,
                                is_new_user=False, # (可透過 user_service 判斷)
                                has_disliked_features=has_disliked_features
                            )
                            filtered_results.append(item)
                        
                        else:
                            logger.info(f"   ❌ 太遠被移除 (>3000m)")
                    
                    logger.info(f"📏 [漏斗監控] 距離 (3000m) 過濾後，最終筆數: {len(filtered_results)}")
                    filtered_results.sort(key=lambda x: x['search_score'], reverse=True)
                    final_data = filtered_results[:10]

                # 🔥 [組員新增] 安全網：如果 Path A 摃龜，強制降級跑 Path B
                if not final_data:
                    logger.info("⚠️ Path A 查無結果，自動降級為 Path B (避免空白)")
                    search_query = None 

            # === Path B: Tag/Geo 搜尋 ===
            # 🔥 [邏輯融合] 結合組員的 search_query 判斷 與 我們的高級 Pipeline
            if not final_data and (cafe_tag or not search_query):
                target_tag = cafe_tag if cafe_tag else ""
                logger.info(f"🌍 [Path B] 啟動地理/標籤搜尋 (Tag: {target_tag if target_tag else '無'})")
                
                pipeline = [
                    {"$geoNear": {
                        "near": {"type": "Point", "coordinates": [current_search_lng, current_search_lat]},
                        "distanceField": "dist_meters", "maxDistance": 3000, "spherical": True
                    }}
                ]
                
                # 🛡️ [維持原版] 保持黑名單過濾
                if blacklist_ids:
                    pipeline.append({"$match": {"place_id": {"$nin": blacklist_ids}}})
                
                if target_tag:
                    pipeline.append({"$match": {"$or": [
                                        {"original_name": {"$regex": target_tag, "$options": "i"}},
                                        {"tags": {"$regex": target_tag, "$options": "i"}}  # 只留最新的神級標籤陣列
                                    ]}})

                # 👑 [維持原版] 放棄組員簡陋的 sort，堅持使用這套神級動態距離衰減算分公式！
                pipeline.append({"$addFields": {
                    "search_score": {
                        "$divide": [{"$ifNull": ["$rating", 0]}, {"$add": [{"$divide": ["$dist_meters", 100]}, 1]}]
                    }
                }})
                pipeline.append({"$sort": {"search_score": -1}})
                pipeline.append({"$limit": 50}) # 維持 50 再去過濾時間

                path_b_results = list(db['cafes'].aggregate(pipeline))
                open_results = filter_by_opening_hours(path_b_results)
                final_data = open_results[:10]

            # === 🔥 [新增] 標籤動態排序與視覺化處理 ===
            def process_display_tags(raw_tags, query_text, btn_tag):
                if not isinstance(raw_tags, list): return []
                
                # 1. 定義黑名單 (絕對不要顯示在 Flex Message 上)
                negative_tags = {"溫度冷", "悶熱", "服務親切", "服務不佳", "服務效率不佳", "停車困難"}
                
                # 2. 定義高價值白名單 (自帶流量的明星標籤)
                high_value_tags = {"工作友善", "不限時", "插座", "Wi-Fi", "深夜", "店貓", "店狗", "老宅", "甜點", "手沖精品"}
                
                # 3. 過濾黑名單
                filtered_tags = [t for t in raw_tags if t not in negative_tags]
                
                # 4. 計算權重
                def get_weight(tag):
                    weight = 0
                    # 絕對優先 (使用者命中)
                    if query_text and tag in query_text: weight += 10
                    if btn_tag and tag == btn_tag: weight += 10
                    # 次要優先 (高價值特徵)
                    if tag in high_value_tags: weight += 5
                    return weight
                
                # 5. 排序並取前 3 個
                sorted_tags = sorted(filtered_tags, key=get_weight, reverse=True)[:3]
                
                # 6. 使用引入的 TAG_EMOJI_MAP 轉成 Emoji 格式 (若字典沒有該 tag，則保持原文字)
                return [TAG_EMOJI_MAP.get(t, t) for t in sorted_tags]
            
            # === 格式化輸出 ===
            formatted_response = []
            for r in final_data:
                # 🎯 挖掘 MongoDB 中的 ratings Object
                db_ratings = r.get("ratings", {})
                rating_val = db_ratings.get("rating", 0.0)
                review_count = db_ratings.get("review_amount", 0)

                formatted_response.append({
                    "place_id": r.get("place_id", str(r.get("_id"))),
                    "final_name": r.get("final_name", "未知店家"),
                    "original_name": r.get("original_name"),
                    "dist_meters": int(r.get("dist_meters", 0)),
                    "rating": rating_val,
                    "display_tags": process_display_tags(r.get("tags", []), search_query, cafe_tag),
                    "attributes": r.get("attributes", {}),
                    "total_ratings": review_count,
                    "match_reason": r.get("matched_review", "符合條件"),
                    # 🔥 [組員新增] 將 opening_hours 傳遞給前端 UI 判斷綠色營業中
                    "opening_hours": r.get("opening_hours", {}),
                    "contact": r.get("contact", {}) 
                })
            return {
                "data": formatted_response,
                "center_lat": current_search_lat,
                "center_lng": current_search_lng
            }

        except Exception as e:
            # 🛡️ [維持原版] 完整錯誤軌跡
            logger.error(f"❌ 推薦服務執行失敗: {e}")
            logger.error(traceback.format_exc()) 
            return {"data": []}