# app/services/recommend_service.py
import logging
import traceback
import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional
import json
from vertexai.language_models import TextEmbeddingInput, TextEmbeddingModel
from database import db_client
from utils import is_google_period_open
from locations import ALL_LOCATIONS
from agents.intent_agent import IntentAgent
from agents.reason_agent import ReasonAgent
from google import genai 
from services.scoring import process_and_score_cafes
from datetime import datetime, timedelta  


logger = logging.getLogger("Coffee_Recommender")

class RecommendService:
    def __init__(self):
        self.intent_agent = IntentAgent()
        self.reason_agent = ReasonAgent()
        
        # 初始化 Vertex AI 的向量模型
        try:
            # 使用 Google 最新一代的企業級文本嵌入模型
            self.embedding_model = TextEmbeddingModel.from_pretrained("gemini-embedding-001")
            logger.info("✅ Vertex AI Embedding 模型初始化成功！")
        except Exception as e:
            logger.error(f"❌ Vertex AI Embedding 初始化失敗: {e}")
            self.embedding_model = None

    def get_embedding(self, text: str) -> Optional[List[float]]:
        try:
            if not self.embedding_model: 
                logger.error("❌ Embedding 模型未準備好")
                return None
            
            # Vertex AI 的標準寫法
            inputs = [TextEmbeddingInput(text, "RETRIEVAL_QUERY")]

            # 呼叫模型，指定使用1536維度
            embeddings = self.embedding_model.get_embeddings(inputs, output_dimensionality=1536)
            vector = embeddings[0].values

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
                        negative_reason: str = None,     # 🌟 新增：使用者拒絕的原因
                        theme: str = None
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

            # 🔥 [修正] 處理剩餘字串：濾除無意義贅字，判斷是否還有實質搜尋價值
            if search_query:
                # 1. 定義常見的無意義贅字 (可依據使用者習慣自由擴充)
                stop_words = [
                    "附近", "推薦", "有沒有", "有", "的", "咖啡廳", "咖啡店", 
                    "店", "幫我找", "在哪", "哪裡", "一下", "我想去", "我想找", "台北"
                ]
                
                # 2. 建立一個測試用字串，把贅字全部拔掉
                test_query = search_query
                for word in stop_words:
                    test_query = test_query.replace(word, "")
                test_query = test_query.strip()
                
                # 3. 如果拿掉地名和贅字後，剩下的字是空的或太短 (例如只剩標點符號)，就放棄語意搜尋
                if not test_query or len(test_query) < 2: 
                    logger.info(f"🧹 [字串清理] 扣除地名後剩餘 '{search_query}' 缺乏明確特徵，跳過 AI，直接交給 Path B (純地理搜尋)")
                    search_query = None

            user_loc = (current_search_lat, current_search_lng)

            
            # === 2. AI 意圖分析 (時間過濾) ===
            filter_open_now = False
            target_datetime = None
            ai_intent = {}
            
            if user_query: # 注意：這裡依然傳入完整的 user_query 給 AI，讓 AI 知道完整情境
                ai_intent = self.intent_agent.analyze_user_intent(user_query)
                # logger.info(f"🧠 AI 意圖分析結果: {ai_intent}")
                
                if ai_intent and ai_intent.get("has_time"):
                    target_datetime = ai_intent.get("target_time")
                    filter_open_now = False # 既然有指定未來時間，就不該強制要求「現在」有營業
                    logger.info(f"🕒 AI 判定明確時間條件: {target_datetime}")
                else:
                    logger.info("🕒 AI 判定沒有指定特定時間。")

            # 🌟 [新增] 深夜特權：如果是在找深夜咖啡廳，強制關閉營業時間過濾！
            is_midnight_search = False
            if (cafe_tag and "深夜" in cafe_tag) or (search_query and "深夜" in search_query):
                is_midnight_search = True

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
            # 情況 B：沒有指定時間
                if is_midnight_search:
                    logger.info("🌙 [深夜特權] 偵測到「深夜」標籤，強制關閉營業時間檢查！")
                    check_time = None
                    filter_open_now = False
                else:
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

            # === 4. 取得雙軌黑名單與 AI Persona ===
            blacklist_ids = []
            user_persona = {}
            if user_id:
                user_info = db['users'].find_one({"user_id": user_id}) or {}

                # 軌道 A：永久黑名單
                permanent_blacklist = user_info.get("blacklist", [])
                if permanent_blacklist:
                    blacklist_ids.extend(permanent_blacklist)

                # 提取 AI 畫像
                user_persona = user_info.get("ai_persona", {})

                # 軌道 B：48 小時冷卻名單 (Soft Ban)
                forty_eight_hours_ago = datetime.now() - timedelta(hours=48)
                cooldown_logs = list(db['interaction_logs'].find({
                    "user_id": user_id,
                    "action": {"$in": ["COOLDOWN", "NO_REASON", "NO"]},
                    "created_at_server": {"$gte": forty_eight_hours_ago} # ✨ 關鍵魔法：只抓最近 48 小時
                }, {"place_id": 1}))

                if cooldown_logs:
                    blacklist_ids.extend([l['place_id'] for l in cooldown_logs if l.get('place_id')])

                blacklist_ids = list(set(blacklist_ids)) # 去除重複

            # 👇👇👇 以下是你原本的超強即時避雷功能，絕對不能刪，我幫你完美保留了！ 👇👇👇

            # 把它加進這次的黑名單裡，確保它絕對不會在下一秒又被推出來！
            if rejected_place_id and rejected_place_id not in blacklist_ids:
                blacklist_ids.append(rejected_place_id)

            # 把負面原因加入向量搜尋 (Path A 的 Prompt Injection)
            if search_query and negative_reason: 
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

            final_candidates = [] # 🌟 所有路徑找出來的候選名單，通通丟進這裡，先不算分！
            # === 🔥 [新增] Path C: 情境高速公路 (分數優先) ===
            final_data = [] # 用來裝最後排好序的店家
            
            if theme:
                logger.info(f"🚀 [Path C] 啟動情境直達車: {theme}")
                score_field = f"score_{theme}"
                
                pipeline_c = [
                    {"$geoNear": {
                        "near": {"type": "Point", "coordinates": [current_search_lng, current_search_lat]},
                        "distanceField": "dist_meters", "maxDistance": 3000, "spherical": True
                    }},
                    {"$match": {score_field: {"$gt": 0.4}}}
                ]
                if blacklist_ids: 
                    pipeline_c.append({"$match": {"place_id": {"$nin": blacklist_ids}}})
                
                pipeline_c.append({"$sort": {score_field: -1}})
                pipeline_c.append({"$limit": 30}) 
                
                path_c_results = list(db['cafes'].aggregate(pipeline_c))
                open_results = filter_by_opening_hours(path_c_results)
                
                final_data = open_results[:10]
                logger.info(f"🏆 情境直達車篩選完成，選出 {len(final_data)} 家神店。")

            # === Path 0: 店名精準直達車 ===
            if search_query and not theme: # 🛡️ 防護罩 1：情境搜尋直接跳過
                import re 
                
                search_names = [search_query]
                
                # 🌟 [神級進化 1：碎紙機還原術]
                if ai_intent and ai_intent.get("extracted_keywords"):
                    extracted = ai_intent["extracted_keywords"]
                    reconstructed = " ".join(extracted)
                    if len(extracted) > 1 and reconstructed.lower() in search_query.lower():
                        search_names.append(reconstructed)
                    else:
                        search_names.extend(extracted)
                
                search_names = list(set(search_names))
                
                # 🛡️ [神級防呆 3：氾濫單字黑名單]
                forbidden_exact_words = {"cafe", "coffee", "咖啡", "咖啡廳", "咖啡店", "店名", "餐廳", "推薦", "附近", "台北"}
                logger.info(f"🔎 [Path 0] 準備比提這些可能店名: {search_names}")
                
                # 🌟 [神級進化 2：模糊比對正則魔法]
                name_or_conditions = []
                for n in search_names:
                    clean_n = n.strip()
                    if len(clean_n) >= 2 and clean_n.lower() not in forbidden_exact_words: 
                        name_or_conditions.append({"final_name": {"$regex": f"^{clean_n}$", "$options": "i"}})
                        name_or_conditions.append({"original_name": {"$regex": f"^{clean_n}$", "$options": "i"}})
                        
                        fuzzy_pattern = ".*".join(re.split(r'[\s\-]+', clean_n))
                        name_or_conditions.append({"final_name": {"$regex": fuzzy_pattern, "$options": "i"}})
                        name_or_conditions.append({"original_name": {"$regex": fuzzy_pattern, "$options": "i"}})
                
                if name_or_conditions:
                    name_pipeline = [
                        {"$geoNear": {
                            "near": {"type": "Point", "coordinates": [current_search_lng, current_search_lat]},
                            "distanceField": "dist_meters", "maxDistance": 50000, "spherical": True 
                        }},
                        {"$match": {"$or": name_or_conditions}}
                    ]
                    if blacklist_ids: name_pipeline.append({"$match": {"place_id": {"$nin": blacklist_ids}}})
                    name_pipeline.append({"$limit": 5})
                    
                    name_results = list(db['cafes'].aggregate(name_pipeline))
                    
                    if name_results:
                        logger.info(f"🎯 [Path 0] 精準命中店家: {len(name_results)} 家")
                        for item in name_results: 
                            item['match_type'] = 'name' 
                        final_candidates = name_results

            # === Path B + A: 先執行Tag篩選再進行向量搜尋 (雙引擎並行架構) ===
            logger.info(f"🌍 [預先篩選] 啟動地理/標籤搜尋作為基底...")
            tag_list = []
            if cafe_tag: tag_list.extend([t.strip() for t in cafe_tag.split(",")])
            if ai_intent and ai_intent.get("extracted_keywords"):
                # 把 AI 抓到的關鍵字也當作標籤去碰碰運氣
                tag_list.extend(ai_intent["extracted_keywords"])
                
            # 建立基底過濾條件 (方圓 5 公里內)
            geo_pipeline = [
                {"$geoNear": {
                    "near": {"type": "Point", "coordinates": [current_search_lng, current_search_lat]},
                    "distanceField": "dist_meters", 
                    "maxDistance": 5000, # 方圓 5 公里
                    "spherical": True
                }}
            ]
            
            # 黑名單與標籤過濾
            if blacklist_ids: 
                geo_pipeline.append({"$match": {"place_id": {"$nin": blacklist_ids}}})
            
            strict_pipeline = list(geo_pipeline)
            if tag_list: 
                and_conditions = []

                for t in tag_list:
                    # 針對「每一個標籤 (t)」，只要滿足以下三個條件的「其中一個」就算過關：
                    # 1. features 裡面這個屬性是 True
                    # 2. scores 裡面這個屬性的分數 >= 0.5
                    # 3. tags 陣列裡面直接包含這個字
                    tag_match = {
                        "$or": [
                            {f"features.{t}": True},
                            {f"scores.{t}": {"$gte": 0.5}},
                            {"tags": t}
                        ]
                    }
                    and_conditions.append(tag_match)

                    # 將所有標籤的條件組合起來 (使用 $and 代表使用者搜尋的多個標籤必須「同時」滿足)
                # 💡 如果您覺得同時滿足太嚴，可以把外層的 "$and" 改成 "$or"
                strict_pipeline.append({"$match": {"$and": and_conditions}})

            strict_pipeline.append({"$limit": 250})
            
            # 取得基底名單
            base_candidates = list(db['cafes'].aggregate(strict_pipeline))
            
            if len(base_candidates) < 15:
                logger.warning(f"⚠️ [降級機制] 嚴格標籤篩選後僅剩 {len(base_candidates)} 家，條件太嚴苛！放寬為純地理範圍搜尋。")
                
                loose_pipeline = list(geo_pipeline)
                loose_pipeline.append({"$limit": 250}) # 只看距離，不管標籤了
                
                # 重新用寬鬆條件抓取候選名單
                base_candidates = list(db['cafes'].aggregate(loose_pipeline))

            # 萃取這批合法店家的 place_id
            valid_place_ids = [doc['place_id'] for doc in base_candidates]
            logger.info(f"🛡️ [預先篩選] 成功圈出 {len(valid_place_ids)} 家符合距離與基本標籤的店家。")

            # === 🎯 步驟二：再執行 Path A (從合格名單中做向量語意排序) ===
            if search_query and valid_place_ids and not theme:
                logger.info(f"🔍 [精確打擊] 在 {len(valid_place_ids)} 家合格店中，尋找最符合 '{search_query}' 的語意...")
                query_vector = self.get_embedding(search_query)
                
                if query_vector:
                    # 故意把向量搜尋的範圍拉大 (numCandidates: 200, limit: 100)
                    pipeline_macro = [
                        {"$vectorSearch": {"index": "vector_index", "path": "vector", "queryVector": query_vector, "numCandidates": 200, "limit": 100}},
                        # 🌟 最關鍵的一行：Post-filtering (後置過濾)，只留下剛剛 Path B 找出來的那些店！
                        {"$match": {"place_id": {"$in": valid_place_ids}}},
                        {"$project": {"place_id": 1, "macro_score": { "$meta": "vectorSearchScore" }, "summary": "$scores.summary"}}
                    ]
                    
                    pipeline_micro = [
                        {"$vectorSearch": {"index": "vector_index", "path": "embedding", "queryVector": query_vector, "numCandidates": 200, "limit": 100}},
                        {"$match": {"place_id": {"$in": valid_place_ids}}},
                        {"$project": {"place_id": 1, "micro_score": { "$meta": "vectorSearchScore" }, "matched_review": "$content"}}
                    ]
                    async def fetch_macro(): return list(db['cafes'].aggregate(pipeline_macro))
                    async def fetch_micro(): return list(db['reviews'].aggregate(pipeline_micro))

                    logger.info("⚡ 啟動平行檢索 (Macro + Micro)...")
                    macro_results, micro_results = await asyncio.gather(fetch_macro(), fetch_micro())
                    logger.info(f"📦 檢索完成: 總結命中 {len(macro_results)} 筆, 評論命中 {len(micro_results)} 筆")

                    fusion_dict = {}
                    for doc in macro_results: fusion_dict[doc["place_id"]] = {"place_id": doc["place_id"], "macro_score": doc["macro_score"], "micro_score": 0.0, "summary": doc.get("summary", ""), "matched_review": ""}
                    for doc in micro_results:
                        pid = doc["place_id"]
                        if pid not in fusion_dict: fusion_dict[pid] = {"place_id": pid, "macro_score": 0.0, "micro_score": doc["micro_score"], "summary": "", "matched_review": doc.get("matched_review", "")}
                        else:
                            if doc["micro_score"] > fusion_dict[pid]["micro_score"]:
                                fusion_dict[pid]["micro_score"] = doc["micro_score"]
                                fusion_dict[pid]["matched_review"] = doc.get("matched_review", "")

                    fused_place_ids = list(fusion_dict.keys())
                    raw_cafes = list(db['cafes'].find({"place_id": {"$in": fused_place_ids}}))
                    
                    raw_results = []
                    for cafe_info in raw_cafes:
                        pid = cafe_info["place_id"]
                        fusion_data_item = fusion_dict[pid]
                        cafe_info['vector_score'] = (fusion_data_item["macro_score"] * 0.4) + (fusion_data_item["micro_score"] * 0.6)
                        cafe_info['summary'] = fusion_data_item["summary"] if fusion_data_item["summary"] else cafe_info.get("scores", {}).get("summary", "")
                        cafe_info['matched_review'] = fusion_data_item["matched_review"]
                        cafe_info['match_type'] = 'vector' 
                        raw_results.append(cafe_info)

                    raw_results = filter_by_opening_hours(raw_results)
                    logger.info(f"⏳ [漏斗監控] 時間過濾後，剩餘筆數: {len(raw_results)}")
                    final_candidates = raw_results

            if not final_candidates and base_candidates:
                logger.info("⚠️ 跳過向量搜尋，直接使用地理與標籤篩選結果")
                final_candidates = base_candidates

            # 🌟🌟🌟 === 終極交接：呼叫外部的統一算分漏斗 === 🌟🌟🌟
            if not theme: # 🛡️ 防護罩 4：情境搜尋已經自己排好前10名，不需要過這個漏斗！
                logger.info(f"🚚 準備將 {len(final_candidates)} 家候選名單送入統一算分漏斗...")
                ignore_time = is_midnight_search or (target_datetime is not None)
                final_data = process_and_score_cafes(
                    candidates=final_candidates,
                    user_loc=(current_search_lat, current_search_lng),
                    user_id=user_id,
                    rejected_tags=rejected_tags,
                    ignore_time_penalty=ignore_time,
                    user_persona=user_persona
                )
                logger.info(f"🏆 算分完成！最終選出 {len(final_data)} 家推薦名單。")



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
                
                # 🚀 直接回傳乾淨的字串陣列！
                return sorted_tags
            
            # === 🔥 [新增] 智能分流：讓 AI 動態生成客製化推薦理由 ===
            personalized_reasons = {}
            
            # 🧠 關鍵邏輯：只有在「有輸入文字 (search_query)」且「不是點擊情境按鈕 (not theme)」時，才呼叫 AI 大魔王
            if search_query and not theme and final_data:
                logger.info(f"🧠 [智能分流] 偵測到複雜文字需求 '{search_query}'，啟動 AI 客製化理由生成...")
                try:
                    # 呼叫外包出去的 ReasonAgent
                    personalized_reasons = await self.reason_agent.generate_reasons_batch(search_query, final_data)
                except Exception as e:
                    logger.error(f"⚠️ AI 生成理由失敗，將自動退回預設文字: {e}")
            else:
                logger.info("⚡ [智能分流] 點擊情境按鈕或無複雜需求，跳過 AI 生成以確保極速體驗！")
 
            # === 格式化輸出 ===
            formatted_response = []
            for r in final_data:
                # 🎯 挖掘 MongoDB 中的 ratings Object
                db_ratings = r.get("ratings", {})
                rating_val = db_ratings.get("rating", r.get("rating", 0.0))
                review_count = db_ratings.get("review_amount", r.get("total_ratings", 0))
                place_id_str = str(r.get("place_id", r.get("_id")))
                
                # ✨ [新增] 動態抽換標籤：如果是情境搜尋，讀取專屬 tags！
                if theme:
                    theme_tags_field = f"tags_{theme}"
                    raw_theme_tags = r.get(theme_tags_field, [])
                    # 把資料庫裡的字串加上 Emoji (透過 TAG_EMOJI_MAP)
                    final_display_tags = raw_theme_tags[:3]
                else:
                    final_display_tags = process_display_tags(r.get("tags", []), search_query, cafe_tag)
                
        
                # 取得預設的 summary 或 matched_review 作為備援
                raw_summary = r.get("summary", r.get("scores", {}).get("summary", ""))
                if not raw_summary: raw_summary = r.get("matched_review", "")
                
                # 🌟 終極版智能分流顯示邏輯：
                if theme or not search_query:
                    # 情況 A：點擊情境按鈕，或「單純傳送定位」時 -> 強制給空字串，隱藏文字區塊，版面極簡化！
                    custom_reason = ""
                else:
                    # 情況 B：如果是手動打字 -> 優先拿 AI 寫好的客製化理由，如果 AI 失敗再退回 raw_summary。
                    custom_reason = personalized_reasons.get(place_id_str, raw_summary)


                formatted_response.append({
                    "place_id": r.get("place_id", str(r.get("_id"))),
                    "final_name": r.get("final_name", "未知店家"),
                    "original_name": r.get("original_name"),
                    "dist_meters": int(r.get("dist_meters", 0)),
                    "rating": rating_val,
                    "display_tags": final_display_tags,
                    "attributes": r.get("attributes", {}),
                    "total_ratings": review_count,
                    "match_reason": r.get("matched_review", "符合條件"),
                    # 🔥 [組員新增] 將 opening_hours 傳遞給前端 UI 判斷綠色營業中
                    "opening_hours": r.get("opening_hours", {}),
                    "contact": r.get("contact", {}) ,
                    "custom_reason": custom_reason # ✨ 把 AI 寫好的這句話傳給前端
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