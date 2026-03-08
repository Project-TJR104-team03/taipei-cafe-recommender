# app/services/scoring.py
import math
from datetime import datetime, timedelta
from geopy.distance import geodesic
from locations import ALL_LOCATIONS
from utils import get_taiwan_now
import logging

logger = logging.getLogger("Coffee_Recommender")

def calculate_comprehensive_score(
    vec_score: float,             # 1. 向量相似度 (0.0 ~ 1.0)
    rating: float,                # 2. 原始星級 (0.0 ~ 5.0)
    total_reviews: int,           # 3. 總評論數
    dist_meters: float,           # 4. 絕對距離 (公尺)
    dist_to_nearest_mrt: float,   # 5. 距離最近捷運站 (公尺)
    hours_until_close: float,     # 6. 距離打烊時間 (小時，負數代表已打烊)
    clicks: int = 0,              # 7. 行為指標：點擊查看地圖次數
    keeps: int = 0,               # 8. 行為指標：加入收藏次數
    dislikes: int = 0,            # 🌟 新增 9. 行為指標：點擊不喜歡(不行)次數
    last_recommended_hours: float = float('inf'), # 距離上次推薦過幾小時
    is_new_user: bool = False,    # 是否為新使用者 (用於控制回饋比率)
    global_avg_rating: float = 4.2, # 全局平均星級 (可依據 DB 狀態調整)
    has_disliked_features: bool = False, # 🌟 新增 10. 是否帶有使用者剛剛拒絕的特徵
    user_persona: dict = None,   # ✨ 新增參數
    cafe_tags: list = None       # ✨ 新增參數
) -> dict:
    """
    計算咖啡廳推薦最終加權分數
    支援個人化偏好匹配與隱性特徵懲罰
    """

    # ---------------------------------------------------------
    # 維度 1~3: 綜合品質指標
    # ---------------------------------------------------------
    m = 200.0  
    bayesian_rating = ((total_reviews / (total_reviews + m)) * rating + 
                       (m / (total_reviews + m)) * global_avg_rating)
    s_static = bayesian_rating / 5.0 

    # B. 營業時間充裕度分數
    if hours_until_close >= 3:
        s_time = 1.0
    elif hours_until_close > 0:
        s_time = hours_until_close / 3.0  
    else:
        s_time = 0.0  
        
    # 整合品質分數 (70% 看評價，30% 看營業時間餘裕)
    score_quality = (s_static * 0.7) + (s_time * 0.3)

    # ---------------------------------------------------------
    # 維度 4~5: 綜合地理指標(線性遞減 + 捷運 Bonus)
    # ---------------------------------------------------------
    # A. 絕對距離線性遞減 (以 5000m 為搜索極限)
    # 例如：0m = 1.0分, 1000m = 0.8分, 2500m = 0.5分, 5000m = 0.0分
    s_geo_abs = max(0.0, 1.0 - (dist_meters / 5000.0))
    
    # B. 捷運交通便利性加分 (Bonus 機制)
    # 只要在捷運站 800m 內，最高給予 0.2 的額外加分
    mrt_bonus = 0.0
    if dist_to_nearest_mrt <= 800.0:
        mrt_bonus = 0.2 * (1.0 - (dist_to_nearest_mrt / 800.0))
    
    # 整合地理分數 (主距離 + 捷運加分，最高不超過 1.0)
    score_location = min(1.0, s_geo_abs + mrt_bonus)

    # ---------------------------------------------------------
    # 🌟 維度 6~8: 行為指標 - 雙向平滑升級版
    # ---------------------------------------------------------
    s_personal = 0.0
    
    if not is_new_user and user_persona and cafe_tags:
        pref_tags = user_persona.get("preferred_tags", [])
        avoid_tags = user_persona.get("avoid_tags", [])
        
        # 🎯 中「喜歡」的標籤加分
        match_pref = len(set(pref_tags) & set(cafe_tags))
        s_personal += min(match_pref * 0.5, 1.0)
        
        # 💣 中「討厭」的標籤扣分
        match_avoid = len(set(avoid_tags) & set(cafe_tags))
        s_personal -= min(match_avoid * 0.5, 1.0)
        
        s_personal = max(-1.0, min(1.0, s_personal))

    # ---------------------------------------------------------
    # 維度 9: 冷啟動防護
    # ---------------------------------------------------------
    p_cold = 0.05 if total_reviews < 10 else 0.0

    # ---------------------------------------------------------
    # 動態權重分配
    # ---------------------------------------------------------
    if is_new_user:
        # 新使用者：依賴 AI 語意與客觀評價，不採計行為影響
        w_vec, w_qual, w_loc, w_pers = 0.50, 0.20, 0.30, 0.00
    else:
        # 老使用者：加入行為偏好權重
        w_vec, w_qual, w_loc, w_pers = 0.40, 0.15, 0.30, 0.15

    # 計算初步總分
    base_score = (w_vec * vec_score) + \
                 (w_qual * score_quality) + \
                 (w_loc * score_location) + \
                 (w_pers * s_personal) + \
                 p_cold
   
    # ---------------------------------------------------------
    # 🌟 隱性特徵懲罰 (劇本二)
    # ---------------------------------------------------------
    if has_disliked_features: 
        base_score *= 0.8

    # ---------------------------------------------------------
    # 推薦冷卻期懲罰 
    # ---------------------------------------------------------
    penalty = 1.0
    if last_recommended_hours < 24:
        penalty = 0.1  
    elif last_recommended_hours < 48:
        penalty = 0.5  

    final_score = base_score * penalty
    final_raw = max(0.0, min(1.0, final_score))

    # ✨ 3. 將分數轉化為 100 分制，並計算細項字串給 Log 用
    ui_score = round(final_raw * 100)

    # 將各權重與實際得分轉為百分比 (四捨五入)
    pt_vec = round(w_vec * vec_score * 100)
    pt_qual = round(w_qual * score_quality * 100)
    pt_loc = round(w_loc * score_location * 100)
    pt_pers = round(w_pers * s_personal * 100)
    
    w_vec_100, w_qual_100, w_loc_100, w_pers_100 = round(w_vec*100), round(w_qual*100), round(w_loc*100), round(w_pers*100)

    match_pref_str = "/".join(list(set(user_persona.get("preferred_tags", [])) & set(cafe_tags))) if user_persona and cafe_tags else ""
    match_avoid_str = "/".join(list(set(user_persona.get("avoid_tags", [])) & set(cafe_tags))) if user_persona and cafe_tags else ""

    mrt_bonus_val = mrt_bonus if 'mrt_bonus' in locals() else 0.0

    details_dict = {
        "pt_vec": pt_vec, "w_vec_100": w_vec_100,
        "pt_qual": pt_qual, "w_qual_100": w_qual_100,
        "pt_loc": pt_loc, "w_loc_100": w_loc_100,
        "pt_pers": pt_pers, "w_pers_100": w_pers_100,
        "bayesian_rating": round(bayesian_rating, 1),
        "original_rating": rating,
        "total_reviews": total_reviews,
        "hours_until_close": round(hours_until_close, 1),
        "dist_meters": int(dist_meters),
        "s_geo_abs": round(s_geo_abs, 2),
        "mrt_bonus": round(mrt_bonus_val, 2),
        "mrt_dist": int(dist_to_nearest_mrt),
        "match_pref": match_pref_str if match_pref_str else "無",
        "match_avoid": match_avoid_str if match_avoid_str else "無",
        "p_cold": p_cold,
        "has_disliked_features": has_disliked_features,
        "penalty": penalty
    }

    # ✨ 改為回傳 dict
    return {
        "raw_score": final_raw,
        "ui_score": ui_score,
        "details_dict": details_dict
    }

# =====================================================================
# 🌟 [新增] 推薦引擎專用的算分輔助模組 (從 recommend_service 抽離)
# =====================================================================

def get_hours_until_close(opening_hours: dict, target_time: datetime = None) -> float:
    if not opening_hours: return 3.0 
    if opening_hours.get('is_24_hours'): return 24.0
    
    periods = opening_hours.get('periods', [])
    if not periods: return 3.0
    
    # 如果有傳入目標時間，就用目標時間；否則用現在時間
    ref_time = target_time if target_time else get_taiwan_now()
    current_iso = ref_time.isoweekday()
    current_day = 0 if current_iso == 7 else current_iso
    current_mins = current_day * 24 * 60 + ref_time.hour * 60 + ref_time.minute
    
    for p in periods:
        open_day = int(p.get('day', 0))
        open_val = int(p.get('open', 0)) if p.get('open') is not None else 0
        close_val = p.get('close')
        if close_val is None: continue
        
        # 轉換 HHMM 為分鐘
        def to_mins(v):
            v = int(v)
            if v > 1440 and v != 2359: return (v // 100) * 60 + (v % 100)
            if v == 2359: return 1439
            return v
            
        o_mins = open_day * 24 * 60 + to_mins(open_val)
        c_day = open_day
        if to_mins(close_val) < to_mins(open_val):
            c_day = (open_day + 1) % 7
            
        c_mins = c_day * 24 * 60 + to_mins(close_val)
        if c_mins < o_mins: c_mins += 7 * 24 * 60
        
        check_mins = current_mins
        if current_mins < o_mins and (current_mins + 7 * 24 * 60) < c_mins:
            check_mins += 7 * 24 * 60
            
        if o_mins <= check_mins < c_mins:
            return (c_mins - check_mins) / 60.0 
            
    return 0.0 

def process_and_score_cafes(candidates: list, user_loc: tuple, user_id: str, rejected_tags: list, ignore_time_penalty: bool = False, user_persona: dict = None, recommend_history: dict = None, target_time: datetime = None) -> list:
    """
    統一算分漏斗：無論是哪一條路徑找出的店，都必須經過這裡進行真實數據清洗與算分！
    """
    scored_data = []
    
    for item in candidates:
        # 1. 動態計算距離 (防呆)
        if 'dist_meters' not in item and 'location' in item and 'coordinates' in item['location']:
            c_loc = (item['location']['coordinates'][1], item['location']['coordinates'][0])
            item['dist_meters'] = geodesic(user_loc, c_loc).meters
            
        dist_meters = item.get('dist_meters', 0)
        # 第一層硬過濾：超過 5 公里直接淘汰 (除非是精準搜尋店名)
        if dist_meters > 5000 and item.get('match_type') != 'name': 
            continue 
        
        # 2. 傳入目標時間進行精算
        hours_until_close = get_hours_until_close(item.get('opening_hours', {}), target_time)
        
        # 如果是純粹的「深夜免死金牌」(無指定未來時間)，無條件給予時間滿分，不懲罰！
        if ignore_time_penalty and not target_time: 
            hours_until_close = 3.0

        # 加固防線：
        # 條件 1：不是找特定店名、條件 2：沒有「深夜」或「指定時間」的免死金牌、條件 3：目前沒營業
        if item.get('match_type') != 'name' and not ignore_time_penalty and hours_until_close <= 0:
            continue

        # 正確從資料庫結構中挖出星星與評論數，並存入 item 中
        db_ratings = item.get("ratings", {})
        item['real_rating'] = db_ratings.get("rating", item.get("rating", 0.0))
        item['real_reviews'] = db_ratings.get("review_amount", item.get("user_ratings_total", item.get("total_ratings", 0)))

        # 3. 互動數據
        stats = item.get('stats', {})
        clicks, keeps, dislikes = stats.get('clicks', 0), stats.get('keeps', 0), stats.get('dislikes', 0)
        
        # 4. 真實捷運距離計算 (搭配 locations.py 字典)
        mrt_dist = item.get('mrt_distance', item.get('attributes', {}).get('mrt_distance'))
        if mrt_dist is None:
            min_mrt_dist = float('inf')
            if 'location' in item and 'coordinates' in item['location']:
                c_loc = (item['location']['coordinates'][1], item['location']['coordinates'][0])
                for loc_name, loc_coords in ALL_LOCATIONS.items():
                    if "站" in loc_name:
                        d = geodesic(c_loc, loc_coords).meters
                        if d < min_mrt_dist: min_mrt_dist = d
            mrt_dist = min_mrt_dist if min_mrt_dist != float('inf') else 800.0
            
        # 5. 使用者狀態與避雷
        is_new = False if user_id else True
        has_disliked_features = False
        
        # ✨ 安全萃取該店家的 tags 給 AI Persona 算分用
        cafe_tags = item.get("tags", [])
        if not cafe_tags and "ai_tags" in item:
            cafe_tags = [t.get("tag", "") for t in item.get("ai_tags", []) if isinstance(t, dict)]

        if rejected_tags:
            if set(rejected_tags) & set(cafe_tags): has_disliked_features = True

        # 🌟 取出該店家的冷卻時間 (新增)
        last_rec_hours = recommend_history.get(item.get('place_id'), float('inf')) if recommend_history else float('inf')

        # 6. 分流算分：判斷是「指定店名」還是「AI 推薦」
        shop_name = item.get("final_name", "未知店家")

        if item.get('match_type') == 'name':
            
            # 如果因為特殊要求 (如找半夜) 發動了免死金牌，或者目前有營業，給予營業加分
            open_bonus = 500.0 if hours_until_close > 0 else 0.0 
            item['search_score'] = 1000.0 - (dist_meters / 10.0) + open_bonus + 1000.0
            item['ui_score'] = 100 # 指定店名直接給 100 分
            item['score_details_dict'] = {}
            
        else:
            # 🧠 正常 AI 推薦漏斗 (Path A / B 專屬)：
            # 乖乖跑 8 維度綜合評估大腦
            score_data = calculate_comprehensive_score(
                vec_score=item.get('vector_score', 0.8),
                rating=item.get('real_rating', 0) or 0,
                total_reviews=item.get('real_reviews', 0),
                dist_meters=dist_meters,
                dist_to_nearest_mrt=mrt_dist,
                hours_until_close=hours_until_close,
                clicks=clicks, keeps=keeps, dislikes=dislikes,
                is_new_user=is_new,
                has_disliked_features=has_disliked_features,
                last_recommended_hours=last_rec_hours,
                user_persona=user_persona, # ✨ 傳入 Persona
                cafe_tags=cafe_tags        # ✨ 傳入 Tags
            )
            item['search_score'] = score_data['raw_score']
            item['ui_score'] = score_data['ui_score']
            item['score_details_dict'] = score_data['details_dict']
            
        scored_data.append(item)
        
    # 統一依照算好的分數 (search_score) 由高到低排序，並只切出前 3 名出菜！
    scored_data.sort(key=lambda x: x.get('search_score', 0), reverse=True)
    top_3_cafes = scored_data[:3]

    # 🌟 終極版 One-Line-Per-Category 極簡 Log
    logger.info("============== 🏆 最終推薦榜單 (前 3 名) ==============")
    for rank, cafe in enumerate(top_3_cafes, 1):
        name = cafe.get("final_name", "未知店家")
        score = cafe.get("ui_score", 0)
        d = cafe.get("score_details_dict", {})
        
        if not d: 
            logger.info(f"Top {rank} | ☕ {name} | ⭐️ 總分: {score} (精準店名直達)")
            continue
            
        macro = round(cafe.get('macro_score', 0) * 100)
        micro = round(cafe.get('micro_score', 0) * 100)
        
        hrs = d.get('hours_until_close', 0)
        open_str = f"滿分(剩{hrs}h)" if hrs >= 3 else f"遞減(剩{hrs}h)"
        if hrs >= 24: open_str = "滿分(24h)"
        
        pen = d.get('penalty', 1.0)
        pen_str = "無" if pen == 1.0 else f"觸發(x{pen})"
        
        logger.info(f"Top {rank} | ☕ {name} | ⭐️ 總分: {score}")
        logger.info(f" ┣ 🧠 意圖({d.get('w_vec_100',0)}%): {d.get('pt_vec',0)}分 │ 店家總結: {macro}, 網友評論: {micro}")
        if cafe.get('match_type') == 'vector':
            hit_review = str(cafe.get("matched_review", "")).replace("\n", " ").strip()
            hit_rev_short = (hit_review[:45] + "...") if len(hit_review) > 45 else (hit_review or "無")
            hit_summary = str(cafe.get("summary", "")).replace("\n", " ").strip()
            hit_sum_short = (hit_summary[:45] + "...") if len(hit_summary) > 45 else (hit_summary or "無")
            logger.info(f" ┣ 💬 語意擷取     │ 評: {hit_rev_short}")
            logger.info(f" ┃                 │ 總: {hit_sum_short}")
        logger.info(f" ┣ 🌟 評價({d.get('w_qual_100',0)}%): {d.get('pt_qual',0)}分 │ 貝氏: {d.get('bayesian_rating',0)} (原{d.get('original_rating',0)}星/{d.get('total_reviews',0)}評), 營業: {open_str}")
        logger.info(f" ┣ 📍 地理({d.get('w_loc_100',0)}%): {d.get('pt_loc',0)}分 │ 距離: {d.get('dist_meters',0)}m(得{d.get('s_geo_abs',0)}), 捷運: {d.get('mrt_dist',0)}m(加{d.get('mrt_bonus',0)})")
        logger.info(f" ┣ 💖 偏好({d.get('w_pers_100',0)}%): {d.get('pt_pers',0)}分 │ 命中喜好: {d.get('match_pref','無')}, 命中地雷: {d.get('match_avoid','無')}")
        logger.info(f" ┗ 🛡️ 調整機制     │ 冷啟動: +{d.get('p_cold',0)}, 隱性地雷: {'觸發(x0.8)' if d.get('has_disliked_features') else '無'}, 冷卻期: {pen_str}")
    logger.info("-------------------------------------------------------------")

    return top_3_cafes