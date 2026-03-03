# app/services/scoring.py
import math
from datetime import datetime, timedelta
from geopy.distance import geodesic
from locations import ALL_LOCATIONS

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
) -> float:
    """
    計算咖啡廳推薦最終加權分數
    支援正負向行為扣分與隱性特徵懲罰
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
    # 維度 4~5: 綜合地理指標
    # ---------------------------------------------------------
    # A. 絕對距離衰減 (1500m 為基準)
    s_geo_abs = math.exp(-dist_meters / 1500.0)
    
    # B. 捷運交通便利性衰減 (500m 為基準，精華區)
    s_geo_mrt = math.exp(-dist_to_nearest_mrt / 500.0)
    
    # 整合地理分數 (60% 看實際距離，40% 看捷運便利性)
    score_location = (s_geo_abs * 0.6) + (s_geo_mrt * 0.4)

    # ---------------------------------------------------------
    # 🌟 維度 6~8: 行為指標 - 雙向平滑升級版
    # ---------------------------------------------------------
    # 計算互動淨值 (Score_act)
    score_act = (keeps * 3.0) + (clicks * 1.0) - (dislikes * 2.0)
    
    # 雙向正規化公式
    if score_act == 0:
        s_behavior = 0.0
    else:
        # 取符號 (1.0 或是 -1.0)
        sign = 1.0 if score_act > 0 else -1.0
        # 套用平滑衰減公式 (確保分數界於 -1.0 ~ 1.0 之間)
        s_behavior = sign * (1.0 - math.exp(-abs(score_act) / 10.0))

    # ---------------------------------------------------------
    # 維度 9: 冷啟動防護
    # ---------------------------------------------------------
    p_cold = 0.05 if total_reviews < 10 else 0.0

    # ---------------------------------------------------------
    # 動態權重分配
    # ---------------------------------------------------------
    if is_new_user:
        # 新使用者：依賴 AI 語意與客觀評價，不採計行為影響
        w_vec, w_qual, w_loc, w_beh = 0.50, 0.20, 0.30, 0.00
    else:
        # 老使用者：加入行為偏好權重
        w_vec, w_qual, w_loc, w_beh = 0.40, 0.15, 0.30, 0.15

    # 計算初步總分
    base_score = (w_vec * vec_score) + \
                 (w_qual * score_quality) + \
                 (w_loc * score_location) + \
                 (w_beh * s_behavior) + \
                 p_cold
                 
    # ---------------------------------------------------------
    # 🌟 [究極進化] AI Persona 靈魂加成系統
    # ---------------------------------------------------------
    if user_persona and cafe_tags:
        pref_tags = user_persona.get("preferred_tags", [])
        avoid_tags = user_persona.get("avoid_tags", [])
        
        match_pref = len(set(pref_tags) & set(cafe_tags))
        base_score += min(match_pref * 0.05, 0.15)
        
        match_avoid = len(set(avoid_tags) & set(cafe_tags))
        base_score -= (match_avoid * 0.15)

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

    # 確保最終分數落在 0.0 ~ 1.0 之間
    return max(0.0, min(1.0, final_score))


# =====================================================================
# 🌟 [新增] 推薦引擎專用的算分輔助模組 (從 recommend_service 抽離)
# =====================================================================

def get_hours_until_close(opening_hours: dict) -> float:
    if not opening_hours: return 3.0 
    if opening_hours.get('is_24_hours'): return 24.0
    
    periods = opening_hours.get('periods', [])
    if not periods: return 3.0
    
    tw_now = datetime.utcnow() + timedelta(hours=8)
    current_iso = tw_now.isoweekday()
    current_day = 0 if current_iso == 7 else current_iso
    current_mins = current_day * 24 * 60 + tw_now.hour * 60 + tw_now.minute
    
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

def process_and_score_cafes(candidates: list, user_loc: tuple, user_id: str, rejected_tags: list, ignore_time_penalty: bool = False, user_persona: dict = None) -> list:
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
        
        # 2. 真實營業時間
        if ignore_time_penalty:
            hours_until_close = 3.0 
        else:
            hours_until_close = get_hours_until_close(item.get('opening_hours', {}))
        
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

        # 6. 分流算分：判斷是「指定店名」還是「AI 推薦」
        if item.get('match_type') == 'name':
            
            # 如果因為特殊要求 (如找半夜) 發動了免死金牌，或者目前有營業，給予營業加分
            open_bonus = 500.0 if hours_until_close > 0 else 0.0 
            
            item['search_score'] = 1000.0 - (dist_meters / 10.0) + open_bonus
            
        else:
            # 🧠 正常 AI 推薦漏斗 (Path A / B 專屬)：
            # 乖乖跑 8 維度綜合評估大腦
            item['search_score'] = calculate_comprehensive_score(
                vec_score=item.get('vector_score', 0.8),
                rating=item.get('rating', 0) or 0,
                total_reviews=item.get('total_ratings', 0),
                dist_meters=dist_meters,
                dist_to_nearest_mrt=mrt_dist,
                hours_until_close=hours_until_close,
                clicks=clicks, keeps=keeps, dislikes=dislikes,
                is_new_user=is_new,
                has_disliked_features=has_disliked_features,
                user_persona=user_persona, # ✨ 傳入 Persona
                cafe_tags=cafe_tags        # ✨ 傳入 Tags
            )        
        
        # 👑 特權：精準店名搜尋，給予超級加分確保排在第一，但依然會被營業時間扣分！
        if item.get('match_type') == 'name':
            item['search_score'] += 1000.0
            
        scored_data.append(item)
        
    # 統一依照算好的分數 (search_score) 由高到低排序，並只切出前 10 名出菜！
    scored_data.sort(key=lambda x: x.get('search_score', 0), reverse=True)
    return scored_data[:10]