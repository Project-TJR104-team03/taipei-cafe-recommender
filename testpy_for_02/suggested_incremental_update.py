# ==========================================
# [增量更新建議] 根據統計頻率自動生成
# 包含統計次數 ([Count: XX]) 供決策參考
# ==========================================

# --- [既有標籤擴充] (原本就有，補上新關鍵字) ---
NORM_RULES['日式風格'].extend(['日式風格']) # [Count: 26]
NORM_RULES['限時'].extend(['限時90分鐘', '用餐限時']) # [Count: 18]

# --- [全新標籤新增] (原本沒有，建議加入系統) ---
NORM_RULES.update({ '平價': ['平價', '價格親民'] }) # [Count: 34
FEATURE_DEFINITION.update({ '平價': ('affordable', True) })
NORM_RULES.update({ '淺焙咖啡': ['淺焙咖啡'] }) # [Count: 19]
FEATURE_DEFINITION.update({ '淺焙咖啡': ('light_roast_coffee', True) })
NORM_RULES.update({ '虹吸咖啡': ['虹吸咖啡', '虹吸式咖啡'] }) # [Count: 32]
FEATURE_DEFINITION.update({ '虹吸咖啡': ('siphon_coffee', True) })
NORM_RULES.update({ '可麗露': ['可麗露'] }) # [Count: 16]
FEATURE_DEFINITION.update({ '可麗露': ('canelé', True) })
NORM_RULES.update({ '自助式': ['自助式', '自助點餐機'] }) # [Count: 28]
FEATURE_DEFINITION.update({ '自助式': ('self_service', True) })
NORM_RULES.update({ '肉桂捲': ['肉桂捲'] }) # [Count: 15]
FEATURE_DEFINITION.update({ '肉桂捲': ('cinnamon_roll', True) })
NORM_RULES.update({ '工業風裝潢': ['工業風裝潢', '工業風'] }) # [Count: 26]
FEATURE_DEFINITION.update({ '工業風裝潢': ('industrial_decor', True) })
NORM_RULES.update({ '包廂': ['包廂'] }) # [Count: 15]
FEATURE_DEFINITION.update({ '包廂': ('private_room', True) })
NORM_RULES.update({ '咖啡豆種類多': ['咖啡豆種類多', '咖啡豆販售', '台灣咖啡豆'] }) # [Count: 32]
FEATURE_DEFINITION.update({ '咖啡豆種類多': ('multiple_coffee_bean_types', True) })
NORM_RULES.update({ '老店': ['老店'] }) # [Count: 14]
FEATURE_DEFINITION.update({ '老店': ('old_shop', True) })
NORM_RULES.update({ '貝果': ['貝果'] }) # [Count: 13]
FEATURE_DEFINITION.update({ '貝果': ('bagel', True) })
NORM_RULES.update({ '漫畫': ['漫畫'] }) # [Count: 13]
FEATURE_DEFINITION.update({ '漫畫': ('comics', True) })
NORM_RULES.update({ '巷弄': ['巷弄'] }) # [Count: 12]
FEATURE_DEFINITION.update({ '巷弄': ('alley', True) })
NORM_RULES.update({ '冷氣強': ['冷氣強'] }) # [Count: 12]
FEATURE_DEFINITION.update({ '冷氣強': ('strong_air_conditioning', True) })
NORM_RULES.update({ '手工餅乾': ['手工餅乾'] }) # [Count: 12]
FEATURE_DEFINITION.update({ '手工餅乾': ('handmade_cookies', True) })
NORM_RULES.update({ '貓咪互動': ['貓咪互動'] }) # [Count: 11]
FEATURE_DEFINITION.update({ '貓咪互動': ('cat_interaction', True) })
NORM_RULES.update({ '桌遊': ['桌遊'] }) # [Count: 11]
FEATURE_DEFINITION.update({ '桌遊': ('board_games', True) })
NORM_RULES.update({ '黑膠唱片': ['黑膠唱片'] }) # [Count: 11]
FEATURE_DEFINITION.update({ '黑膠唱片': ('vinyl_records', True) })
NORM_RULES.update({ '拉花': ['拉花'] }) # [Count: 11]
FEATURE_DEFINITION.update({ '拉花': ('latte_art', True) })
NORM_RULES.update({ '座位少': ['座位少', '座位擁擠'] }) # [Count: 19]
FEATURE_DEFINITION.update({ '座位少': ('limited_seating', True) })
NORM_RULES.update({ '文青風格': ['文青風格'] }) # [Count: 10]
FEATURE_DEFINITION.update({ '文青風格': ('artsy_style', True) })
NORM_RULES.update({ '英文菜單': ['英文菜單'] }) # [Count: 10]
FEATURE_DEFINITION.update({ '英文菜單': ('english_menu', True) })
NORM_RULES.update({ '可外帶': ['可外帶'] }) # [Count: 10]
FEATURE_DEFINITION.update({ '可外帶': ('takeout', True) })
NORM_RULES.update({ '復古風格': ['復古風格', '懷舊氛圍'] }) # [Count: 18]
FEATURE_DEFINITION.update({ '復古風格': ('retro_style', True) })
NORM_RULES.update({ '咖啡調酒': ['咖啡調酒'] }) # [Count: 9]
FEATURE_DEFINITION.update({ '咖啡調酒': ('coffee_cocktails', True) })
NORM_RULES.update({ '韓系風格': ['韓系風格'] }) # [Count: 9]
FEATURE_DEFINITION.update({ '韓系風格': ('korean_style', True) })
NORM_RULES.update({ '落地窗': ['落地窗'] }) # [Count: 9]
FEATURE_DEFINITION.update({ '落地窗': ('floor_to_ceiling_windows', True) })
NORM_RULES.update({ '水餃': ['水餃'] }) # [Count: 9]
FEATURE_DEFINITION.update({ '水餃': ('dumplings', True) })
NORM_RULES.update({ '場地租借': ['場地租借'] }) # [Count: 8]
FEATURE_DEFINITION.update({ '場地租借': ('venue_rental', True) })
NORM_RULES.update({ '高價位': ['高價位'] }) # [Count: 8]
FEATURE_DEFINITION.update({ '高價位': ('expensive', True) })
