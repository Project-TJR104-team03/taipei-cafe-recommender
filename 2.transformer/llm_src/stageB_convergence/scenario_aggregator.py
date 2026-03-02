import pandas as pd
import logging
import json
# 從 config 中把權重矩陣跟翻譯字典一起 import 進來
from tag_config import SCENARIO_CONFIG, FEATURE_TO_ZH 

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ==========================================
# 1. 驚喜標籤過濾邏輯 (Information Gain Filter)
# ==========================================
def get_surprise_tags(row_dict, scenario_name, config, top_n=3, threshold=0.6):
# ... 後面的程式碼完全不變 ...
    """
    從店家的所有特徵中，排除該場景的「基礎要求」，挑出分數最高的 Top N 亮點。
    """
    # 1. 取得該懶人按鈕的「理所當然」特徵 (要排除的項目)
    baseline_features = set(config[scenario_name].get("positive_features", {}).keys())
    
    surprise_candidates = []
    
    # 2. 遍歷店家所有的資料
    for feature_key, score in row_dict.items():
        # 確保是數值型態的分數
        if isinstance(score, (int, float)):
            # 條件：不是基礎特徵、分數 >= 門檻、而且在我們的中文翻譯字典裡
            if feature_key not in baseline_features and score >= threshold and feature_key in FEATURE_TO_ZH:
                # 排除負面感受的標籤 (我們不希望驚喜標籤出現"服務差"之類的)
                if score > 0 and feature_key not in ["bad_service", "stuffy", "is_service_slow", "parking_difficult", "has_time_limit"]:
                    surprise_candidates.append((feature_key, score))
            
    # 3. 依照分數由高到低排序 (降冪)
    surprise_candidates.sort(key=lambda x: x[1], reverse=True)
    
    # 4. 取出 Top N，並轉成中文
    top_tags = [FEATURE_TO_ZH[feat] for feat, score in surprise_candidates[:top_n]]
    
    return top_tags

# ==========================================
# 3. 主聚合運算子 (Main Operator)
# ==========================================
def apply_scenario_scores(df, config=SCENARIO_CONFIG):
    logger.info("開始執行場景聚合運算與驚喜標籤生成...")
    df.fillna(0.0, inplace=True) # 解毒 NaN
    
    scenario_columns = {
        "適合辦公": "score_workspace",
        "質感約會": "score_dating",
        "毛孩同樂": "score_pet_friendly",
        "獨處放鬆": "score_relax"
    }

    for scenario_name, weights in config.items():
        col_name = scenario_columns.get(scenario_name)
        if not col_name: continue
            
        # 計算懶人按鈕總分
        def calculate_score(row):
            total_score = 0.0
            for feature, weight in weights.get("positive_features", {}).items():
                total_score += float(row.get(feature, 0.0)) * weight
            for feature, weight in weights.get("negative_features", {}).items():
                total_score += float(row.get(feature, 0.0)) * weight
            return max(0.0, round(total_score, 3))

        df[col_name] = df.apply(calculate_score, axis=1)
        
        # 產生專屬驚喜標籤 (排除該場景基礎特徵後的高分項目)
        tag_col_name = f"tags_{col_name}"
        df[tag_col_name] = df.apply(
            lambda row: get_surprise_tags(row.to_dict(), scenario_name, config), 
            axis=1
        )
        logger.info(f"✅ 成功建立: {col_name} 與其專屬驚喜標籤 {tag_col_name}")

    return df

# ==========================================
# 4. 內部紅隊測試 (Execution)
# ==========================================
if __name__ == "__main__":
    INPUT_FILE = "final_scored_data.json"
    OUTPUT_FILE = "cafes_with_scenarios_final.csv"
    
    try:
        with open(INPUT_FILE, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)
            
        records = []
        for pid, info in raw_data.items():
            record = {"place_id": info.get("place_id"), "place_name": info.get("place_name")}
            scores = info.get("metadata_for_filtering", {}).get("feature_scores", {})
            record.update(scores)
            records.append(record)
            
        df = pd.DataFrame(records)
        df_enriched = apply_scenario_scores(df)
        df_enriched.to_csv(OUTPUT_FILE, index=False)
        logger.info(f"🎉 運算完成！檔案已儲存至: {OUTPUT_FILE}")
        
        # 印出結果驗證
        print("\n🔍 【適合辦公】Top 3 店家及其驚喜標籤 (應排除插座/網路等)：")
        cols = ['place_name', 'score_workspace', 'tags_score_workspace']
        top_workspace = df_enriched[cols].sort_values(by='score_workspace', ascending=False).head(3)
        print(top_workspace.to_string(index=False))
        
    except Exception as e:
        logger.error(f"執行失敗: {e}")