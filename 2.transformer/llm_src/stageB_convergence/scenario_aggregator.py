import pandas as pd
import logging
import json
import os
import io
from google.cloud import storage
from dotenv import load_dotenv

# 從 config 中把權重矩陣跟翻譯字典一起 import 進來
from configs import tag_config
from configs.tag_config import SCENARIO_CONFIG, FEATURE_TO_ZH

load_dotenv()

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
# 2. 主聚合運算子 (Main Operator)
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
# 3. 雲端執行引擎 (Cloud Runner)
# ==========================================
class StageB_CloudCalculator:
    def __init__(self, project_id, bucket_name):
        self.storage_client = storage.Client(project=project_id)
        self.bucket = self.storage_client.bucket(bucket_name)

    def process_and_upload(self, input_gcs_path, output_gcs_path):
        logger.info(f"📥 正在從 GCS 下載評分資料: gs://{self.bucket.name}/{input_gcs_path}")
        
        # 1. 直接在記憶體中讀取 GCS 的 JSON
        in_blob = self.bucket.blob(input_gcs_path)
        if not in_blob.exists():
            raise FileNotFoundError(f"❌ 找不到來源檔案: {input_gcs_path}")
            
        raw_data = json.loads(in_blob.download_as_text(encoding='utf-8'))
            
        # 2. 資料轉換與清理
        records = []
        for pid, info in raw_data.items():
            record = {"place_id": info.get("place_id"), "place_name": info.get("place_name")}
            scores = info.get("metadata_for_filtering", {}).get("feature_scores", {})
            record.update(scores)
            records.append(record)
            
        df = pd.DataFrame(records)
        
        # 3. 執行核心運算
        df_enriched = apply_scenario_scores(df)
        
        # 4. 直接將 DataFrame 轉為字串並上傳至 GCS (不落地，拯救 I/O)
        logger.info(f"☁️ 正在將運算結果上傳至 GCS: gs://{self.bucket.name}/{output_gcs_path}")
        csv_buffer = df_enriched.to_csv(index=False, encoding='utf-8-sig')
        out_blob = self.bucket.blob(output_gcs_path)
        out_blob.upload_from_string(csv_buffer, content_type='text/csv')
        
        logger.info(f"🎉 雲端運算完成！場景分數已安全降落。")
        
        # 印出結果驗證 (Cloud Logging 會自動捕捉)
        print("\n🔍 【適合辦公】Top 3 店家及其驚喜標籤：")
        cols = ['place_name', 'score_workspace', 'tags_score_workspace']
        top_workspace = df_enriched[cols].sort_values(by='score_workspace', ascending=False).head(3)
        print(top_workspace.to_string(index=False))

# ==========================================
# [總司令部]
# ==========================================
if __name__ == "__main__":
    PROJECT_ID = os.getenv("PROJECT_ID")
    BUCKET_NAME = os.getenv("BUCKET_NAME")
    
    # 輸入：Stage B 剛產出的打分 JSON
    INPUT_PATH = os.getenv("GCS_FINAL_SCORED_PATH", "transform/stageB/final_scored_data.json")
    
    # 輸出：準備餵給 Stage C / MongoIngestor 的 CSV
    OUTPUT_PATH = os.getenv("GCS_SCENARIO_CSV_PATH", "transform/stageB/cafes_with_scenarios_final.csv")
    
    try:
        calculator = StageB_CloudCalculator(PROJECT_ID, BUCKET_NAME)
        calculator.process_and_upload(INPUT_PATH, OUTPUT_PATH)
    except Exception as e:
        logger.error(f"❌ 雲端任務執行失敗: {e}")