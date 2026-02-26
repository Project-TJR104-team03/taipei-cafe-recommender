import pandas as pd
import numpy as np
import re
import os
from io import BytesIO
from google.cloud import storage
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ReviewPreFilter:
    def __init__(self, bucket_name, gcs_raw_path, local_output_path):
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)
        self.gcs_raw_path = gcs_raw_path
        self.local_output_path = local_output_path
        self.reference_date = datetime.now()

    def _parse_reviewer_count(self, text):
        if pd.isna(text): return 0
        nums = re.findall(r'\d+', str(text).replace(',', ''))
        return int(nums[0]) if nums else 0

    def calculate_quality_score(self, df):
        # A. 權威分 (35%)
        level_weights = {'愛食記部落客': 1.5, '在地嚮導': 1.2, '一般評論者': 1.0}
        df['auth_base'] = df['reviewer_level'].map(level_weights).fillna(1.0)
        df['reviewer_count_int'] = df['reviewer_amount'].apply(self._parse_reviewer_count)
        df['score_auth'] = df['auth_base'] * np.log1p(df['reviewer_count_int'])

        # B. 語意初探 (30%) 
        core_keywords = [
            "電", "插", "網", "wifi", "訊號", "斷", "慢", "連不", "吃",
            "擠", "窄", "小", "寬", "桌", "位", "沙發", "凳", "落地窗", "窗",
            "悶", "熱", "涼", "冷氣", "空調", "吵", "靜", "雜", "聲", "音", "光", "明", "暗", "冷",
            "裝潢", "設計", "工業", "老宅", "風格", "質感", "美", "文青", "復古", "現代", "風",
            "限", "趕", "低消", "規", "錢", "費", "消費", "時間", "排", "訂", "客滿"
        ]
        df['score_semantic'] = df['content'].astype(str).apply(
            lambda x: sum(1 for kw in core_keywords if kw in x) / len(core_keywords)
        )

        # C. 內容深度 (20%)
        df['score_depth'] = np.log1p(df['content'].astype(str).str.len())

        # D. 時間衰減 (15%) - 純計分，不強制刪除
        df['review_datetime'] = pd.to_datetime(df['full_date'], errors='coerce')
        # 防止 NaT 報錯，給予一個極早的預設時間，讓衰減分數歸零但保留資料
        df['review_datetime'] = df['review_datetime'].fillna(pd.Timestamp('2010-01-01'))
        
        max_days = 3 * 365
        df['days_diff'] = (self.reference_date - df['review_datetime']).dt.days
        # 超過三年的評論，score_recency 會變為 0，但不影響其參與其他三項計分
        df['score_recency'] = (1 - (df['days_diff'] / max_days)).clip(0, 1)

        # 歸一化
        for col in ['score_auth', 'score_depth', 'score_semantic']:
            if df[col].max() != df[col].min():
                df[col] = (df[col] - df[col].min()) / (df[col].max() - df[col].min())
            else:
                df[col] = 1.0

        df['quality_score'] = (
            df['score_auth'] * 0.35 +
            df['score_semantic'] * 0.30 +
            df['score_depth'] * 0.20 +
            df['score_recency'] * 0.15
        )
        return df

    def run(self):
        logger.info(f"從 GCS 下載原始資料: gs://{self.bucket.name}/{self.gcs_raw_path}")
        try:
            blob = self.bucket.blob(self.gcs_raw_path)
            df = pd.read_csv(BytesIO(blob.download_as_bytes()))
            
            # 欄位自檢
            required_cols = ['place_id', 'place_name', 'content', 'full_date', 'reviewer_level', 'reviewer_amount']
            missing = [c for c in required_cols if c not in df.columns]
            if missing:
                logger.error(f"遺漏必要欄位: {missing}")
                return None
                
        except Exception as e:
            logger.error(f"雲端讀取或解析失敗: {e}")
            return None
        
        # 1. 預過濾 (⭐️ 完全採用 place_id 作為基準)
        df = df.dropna(subset=['content']).drop_duplicates(subset=['place_id', 'content'])
        
        # 2. 評分
        df_scored = self.calculate_quality_score(df)
        
        # 3. 核心邏輯：每家店取 Top 50 品質優選 (⭐️ 統一採用 place_id)
        df_top_50 = (
            df_scored.sort_values(['place_id', 'quality_score'], ascending=[True, False])
            .groupby('place_id')
            .head(50)
            .reset_index(drop=True)
        )
        
        # 4. 本地存取
        df_top_50.to_csv(self.local_output_path, index=False, encoding='utf-8-sig')
        logger.info(f"蒸餾完成。本地產出: {len(df_top_50)} 筆")
        return df_top_50

if __name__ == "__main__":
    CONFIG = {
        "bucket_name": "XXX",
        "gcs_raw_path": "XXX",
        "local_output_path": "XXX"
    }
    filter_engine = ReviewPreFilter(**CONFIG)
    filter_engine.run()