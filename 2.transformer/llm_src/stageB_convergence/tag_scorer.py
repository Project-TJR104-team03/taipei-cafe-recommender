import json
import logging
import tag_config
from typing import Dict, Any, Optional

# 設定系統觀測性 (Observability)
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - [TAG-SCORER] - %(levelname)s - %(message)s'
)

class TagScorer:
    """
    特徵權重評分引擎 (Feature Scoring Engine) v2.0
    優化重點：配置驅動、維度收斂、零除錯誤保護。
    """
    def __init__(self):
        self.cfg = tag_config.SCORING_CONFIG
        self.contradiction_pairs = tag_config.CONTRADICTION_PAIRS

    def _get_consensus(self, feature_key: str, conflict_alerts: Dict[str, Any]) -> int:
        """提取共識等級，包含邊界值校驗"""
        consensus = conflict_alerts.get(feature_key, {}).get("consensus_level", self.cfg["DEFAULT_CONSENSUS"])
        return max(int(consensus), self.cfg["MIN_CONSENSUS"])

    def _calculate_base_score(self, feature_val: bool, consensus: int) -> float:
        """
        基礎信心加權公式：
        Score = min(1.0, Base + (Consensus * Step))
        """
        if not feature_val:
            return 0.0
        
        raw_score = self.cfg["BASE_WEIGHT"] + (consensus * self.cfg["STEP_WEIGHT"])
        return round(min(raw_score, 1.0), 2)

    def _resolve_contradiction(self, pos_key: str, neg_key: str, 
                             features: Dict[str, Any], 
                             conflict_alerts: Dict[str, Any]) -> Optional[float]:
        """
        處理互斥標籤的收斂邏輯。
        當正面與負面特徵同時存在時，計算共識佔比。
        """
        pos_val = features.get(pos_key)
        neg_val = features.get(neg_key)

        # 兩者皆為 True: 執行信心對決
        if pos_val is True and neg_val is True:
            pos_con = self._get_consensus(pos_key, conflict_alerts)
            neg_con = self._get_consensus(neg_key, conflict_alerts)
            total = pos_con + neg_con
            return round(pos_con / total, 2) if total > 0 else 0.5
        
        # 只有單方為 True
        if pos_val is True: return self._calculate_base_score(True, self._get_consensus(pos_key, conflict_alerts))
        if neg_val is True: return 0.0 # 負面特徵為真，則正面指標設為 0
        
        return None

    def score_features(self, features: Dict[str, Any], conflict_alerts: Dict[str, Any]) -> Dict[str, float]:
        """計算單店的所有特徵分數並執行維度收斂"""
        scores = {}
        processed_keys = set()

        # 1. 處理矛盾對 (Priority: Convergence)
        for metric_name, pair in self.contradiction_pairs.items():
            pos_key, neg_key = pair["positive"], pair["negative"]
            
            final_val = self._resolve_contradiction(pos_key, neg_key, features, conflict_alerts)
            if final_val is not None:
                scores[metric_name] = final_val
                processed_keys.update([pos_key, neg_key])
                logging.debug(f"Resolved {metric_name} to {final_val}")

        # 2. 處理剩餘獨立標籤
        for feature_key, feature_val in features.items():
            if feature_key in processed_keys or feature_val is None:
                continue
                
            consensus = self._get_consensus(feature_key, conflict_alerts)
            scores[feature_key] = self._calculate_base_score(feature_val, consensus)

        return scores

def run_scoring_pipeline(merged_file: str, audit_file: str, output_file: str):
    """執行批次評分任務"""
    logging.info("Starting Scoring Pipeline...")
    
    try:
        with open(merged_file, 'r', encoding='utf-8') as f:
            merged_dict = json.load(f)
        with open(audit_file, 'r', encoding='utf-8') as f:
            audit_dict = json.load(f)
    except Exception as e:
        logging.error(f"IO Error: {e}")
        return

    scorer = TagScorer()
    final_output = {}

    for place_id, data in merged_dict.items():
        try:
            # 數據提取
            features = data.get("metadata_for_filtering", {}).get("features", {})
            audit_results = audit_dict.get(place_id, {}).get("audit_results", {})
            
            # 將 Conflict Alerts 轉為 Map 提高檢索效率
            conflict_map = {a["key"]: a for a in audit_results.get("conflict_alerts", [])}
            
            # 計算分數
            scores = scorer.score_features(features, conflict_map)
            
            # 寫入結果
            data["metadata_for_filtering"]["feature_scores"] = scores
            final_output[place_id] = data
            
        except KeyError as e:
            logging.warning(f"Skipping {place_id} due to missing keys: {e}")
        except Exception as e:
            logging.error(f"Unexpected error in {place_id}: {e}")

    # 存檔
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(final_output, f, ensure_ascii=False, indent=2)
    
    logging.info(f"Scoring complete. Total shops processed: {len(final_output)}")

if __name__ == "__main__":
    run_scoring_pipeline(
        merged_file="normalized_merged_data.json",
        audit_file="final_readable_audit.json",
        output_file="final_scored_data.json"
    )