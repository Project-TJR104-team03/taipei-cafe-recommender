import json
import logging
import os
from google.cloud import storage
from dotenv import load_dotenv
from configs import tag_config
from typing import Dict, Any, List, Optional, Set, Tuple

load_dotenv()
# 設定系統觀測性 (Observability) 日誌格式
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TagsMerger:
    """
    Data Normalization & Fusion 核心引擎
    負責單筆資料的歸一化、Schema 防禦與語義萃取
    """
    def __init__(self, tag_config_dict: Dict[str, Any]):
        self.allowed_features: Set[str] = set(tag_config_dict.get("allowed_features", []))
        self.allowed_tags: Set[str] = set(tag_config_dict.get("allowed_tags", []))

    def _parse_boolean_or_null(self, value: Any) -> Optional[bool]:
        """防禦性轉型：嚴格處理 None, JSON null, Boolean 與字串"""
        if value is None:
            return None
        if isinstance(value, bool):
            return value
        
        val_str = str(value).strip().lower()
        if val_str in ['null', 'none', 'unknown', '']:
            return None
        if val_str == 'true':
            return True
        if val_str == 'false':
            return False
        return None

    def _resolve_features(self, raw_features: Dict[str, Any], stage_a_features: Dict[str, Any], conflicts: List[Dict[str, Any]]) -> Dict[str, Optional[bool]]:
        """處理布林特徵歸一化，遵循優先級覆蓋機制"""
        resolved_features = {}
        conflict_map = {c['key']: self._parse_boolean_or_null(c['reality_check']) for c in conflicts}

        for feature_key in self.allowed_features:
            final_value = None
            if feature_key in conflict_map:
                final_value = conflict_map[feature_key]
            elif feature_key in stage_a_features and stage_a_features[feature_key] is not None:
                final_value = stage_a_features[feature_key]
            elif feature_key in raw_features:
                final_value = raw_features[feature_key]

            resolved_features[feature_key] = self._parse_boolean_or_null(final_value)

        return resolved_features

    def _process_tags(self, raw_official_tags: Dict[str, List[str]], official_tags_audit: Dict[str, List[str]]) -> Tuple[List[str], List[str]]:
        """過濾合法標籤：聯集(官方標籤 + AI標籤)，並攔截未知標籤至增量緩衝區"""
        valid_tags = []
        incremental_tags = []
        
        # 安全地提取清單，避免原始資料格式異常
        raw_tags_list = []
        if isinstance(raw_official_tags, dict):
            for tags in raw_official_tags.values():
                if isinstance(tags, list):
                    raw_tags_list.extend([t for t in tags if t])
                    
        ai_tags_list = []
        if isinstance(official_tags_audit, dict):
            for tags in official_tags_audit.values():
                if isinstance(tags, list):
                    ai_tags_list.extend([t for t in tags if t])

        # 兩者聯集 (Union) 並去重
        all_extracted_tags = set(raw_tags_list + ai_tags_list)

        for tag in all_extracted_tags:
            if tag in self.allowed_tags:
                valid_tags.append(tag)
            else:
                incremental_tags.append(tag)

        return valid_tags, incremental_tags

    def _build_embedding_content(self, place_name: str, audit_summary: Dict[str, Any], incremental_tags: List[str], evidence_map: Dict[str, str]) -> str:
        """建構高維度語義搜尋 Vector Text"""
        summary_text = audit_summary.get("overall_vibe", "")
        incremental_text = f" 特別亮點：{', '.join(incremental_tags)}。" if incremental_tags else ""
        evidence_texts = [f"[{k}] {v}" for k, v in evidence_map.items() if v]
        evidence_text = f" 評論佐證：{'; '.join(evidence_texts)}。" if evidence_texts else ""

        return f"{place_name}。{summary_text}{incremental_text}{evidence_text}"

    def merge(self, place_id: str, raw_data: Dict[str, Any], stage_a_data: Dict[str, Any]) -> Dict[str, Any]:
        """單店合併主函式"""
        raw_features = raw_data.get("features", {})
        audit_results = stage_a_data.get("audit_results", {})
        
        resolved_features = self._resolve_features(
            raw_features, 
            audit_results.get("features", {}), 
            audit_results.get("conflict_alerts", [])
        )
        
        # [修復點] 正確傳入雙參數進行聯集
        valid_tags, new_tags = self._process_tags(
            raw_data.get("official_tags", {}),
            audit_results.get("official_tags_audit", {})
        )
        
        embedding_content = self._build_embedding_content(
            stage_a_data.get("place_name", "未知店家"), 
            audit_results.get("audit_summary", {}), 
            new_tags, 
            audit_results.get("evidence_map", {})
        )

        return {
            "place_id": place_id,
            "place_name": stage_a_data.get("place_name", ""),
            "metadata_for_filtering": {
                "features": resolved_features,
                "tags": valid_tags
            },
            "new_incremental_features": new_tags,
            "content_for_embedding": embedding_content,
            "system_metrics": {
                "conflict_resolved_count": len(audit_results.get("conflict_alerts", [])),
                "incremental_tags_caught": len(new_tags)
            }
        }


def run_batch_pipeline(PROJECT_ID: str, BUCKET_NAME: str, gcs_raw_file: str, gcs_stage_a_file: str, gcs_output_file: str, tag_config_dict: Dict[str, Any]):
    logger.info("Initializing Batch Merge Pipeline on GCS...")
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)

    try:
        raw_data_input = json.loads(bucket.blob(gcs_raw_file).download_as_text(encoding='utf-8'))
        stage_a_dict = json.loads(bucket.blob(gcs_stage_a_file).download_as_text(encoding='utf-8'))
    except Exception as e:
        logger.error(f"GCS 讀取失敗: {e}。請確認路徑。")
        return

    # 自動轉型防護網
    if isinstance(raw_data_input, list):
        logger.info("Detected Raw Data as a List. Auto-converting to Dictionary...")
        raw_data_dict = {item.get("place_id"): item for item in raw_data_input if isinstance(item, dict) and "place_id" in item}
    else:
        raw_data_dict = raw_data_input

    merger = TagsMerger(tag_config_dict)
    final_results = {}
    error_logs = []

    total_shops = len(stage_a_dict)
    logger.info(f"Loaded {total_shops} shops. Starting merge...")

    for place_id, stage_a_data in stage_a_dict.items():
        try:
            if not isinstance(stage_a_data, dict):
                raise ValueError(f"Stage A data for {place_id} is not a valid JSON Object.")
                
            raw_data = raw_data_dict.get(place_id, {})
            merged_record = merger.merge(place_id, raw_data, stage_a_data)
            final_results[place_id] = merged_record
        except Exception as e:
            logger.error(f"Error processing shop [ {place_id} ]: {str(e)}")
            error_logs.append({"place_id": place_id, "error": str(e)})

    # 寫入GCS
    try:
        bucket.blob(gcs_output_file).upload_from_string(json.dumps(final_results, ensure_ascii=False, indent=2), content_type='application/json')
        
        logger.info("================ Pipeline Summary ================")
        logger.info(f"Total processed: {total_shops}")
        logger.info(f"Successfully merged: {len(final_results)}")
        logger.info(f"Failed/Errors: {len(error_logs)}")
        logger.info(f"Output saved to: gs://{BUCKET_NAME}/{gcs_output_file}")
        logger.info("==================================================")
        
        if error_logs:
            error_log_path = gcs_output_file.replace(".json", "_errors.json")
            bucket.blob(error_log_path).upload_from_string(json.dumps(error_logs, ensure_ascii=False, indent=2), content_type='application/json')
            logger.warning(f"Check error logs at gs://{BUCKET_NAME}/{error_log_path}")

    except Exception as e:
        logger.error(f"Failed to save output file: {str(e)}")

# ==========================================
# 執行區塊 (動態解析 Config)
# ==========================================
if __name__ == "__main__":
    dynamic_features = set()
    for feature_tuple in tag_config.FEATURE_DEFINITION.values():
        dynamic_features.add(feature_tuple[0])
    for mapping_tuple in tag_config.TAG_MAPPING.values():
        dynamic_features.add(mapping_tuple[2])
        
    dynamic_tags = set(tag_config.NORM_RULES.keys())
    dynamic_tags.update(tag_config.POSITIVE_TAG_RULES)

    DYNAMIC_TAG_CONFIG = {
        "allowed_features": list(dynamic_features),
        "allowed_tags": list(dynamic_tags)
    }

    run_batch_pipeline(
        PROJECT_ID=os.getenv("PROJECT_ID"),
        BUCKET_NAME=os.getenv("BUCKET_NAME"),
        gcs_raw_file=os.getenv("GCS_CAFE_DATA_FINAL_PATH"),
        gcs_stage_a_file=os.getenv("GCS_FINAL_AUDIT_JSON_PATH"),
        gcs_output_file=os.getenv("GCS_NORMALIZED_MERGED_PATH", "transform/stageB/normalized_merged_data.json"),
        tag_config_dict=DYNAMIC_TAG_CONFIG
    )