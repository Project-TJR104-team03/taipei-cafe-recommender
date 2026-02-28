import json
import os
import logging
import re
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv()

# ==========================================
# [策略優化] 具備容錯機制的解析器
# ==========================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

"""自動尋找母資料夾底下，最新生成的預測結果目錄"""
def get_latest_prediction_folder(bucket, base_prefix):
    logger.info(f"🔍 正在尋找 {base_prefix} 底下最新的預測結果...")
    
    # 掃描母目錄下的所有檔案
    blobs = list(bucket.list_blobs(prefix=base_prefix))
    
    # 只挑選是 JSONL 且名稱包含 predictions 的檔案
    jsonl_blobs = [b for b in blobs if b.name.endswith(".jsonl") and "predictions" in b.name]
    
    if not jsonl_blobs:
        raise FileNotFoundError(f"在 {base_prefix} 找不到任何預測結果！")
        
    # 依照檔案的更新時間 (updated) 降冪排序，取最新的那一個檔案
    jsonl_blobs.sort(key=lambda x: x.updated, reverse=True)
    latest_blob = jsonl_blobs[0]
    
    # 擷取該檔案所在的資料夾路徑
    # e.g., batch_output/stage_a_full_audit/20260226/prediction-.../
    latest_folder = "/".join(latest_blob.name.split("/")[:-1]) + "/"
    
    logger.info(f"🎯 鎖定最新預測目錄: {latest_folder}")
    return latest_folder


def process_gcs_results(project_id, bucket_name, folder_path, gcs_output_path):
    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)

    actual_folder_path = get_latest_prediction_folder(bucket, folder_path)
    blobs = bucket.list_blobs(prefix=actual_folder_path)
    
    all_results = {}
    failed_logs = []  # 儲存失敗的 PID 與原因
    success_count = 0

    logger.info(f"🌐 連線至: gs://{bucket_name}/{folder_path}")

    for blob in blobs:
        if not blob.name.endswith(".jsonl") or "predictions" not in blob.name:
            continue
        
        content = blob.download_as_text()
        for line in content.splitlines():
            if not line.strip(): continue
            
            try:
                raw_data = json.loads(line)
                pid = raw_data.get("custom_id")
                pname = raw_data.get("place_name", "Unknown")
                
                # --- 優化 A: 防禦性提取 ---
                candidates = raw_data.get('response', {}).get('candidates', [])
                if not candidates:
                    # 處理安全過濾或其他導致無回傳的情況
                    feedback = raw_data.get('response', {}).get('promptFeedback', {})
                    block_reason = feedback.get('blockReason', 'Unknown Block / No Candidate')
                    raise ValueError(f"AI 無回傳內容 (原因: {block_reason})")

                raw_text = candidates[0].get('content', {}).get('parts', [{}])[0].get('text', "")
                
                # --- 優化 B: 正則提取 JSON ---
                # 即使 AI 多寫了閒聊文字，這段也能抓出正確的 JSON 區塊
                json_match = re.search(r"\{.*\}", raw_text, re.DOTALL)
                if not json_match:
                    raise ValueError("無法從 AI 回傳中找到有效的 JSON 結構")
                
                prediction_json = json.loads(json_match.group())
                
                all_results[pid] = {
                    "place_name": pname,
                    "audit_results": prediction_json.get("audit_results", prediction_json)
                }
                success_count += 1
                
            except Exception as e:
                # --- 優化 C: 錯誤日誌化 ---
                failed_logs.append({"pid": pid, "error": str(e)})
                logger.warning(f"⚠️ 店家 {pid} 解析失敗: {str(e)}")

    # 上傳成功結果至 GCS
    bucket.blob(gcs_output_path).upload_from_string(
        json.dumps(all_results, ensure_ascii=False, indent=2),
        content_type='application/json'
    )
    
    if failed_logs:
        failed_path = gcs_output_path.replace(".json", "_failed.json")
        bucket.blob(failed_path).upload_from_string(
            json.dumps(failed_logs, ensure_ascii=False, indent=2),
            content_type='application/json'
        )
    logger.info(f"✅ 解析完成並上傳至 GCS: {gcs_output_path}")

    logger.info(f"✅ 完成！成功: {success_count} | 失敗: {len(failed_logs)}")
    if failed_logs:
        logger.info(f"📋 失敗清單已存至 audit_failed_list.json")

if __name__ == "__main__":
    process_gcs_results(
        os.getenv("PROJECT_ID"),
        os.getenv("BUCKET_NAME"),
        os.getenv("GCS_AI_PREDICTION_FOLDER"), # 從 Console 複製的資料夾路徑
        os.getenv("GCS_FINAL_AUDIT_JSON_PATH", "transform/stageA/final_readable_audit.json")
    )