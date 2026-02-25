import json
import logging
import re
from google.cloud import storage

# ==========================================
# [策略優化] 具備容錯機制的解析器
# ==========================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def process_gcs_results(project_id, bucket_name, folder_path, output_file="final_readable_audit.json"):
    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=folder_path)
    
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
                    reason = raw_data.get('response', {}).get('promptFeedback', {}).get('blockReason', 'Unknown Block')
                    raise ValueError(f"AI 無回傳內容 (原因: {reason})")

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

    # 儲存結果與失敗日誌
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2)
    
    if failed_logs:
        with open("audit_failed_list.json", "w", encoding='utf-8') as f_fail:
            json.dump(failed_logs, f_fail, ensure_ascii=False, indent=2)

    logger.info(f"✅ 完成！成功: {success_count} | 失敗: {len(failed_logs)}")
    if failed_logs:
        logger.info(f"📋 失敗清單已存至 audit_failed_list.json")

if __name__ == "__main__":
    # 這裡記得填入妳「新的個人 Bucket」資訊
    MY_PROJECT = "XXX" 
    MY_BUCKET = "XXX"
    # 從 GCP 控制台複製最新的路徑
    MY_FOLDER = "XXX"

    process_gcs_results(MY_PROJECT, MY_BUCKET, MY_FOLDER)