# ☕ TJR104 台北咖啡廳 AI 推薦系統

這是一個基於 **RAG (Retrieval-Augmented Generation)** 技術的 LINE 智能客服系統。
透過自動化爬蟲、Gemini AI 標籤化與 MongoDB 向量搜尋，為使用者提供最精準的咖啡廳推薦。

---

## 🏗️ 系統架構圖
1. **資料採集**: Cloud Run (Selenium) ➔ GCS (Data Lake)
2. **資料處理**: Cloud Functions (Gemini Pro) ➔ 向量化與標籤化
3. **儲存引擎**: MongoDB Atlas (Vector Search + GeoJSON)
4. **服務介面**: LINE Messaging API ➔ FastAPI (Cloud Run)
<img src="./docs/FlowChart.drawio.svg" width="100%">

---

## 📁 資料湖結構 (GCS Bucket)
`gs://tjr104-cafe-datalake/`
* `/raw/store/`: 店家基礎資訊 (Place ID, 名稱)
* `/raw/store_dynamic/`: 店家動態資訊 (評分、營業狀態、評論數)
* `/raw/comments/`: 原始評論資料 (用於增量更新)
* `/raw/manual_upload/`: 外部 CSV 手動上傳區 (觸發 Pipeline)
* `/processed/`: AI 清洗後準備匯入 DB 的結構化資料

---

## 🔐 權限與身分說明 (IAM)
本專案嚴格遵循「最小權限原則」，使用以下 Service Accounts：
* **`AIRFLOW_SA`**: 負責調度、監控 GCS 與啟動 Cloud Run。
* **`WORKER_SA`**: 爬蟲與 ETL 核心，具備 `Storage Object Admin` 權限。
* **`GITHUB_SA`**: 負責 CI/CD 自動化部署。

---

