# 使用官方 Python 輕量級影像
FROM python:3.10-slim

# 設定工作目錄
WORKDIR /app

# 將環境依賴檔案複製到容器內
COPY requirements.txt .

# 安裝套件 (不使用快取以減小影像體積)
RUN pip install --no-cache-dir -r requirements.txt

# 將整個專案的程式碼複製進容器
COPY . .

# Cloud Run 會動態提供 PORT 環境變數，預設為 8080
ENV PORT=8080

# 啟動 FastAPI 伺服器 (綁定 0.0.0.0 以及系統提供的 PORT)
CMD exec uvicorn main:app --host 0.0.0.0 --port ${PORT}