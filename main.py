import os
import time
import sys

def main():
    print("=== 台北咖啡廳專案：雲端任務啟動 ===")
    
    project_id = os.getenv("PROJECT_ID", "Unknown")
    print(f"目前執行專案: {project_id}")
    
    print("正在初始化爬蟲引擎...")
    time.sleep(2)  

    #這裡到時候啟動其他py檔
    
    print("任務執行完畢，準備關閉容器。")
    
    # 正常結束，回傳 0 代表成功
    sys.exit(0)

if __name__ == "__main__":
    main()

#等程式碼陸續更新後，再統一用此檔案執行
