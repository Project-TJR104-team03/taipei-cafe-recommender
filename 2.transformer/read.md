graph TD
    %% Phase 2: Trigger
    A[(GCS Bucket: Raw CSV)] -->|Event Trigger| B{Cloud Functions}
    B --> C[讀取新店家資料]
    C --> D{增量檢查: ID 是否已存在?}
    
    subgraph "Phase 3: AI 精煉與特徵工程 (AI Refinement)"
        D -->|否| E[Regex 初步清洗: 去除符號/雜訊/冗餘標籤]
        
        %% 分流處理
        E --> D1[Google Maps Metadata]
        E --> D2[User Reviews]
        
        %% 軌道一：店家屬性精煉
        D1 --> F1[Gemini API: 店名校正與分店拆分]
        F1 --> F2[Gemini API: 設施服務提取與 SEO 標籤化]
        
        %% 軌道二：評論語義提取
        D2 --> E2[Quality Filtering: 篩選優質評論]
        E2 --> G1[Gemini API: 評論關鍵字與情緒分析]
        
        %% 核心匯流：特徵聚合與賦分
        F2 --> H[Feature Aggregation: 綜合店家屬性與評論特徵]
        G1 --> H
        H --> H1[Tag Weight Calculation: 標籤權重賦分]
    end

    subgraph "Phase 4: 多重向量化存儲 (Multi-Vector Storage)"
        %% 路徑一：店家綜合向量
        H1 -->|綜合描述文本| J1[Embedding Model: 店家綜合向量]
        
        %% 路徑二：個別優質評論向量
        G1 -->|單則評論文本| J2[Embedding Model: 個別評論向量]
        
        J1 --> K[(MongoDB Atlas: Vector Search)]
        J2 --> K
        H1 --> I[Metadata 結構化打包 JSON]
        I --> K
    end

    %% 樣式設定
    style F1 fill:#f96,stroke:#333,stroke-width:2px
    style F2 fill:#f96,stroke:#333,stroke-width:2px
    style G1 fill:#f96,stroke:#333,stroke-width:2px
    style H1 fill:#fff4dd,stroke:#d4a017,stroke-width:2px
    style J1 fill:#bbf,stroke:#333,stroke-width:2px
    style J2 fill:#bbf,stroke:#333,stroke-width:2px
    style K fill:#555,stroke:#333,color:#fff

🛠️ 技術實作細節 (Technical Implementation)
1. 數據清洗與治理 (Data Pre-processing)
   •Regex Cleaning (成本與品質優化)：
    •在送入 AI 之前，預先移除地圖資料中的特殊符號（如 ✔, ❌）與冗餘標籤（如 服務項目：）。
    •效益：顯著降低 Gemini API 的 Token 消耗，並避免 AI 被無意義符號干擾，提升特徵提取的準確度。
   •店名校正與正規化 (Name Normalization)：
    •利用 Gemini API 將不統一的店名（如：路易莎、LOUISA）正規化為統一實體，並分離分店名稱，確保資料庫層級的 資料一致性 (Consistency)。

2. 雙層級向量架構 (Dual-Layer Vector Architecture)
   為了支援不同維度的搜尋需求，本專案設計了雙重向量索引策略：
   •店家綜合向量 (Store Aggregated Vector)：
    •內容：整合「校正店名 + SEO 標籤 + 評論特徵摘要」。
    •用途：處理概括性查詢（例如：「適合下午工作的安靜咖啡廳」）。
   •個別評論向量 (Individual Review Vector)：
    •內容：針對過濾後的單則優質評論進行獨立向量化。
    •用途：處理細節經驗匹配（例如：「肉桂捲有濃郁辛香料味的店」）。

3. 標籤權重賦分機制 (Tag Weighting)
系統結合「官方標籤」與「評論情緒分析」結果，為每個 Tag 計算動態權重分數。這使搜尋結果具備 「推薦強度」，而非單純的關鍵字匹配。