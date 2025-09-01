import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta

def generate_dummy_data(rows=10000):
    """
    ブログサイトのアクセスログをシミュレートしたダミーデータを生成する。
    """
    start_date = datetime(2025, 1, 1)
    
    # タイムスタンプをランダムに生成
    timestamps = [start_date + timedelta(seconds=np.random.randint(0, 31536000)) for _ in range(rows)]
    
    # ユーザーIDを生成し、一部を欠損させる
    user_ids = [f'user_{np.random.randint(1, 5000)}' for _ in range(rows)]
    user_ids = [None if np.random.rand() < 0.1 else uid for uid in user_ids] # 10%を欠損
    
    # 記事IDを生成
    article_ids = [f'article_{np.random.randint(101, 150)}' for _ in range(rows)]
    
    # HTTPステータスコードを生成（200: 85%, 404: 10%, 500: 5%）
    status_codes = np.random.choice([200, 404, 500], rows, p=[0.85, 0.1, 0.05])

    data = {
        'timestamp': timestamps,
        'user_id': user_ids,
        'article_id': article_ids,
        'status_code': status_codes
    }
    
    return pd.DataFrame(data)

def save_to_csv(df, file_path):
    """
    DataFrameをCSVファイルとして保存する。
    """
    # フォルダが存在しない場合は作成
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    df.to_csv(file_path, index=False)
    print(f"ダミーデータが {file_path} に保存されました。")

if __name__ == "__main__":
    df = generate_dummy_data()
    # 保存先を 'data/blog_access_logs_raw.csv' に変更
    save_to_csv(df, '../data/blog_access_logs_raw.csv')