import pandas as pd
import numpy as np

df=pd.read_csv('./data/min_data.csv')

# メタ系統の情報の削除
columns_url = [col for col in df.columns if 'url' in col]
columns_scrape = [col for col in df.columns if 'scrape' in col]
columns_id = [col for col in df.columns if 'id' in col]

description_list = ['description', 'overview', 'about', 'name']
columns_to_add = []
for col in df.columns:
    for keyword in description_list:
        if keyword in col:
            columns_to_add.append(col)
            break

num_rows = len(df)
valid_counts = df.count()
null_columns = valid_counts[valid_counts / num_rows < 0.5].index.to_list()

# 削除すコラム
list = columns_url + columns_scrape +columns_id+columns_to_add +null_columns
drop_list = set(list)
 
df_cleansed = df.drop(columns=drop_list, axis=1)
df_cleansed=df_cleansed.reset_index(drop=True)

# テーブル分割（大枠）
host_df = df_cleansed[[col for col in df_cleansed.columns if 'host' in col]]

review_df = df_cleansed[[col for col in df_cleansed.columns if 'review' in col]]

state_df = df_cleansed[[col for col in df_cleansed.columns if 'availability' in col]]

keywords_to_exclude = ['host', 'review', 'availability']
listing_columns = [col for col in df_cleansed.columns if not any(keyword in col for keyword in keywords_to_exclude)]
listing_df = df_cleansed[listing_columns]

 ## hostテーブルの処理
host_drop_list = ['host_verifications', 'host_has_profile_pic','host_location']
host_df_dropped = host_df.drop(columns=host_drop_list, axis=1)

print(host_df_dropped['host_since'])
host_df_dropped['host_since'] = pd.to_datetime(host_df_dropped['host_since'])
host_to_process = ['host_response_rate', 'host_acceptance_rate']
for col in host_to_process:
    host_df_dropped[col]=host_df_dropped[col].str.replace('%', '').astype(float)/100

 ## レビューテーブルの処理
review_to_process = ['first_review', 'last_review']
for col in review_to_process:
    review_df.loc[:, col] = pd.to_datetime(review_df[col])

 ## listingテーブルの処理
other_drop_list = ['source', 'neighbourhood','minimum_minimum_nights',
       'maximum_minimum_nights', 'minimum_maximum_nights',
       'maximum_maximum_nights', 'minimum_nights_avg_ntm',
       'maximum_nights_avg_ntm', 'license','amenities']

df_listing = listing_df.drop(columns=other_drop_list, axis=1)
df_listing['price'] = df_listing['price'].str.replace('[$,]', '', regex=True).astype(float)
