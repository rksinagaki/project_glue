from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_date, regexp_replace, trim, lit, sum

# Sparkセッションを初期化
# Glueジョブではこの行は不要です。
spark = SparkSession.builder.appName("DataCleansing").getOrCreate()

# S3からCSVファイルを読み込む
# Glueジョブでは、s3://... のようにS3 URIを直接指定します
df = spark.read.csv("s3://your-raw-data-bucket/data/min_data.csv", header=True, inferSchema=True)

# 削除するカラムのリストを作成
columns_to_drop = set()
metadata_keywords = ['url', 'scrape', 'id', 'description', 'overview', 'about', 'name']
for keyword in metadata_keywords:
    columns_to_drop.update([c for c in df.columns if keyword in c])

# 欠損値が50%を超えるカラムを特定
num_rows = df.count()
null_counts = df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns]).collect()[0]
null_columns = [k for k in null_counts.asDict() if null_counts[k] / num_rows > 0.5]
columns_to_drop.update(null_columns)

# 一括でカラムを削除
df_cleansed = df.drop(*list(columns_to_drop))

# メインのDataFrameを複数のテーブルに分割
keywords_to_exclude = ['host', 'review', 'availability']
listing_columns = [c for c in df_cleansed.columns if not any(keyword in c for keyword in keywords_to_exclude)]

listing_df = df_cleansed.select(*listing_columns)
host_df = df_cleansed.select(*[c for c in df_cleansed.columns if 'host' in c])
review_df = df_cleansed.select(*[c for c in df_cleansed.columns if 'review' in c])
state_df = df_cleansed.select(*[c for c in df_cleansed.columns if 'availability' in c])

# hostテーブルの処理
host_drop_list = ['host_verifications', 'host_has_profile_pic', 'host_location']
host_df = host_df.drop(*host_drop_list)
host_df = host_df.withColumn('host_since', to_date(col('host_since'), 'yyyy-MM-dd'))
host_df = host_df.withColumn('host_response_rate', regexp_replace(col('host_response_rate'), '%', '').cast('float') / 100)
host_df = host_df.withColumn('host_acceptance_rate', regexp_replace(col('host_acceptance_rate'), '%', '').cast('float') / 100)

# reviewテーブルの処理
review_df = review_df.withColumn('first_review', to_date(col('first_review'), 'yyyy-MM-dd'))
review_df = review_df.withColumn('last_review', to_date(col('last_review'), 'yyyy-MM-dd'))

# listingテーブルの処理
other_drop_list = ['source', 'neighbourhood', 'minimum_minimum_nights',
                   'maximum_minimum_nights', 'minimum_maximum_nights',
                   'maximum_maximum_nights', 'minimum_nights_avg_ntm',
                   'maximum_nights_avg_ntm', 'license', 'amenities']
df_listing = listing_df.drop(*other_drop_list)
df_listing = df_listing.withColumn('price', regexp_replace(regexp_replace(col('price'), '\$|', ''), ',', '').cast('float'))

# 加工済みのテーブルをS3のそれぞれの場所にParquet形式で保存
df_listing.write.mode("overwrite").parquet("s3://your-processed-data-bucket/listing/")
host_df.write.mode("overwrite").parquet("s3://your-processed-data-bucket/host/")
review_df.write.mode("overwrite").parquet("s3://your-processed-data-bucket/review/")
state_df.write.mode("overwrite").parquet("s3://your-processed-data-bucket/state/")