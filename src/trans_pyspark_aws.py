import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_date, regexp_replace, trim, lit, sum

## @params: [JOB_NAME]
# パラメーター取得
args = getResolvedOptions(sys.argv, ['JOB_NAME','INPUT_PATH','OUTPUT_PATH'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#環境変数呼び出し
s3_path = args['INPUT_PATH']
output_path = args['OUTPUT_PATH']

# 加工処理
# データの読み込み
df = spark.read.csv(
    s3_path,
    header=True,
    inferSchema=True,
    sep=",",
    quote='"',
    escape='"',
    multiLine=True
)

# 削除するカラムのリストを作成
columns_to_drop = set()
metadata_keywords = ['url', 'scrape', 'id', 'description', 'overview', 'about', 'name']
for keyword in metadata_keywords:
    columns_to_drop.update([c for c in df.columns if keyword in c])

# 一括でカラムを削除
df_cleansed = df.drop(*list(columns_to_drop))

# メインのDataFrameを複数のテーブルに分割
keywords_to_exclude = ['host', 'review', 'availability']
listing_columns = [c for c in df_cleansed.columns if not any(keyword in c for keyword in keywords_to_exclude)]

listing_df = df_cleansed.select(*list(listing_columns))
host_df = df_cleansed.select(*[c for c in df_cleansed.columns if 'host' in c])
review_df = df_cleansed.select(*[c for c in df_cleansed.columns if 'review' in c])
state_df = df_cleansed.select(*[c for c in df_cleansed.columns if 'availability' in c])

# hostテーブルの処理
host_drop_list = ['host_verifications', 'host_has_profile_pic', 'host_location']
host_df = host_df.drop(*list(host_drop_list))
host_df = host_df.withColumn('host_since', to_date('host_since', 'yyyy-MM-dd'))
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

# 加工済みのテーブルをParquet形式で保存
df_listing.write.mode("overwrite").parquet(f"{output_path}listing/")
host_df.write.mode("overwrite").parquet(f"{output_path}host/")
review_df.write.mode("overwrite").parquet(f"{output_path}review/")
state_df.write.mode("overwrite").parquet(f"{output_path}state/")

job.commit()