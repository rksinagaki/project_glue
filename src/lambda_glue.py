import boto3
import os

def lambda_handler(event, context):
    # 環境変数からGlueジョブ名を取得
    # glue_job_name = os.environ['GLUE_JOB_NAME']
    glue_job_name = 'glue_job_transform'

    # S3バケット名とオブジェクトキーを取得
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_key = event['Records'][0]['s3']['object']['key']

    print(f"S3イベントを検知しました。バケット: {bucket_name}, ファイル: {file_key}")

    # Glueクライアントを初期化
    glue_client = boto3.client('glue')

    # Glueジョブを実行
    try:
        response = glue_client.start_job_run(
            JobName=glue_job_name,
            Arguments={
                '--S3_INPUT_PATH': f"s3://{bucket_name}/{file_key}"
            }
        )
        print(f"Glueジョブ '{glue_job_name}' を起動しました。")
        print(response)

    except Exception as e:
        print(f"Glueジョブの起動に失敗しました: {e}")
        raise e