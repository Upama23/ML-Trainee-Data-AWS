import json
import boto3
import http.client
import pandas as pd 
import urllib3
import http.client
import psycopg2
import os

s3 = boto3.client('s3')

url = "https://jsonplaceholder.typicode.com/posts"
http = urllib3.PoolManager()

def lambda_handler(event, context):
    # TODO implement

    s3_client = boto3.client('s3')
    response = http.request('GET', url)
    
    bucket_name = "apprentice-training-ml-upama-dev-raw-data"  # Replace with your S3 bucket name
    object_key = "data.json"  # Replace with the desired object key (path) in the bucket
    
    s3_client.put_object(
        Bucket=bucket_name,
        Key=object_key,
        Body=response.data,  # Upload the raw data bytes
        ContentType="application/json"  # Set appropriate content type
    )
    
    data = response.data.decode('utf-8')
    json_data = json.loads(data)
    df = pd.DataFrame(json_data)
    cleaned_df = df.dropna()
    cleaned_df['title'] = cleaned_df['title'].str.strip()
    cleaned_df['title'] = cleaned_df['title'].str.lower()
    cleaned_df['body'] = cleaned_df['body'].str.replace('[^\w\s]', '')
    
    
    df = cleaned_df.to_json(orient="records")
    
    bucket_name = "apprentice-training-ml-upama-dev-cleaned-data"
    object_key = "data1.json"
    
    s3_client.put_object(
        Bucket=bucket_name,
        Key=object_key,
        Body=df.encode("utf-8"),  # Upload the raw data bytes
        ContentType="application/json"  # Set appropriate content type
    )
    
    try:
        conn = psycopg2.connect(
        host = os.environ['host'],
        database = os.environ['database'],
        user = os.environ['user'],
        password = os.environ['password']
    )
        print("Connected to postgres")
    except Exception as e:
        print(e)
     
        
    cursor = conn.cursor()
    
    cursor.execute ("""
    Create table if not exists etl_upama_posts_table(
    userId int,
    id int,
    title varchar(255),
    body varchar(255)
    )
    """)
    
    # Prepare data for insertion
    data_to_insert = [tuple(row) for row in cleaned_df.values]

    # Insert data into the table
    insert_query = f"""
    INSERT INTO etl_upama_posts_table 
    (userId, id,title, body) 
    VALUES (%s, %s, %s, %s)
    """
    cursor.executemany(insert_query, data_to_insert)
    conn.commit()
    
    print("Data uploaded to S3 successfully.")
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
