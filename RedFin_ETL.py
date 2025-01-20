from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
import pandas as pd
import boto3
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

s3_client = boto3.client('s3')

# s3 buckets
raw_bucket_name = 'redfinrawbucket'
transform_bucket_name = 'redfinbucket'

# url link from - https://www.redfin.com/news/data-center/
url_by_city = 'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/city_market_tracker.tsv000.gz'

def extract_data(**kwargs):
    url = kwargs['url']
    df = pd.read_csv(url, compression='gzip', sep='\t')
    now = datetime.now()
    date_now_string = now.strftime("%d%m%Y%H%M%S")
    file_str = 'redfin_data_' + date_now_string
    df.to_csv(f"{file_str}.csv", index=False)
    output_file_path = f"/home/ubuntu/{file_str}.csv"
    output_list = [output_file_path, file_str]

    # Upload raw data to S3 raw bucket
    raw_object_key = f"{file_str}.csv"
    s3_client.upload_file(output_file_path, raw_bucket_name, raw_object_key)

    return output_list

def transform_data(task_instance):
    data = task_instance.xcom_pull(task_ids="tsk_extract_redfin_data")[0]
    object_key = task_instance.xcom_pull(task_ids="tsk_extract_redfin_data")[1]
    df = pd.read_csv(data)

    # Remove commas from the 'city' column
    df['city'] = df['city'].str.replace(',', '')
    cols = ['period_begin','period_end','period_duration', 'region_type', 'region_type_id', 'table_id',
    'is_seasonally_adjusted', 'city', 'state', 'state_code', 'property_type', 'property_type_id',
    'median_sale_price', 'median_list_price', 'median_ppsf', 'median_list_ppsf', 'homes_sold',
    'inventory', 'months_of_supply', 'median_dom', 'avg_sale_to_list', 'sold_above_list', 'parent_metro_region_metro_code', 'last_updated']
   
    df = df[cols]
    df = df.dropna()

    # Change the period_begin and period_end to date time object and extract years and months.
    df['period_begin'] = pd.to_datetime(df['period_begin'])
    df['period_end'] = pd.to_datetime(df['period_end'])

    df["period_begin_in_years"] = df['period_begin'].dt.year
    df["period_end_in_years"] = df['period_end'].dt.year

    df["period_begin_in_months"] = df['period_begin'].dt.month
    df["period_end_in_months"] = df['period_end'].dt.month
    
    # Map the month number to their respective month name.
    month_dict = {
        "period_begin_in_months": {
            1 : "Jan",
             2 :  "Feb",
             3: "Mar",
             4: "Apr",
             5: "May",
             6: "Jun",
             7: "Jul",
             8: "Aug",
             9: "Sep",
             10: "Oct",
             11: "Nov",
             12: "Dec",
        },
        "period_end_in_months": {
            1 : "Jan",
             2 :  "Feb",
             3: "Mar",
             4: "Apr",
             5: "May",
             6: "Jun",
             7: "Jul",
             8: "Aug",
             9: "Sep",
             10: "Oct",
             11: "Nov",
             12: "Dec"
        }}
    
    df = df.replace(month_dict)
    print('Num of rows:', len(df))
    print('Num of cols:', len(df.columns))
        
    # Save transformed data locally
    transformed_file_path = f"/home/ubuntu/transformed_{object_key}.csv"
    df.to_csv(transformed_file_path, index=False)
    
    return transformed_file_path

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 20),
    'email': ['matheenroy@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=40)
}

with DAG('redfin_analytics_dag',
    default_args=default_args,
    catchup=False) as dag:

    extract_redfin_data = PythonOperator(
    task_id= 'tsk_extract_redfin_data',
    python_callable=extract_data,
    op_kwargs={'url': url_by_city}
    )

    transform_redfin_data = PythonOperator(
    task_id= 'tsk_transform_redfin_data',
    python_callable=transform_data
    )

    load_to_s3 = BashOperator(
        task_id = 'tsk_load_to_s3',
        bash_command = 'aws s3 mv {{ ti.xcom_pull("tsk_transform_redfin_data") }} s3://redfinbucket',
    )

    transfer_s3_to_redshift = S3ToRedshiftOperator(
    task_id="tsk_transfer_s3_to_redshift",
    aws_conn_id='aws_s3_conn',
    redshift_conn_id='conn_id_redshift',
    s3_bucket= transform_bucket_name,
    s3_key='{{ ti.xcom_pull(task_ids="tsk_transform_redfin_data").split("/")[-1] }}',
    schema="PUBLIC",
    table="redfindata",
    copy_options=["csv IGNOREHEADER 1"]
    )

    extract_redfin_data >> transform_redfin_data >> load_to_s3 >> transfer_s3_to_redshift
