import boto3
from time import sleep
from datetime import date, timedelta, datetime
import pandas as pd
import io
import os


athena = boto3.client("athena")
s3=boto3.resource('s3')
output_s3_bucket=os.environ['BUCKET']
output_s3_key=os.environ['KEY']
database = os.environ['Database']

'''
:summary: - populates the query by formatting the string with from date, to date and the arn string
:param1:  - from_date is used to query for cycle_frdate
:param2:  - to_date is used to query for cycle_todate
:param3:  - arn_string is used to filter trs_agent
:return:  - formatted query
'''
def populate_query(to_date):
    date_format_str = "%Y-%m-%d"
    
    to_date_str = to_date.strftime(date_format_str)

    query = '''
        SELECT batchclosedt, scheme, plan, sum(netunits) units, avg(nav) nav, sum(aum) aum from aum_dtable 
        where fund = '108'
        and batchclosedt = '{}'
        and scheme not in (select distinct fm_Scheme from "mirror".uti_dbo_mcr_schexclude_ro)
        and concat(scheme,plan) not in (select concat(scheme,plan) from "mirror".global_plan_exclude where fund='108')
        and scheme not in (select distinct segregatedscheme from "mirror".uti_dbo_segregatedschememaster_ro where segregateddate < '{}')
        group by 1,2,3
        order by 3,2,1
    '''
    p_query = query.format(to_date_str, to_date_str)
    print(p_query)
    return p_query
    

'''
:summary: - triggers the populate_query function
            executes the query using athena.start_query_execution
            gets the query_execution_id from the response for the query execution
:param2:  - from_date is passed to populate_query
:return:  - query_execution_id obtained from response
'''
def start_query_execution(to_date):
    
    query = populate_query(to_date)
    
    response = athena.start_query_execution(
        QueryString= query,
        QueryExecutionContext = {
            'Database' :database
        },
        ResultConfiguration={"OutputLocation":"s3://{bucket}/{key}/".format(bucket=output_s3_bucket,key=output_s3_key)}
    )
    query_execution_id = response["QueryExecutionId"]
    wait_for_query_to_complete(query_execution_id)
    
    return query_execution_id

'''
:summary:  - obtains response from the s3 bucket using the query_execution_id
             converts the csv file to a dataframe
:param1:   - results are stored in s3 with query_execution_id as the filename, using that here to fetch data
:return:   - returns the csv data in the form of a dataframe
'''
def obtain_data(query_execution_id):
    response = s3 \
    .Bucket(output_s3_bucket) \
    .Object(key= output_s3_key + '/' + query_execution_id + '.csv') \
    .get()
    
    data = pd.read_csv(io.BytesIO(response['Body'].read()), encoding='utf8')
    return data

'''
:summary:  - waits for the query to complete and raises exception in necessary cases
:param1:   - query_execution_id to check the status of the query
'''
def wait_for_query_to_complete(query_execution_id):
    is_query_running = True
    backoff_time =2
    while is_query_running:
        response = athena.get_query_execution(QueryExecutionId=query_execution_id)
        status = response["QueryExecution"]["Status"]["State"]
        if status == "SUCCEEDED":
            is_query_running=False
        elif status in ["CANCELLED","FAILED"]:
            raise AthenaQueryFailed(status)
        elif status in ["QUEUED","RUNNING"]:
            print("INFO| backing off for {} seconds".format(backoff_time))
            sleep(backoff_time)
        else:
            raise AthenaQueryFailed(status)
        backoff_time = 2*backoff_time
        
class AthenaQueryFailed(Exception):
    pass
