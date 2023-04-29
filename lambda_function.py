import json
import get_daily_ucr
import send_email
from datetime import date, timedelta, datetime

def lambda_handler(event, context):
     # Entry Point
    try:
        trigger_date = date.today() 
        # trigger_date=trigger_date - timedelta(days=1)
        cycle_to_date = trigger_date - timedelta(days=1)
        
        # Executes the query to get the instapay details 
        query_execution_id = get_daily_ucr.start_query_execution(cycle_to_date)
        
        # Obtains the data in the form of a dataframe using the query_execution_id
        ucr_data = get_daily_ucr.obtain_data(query_execution_id)
        
        print(type(ucr_data))
        
        #send mail with the message after sp execution
        mail = send_email.send_email_with_message("Please find the UCR data attached. [DFS schemes only]", ucr_data)
        print(mail)
        
        return {
            'statusCode': 200,
            'body': json.dumps("Process Successful!")
            }
    except Exception as ex:
        print(ex)
        return {
            'statusCode': 400,
            'body': json.dumps(ex)
        }

