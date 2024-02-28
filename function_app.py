import logging
import azure.functions as func

import requests
import pandas as pd
from sqlalchemy import create_engine, text, inspect
import time
import traceback
import copy
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
import json
from oauthlib import oauth1
from datetime import date
import time
from datetime import timedelta
import mysql
import numpy as np
 

max_retries = 3
# URL to connect to Pedego's NetSuite
url = "https://4550201.suitetalk.api.netsuite.com/services/rest/query/v1/suiteql"

consumer = "4e7920040841eb8a3f65a3a90b0f142cdd22e4db26c733e4d0f7decaf03c46f5"
consumer_secret = "988bc3bde021a1b4f018bc2e87f31d338a4ab19cc3c1ab0a7efed06abc731e6e"
token = "102813540121dd09f08df547d176787de25ea477bfdb6c844179c7dda08f9008"
token_secret = "563a75d6b47db6f42456a59f63de8e6114edb9f4fb7072ea8cc8f54bb573b3dc"

connection_string = 'mssql+pyodbc:///?odbc_connect=Driver={ODBC Driver 18 for SQL Server};Server=tcp:mysqlserverpedego.database.windows.net,1433;Database=pedego;Uid=azureuser;Pwd=TbZQJ@TNaS1MWp4BPMJ$;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'

# We can send error emails through Azure Function (https://medium.com/@elnably/setting-up-email-alerts-when-your-azure-functions-fail-a049f766309e)

# Will be using Azure Secrets to manage secret info.

class APIDataFetcher:
    '''
    Runs the SuiteQL to retrieve data and inserts it into the Azure MySQL database.
    '''
    def __init__(self, query):
        self.existing_data = None
        self.combined_df = None
        self.dataframes = None
        self.df_section = None
        self.data_Items = None
        self.response = None
        self.url = url
        self.query = query
        self.number_of_rows = None

    def create_header_non_self():
        http_method = "POST"
        realm = "4550201" 
        client = oauth1.Client(consumer, 
                             client_secret=consumer_secret, 
                             resource_owner_key=token, 
                             resource_owner_secret=token_secret, 
                             signature_method=oauth1.SIGNATURE_HMAC_SHA256, 
                             signature_type=oauth1.SIGNATURE_TYPE_AUTH_HEADER,
                             realm=realm 
                             )
        uri, headers, body = client.sign(url, http_method=http_method)
        headers['prefer']='transient'
        return headers

    def create_header(self):
        '''
        Creates header to retrieve data through the API???
        '''
        http_method = "POST"
        realm = "4550201"
        client = oauth1.Client(consumer, 
                             client_secret=consumer_secret, 
                             resource_owner_key=token, 
                             resource_owner_secret=token_secret, 
                             signature_method=oauth1.SIGNATURE_HMAC_SHA256, 
                             signature_type=oauth1.SIGNATURE_TYPE_AUTH_HEADER,
                             realm=realm 
                             )
        uri, headers, body = client.sign(self.url, http_method=http_method)
        headers['prefer']='transient'
        return headers
        
    def fetch_data(self):
        '''
        Compiles data extracted through the API???
        '''
        self.dataframes = []
        while self.url:
            self.response = self._get_api_response()
            self.data_Items = self.response['items']
            self.df_section = pd.DataFrame(self.data_Items)
            self.df_section.drop('links', axis=1, inplace=True)
            self.dataframes.append(self.df_section)
            self._update_url(self.response)
        return self._CombineList(self.dataframes)

    def _get_api_response(self):
        '''
        Gets the API response.
        '''
        headers = self.create_header()
        main_response = requests.post(self.url, headers=headers, json=self.query)
        return main_response.json()

    def _update_url(self, main_data):
        '''
        Updates the URL for the API to determine if more data is coming in???
        '''
        self.url = None
        for link in main_data['links']:
            if link['rel'] == 'next':
                self.url = link['href']
                break

    def _CombineList(self, dataframes):
        '''
        Combines two dataframes.
        '''
        self.combined_df = pd.concat(dataframes, ignore_index=True)
        return self.combined_df

class RefreshTimer:
    '''
    Times how long it takes to add new data to the Azure MySQL database.
    '''
    def __init__(self):
        self.dirty_seconds = None
        self.dirty_minutes = None
        self.total_time = 0.0
        self.clean_time = None
        self.minutes = None
        self.seconds = None
        self.table_name = None
        self.start_time = None
        self.ellapsed_time = None

    def settingName(self, name):
        '''
        Sets the name of the table whose refresh is being timed.
        '''
        self.table_name = name

    def startInfo(self):
        '''
        Starts the refresh timer.
        '''
        logging.info(f"{self.table_name} started.")
        self.start_time = time.time()

    def endtimer(self, num_of_rows):
        '''
        Ends the refresh timer and stores a clean version of the results.
        '''
        self.ellapsed_time = time.time() - self.start_time
        self.total_time = self.total_time + self.ellapsed_time
        self.dirty_minutes, self.dirty_seconds = divmod(self.ellapsed_time, 60)
        self.minutes = int(self.dirty_minutes)
        self.seconds = int(self.dirty_seconds)
        self.clean_time = f"{self.minutes} minutes, {self.seconds:02d} seconds"
        logging.info(f"{self.table_name} finished. The process took {self.clean_time}. {num_of_rows} rows affected.")

def InitializeFunction ():
    name_of_retries = []
    engine_new = create_engine(connection_string, echo=False)
    Session = sessionmaker(bind=engine_new)
    session_new = Session()
    info = RefreshTimer()
    info.settingName('Inventory Over Time')
    info.startInfo()

    return name_of_retries, session_new, info

def GetData (query, info, retries):
    all_results_list = []
    while retries < max_retries:
        try:
            api_section_class = APIDataFetcher(query=query)
            results_df = api_section_class.fetch_data()
            all_results_list.append(results_df)
            nor = len(results_df)
            break
        except Exception as e:
            retries = retries + 1
            error_message = str(e)
            traceback_message = traceback.format_exc()
            time.sleep(10)
            logging.info(f"Slice of data failed for Inventory.\n\n Error message: {error_message} \n\n Traceback message: {traceback_message}")
    if retries == max_retries:
        nor = 0
        logging.info(f"Retry Limit Reached - NetSuite {Table_Name} Refresh Failed on Section Inventory")

    info.endtimer(nor)

    return all_results_list


app = func.FunctionApp()

@app.schedule(schedule="0 0 1 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def InventoryOverTime(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')
    
    total_retries, name_of_retries, session_new, info = InitializeFunction()
 
    Table_Name = "InventoryOverTime"

    main_query = {
        "q": "SELECT * FROM InventoryBalance"
    }

    GetData(main_query, info, retries=0)





    today = date.today()




    def combine_and_filter_data(dataframes):
        combined_df = pd.concat(dataframes, ignore_index=True)
        return combined_df


    def insert_to_database(all_df):
        if len(all_df) > 0:
            all_df.to_sql(name=Table_Name, con=session_new.connection(), if_exists='append', index=False, chunksize=1000)


    info.settingName("Final Push to Database")
    info.startInfo()
    final_df = combine_and_filter_data(all_results_list)
    final_df['Date'] = today

    Failed = False
    try:
        insert_to_database(final_df)
        session_new.commit()
    except SQLAlchemyError as e:
        Failed = True
        error_message = str(e)
        traceback_message = traceback.format_exc()
        session_new.rollback()
        logging.info(f"KW - NS INSERTION Error Occured for {Table_Name}")
    finally:
        session_new.close()
        number_of_rows = len(final_df)
        info.endtimer(number_of_rows)

    dirty_minutes, dirty_seconds = divmod(info.total_time, 60)
    total_minutes = int(dirty_minutes)
    total_seconds = int(dirty_seconds)
    clean_total_time = f"{total_minutes} minutes, {total_seconds:02d} seconds"


    # Sending the final email
    if Failed == False:
        logging.info(f"{Table_Name} Daily NS Refresh Successful Congrats! Data for the {Table_Name} Table was successfully refreshed.\nThe total number of rows that were inserted into the table were {number_of_rows} \n\n\nStats for the refresh:\n\nTotal time:{clean_total_time}\n\n Time for each sectionn\nNumber of retires: {total_retries}\nSections of retry: {name_of_retries}")


    logging.info('Python timer trigger function executed.')




@app.timer_trigger(schedule="0 0 1 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def another_test(myTimer: func.TimerRequest) -> None:
    
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed.')