import logging
import azure.functions as func

import requests
import pandas as pd
from sqlalchemy import create_engine, text, Table, MetaData
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
import os


consumer = os.environ['consumer']
consumer_secret = os.environ['consumerSecret']
token = os.environ['token']
token_secret = os.environ['tokenSecret']
realm = os.environ['realm']
connection_string = os.environ['connectionString']

app = func.FunctionApp()

max_retries = 3

url = f"https://{realm}.suitetalk.api.netsuite.com/services/rest/query/v1/suiteql"

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
        self.realm = realm

    # def create_header_non_self():
    #     http_method = "POST"
    #     realm = realm
    #     client = oauth1.Client(consumer, 
    #                          client_secret=consumer_secret, 
    #                          resource_owner_key=token, 
    #                          resource_owner_secret=token_secret, 
    #                          signature_method=oauth1.SIGNATURE_HMAC_SHA256, 
    #                          signature_type=oauth1.SIGNATURE_TYPE_AUTH_HEADER,
    #                          realm=realm 
    #                          )
    #     uri, headers, body = client.sign(url, http_method=http_method)
    #     headers['prefer']='transient'
    #     return headers

    def create_header(self):
        '''
        Creates header to retrieve data through the API
        '''
        http_method = "POST"
        realm = self.realm
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
        Compiles data extracted through the API
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
        Updates the URL for the API to determine if more data is coming in
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


class DataProcessingService:
    def __init__(self, data_fetcher: APIDataFetcher, refresh_timer: RefreshTimer, table_name, unique_identifier):
        self.data_fetcher = data_fetcher
        self.info = refresh_timer
        self.retries = 0
        self.name_of_retires = []
        self.all_results_list = []
        self.today = date.today()
        self.engine = create_engine(connection_string, echo=False)
        self.Session = sessionmaker(bind=self.engine)
        self.table_name = table_name
        self.nor = 0
        self.unique_identifier = unique_identifier

    def GetData(self):
        self.info.settingName(self.table_name)
        self.info.startInfo()
        while self.retries < max_retries:
            try:
                data_df = self.data_fetcher.fetch_data()
                self.processed_data = self._process_data(data_df)
                self.nor = len(self.processed_data)
                break
            except Exception as e:
                self.retries = self.retries + 1
                error_message = str(e)
                traceback_message = traceback.format_exc()
                time.sleep(20)
                logging.info(f"Slice of data failed for {self.table_name}.\n\n Error message: {error_message} \n\n Traceback message: {traceback_message}")
        if self.retries == max_retries:
            self.nor = 0
            logging.info(f"Retry Limit Reached - NetSuite {self.table_name} Refresh Failed")
            self.info.endtimer(self.nor)
        else:
            self.info.endtimer(self.nor)
       
        

    def _process_data(self, data_df):

        processed_data = data_df  
        
        return processed_data
    
    
    def InsertData(self):
        self.info.settingName("Final Push to Database")
        self.info.startInfo()
        Failed = False
        try:
            if len(self.processed_data) > 0:
                self.processed_data['Date'] = self.today
                self.processed_data.to_sql(name=self.table_name, con=self.Session.connection(), if_exists='append', index=False, chunksize=1000)
                self.Session.commit()
        except SQLAlchemyError as e:
            Failed = True
            error_message = str(e)
            traceback_message = traceback.format_exc()
            self.Session.rollback()
            logging.info(f"KW - NS INSERTION Error Occured for {self.table_name}\n Error Message: {error_message} \n Traceback Message: {traceback_message}")
        finally:
            self.Session.close()
            self.info.endtimer(len(self.nor))
            self.FinishingUp(Failed)


    def FinishingUp(self, Failed):
        dirty_minutes, dirty_seconds = divmod(self.info.total_time, 60)
        total_minutes = int(dirty_minutes)
        total_seconds = int(dirty_seconds)
        clean_total_time = f"{total_minutes} minutes, {total_seconds:02d} seconds"
        if Failed == False:
            logging.info(f"{self.table_name} Daily NS Refresh Successful Congrats! Data for the {self.table_name} Table was successfully refreshed.\nThe total number of rows that were inserted into the table were {self.nor} \n\n\nStats for the refresh:\n\nTotal time: {clean_total_time}\n\nNumber of retires: {self.retries}")
        else:
            logging.info("Failed to insert into Database")

        
    def UpdateData(self):
        if self.nor > 0:
            self.info.settingName("Final Push to Database")
            self.info.startInfo()
            df = self.processed_data.apply(lambda x: x.where(pd.notnull(x), None), axis=1)
            metadata = MetaData()
            table = Table(self.table_name, metadata, autoload_with=self.engine)
            id_list = self.processed_data[self.unique_identifier].unique().tolist()
            id_list_str = ', '.join([str(id) for id in id_list])
            select_sql = f"SELECT {self.unique_identifier} FROM {self.table_name} WHERE {self.unique_identifier} IN ({id_list_str})"
            failed_db_query = False
            try:
                with self.engine.connect() as conn:
                    result = conn.execute(text(select_sql))

                    existing_ids = result.fetchall()

                    id_list = [int(row[0]) for row in existing_ids]
                
            except SQLAlchemyError as e:
                failed_db_query = True
                error_message = str(e)
                traceback_message = traceback.format_exc()
                logging.info(f"KW - NS INSERTION Error Occured for {self.table_name}\n Error Message: {error_message} \n Traceback Message: {traceback_message}")
        
            if failed_db_query == False:
                try:
                    with self.Session() as conn:
                        for index, row in df.iterrows():
                            uniquekey = int(row[self.unique_identifier])
                            column = getattr(table.c, self.unique_identifier)
                            
                            update_values = {column: row[column] for column in df.columns if column != self.unique_identifier}

                            if uniquekey in id_list:
                                update_stmt = table.update().where(column == uniquekey).values(update_values)

                                conn.execute(update_stmt)
                            else:
                                insert_values = update_values.copy()
                                insert_values[self.unique_identifier] = uniquekey
                                insert_stmt = table.insert().values(insert_values)
                                conn.execute(insert_stmt)
                        conn.commit()

                except SQLAlchemyError as e:
                    self.Session.rollback()
                    error_message = str(e)
                    traceback_message = traceback.format_exc()

                    logging.info(f"KW - NS INSERTION Error Occured for {self.table_name}\n Error Message: {error_message} \n Traceback Message: {traceback_message}")
                finally: 
                    self.info.endtimer(self.nor)
                    self.FinishingUp(failed_db_query)
        else:
            logging.info(f'No updates occured. There were no changes over the past day for {self.table_name}')


@app.schedule(schedule="0 55 0 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def WarmingUp(myTimer: func.TimerRequest) -> None:
    warmup_engine = create_engine(connection_string, echo=False, connect_args={'connect_timeout': 60})
    try:
        with warmup_engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            main = result.fetchall()
            logging.info(main[0][0 ])
    except SQLAlchemyError as e:
        logging.info("Warming up failed")


@app.schedule(schedule="0 0 1 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def Classifications(myTimer: func.TimerRequest) -> None:
    logging.info(consumer) 

    unique_identifier = 'id'
    
    Table_Name = "Classification" 

    netsuite_table_name = 'Classification'

    query_items = 'externalid, fullname, id, includechildren, isinactive, lastmodifieddate, name, subsidiary, parent, custrecord_n101_cseg_business_unit'

    main_query = {
        "q": f"SELECT {query_items} FROM '{netsuite_table_name}' AND (lastmodifieddate >= TRUNC(SYSDATE - 1) AND lastmodifieddate < TRUNC(SYSDATE))"
    }

    data_fetcher = APIDataFetcher(query=main_query)
    refresh_timer = RefreshTimer()

    processing_service = DataProcessingService(data_fetcher, refresh_timer, table_name=Table_Name, unique_identifier=unique_identifier)

    processing_service.GetData()

    processing_service.UpdateData()


@app.schedule(schedule="0 3 1 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def Invoices(myTimer: func.TimerRequest) -> None:
    logging.info(consumer)

    unique_identifier = 'id'
    
    Table_Name = "Invoices" 

    netsuite_table_name = 'CustInvc'

    query_items = 'closedate, createddate, duedate, entity, estgrossprofit, id, lastmodifieddate, ordpicked, postingperiod, printedpickingticket, shipdate, status, trandate, shipcarrier'

    main_query = {
        "q": f"SELECT {query_items} FROM transaction WHERE type = '{netsuite_table_name}' AND (lastmodifieddate >= TRUNC(SYSDATE - 1) AND lastmodifieddate < TRUNC(SYSDATE))"
    }

    data_fetcher = APIDataFetcher(query=main_query)
    refresh_timer = RefreshTimer()

    processing_service = DataProcessingService(data_fetcher, refresh_timer, table_name=Table_Name, unique_identifier=unique_identifier)

    processing_service.GetData()

    processing_service.UpdateData()


@app.schedule(schedule="0 7 1 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def ItemCategory(myTimer: func.TimerRequest) -> None:
    logging.info(consumer)

    unique_identifier = 'id'
    
    Table_Name = "ItemCategory" 

    netsuite_table_name = 'CUSTOMLIST_ITEM_CATEGORY'

    query_items = 'id, name'

    main_query = {
        "q": f"SELECT {query_items} FROM '{netsuite_table_name}' WHERE (lastmodifieddate >= TRUNC(SYSDATE - 1) AND lastmodifieddate < TRUNC(SYSDATE))"
    }

    data_fetcher = APIDataFetcher(query=main_query)
    refresh_timer = RefreshTimer()

    processing_service = DataProcessingService(data_fetcher, refresh_timer, table_name=Table_Name, unique_identifier=unique_identifier)

    processing_service.GetData()

    processing_service.UpdateData()


@app.schedule(schedule="0 12 1 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def Customers(myTimer: func.TimerRequest) -> None:
    logging.info(consumer)

    unique_identifier = 'id'
    
    Table_Name = "Customers" 

    netsuite_table_name = 'Customers'

    query_items = 'id, entitytitle, isperson, defaultshippingaddress'

    main_query = {
        "q": f"SELECT {query_items} FROM '{netsuite_table_name}' WHERE (lastmodifieddate >= TRUNC(SYSDATE - 1) AND lastmodifieddate < TRUNC(SYSDATE))"
    }

    data_fetcher = APIDataFetcher(query=main_query)
    refresh_timer = RefreshTimer()

    processing_service = DataProcessingService(data_fetcher, refresh_timer, table_name=Table_Name, unique_identifier=unique_identifier)

    processing_service.GetData()

    processing_service.UpdateData()


@app.schedule(schedule="0 15 1 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def InventoryOverTime(myTimer: func.TimerRequest) -> None:
    logging.info(consumer)

    unique_identifier = 'inventorynumber'
    
    Table_Name = "InventoryOverTime" 

    netsuite_table_name = 'InventoryBalance'

    query_items = 'binnumber, committedqtyperlocation, committedqtyperseriallotnumber, committedqtyperseriallotnumberlocation, inventorynumber, item, location, quantityavailable, quantityonhand, quantitypicked'

    main_query = {
        "q": f"SELECT {query_items} FROM '{netsuite_table_name}'"
    }

    data_fetcher = APIDataFetcher(query=main_query)
    refresh_timer = RefreshTimer()

    processing_service = DataProcessingService(data_fetcher, refresh_timer, table_name=Table_Name, unique_identifier=unique_identifier)

    processing_service.GetData()

    processing_service.InsertData()


@app.schedule(schedule="0 22 1 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def InvoiceTransactionLines(myTimer: func.TimerRequest) -> None:
    logging.info(consumer)

    unique_identifier = 'uniquekey'
    
    Table_Name = "InvoicesLineItems" 

    netsuite_table_name = 'CustInvc'

    query_items = 'uniquekey, transaction, linesequencenumber, item, location, netamount, subsidiary, linelastmodifieddate, itemtype, isclosed, isfullyshipped'

    main_query = {
        "q": f"SELECT {query_items} FROM transactionLine tl INNER JOIN transaction t ON tl.transaction = t.id WHERE t.type = '{netsuite_table_name}' AND (linelastmodifieddate >= TRUNC(SYSDATE - 1) AND linelastmodifieddate < TRUNC(SYSDATE))"
    }

    data_fetcher = APIDataFetcher(query=main_query)
    refresh_timer = RefreshTimer()

    processing_service = DataProcessingService(data_fetcher, refresh_timer, table_name=Table_Name, unique_identifier=unique_identifier)

    processing_service.GetData()

    processing_service.UpdateData()


@app.schedule(schedule="0 28 1 * * *", arg_name="myTimer", run_on_startup=False,
            use_monitor=False) 
def ItemFulfillments(myTimer: func.TimerRequest) -> None:
    logging.info(consumer)

    unique_identifier = 'id'
    
    Table_Name = "ItemFulfillments" 

    netsuite_table_name = 'ItemShip'

    query_items = 'uniquekey, transaction, linesequencenumber, item, location, netamount, subsidiary, linelastmodifieddate, itemtype, isclosed, isfullyshipped'

    main_query = {
        "q": f"SELECT {query_items} FROM transaction WHERE type = '{netsuite_table_name}' AND (lastmodifieddate >= TRUNC(SYSDATE - 1) AND lastmodifieddate < TRUNC(SYSDATE))"
    }

    data_fetcher = APIDataFetcher(query=main_query)
    refresh_timer = RefreshTimer()

    processing_service = DataProcessingService(data_fetcher, refresh_timer, table_name=Table_Name, unique_identifier=unique_identifier)

    processing_service.GetData()

    processing_service.UpdateData()


@app.schedule(schedule="0 32 1 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def ItemFullTransactionLines(myTimer: func.TimerRequest) -> None:
    logging.info(consumer)

    unique_identifier = 'uniquekey'
    
    Table_Name = "ItemFulfillmentLineItems" 

    netsuite_table_name = 'ItemShip'

    query_items = 'uniquekey, transaction, linesequencenumber, item, location, netamount, subsidiary, linelastmodifieddate, itemtype, isclosed, isfullyshipped'

    main_query = {
        "q": f"SELECT {query_items} FROM transactionLine tl INNER JOIN transaction t ON tl.transaction = t.id WHERE t.type = '{netsuite_table_name}' AND (linelastmodifieddate >= TRUNC(SYSDATE - 1) AND linelastmodifieddate < TRUNC(SYSDATE))"
    }

    data_fetcher = APIDataFetcher(query=main_query)
    refresh_timer = RefreshTimer()

    processing_service = DataProcessingService(data_fetcher, refresh_timer, table_name=Table_Name, unique_identifier=unique_identifier)

    processing_service.GetData()

    processing_service.UpdateData()


@app.schedule(schedule="0 39 1 * * *", arg_name="myTimer", run_on_startup=False,
            use_monitor=False) 
def SalesOrders(myTimer: func.TimerRequest) -> None:
    logging.info(consumer)

    unique_identifier = 'id'
    
    Table_Name = "SalesOrders" 

    netsuite_table_name = 'SalesOrd'

    query_items = 'closedate, createddate, duedate, entity, estgrossprofit, id, lastmodifieddate, ordpicked, postingperiod, printedpickingticket, shipdate, status, trandate, shipcarrier'

    main_query = {
        "q": f"SELECT {query_items} FROM transaction WHERE type = '{netsuite_table_name}' AND (lastmodifieddate >= TRUNC(SYSDATE - 1) AND lastmodifieddate < TRUNC(SYSDATE))"
    }

    data_fetcher = APIDataFetcher(query=main_query)
    refresh_timer = RefreshTimer()

    processing_service = DataProcessingService(data_fetcher, refresh_timer, table_name=Table_Name, unique_identifier=unique_identifier)

    processing_service.GetData()

    processing_service.UpdateData()


@app.schedule(schedule="0 43 1 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def SalesOrdTransactionLines(myTimer: func.TimerRequest) -> None:
    logging.info(consumer)

    unique_identifier = 'uniquekey'
    
    Table_Name = "SalesOrderTransactionLine" 

    netsuite_table_name = 'SalesOrd'

    query_items = 'uniquekey, transaction, linesequencenumber, item, location, netamount, subsidiary, linelastmodifieddate, itemtype, isclosed, isfullyshipped, quantity'

    main_query = {
        "q": f"SELECT {query_items} FROM transactionLine tl INNER JOIN transaction t ON tl.transaction = t.id WHERE t.type = '{netsuite_table_name}' AND (linelastmodifieddate >= TRUNC(SYSDATE - 1) AND linelastmodifieddate < TRUNC(SYSDATE))"
    }

    data_fetcher = APIDataFetcher(query=main_query)
    refresh_timer = RefreshTimer()

    processing_service = DataProcessingService(data_fetcher, refresh_timer, table_name=Table_Name, unique_identifier=unique_identifier)

    processing_service.GetData()

    processing_service.UpdateData()


@app.schedule(schedule="0 50 1 * * *", arg_name="myTimer", run_on_startup=False,
            use_monitor=False) 
def Items(myTimer: func.TimerRequest) -> None:
    logging.info(consumer)

    unique_identifier = 'id'
    
    Table_Name = "Items" 

    netsuite_table_name = 'item'

    query_items = 'id, class, displayname, lastmodifieddate, custitem_model, custitem_item_category, totalquantityonhand, custitem_ped_model, custitem_ped_battery_size'

    main_query = {
        "q": f"SELECT {query_items} FROM '{netsuite_table_name}' WHERE (lastmodifieddate >= TRUNC(SYSDATE - 1) AND lastmodifieddate < TRUNC(SYSDATE))"
    }

    data_fetcher = APIDataFetcher(query=main_query)
    refresh_timer = RefreshTimer()

    processing_service = DataProcessingService(data_fetcher, refresh_timer, table_name=Table_Name, unique_identifier=unique_identifier)

    processing_service.GetData()

    processing_service.UpdateData()


@app.schedule(schedule="0 57 1 * * *", arg_name="myTimer", run_on_startup=False,
            use_monitor=False) 
def Locations(myTimer: func.TimerRequest) -> None:
    logging.info(consumer)

    unique_identifier = 'id'
    
    Table_Name = "Location" 

    netsuite_table_name = 'Location'

    query_items = 'custrecord1, custrecord_loc_shiphawk_warehouse_code, fullname, id, includechildren, isinactive, lastmodifieddate, mainaddress, makeinventoryavailable, makeinventoryavailablestore, name, returnaddress, subsidiary, usebins, custrecord_n103_cseg_business_unit, locationtype, parent'

    main_query = {
        "q": f"SELECT {query_items} FROM '{netsuite_table_name}' WHERE (lastmodifieddate >= TRUNC(SYSDATE - 1) AND lastmodifieddate < TRUNC(SYSDATE))"
    }

    data_fetcher = APIDataFetcher(query=main_query)
    refresh_timer = RefreshTimer()

    processing_service = DataProcessingService(data_fetcher, refresh_timer, table_name=Table_Name, unique_identifier=unique_identifier)

    processing_service.GetData()

    processing_service.UpdateData()