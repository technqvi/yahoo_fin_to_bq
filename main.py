import yfinance as yf
from google.cloud import bigquery
import pandas as pd

from datetime import datetime,date,timedelta

import functions_framework

@functions_framework.http
def load_yahoo_to_bq(request):

    is_history_data=True
    #write_method='WRITE_TRUNCATE'
    write_method='WRITE_APPEND'
    table_id="pongthorn.FinDW.yahoo_fin_asset_price"

    symbolList=['SPY'] # load history
    #symbolList=['ETH-USD','MATIC-USD'] # load history
    #symbolList=['ACWI','SPY','EEM','ETH-USD','MATIC-USD']  # load daily
    
    is_ok='error'

    colListYahoo=['Date','Symbol','Open','High','Low','Close','Volume']
    colListMapping={'Date':'datetime','Symbol':'symbol','Open':'open','High':'high','Low':'low','Close':'close','Volume':'volume'}


    if is_history_data==True:
        # load since 01-10-22 you fill 02-10-22
        my_start_date_minute1d=datetime(2022,10,23)
        my_end_date_minute1d=datetime(2022,10,30)
        yahoo_start_date=my_start_date_minute1d+timedelta(days=-1)

        #my_end_date_minute1d=today
        yahoo_end_date=my_end_date_minute1d

        print("Load historical data")
        print(f"My date range: {my_start_date_minute1d} to {my_end_date_minute1d}")
        print(f"Yahoo load since {yahoo_start_date} to {yahoo_end_date}")

    else:

        today=date.today()
        today=datetime(today.year,today.month,today.day)
        prev_today=today+timedelta(days=-1)
        print(f"Load data of today")
        print(f"Yahoo load  : {prev_today}" )


    conn = bigquery.Client()
    # for symbol_name in symbolList:
    #     sql_last_item=f"""
    #     SELECT COUNT (1) as count_rows  FROM `{table_id}`
    #     WHERE date < '{today}' and symbol='{symbol_name}'
    #     """
    #     # print(sql_last_item)
    #     query_job =conn.query(query=sql_last_item)
    #     results = query_job.result()
    #     total_item=0
    #     for row in results:
    #         total_item=row.count_rows
    #     print(f"No rows before {total_item}")


 

    df_all=pd.DataFrame(columns=colListYahoo)
    # if  total_item==0 :
    for symbol_name in symbolList:
        if is_history_data:
            dfx = yf.download(symbol_name,interval='1d',start=my_start_date_minute1d,end=my_end_date_minute1d)
        else:
            # dfx = yf.download(symbol_name,interval='1d',period='1d')
            dfx = yf.download(symbol_name,interval='1d',start=prev_today,end=today)
        if (dfx.empty==False) or (dfx is not None):
            dfx['Symbol'] = symbol_name
            dfx=dfx.reset_index()
            dfx=dfx[colListYahoo]
            df_all=pd.concat([df_all,dfx])


    if df_all.empty==False:
        df_all=df_all.rename(columns=colListMapping)

        impport_at_val=datetime.now()
        df_all['import_at']=impport_at_val
        print(df_all)
    else:
        print(f"No data to download from yahoo")


    if df_all.empty==False:
        try:

            client = bigquery.Client()
            job_config = bigquery.LoadJobConfig(
                write_disposition=write_method,
            )
            job = client.load_table_from_dataframe(
                df_all, table_id, job_config=job_config,
            )  # Make an API request.
            job.result()  # Wait for the job to complete.
            print(f"Total ", len(df_all), f" Add transaction to {table_id} bigquery successfully")


        except:
          print(job.error_result)
    else:
        print(f"No data to write into bigquery")

    is_ok='ok'



    return  is_ok



if __name__ == "__main__":
 result=load_yahoo_to_bq(None)
 print(result)


