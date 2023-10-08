import pandas as pd
import yfinance as yf
import numpy as np
import math
import os
from tqdm import tqdm

def update_store_data(self,tickers_batch, batch_nbr, first_day, end_day, present,very_first_day):
    
    # it is time to download the data
    # create data frames to hold the new data
    data_open_temp = pd.DataFrame()
    data_close_temp = pd.DataFrame()
    data_high_temp = pd.DataFrame()
    data_low_temp = pd.DataFrame()
    data_vol_temp = pd.DataFrame()

    progress_bar = tqdm(total= len(tickers_batch), desc=f"{batch_nbr}")

    for ticker in tickers_batch:

        data = yf.download(ticker, start=first_day, end=end_day, progress=False)
    
        df_open = data[['Open']]  # Keep only the 'Open' column
        df_open.columns = [ticker]  # Rename the column to the ticker symbol
        df_close = data[['Close']]  # Keep only the 'Open' column
        df_close.columns = [ticker]  # Rename the column to the ticker symbol
        df_high = data[['High']]  # Keep only the 'Open' column
        df_high.columns = [ticker]  # Rename the column to the ticker symbol
        df_low = data[['Low']]  # Keep only the 'Open' column
        df_low.columns = [ticker]  # Rename the column to the ticker symbol
        df_vol = data[['Volume']]  # Keep only the 'Open' column
        df_vol.columns = [ticker]  # Rename the column to the ticker symbol

        #we want to exclude the first day only if we are updating (not creating data from scratch)
        if present: 
            df_open = df_open.iloc[1:]
            df_close = df_close.iloc[1:]
            df_high = df_high.iloc[1:]
            df_low = df_low.iloc[1:]
            df_vol = df_vol.iloc[1:]
         
        #append the generated ticker column
        data_open_temp = pd.concat([data_open_temp, df_open], axis=1)
        data_close_temp = pd.concat([data_close_temp, df_close], axis=1)
        data_high_temp = pd.concat([data_high_temp, df_high], axis=1)
        data_low_temp = pd.concat([data_low_temp, df_low], axis=1)
        data_vol_temp = pd.concat([data_vol_temp, df_vol], axis=1)
        
        progress_bar.update(1)
    
    progress_bar.close()
    # Add date as the first column
    data_open_temp.reset_index(inplace=True)
    data_close_temp.reset_index(inplace=True)
    data_high_temp.reset_index(inplace=True)
    data_low_temp.reset_index(inplace=True)
    data_vol_temp.reset_index(inplace=True)
   
    # append to exisiting data if present 
    if present: 
        data_open_temp['Date']=pd.to_datetime(data_open_temp.Date)
        data_close_temp['Date']=pd.to_datetime(data_close_temp.Date)
        data_high_temp['Date']=pd.to_datetime(data_high_temp.Date)
        data_low_temp['Date']=pd.to_datetime(data_low_temp.Date)
        data_vol_temp['Date']=pd.to_datetime(data_vol_temp.Date)
        
        print("updating the csv files: .. ")
        data_open_temp.to_csv(f"../DATABASE/pricevolume/{batch_nbr}_openprice_larger1Bcap_{very_first_day}.csv", mode='a', header=False, index=False)
        data_low_temp.to_csv(f"../DATABASE/pricevolume/{batch_nbr}_lowprice_larger1Bcap_{very_first_day}.csv", mode='a', header=False, index=False)
        data_high_temp.to_csv(f"../DATABASE/pricevolume/{batch_nbr}_highprice_larger1Bcap_{very_first_day}.csv", mode='a', header=False, index=False)
        data_close_temp.to_csv(f"../DATABASE/pricevolume/{batch_nbr}_closeprice_larger1Bcap_{very_first_day}.csv", mode='a', header=False, index=False)
        data_vol_temp.to_csv(f"../DATABASE/pricevolume/{batch_nbr}_volume_larger1Bcap_{very_first_day}.csv", mode='a', header=False, index=False)
        print("Done updating.\n")

    else: 
        # save the new data files 
        print("saving data files ...:")
        data_open_temp.to_csv(f"../DATABASE/pricevolume/{batch_nbr}_openprice_larger1Bcap_{very_first_day}.csv", index=False)
        data_close_temp.to_csv(f"../DATABASE/pricevolume/{batch_nbr}_closeprice_larger1Bcap_{very_first_day}.csv", index=False)
        data_high_temp.to_csv(f"../DATABASE/pricevolume/{batch_nbr}_highprice_larger1Bcap_{very_first_day}.csv", index=False)
        data_low_temp.to_csv(f"../DATABASE/pricevolume/{batch_nbr}_lowprice_larger1Bcap_{very_first_day}.csv", index=False)
        data_vol_temp.to_csv(f"../DATABASE/pricevolume/{batch_nbr}_volume_larger1Bcap_{very_first_day}.csv", index=False)
        print("Done saving.\n")
        

def run_delete_process(self): 
    folder_path = '../DATABASE/pricevolume'
    files_in_folder = os.listdir(folder_path)
    for file_name in files_in_folder:
        file_path = os.path.join(folder_path, file_name)
        try:
            os.remove(file_path)
        except Exception as e:
            print(f"Error deleting {file_name}: {e}")
    print ("Deletion complete!")
    

def pricevolume_check(self, batch_nbr, very_first_day): 
    try: 
        data_open = pd.read_csv(f"../DATABASE/pricevolume/{batch_nbr}_openprice_larger1Bcap_{very_first_day}.csv")
        return True, data_open.iloc[-1, 0]
    except:
        return False, very_first_day