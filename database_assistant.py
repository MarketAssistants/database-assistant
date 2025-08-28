from tqdm import tqdm
from datetime import datetime
from date import Date
import time
import psycopg2
import numpy as np
import json
import re
from datetime import datetime, timezone

# Timing utilities imported at module level to avoid binding as methods
try:
    from .timing import last_business_day, tomorrow_date
except ImportError:
    from timing import last_business_day, tomorrow_date

class Database_Assistant: 
    '''
    Imagine this as guy responsible of updating, maintaining and performing 
    various operations on the database when instructed to through method calling.
    '''
    #constructor
    def __init__(self):
         # constant attributes (changing requires restructuring as of now)
         self.BATCH_SIZE = 500
         self.pricevolume_veryfirstday = '2000-01-01'
         self.desired_market_cap = 1000000000
         self.enonciation = "=Database-assistant=: "
         # database state attributes

         self.tickers_batches = self.get_tickers_z()
         self.tickers_indices = ['^GSPC','GC=F', '^VIX', 'DOW', 'CL=F']
         self.len_tickers_batches = len(self.tickers_batches)
         self.update_DB_state()
    
    def update_DB_state(self): 
         self.pricevolume_nonemptyDB, self.pricevolume_lastDateinDB  = self.pricevolume_check(self.pricevolume_veryfirstday)
         self.pricevolume_nonemptyIndicesDB, self.pricevolume_lastDateinIndicesDB  = self.pricevolume_check_indices(self.pricevolume_veryfirstday)
         self.pricevolume_lastDateinAPI = last_business_day()
         print("last day in API:", last_business_day())
         self.DB_in_sync = [True if   self.pricevolume_lastDateinAPI == lastDate else False for lastDate in self.pricevolume_lastDateinDB]
         self.IndicesDB_in_sync = self.pricevolume_lastDateinAPI == self.pricevolume_lastDateinIndicesDB
         
    # utility methods
    # Replace ambiguous import with explicit module load of local tickers.py
    import importlib.util as _ilu, pathlib as _pl
    _tickers_spec = _ilu.spec_from_file_location("database_assistant._tickers_mod", _pl.Path(__file__).with_name("tickers.py"))
    _tickers_mod = _ilu.module_from_spec(_tickers_spec)
    _tickers_spec.loader.exec_module(_tickers_mod)
    from data_update_store_read import update_store_data, run_delete_process
    from data_update_store_read import pricevolume_check, pricevolume_check_indices
    from data_update_store_read import read_data, read_indices_data
    from info import retrieve_ticker_info

    #customized methods
    def get_tickers_z(self):
        # Use local tickers module; its functions expect self as first arg
        self.tickers = self._tickers_mod.import_tickers(self, self.desired_market_cap)
        self.len_tickers = len(self.tickers)
        return self._tickers_mod.batchify_tickers(self, self.tickers, self.BATCH_SIZE)

    def wipe_out(self):
        user_input = input("=Database-assistant=: Do you really want to delete the ALL the price volume data? (yes/no): ").lower()
        if not ( user_input == "yes") :
                return
        self.run_delete_process()
        
    def update_pricevolume_data(self, batches = [611],end_day= "LATEST"): 
        '''
        date format is: yyyy-mm-dd
        '''
        if batches == [611]: 
             batches_towork = [i for i in range(0, self.len_tickers_batches)]
        else:
            batches_towork = [x-1 for x in batches]

        if end_day == "LATEST": 
            end_day = tomorrow_date()

        progress_bar = tqdm(total= len(batches_towork)+1, desc="overall")
        for batch_nbr in batches_towork: 
            batch = self.tickers_batches[batch_nbr]
        #check if already up to date
            if self.DB_in_sync[batch_nbr]: 
                print(f"=Database-assistant=: Database (Batch {batch_nbr+1}) already up-to-date.")
                progress_bar.update(1)
                continue
            #check if date requested is older than last date stored
            if datetime.strptime(end_day,"%Y-%m-%d") < datetime.strptime(self.pricevolume_lastDateinDB[batch_nbr],"%Y-%m-%d"): 
                print(f"=Database-assistant=: Database (Batch {batch_nbr+1}) already updated beyond requested date.")
                progress_bar.update(1)
                continue
        
            #check if database is empty
            if not self.pricevolume_nonemptyDB[batch_nbr]:
                user_input = input(f"=Database-assistant=: Database (Batch {batch_nbr+1}) is empty. You want to download and store data starting {self.pricevolume_veryfirstday}? (yes/no): ").lower()
                if not ( user_input == "yes") :
                    progress_bar.update(1)
                    continue
            
            self.update_store_data(batch, batch_nbr+1, self.pricevolume_lastDateinDB[batch_nbr], end_day, self.pricevolume_nonemptyDB[batch_nbr], self.pricevolume_veryfirstday)
            progress_bar.update(1)
            print("Sleeping for a minute before getting next batch data...")
            time.sleep(60)

        #check indices data
        if self.IndicesDB_in_sync: 
            print(f"=Database-assistant=: Database (Indices) already up-to-date.")
        elif datetime.strptime(end_day,"%Y-%m-%d") < datetime.strptime(self.pricevolume_lastDateinIndicesDB,"%Y-%m-%d"): 
            print(f"=Database-assistant=: Database (Indices) already updated beyond requested date.")
        elif not self.pricevolume_nonemptyDB[batch_nbr]:
            user_input = input(f"=Database-assistant=: Database (Indices) is empty. You want to download and store data starting {self.pricevolume_veryfirstday}? (yes/no): ").lower()
            if user_input == "yes":
                progress_bar.update(1)
        else:
            print("Sleeping a bit before getting indices data...")
            time.sleep(10)
            self.update_store_data(self.tickers_indices, "indices", self.pricevolume_lastDateinIndicesDB, end_day, self.pricevolume_nonemptyIndicesDB, self.pricevolume_veryfirstday)

        progress_bar.update(1)
        progress_bar.close()
        print("=Database-assistant=: Update Complete.")
        self.update_DB_state()

    
    #Next we can implement getters to ask questions to the Database assistant 
    #Let him answer using print statements
    #for ex, question: get_last_day_in_DB()
    #response: print("=Database-assistant=: last day in DB is {self. ...}")
        
    def get_DB_state(self, printing=False): 
        state = (self.pricevolume_nonemptyDB, self.DB_in_sync, self.pricevolume_lastDateinDB, self.pricevolume_lastDateinAPI)
        if printing: 
            print("=Database-assistant=: Here is the state of the DB.")
            print("Empty batches: ", [not(element) for element in self.pricevolume_nonemptyDB])
            print("In Sync batches: ", self.DB_in_sync)
            print("Empty indices: ", not(self.pricevolume_nonemptyIndicesDB))
            print("In Sync indices: ", self.IndicesDB_in_sync)
            print("Last date stored in DB: ", [self.pricevolume_lastDateinDB[idx] if pv_nempty==True else None for idx,pv_nempty in enumerate(self.pricevolume_nonemptyDB )])
            print("Last date available through API in Batches: ", self.pricevolume_lastDateinAPI)
        return state
    

    def get_tickers(self): 
        return self.tickers,self.len_tickers  
    def get_indices(self): 
        return self.tickers_indices
    def get_ticker_batches_n_length(self): 
        return self.tickers_batches, self.len_tickers_batches
    
    def get_tickers_market_cap_dict(self): 
        return self.ticker_marketcap_dict
    

    def get_batches_len(self): 
        batch_length = [] 
        for batch in self.tickers_batches: 
            batch_length.append(len(batch))
        return batch_length


    def convert_numpy_types(self, obj):
        """Convert numpy types to native Python types for database storage"""
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, (np.str_,)):  # Removed np.unicode_ as it's deprecated in NumPy 2.0
            return str(obj)
        elif isinstance(obj, (np.intc, np.intp, np.int8, np.int16, np.int32, np.int64,
                             np.uint8, np.uint16, np.uint32, np.uint64)):
            return int(obj)
        elif isinstance(obj, (np.float16, np.float32, np.float64)):  # Removed np.float_ and np.int_ for NumPy 2.0 compatibility
            return float(obj)
        elif isinstance(obj, (np.bool_)):
            return bool(obj)
        elif isinstance(obj, dict):
            return {key: self.convert_numpy_types(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self.convert_numpy_types(item) for item in obj]
        elif isinstance(obj, tuple):
            return tuple(self.convert_numpy_types(item) for item in obj)
        else:
            return obj

    def clean_numpy_string(self, value):
        """Clean numpy string representations from string values"""
        if isinstance(value, str):
            # Replace np.str_('value') with 'value'
            value = re.sub(r"np\.str_\('([^']+)'\)", r"'\1'", value)
            # Replace np.float64(value) with value
            value = re.sub(r"np\.float64\(([^)]+)\)", r"\1", value)
            # Replace other numpy types
            value = re.sub(r"np\.\w+\(([^)]+)\)", r"\1", value)
        return value

    def insert_technical_analysis_metrics(self, results, analysis_date=None):
        """
        Insert technical analysis metrics into PostgreSQL database
        
        Args:
            results: List of technical analysis results in the format:
                    [{"TICKER": {"get_latest_technical_analysis": {...metrics...}}}]
            analysis_date: Date string (YYYY-MM-DD) or None for current date
        
        Returns:
            int: Number of successfully inserted records
        """
        print(f"📊 Starting database insertion for {len(results)} technical analysis results...")
        
        if analysis_date is None:
            analysis_date = datetime.now(timezone.utc).date()
        elif isinstance(analysis_date, str):
            analysis_date = datetime.strptime(analysis_date, '%Y-%m-%d').date()
        
        # Database connection configuration
        conn_config = {
            "host": "localhost",
            "port": 5433,
            "dbname": "traderdeckmain",
            "user": "traderadmin",
            "password": "rRvjVD70#-v<MK7"
        }
        
        try:
            conn = psycopg2.connect(**conn_config)
            cur = conn.cursor()
            
            # Convert results and clean numpy types
            cleaned_results = self.convert_numpy_types(results)
            
            successful_inserts = 0
            
            for result_item in cleaned_results:
                for ticker, analysis_data in result_item.items():
                    if 'get_latest_technical_analysis' in analysis_data:
                        metrics = analysis_data['get_latest_technical_analysis']
                        
                        # Extract and clean metrics
                        close_price = float(metrics.get('close_price', 0))
                        high_price = float(metrics.get('high_price', 0))
                        low_price = float(metrics.get('low_price', 0))
                        open_price = float(metrics.get('open_price', 0))
                        volume = int(metrics.get('volume', 0))
                        
                        ma_10 = metrics.get('ma_10')
                        ma_10 = float(ma_10) if ma_10 is not None else None
                        
                        ema_21 = metrics.get('ema_21')
                        ema_21 = float(ema_21) if ema_21 is not None else None
                        
                        ma_50 = metrics.get('ma_50')
                        ma_50 = float(ma_50) if ma_50 is not None else None
                        
                        ma_200 = metrics.get('ma_200')
                        ma_200 = float(ma_200) if ma_200 is not None else None
                        
                        rsi_14 = metrics.get('rsi_14')
                        rsi_14 = float(rsi_14) if rsi_14 is not None else None
                        
                        fibonacci_retracement = metrics.get('fibonacci_retracement')
                        fibonacci_retracement = float(fibonacci_retracement) if fibonacci_retracement is not None else None
                        
                        # Clean and prepare string fields
                        support_levels = metrics.get('support_levels', '')
                        if support_levels:
                            support_levels = self.clean_numpy_string(str(support_levels))
                        
                        rsi_support = metrics.get('rsi_support', '')
                        if rsi_support:
                            rsi_support = self.clean_numpy_string(str(rsi_support))
                        
                        print(f"   💾 Inserting {ticker} for {analysis_date}...")
                        
                        try:
                            cur.execute("""
                                INSERT INTO technical_analysis (
                                    ticker_symbol,
                                    analysis_date,
                                    close_price,
                                    high_price,
                                    low_price,
                                    open_price,
                                    volume,
                                    ma_10,
                                    ema_21,
                                    ma_50,
                                    ma_200,
                                    rsi_14,
                                    rsi_support,
                                    support_level,
                                    fibonacci_retracement,
                                    created_at
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (ticker_symbol, analysis_date) DO UPDATE SET
                                close_price = EXCLUDED.close_price,
                                high_price = EXCLUDED.high_price,
                                low_price = EXCLUDED.low_price,
                                open_price = EXCLUDED.open_price,
                                volume = EXCLUDED.volume,
                                ma_10 = EXCLUDED.ma_10,
                                ema_21 = EXCLUDED.ema_21,
                                ma_50 = EXCLUDED.ma_50,
                                ma_200 = EXCLUDED.ma_200,
                                rsi_14 = EXCLUDED.rsi_14,
                                rsi_support = EXCLUDED.rsi_support,
                                support_level = EXCLUDED.support_level,
                                fibonacci_retracement = EXCLUDED.fibonacci_retracement,
                                created_at = EXCLUDED.created_at
                            """, (
                                ticker,
                                analysis_date,
                                close_price,
                                high_price,
                                low_price,
                                open_price,
                                volume,
                                ma_10,
                                ema_21,
                                ma_50,
                                ma_200,
                                rsi_14,
                                rsi_support,
                                support_levels,
                                fibonacci_retracement,
                                datetime.now(timezone.utc)
                            ))
                            
                            successful_inserts += 1
                            print(f"   ✅ Successfully inserted/updated {ticker}")
                            
                        except Exception as e:
                            print(f"   ❌ Error inserting {ticker}: {e}")
                            continue
            
            # Commit all changes
            conn.commit()
            
            # Get total count
            cur.execute("SELECT COUNT(*) FROM technical_analysis;")
            total_count = cur.fetchone()[0]
            
            print(f"\n🎉 Database insertion complete!")
            print(f"   ✅ Successfully inserted/updated: {successful_inserts} records")
            print(f"   📊 Total rows in technical_analysis table: {total_count}")
            
            cur.close()
            conn.close()
            
            return successful_inserts
            
        except psycopg2.Error as e:
            print(f"❌ PostgreSQL Error: {e}")
            return 0
        except Exception as e:
            print(f"❌ Unexpected error during database insertion: {e}")
            return 0

