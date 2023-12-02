from tqdm import tqdm
from datetime import datetime
from date import Date

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
         self.len_tickers_batches = len(self.tickers_batches)
         self.update_DB_state()
    
    def update_DB_state(self): 
         self.pricevolume_nonemptyDB, self.pricevolume_lastDateinDB  = self.pricevolume_check(self.pricevolume_veryfirstday)
         self.pricevolume_lastDateinAPI = self.last_business_day()
         print("last day in API:", self.last_business_day())
         self.DB_in_sync = [True if   self.pricevolume_lastDateinAPI == lastDate else False for lastDate in self.pricevolume_lastDateinDB]

         
    # utility methods
    from tickers import import_tickers, batchify_tickers
    from data_update_store_read import update_store_data, run_delete_process
    from data_update_store_read import pricevolume_check, read_data
    from timing import last_business_day,tomorrow_date


    #customized methods
    def get_tickers_z(self):
        self.tickers = self.import_tickers(self.desired_market_cap)
        self.len_tickers = len(self.tickers)
        return self.batchify_tickers(self.tickers,self.BATCH_SIZE)

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
            end_day = self.tomorrow_date()

        progress_bar = tqdm(total= len(batches_towork), desc="overall")
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
            print("Last date stored in DB: ", [self.pricevolume_lastDateinDB[idx] if pv_nempty==True else None for idx,pv_nempty in enumerate(self.pricevolume_nonemptyDB )])
            print("Last date available through API in Batches: ", self.pricevolume_lastDateinAPI)
        return state
    

    def get_tickers(self): 
         return self.tickers,self.len_tickers  
    def get_ticker_batches_n_length(self): 
         return self.tickers_batches, self.len_tickers_batches
    

    def get_batches_len(self): 
        batch_length = [] 
        for batch in self.tickers_batches: 
            batch_length.append(len(batch))
        return batch_length

         

    