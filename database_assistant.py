from tqdm import tqdm
from datetime import datetime

class Database_Assistant: 
    '''
    Imagine this as guy reponsible of updating, maintaining and performing 
    various operations on the database when instructed to through method calling.
    '''
    
    #constructor
    def __init__(self):
         # constant attributes (changing requires restructuring as of now)
         self.BATCH_SIZE = 500
         self.pricevolume_veryfirstday = '2000-01-01'
         self.desired_market_cap = 1000000000

         # database state attributes
         self.update_DB_state()
    
    def update_DB_state(self): 
         self.pricevolume_nonemptyDB, self.pricevolume_lastDateinDB  = self.pricevolume_check(1,self.pricevolume_veryfirstday)
         self.pricevolume_lastDateinAPI = self.last_business_day()
         print("last day in API:", self.last_business_day())
         self.DB_in_sync = True if self.pricevolume_lastDateinAPI == self.pricevolume_lastDateinDB else False

         
    # utility methods
    from tickers import import_tickers, batchify_tickers
    from data_update_store import update_store_data, run_delete_process
    from data_update_store import pricevolume_check
    from timing import last_business_day


    #customized methods
    def get_tickers(self):
        tickers = self.import_tickers(self.desired_market_cap)
        return self.batchify_tickers(tickers,self.BATCH_SIZE)

    def wipe_out(self):
        user_input = input("=Database-assistant=: Do you really want to delete the ALL the price volume data? (yes/no): ").lower()
        if not ( user_input == "yes") :
                return
        self.run_delete_process()
        


    def update_pricevolume_data(self, end_day= "LATEST"): 
        '''
        date format is: yyyy-mm-dd
        '''
        if end_day == "LATEST": 
            end_day = self.pricevolume_lastDateinAPI

        #check if already up to date
        if self.DB_in_sync: 
             print("=Database-assistant=: Database already up-to-date.")
             return
        #check if date requested is older than last date stored
        if datetime.strptime(end_day,"%Y-%m-%d") < datetime.strptime(self.pricevolume_lastDateinDB,"%Y-%m-%d"): 
             print("=Database-assistant=: Database already updated beyond requested date.")
             return
        
        #check if database is empty
        if not self.pricevolume_nonemptyDB:
            user_input = input(f"=Database-assistant=: Database is empty. You want to download and store data starting {self.pricevolume_veryfirstday}? (yes/no): ").lower()
            if not ( user_input == "yes") :
                return
            
        batch_nbr =1
        batch_sizes = []
        tickers_batches = self.get_tickers()
        progress_bar = tqdm(total= len(tickers_batches), desc="overall")
        for batch in tickers_batches: 
            batch_sizes.append(len(batch))
            self.update_store_data(batch, batch_nbr, self.pricevolume_lastDateinDB, end_day, self.pricevolume_nonemptyDB, self.pricevolume_veryfirstday)
            batch_nbr +=1
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
            print("Empty: ", not(self.pricevolume_nonemptyDB))
            print("In Sync: ", self.DB_in_sync)
            print("Last date stored in DB: ", self.pricevolume_lastDateinDB if self.pricevolume_nonemptyDB ==True else None)
            print("Last date available through API: ", self.pricevolume_lastDateinAPI)

        return state
         

    