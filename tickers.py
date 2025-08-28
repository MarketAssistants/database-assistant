import pandas as pd
from config import DATABASE_DIR

def import_tickers(self,desired_market_cap): 

        #update the list by downloading a new one from Nasdaq
        df_t = pd.read_csv(f"{DATABASE_DIR}/nasdaq_screener.csv")
        # Filter tickers based on market cap
        tickers = df_t.loc[df_t['Market Cap'] >= desired_market_cap, 'Symbol'].tolist()
        ticker_names = df_t.loc[df_t['Market Cap'] >= desired_market_cap, 'Name'].tolist()
        ticker_marketcap = df_t.loc[df_t['Market Cap'] >= desired_market_cap, 'Market Cap'].tolist()

        #fixes some tickers like BRK
        tickers = [t.replace("/", "-") for t in tickers]
        
        ticker_symbol_name = {}
        for tick,name in zip(tickers,ticker_names):
            ticker_symbol_name[tick] = name

        ticker_marketcap_dict = {}
        for tick,marketcap in zip(tickers,ticker_marketcap):
            ticker_marketcap_dict[tick] = marketcap

        
        self.ticker_symbol_name_map = ticker_symbol_name
        self.ticker_marketcap_dict = ticker_marketcap_dict
        self.ticker_marketcap_list = ticker_marketcap
        return tickers

def batchify_tickers (self,tickers, batch_size): 
    tickers_batches = []
    tickers_len = len(tickers)

    cntr = 0
    while (batch_size*(cntr+1) <= tickers_len): 
        tickers_batches.append(tickers[cntr*batch_size : (cntr+1)*batch_size])
        cntr +=1

    # append the leftover
    if not (batch_size*(cntr) == tickers_len):    
        tickers_batches.append(tickers[(cntr)*batch_size : tickers_len])

    return tickers_batches