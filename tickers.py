import pandas as pd

def import_tickers(self,desired_market_cap): 

        #update the list by downloading a new one from Nasdaq
        df_t = pd.read_csv('/Users/yb97/Desktop/ProgrammingStuff/Github/DATABASE/nasdaq_screener.csv')
        # Filter tickers based on market cap
        tickers = df_t.loc[df_t['Market Cap'] >= desired_market_cap, 'Symbol'].tolist()
        
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