import yfinance as yf

print("Downloading AAPL...")
df = yf.download("AAPL", start="2024-07-01", end="2024-07-20")
print(df)
