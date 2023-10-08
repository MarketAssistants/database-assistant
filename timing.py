from datetime import datetime, timedelta
import pytz

def today_date(self): 
    today_date = datetime.now()
    formatted_date = today_date.strftime("%Y-%m-%d")
    return formatted_date

def tomorrow_date(self): 
    today = datetime.now().date()
    tomorrow = today + timedelta(days=1)
    return tomorrow.strftime("%Y-%m-%d")

def last_business_day(self):
    eastern = pytz.timezone('US/Eastern')
    current_time = datetime.now(eastern)
    #new data is available shorty after market close
    target_time = current_time.replace(hour=16, minute=30, second=0, microsecond=0)
    today = datetime.now()
    if current_time < target_time: #revert to yesterday if today's date is not available yet
        today= today - timedelta(days=1)

    while today.weekday() >= 5:  # 5 is Saturday, 6 is Sunday
        today -= timedelta(days=1)
    return today.strftime("%Y-%m-%d")
