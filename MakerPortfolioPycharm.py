import yfinance as yf, pandas as pd, shutil, os, time, glob, smtplib, ssl
import datetime
import time
from time import mktime
from datetime import date
from sqlalchemy import create_engine, text
import finnhub as fh
import numpy as np

sqlEngine = create_engine('mysql+pymysql://root:password@localhost/staging', pool_recycle=3600)
dbConnection = sqlEngine.connect()

dbConnection.execute("SET GLOBAL local_infile=1;")
dbConnection.execute("DROP TABLE staging.pycharm_output")
dbConnection.execute("drop table staging.fearandgreedindexv1")


histdatadf = []
finnhub_client = fh.Client(api_key="")
#email me if you want to use my api key


Hist_data = []
Hist_data = pd.DataFrame.from_dict(Hist_data)

# date_time = datetime.datetime(2021, 8, 27, 21, 20)
date_time = datetime.datetime.now()
#date_time = datetime.datetime(2024, 2, 7, 00, 00)

to_date = round(time.mktime(date_time.timetuple()))

date_time = datetime.datetime(2018, 1, 1, 00, 00)

from_date = round(time.mktime(date_time.timetuple()))

# date_time = datetime.datetime(2000, 1, 1, 00, 00)

# to_date = round(time.mktime(date_time.timetuple()))

# date_time = datetime.datetime(2008, 1, 1, 00, 00)

# from_date = round(time.mktime(date_time.timetuple()))

sqlEngine = create_engine()
dbConnection = sqlEngine.connect()
#stocks.tickerlist2025
#staging.tickerpull
TICKERS_df = pd.read_sql(f"select ticker from stocks.tickerlist2025v2",
                         dbConnection)
tickers = TICKERS_df['ticker'].values.tolist()

# tickers = gt.get_tickers_filtered(mktcap_min=200)


TableStatus = 'append'
# TableStatus = 'replace'



histdatadf = []
TickersLeft = len(tickers)
windowsize = 1
i = 0

Hist_data = []
Hist_data = pd.DataFrame.from_dict(Hist_data)

# initialize the dataframe
timedata = [pd.Timestamp("now")]
timetrackdf = pd.DataFrame(timedata, columns=['timetrack'])

max_apis = 149
sleeptime = 0.5

#some tickers don't have data from the whole timeframe
#so SPY is used to keep every tickers dataframe the same length

mfi13day =  finnhub_client.stock_candles('SPY', 'D', from_date, to_date)
mfi13daydf = pd.DataFrame.from_dict(mfi13day)
SPY = mfi13daydf;

currentticker = tickers[i]


while i < len(tickers):
    ct = datetime.datetime.now()
    print(f'Starting.....', ct)
    currentticker = tickers[i]  # Gets the current stock ticker
    Hist_data = []
    # df2['MyDate'] = pd.to_datetime(df2['t'], unit='s');

    #this keeps the apis per minute below 150
    try:
        mfi13daydf = []
        # before calling API
        api_count = len(timetrackdf[(timetrackdf['timetrack'] >= pd.Timestamp("now") - datetime.timedelta(seconds=60))])
        x = 0
        while x < 1:
            api_count = len(
                timetrackdf[(timetrackdf['timetrack'] >= pd.Timestamp("now") - datetime.timedelta(seconds=60))])
            if api_count >= max_apis:
                ct = datetime.datetime.now()
                print(f'sleeping for APIs.....', ct, 'API Count:', api_count)
                time.sleep(sleeptime)
            else:
                ct = datetime.datetime.now()
                print(f'continue.....', ct, 'API Count:', api_count)
                # Add row to DF for tracking time
                new_row = [{'timetrack': pd.Timestamp("now")}]
                timetrackdf = timetrackdf.append(new_row)
                # done with tracking
                # Now call the API
                mfi13day = finnhub_client.stock_candles(currentticker, 'D', from_date, to_date)

                x += 1
            mfi13daydf = pd.DataFrame.from_dict(mfi13day)
            Hist_data = mfi13daydf;
    except ValueError as vx:
        print(vx)
    except Exception as ex:
        print(ex)
    else:
        print('1');


    try:

        #technical indicator calculations removed for intellectual property concerns
        #NATR used as example

        histdatadf = []
        histdatadf = pd.DataFrame.from_dict(Hist_data)
        close = histdatadf['c'] / histdatadf['c'][0:1].squeeze()
        sma = []
        smalist = []

        close = Hist_data['c']

        truerange = []
        truerangelist = []
        truerangeyesterdayclose = histdatadf['c'].shift(+1)
        truerangehigh = histdatadf['h']
        truerangelow = histdatadf['l']
        truerangepart1 = (truerangeyesterdayclose - truerangelow).abs()
        truerangepart2 = truerangehigh - truerangelow
        truerangepart3 = (truerangeyesterdayclose - truerangehigh).abs()
        truerangedf = []
        truerangedf = pd.DataFrame.from_dict(truerangedf)
        truerangedf['highlow'] = truerangepart1
        truerangedf['yestlow'] = truerangepart2
        truerangedf['yesthigh'] = truerangepart3
        truerange = truerangedf[['yesthigh', 'yestlow', 'highlow']].max(axis=1)
        truerange = truerange.fillna(0)
        histdatadf['smootheddatatruerangev2'] = truerange

        x = 5

        atrsigma = (truerange[0: x]).sum()
        atrsigma = atrsigma.squeeze()
        firstatr = atrsigma * (1 / x)
        y = 1
        atrlist = []
        atr = 0


        atrstart = []
        atrstart = truerange.copy()
        atrstart[x - 1:x] = firstatr
        atrstart[0:x - 1] = 0


        def wwma(values, n):
            """
             J. Welles Wilder's EMA
            """
            return values.ewm(alpha=1 / n, adjust=False).mean()


        atr_values = []

        atr_values = wwma(atrstart, 5)

        natr = atr_values.squeeze() / histdatadf['c'] * 100

        y = 1
        natr = natr.fillna(0)
        myStr = "smoothv2atr"
        myInt = x
        newStr = str(myInt) + myStr
        natr = pd.DataFrame.from_dict(natr)
        histdatadf[newStr] = atr_values
        myStr = "smoothv2natr"
        myInt = x
        newStr = str(myInt) + myStr
        histdatadf[newStr] = natr



        histdatadf['MyDate'] = pd.to_datetime(histdatadf['t'], unit='s')
        histdatadf.insert(0, 'ticker', currentticker)

        #fear and greed index calculations would be here
        #I know mentioning it reveals part of the strategy
        #but it is unique from the real one, so it shouldn't matter
        #actual code is removed for intellectual property concerns



        # these are the columns used for calculating percent changes
        histdatadf['TomorrowsClose'] = 0.
        histdatadf['TomorrowsClose'] = Hist_data['c'].shift(-1)

        histdatadf['ThreeDayClose'] = 0.
        histdatadf['ThreeDayClose'] = Hist_data['c'].shift(-3)

        histdatadf['FiveDayClose'] = 0.
        histdatadf['FiveDayClose'] = Hist_data['c'].shift(-5)

        histdatadf['TenDayClose'] = 0.
        histdatadf['TenDayClose'] = Hist_data['c'].shift(-10)

        histdatadf['TwentyDayClose'] = 0.
        histdatadf['TwentyDayClose'] = Hist_data['c'].shift(-20)

        histdatadf['fiftyDayClose'] = 0.
        histdatadf['fiftyDayClose'] = Hist_data['c'].shift(-50)

        histdatadf['TwoDayClose'] = 0.
        histdatadf['TwoDayClose'] = Hist_data['c'].shift(-2)

        histdatadf['YesterdayClose'] = 0.
        histdatadf['YesterdayClose'] = Hist_data['c'].shift(+1)

        histdatadf['Close5DayLag'] = Hist_data['c'].shift(+5)
        histdatadf['Close10DayLag'] = Hist_data['c'].shift(+10)
        histdatadf['Close100DayLag'] = Hist_data['c'].shift(+100)
        histdatadf['Close200DayLag'] = Hist_data['c'].shift(+200)
        histdatadf['Close500DayLag'] = Hist_data['c'].shift(+500)

        histdatadf = histdatadf.fillna(0)
        #this sends the code to MySQL
        print(f'Loaded initial DF with ticker {currentticker}. Tickers Left to Process {TickersLeft}')
        histdatadf = histdatadf.replace([np.inf, -np.inf], np.nan)
        frame = histdatadf.to_sql('pycharm_output', dbConnection, if_exists=TableStatus, index=False);
    except ValueError as vx:
        print(vx)
    except Exception as ex:
        print(ex)
    else:
        print('49');
    i += 1
    print(
        f'Table stockdailyvalues appended successfully with ticker {currentticker}. Tickers Left to Process {TickersLeft}')
    ct = datetime.datetime.now()
    print(f'sleeping.....', ct)
    TickersLeft -= 1
print(len(tickers))

#more fear and greed index code removed for intellectual property concerns

frame = fearandgreedindex.to_sql('fearandgreedindexv1', dbConnection, if_exists=TableStatus, index=False);

#this starts the Rscript
import subprocess
res = subprocess.call("Rscript C:/Users/wend0/OneDrive/Documents/Stocks/TrainingScripts/Predictfile.R", shell=True)

