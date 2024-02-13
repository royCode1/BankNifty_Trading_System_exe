import datetime
import time
import pandas as pd
import pytz
from datetime import datetime,timedelta
from fyers_apiv3 import fyersModel
from Connect import XTSConnect
import math
import re

#Fyers Login
app_id = open("fyers_client_id.txt",'r').read()
access_token = open("fyers_access_token.txt",'r').read()
fyers = fyersModel.FyersModel(token=access_token,is_async=False,client_id=app_id)

#XTS Interactive Login
API_KEY = "b8d46c4ff332a0defa0720"
API_SECRET = "Vrqm404#E6"
source = "WEBAPI"
clientID="AR17"

xt = XTSConnect(API_KEY, API_SECRET, source)

response = xt.interactive_login()
print("Login: ", response)

#XTS Marketdata Login
API_KEY_MD = "fe5bfd5b9a44d9dc446725"
API_SECRET_MD = "Kubt712@A$"

xtm = XTSConnect(API_KEY_MD, API_SECRET_MD, source)
response=xtm.marketdata_login()
print("Login: ", response)

#Symbol For Fetching Market Data Through Fyers API
d={
    "symbols":"NSE:NIFTYBANK-INDEX"
}

instrument=d['symbols']

def getHistorical(ticker,duration):
    global sdata
    start=datetime.today()-timedelta(duration)
    end=datetime.today()
    while start <= end:
        end2=start+timedelta(60)
        data = {
            "symbol":ticker,
            "resolution":"D",
            "date_format":"1",
            "range_from":start.strftime("%Y-%m-%d"),
            "range_to":end2.strftime("%Y-%m-%d"),
            "cont_flag":"1"
        }
        s=fyers.history(data=data)
        s=pd.DataFrame(s['candles'])
        sdata=s
        sdata=pd.concat([sdata],ignore_index=True)
        start=end2+timedelta(1)
    sdata.columns=['date','open','high','low','close','volume']
    sdata['date']=pd.to_datetime(sdata['date'],unit='s').dt.tz_localize(pytz.utc)
    ist = pytz.timezone('Asia/Kolkata')
    sdata['date']=sdata['date'].dt.tz_convert(ist)
    sdata['date']=sdata['date'].dt.tz_localize(None)
    sdata=sdata.set_index('date')
    return sdata

def getOHLC(ticker,interval,duration):
    range_from = datetime.today()-timedelta(minutes=interval)
    range_to = datetime.today()-timedelta(minutes=interval)

    from_date_string = range_from.strftime("%Y-%m-%d")
    to_date_string = range_to.strftime("%Y-%m-%d")
    data = {
        "symbol":ticker,
        "resolution":interval,
        "date_format":"1",
        "range_from":from_date_string,
        "range_to":to_date_string,
        "cont_flag":"1"
    }

    response = fyers.history(data=data)['candles']

    # Create a DataFrame
    columns = ['Timestamp','open','high','low','close','volume']
    df = pd.DataFrame(response, columns=columns)

    # Convert Timestamp to datetime in UTC
    df['Timestamp2'] = pd.to_datetime(df['Timestamp'],unit='s').dt.tz_localize(pytz.utc)

    # Convert Timestamp to IST
    ist = pytz.timezone('Asia/Kolkata')
    df['Timestamp2'] = df['Timestamp2'].dt.tz_convert(ist)
    # Filter rows where 'Timestamp2' is less than 15:30
    filtered_df = df[df['Timestamp2'].dt.time < pd.to_datetime('23:30').time()]

    return (filtered_df)

def get_ltp():
    try:
        time.sleep(1)
        ltp = fyers.quotes(d)
        a = ltp['d'][0]['v']['lp']
        return float(a)
    except Exception as e:
        print('Role', "Failed : {} ".format(e))


def fut_lTP():
    try:
        time.sleep(1)
        temp =  xtm.get_quote(Instruments=[{'exchangeSegment': 2, 'exchangeInstrumentID': getBankNiftyFutID()}], xtsMessageCode=1501,publishFormat='JSON')['result']['listQuotes'][0]
        #print(temp)
        startnum = temp.find('LastTradedPrice')
        #print(startnum)
        val1 = temp[startnum + 16:startnum+16+10]
        # xt.search_by_instrumentid(Instruments=[{'exchangeSegment': 2, 'exchangeInstrumentID': id}])['result'][0]['Bhavcopy']['Close']
        #print("####")
        #print(val1)
        val2 =  re.sub('[^\d\.]', '',  val1)
        print(val2)
        return float(val2)
    except Exception as e:
        print('Role', "Failed : {} ".format(e))
def long_stop(long_price,system_low,per):
    system_points=long_price - system_low

    if  long_price*per< system_points:
        stop_price=round(long_price*(1-per),2)
    else:
        stop_price=system_low

    return stop_price

def short_stop(short_price,system_high,per):
    system_points=system_high - short_price

    if short_price*per < system_points:
        stop_price=round(short_price*(1+per),2)
    else:
        stop_price=system_high

    return stop_price

def getBankNiftyExpiryDate():
    banknifty_expiry = {

        datetime(2024, 1, 24).date(): "25Jan2024",
        datetime(2024, 2, 28).date(): "29Feb2024",
        datetime(2024, 3, 26).date(): "27Mar2024",
        datetime(2024, 4, 23).date(): "24Apr2024",
        datetime(2024, 5, 28).date(): "29May2024",
        datetime(2024, 6, 25).date(): "26Jun2024",
    }

    today = datetime.now().date()

    for date_key, value in banknifty_expiry.items():
        if today <= date_key:
            return value

def getBankNiftyFutID():

    date_str=getBankNiftyExpiryDate()

    response = xtm.get_future_symbol(
        exchangeSegment=2,
        series='FUTIDX',
        symbol='BANKNIFTY',
        expiryDate=date_str)

    id=response['result'][0]['ExchangeInstrumentID']

    return id

def getBankNiftyFutSymb():

    date_str=getBankNiftyExpiryDate()

    response = xtm.get_future_symbol(
        exchangeSegment=2,
        series='FUTIDX',
        symbol='BANKNIFTY',
        expiryDate=date_str)
    #print(response)
    symb=response['result'][0]['Description']
    return symb
    #print('Future Symbol:', symb)

def placeOrderXts(t_type,qty,order_type):
    symb=getBankNiftyFutSymb()
    papertrading = 0 #if this is 1, then real trades will be placed
    dt = datetime.now()
    print(dt.hour,":",dt.minute,":",dt.second ," => ",t_type," ",symb," ",qty," ",order_type)
    try:
        if (papertrading == 1):
            order = xt.place_order(
                exchangeSegment=xt.EXCHANGE_NSEFO,
                exchangeInstrumentID=getBankNiftyFutID(),
                productType=xt.PRODUCT_NRML,
                orderType=xt.ORDER_TYPE_LIMIT if order_type == 'LIMIT' else xt.ORDER_TYPE_MARKET,
                orderSide=xt.TRANSACTION_TYPE_BUY if t_type == 'BUY' else xt.TRANSACTION_TYPE_SELL,
                timeInForce=xt.VALIDITY_DAY,
                disclosedQuantity=0,
                orderQuantity=qty,
                limitPrice=0,
                orderUniqueIdentifier="454845",
                stopPrice=0,
                clientID=clientID)
            return order
        else:
            return 0

    except Exception as e:
        print(dt.hour,":",dt.minute,":",dt.second ," => ", symb," Order Failed : {} ".format(e))


#print(getHistoical(instrument,365))
df=getHistorical(instrument,365)
prev_close=df['close'].iloc[-1]                        #Previous Day Close
#print(close)

#Pivot High Low Calculation
df['swing_high'] = df['close'][
    (df['close'].shift(1) < df['close']) &
    (df['close'].shift(-1) < df['close'])]

df['swing_low'] = df['close'][
    (df['close'].shift(1) > df['close']) &
    (df['close'].shift(-1) > df['close'])]

df['swing'] = df['swing_high'].apply(lambda x: x if math.isnan(x) else 'SH')
df['dummy']=df['swing_low'].apply(lambda x: 'SH' if math.isnan(x) else 'SL')
df['swing']=df['swing'].where(df['dummy']!='SL',df['dummy'])
df=df.drop(['dummy','swing_high','swing_low'],axis=1)

low_date=df['swing'].where(df['swing']=='SL').last_valid_index()
low_index=df.index.get_loc(low_date)
swing_low=df.close.iloc[low_index]             #Pivot Low
print(swing_low)
#print(low_date)

high_date=df['swing'].where(df['swing']=='SH').last_valid_index()
high_index=df.index.get_loc(high_date)
swing_high=df.close.iloc[high_index]          #Pivot High
print(swing_high)
#print(type(swing_high))
#quit()
#print(high_date)

################################INPUTS##################################

#Entry Time
entryHour=9
entryMinute=15
entrySecond=0

startTime = datetime(datetime.now().year, datetime.now().month, datetime.now().day, entryHour, entryMinute, entrySecond)
startTime = startTime.time()
print(startTime)

#quit()
lot_size=1
qty=15*lot_size
x=1
timeFrame=1
max_trade=2               #Max Trade Allowed Per Day
number_of_trade=0         #Count of Number of Trades Per Day
intra_per=0.01            #Intraday SL Percentage
pos_per=0.02              #Positional Sl Percentage
intra_sl=0                #Intraday SL
gap_exit=0                #Control Variable For Gaps
df = pd.DataFrame(columns=['Date','Trade Direction','Entry_Price','Exit_Price','Exit_Date','PnL'])
#print(type)
#Lists For Storing OHLC Data
close=[]
opens=[]
high=[]
low=[]
volume = []
#gap_down_exit=0
test_swing_high=45000

#Getting Position Status
emptyList=[]
with open("position_status.txt") as file:
    for item in file:
        emptyList.append(item)

#st
#0 means no trde, but want to enter.
#1 means positional buy trade.
#2 means positional sell trade.
temp1 = emptyList[0].find('=')
temp2 = emptyList[0].find('\n')
st = float(emptyList[0][temp1+2:temp2])
print("St: ",st)

temp1 = emptyList[1].find('=')
temp2 = emptyList[1].find('\n')
pos_sl = float(emptyList[1][temp1+2:temp2])               #Positional SL
print("Positional Sl: ",pos_sl)

temp1 = emptyList[2].find('=')
temp2 = emptyList[2].find('\n')
long_price = float(emptyList[2][temp1+2:temp2])
print("Long Price: ",long_price)

temp1 = emptyList[3].find('=')
temp2 = emptyList[3].find('\n')
short_price = float(emptyList[3][temp1+2:temp2])
print("Short Price: ",short_price)

while x == 1:
    dt1 = datetime.now()
    #keep checking if entry time is reached
    if (dt1.time() >= startTime):
        print("Time Reached!!!")
        x = 2
    else:
        time.sleep(1)
        print(dt1 , "Waiting for Start Time!!!")

while x == 2:
    dt1 = datetime.now()
    #Find OHLC at the end of the timeframe
    if dt1.second <= 1 and dt1.minute % timeFrame == 0:
        data=getOHLC(instrument,timeFrame,5)

        #print(dt1)
        #print(data)

        opens = data['open'].to_numpy()
        high = data['high'].to_numpy()
        low = data['low'].to_numpy()
        close = data['close'].to_numpy()
        volume = data['volume'].to_numpy()

        ltp=get_ltp()
        print("LTP: ",ltp)
        #quit()

        #For Short Trade
        if st==2:
            if dt1.hour == 9 and dt1.minute == 30:
                if opens[-1] > swing_high*1.02:
                    gap_exit=1
                elif opens[-1] > prev_close*1.02:
                    print("Exiting Short!!!")
                    exit_order=placeOrderXts('MARKET',qty,'BUY')
                    if swing_high < close[-1]:
                        swing_high=close[-1]
                    st=0
            if ltp > swing_high*1.02 and gap_exit==1:
                print("Exiting Short Position After Gap-Up From System High!!!")
                exit_order=placeOrderXts('MARKET',qty,'BUY')
                st=0
            elif intra_sl !=0 and ltp > intra_sl:
                print("Exiting Short Trade Intraday After 1% Sl Hit!!!")
                exit_order=placeOrderXts('MARKET',qty,'BUY')
                st=0
            elif pos_sl != 0  and ltp > pos_sl:
                print("Exiting Positional Short After 2% Sl Hit!!!")
                exit_order=placeOrderXts('MARKET',qty,'BUY')
                st=0
                pos_sl=0
            if ltp <= short_price*0.98:
                print("Moving StopLoss to Entry Point After Trade In The Money By 2%")
                pos_sl=short_price
            time.sleep(1)


        #For Long Trade
        if st==1:
            if dt1.hour == 9 and dt1.minute == 30:
                if opens[-1] < swing_low*0.98:
                    gap_exit=1
                elif opens[-1] < prev_close*0.98:
                    print("Exiting Long")
                    exit_order=placeOrderXts('MARKET',qty,'SELL')
                    if swing_low > close[-1]:
                        swing_low=close[-1]
                    st=0
            if ltp < swing_low*0.98 and gap_exit==1:
                print("Exiting Long Position After Gap-Down From System Low!!!")
                exit_order=placeOrderXts('MARKET',qty,'SELL')
                st=0
            elif intra_sl !=0 and ltp < intra_sl:
                print("Exiting Long Trade Intraday After 1% Sl Hit or BEP!!!")
                exit_order=placeOrderXts('MARKET',qty,'SELL')
                st=0
            elif pos_sl !=0 and ltp < pos_sl:
                print("Exiting Positional Long After 2% SL Hit or BEP!!!")
                exit_order=placeOrderXts('MARKET',qty,'SELL')
                st=0
                pos_sl=0
            if ltp >= long_price*1.02:
                print("Moving StopLoss to Entry Point After Trade In The Money By 2%")
                pos_sl=long_price
            time.sleep(1)

        #ttime = data['date']

        print(close[-1])
        time.sleep(1)
        print("Swing High: ",swing_high)
        print("Swing Low: ",swing_low)

        if (ltp > swing_high and number_of_trade < max_trade and gap_exit==0) and (st==0 or st==2):
            print("Swing High Broken!!!")
            if st==2:
                print("Exiting Short Trade Before Going Long!!!")
                exit_order=placeOrderXts('MARKET',qty,'BUY')
                time.sleep(1)
                entry_order=placeOrderXts('MARKET',qty,'BUY')
            else:
                print("Going Long!!!")
                entry_order=placeOrderXts('MARKET',qty,'BUY')
            st=1
            long_price=swing_high
            number_of_trade=number_of_trade+1
            if long_stop(long_price,swing_low,intra_per) != swing_low:
                intra_sl=long_stop(long_price,swing_low,intra_per)
                print("Intraday SL: ",intra_sl)
            else:
                intra_sl=0

        elif (ltp < swing_low and number_of_trade < max_trade and gap_exit==0) and (st==0 or st==1):
            print("Swing Low Broken!!!")
            if st==1:
                print("Exiting Long Trade Before Going Short!!!")
                exit_order=placeOrderXts('MARKET',qty,'SELL')
                time.sleep(1)
                entry_order=placeOrderXts('MARKET',qty,'SELL')
            else:
                print("Going Long!!!")
                entry_order=placeOrderXts('MARKET',qty,'SELL')
            st=2
            short_price=swing_low
            number_of_trade=number_of_trade+1
            if short_stop(short_price,swing_high,intra_per) != swing_high:
                intra_sl=short_stop(short_price,swing_high,intra_per)
            else:
                intra_sl=0

        else:
            print("No Entry Yet. Current Minute", dt1.minute)
            time.sleep(1)

        #Exiting the GapUp/GapDown Trade at Day End if it is trading below the pivot(long) or above the pivot(Short)
        if (dt1.hour == 15 and dt1.minute == 15):
            if gap_exit==1 and st!=0:
                if st==1 and ltp < swing_low:
                    print("Exiting Long Position After Gap-Down from System Low!!!")
                    exit_order=placeOrderXts('MARKET',qty,'SELL')
                elif st==2 and ltp > swing_high:
                    print("Exiting Short Position After Gap-UP from System High!!!")
                    exit_order=placeOrderXts('MARKET',qty,'BUY')
                st=0
            elif number_of_trade == 0 and st == 1 and datetime.now().strftime("%b")!=getBankNiftyExpiryDate()[2:5]:
                print("Roll Over Long")
                entry_order=placeOrderXts('MARKET',qty,'BUY')
            elif number_of_trade == 0 and st == 2 and datetime.now().strftime("%b")!=getBankNiftyExpiryDate()[2:5]:
                print("Roll Over Short")
                entry_order=placeOrderXts('MARKET',qty,'SELL')



    if (dt1.hour >= 15 and dt1.minute >= 30):
        if st==1 :
            print("Carrying Long Position!!!")
            if long_stop(long_price,swing_low,pos_per) != swing_low:
                pos_sl=long_stop(long_price,swing_low,pos_per)
            else:
                pos_sl=0
        elif st==2 :
            print("Carrying Short Position!!!")
            if short_stop(short_price,swing_high,pos_per) != swing_high:
                pos_sl=short_stop(short_price,swing_high,pos_per)
            else:
                pos_sl=0
        else:
            print("Carrying No Position For The Day!!!")
        print('End Of the Day')
        x = 3
        break

file = open("position_status.txt","w")
file.write("st = "+str(st)+"\n"+"Position StopLoss = "+str(pos_sl)+"\n"+"Long Price = "+str(long_price)+"\n"+"Short Price = "+str(short_price)+"\n")
file.close()
df.to_csv('tradelog.csv',mode='a',index=True,header=True)
