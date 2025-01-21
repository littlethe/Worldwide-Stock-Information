from datetime import datetime,timedelta
import pandas as pd
import threading
import time
import TickerLibrary as TL

UpdateInterval  = 60*60*24*2 #間隔要多久才能更新(秒)
SaveInterval    = 35000 #多少筆存一次
GettingStatus = {-2,-1,0,1,2,3}
#GettingStatus = {-1,0,1,2,3}
#GettingStatus = {-1}
#GettingStatus = {-2,-1}
#GettingStatus = {-2,-1,0,1,2,3} #-2:查不到 -1:連線出錯 0:沒有查過 1:查得到但沒有數據 2:只有價錢數據 3:完整數據 
   
#ExchangeWorlds   = {1,2,3}#0:全部 1:east 2:middle 3:west -1:test
ExchangeWorlds   = {3}

TimeWords               = {'Date','RecentQuarter','YearEnd'}

#欄位attributes為回傳得到的屬性數，1是讀不到，6是只有公司名，以下為attributes的篩選門檻
AttributesAvailable          = 2     #大於等於2表示yahoo finance有查到
#AttributesBasis             = 7     #7到24為屬性不完整
AttributesPrice             = 25    #完整價錢資訊至少要25
AttributesFinance           = 50    #完整財務資訊至少要50

PrintInterval           = 1000 #多少筆顯示一次

#200約1分鐘1800筆，10萬筆56分(最快的，線程超過200速度也一樣)
#100約1分鐘1300筆，10萬筆77分
#50約1分鐘900筆，10萬筆111分(2小時)
#20約1分鐘630筆，10萬筆159分(2.5小時)
#10約1分鐘630筆，10萬筆159分(2.5小時)，無error
#5約1分鐘370筆，10萬筆270分(4.5小時)，無error
#1約1分鐘99筆，10萬筆1010分(17小時)，無error
ThreadsNumber           = 1

SleepTime               = 0.2

MaxUpdatingCount        = 10

Num                     = 0 #計數用，不用改


def processValue(attribute,value,symbol):
    if pd.isna(value):
        return value

    if attribute in TL.UTFAttributes:
        value = TL.replaceUTF(value)
    
    if any(word in attribute for word in TimeWords):
        value = 0 if value < 0 else value
        return datetime(1970, 1, 1) + timedelta(seconds=value)
    return value

def renameSymbol(symbol,suffix):
    
    symbol  = symbol.replace('.PR.', '-P')    
    symbol  = symbol.replace('.', '-')
    symbol  = symbol.replace(' ', '-')
    
    if suffix == '-':
        return symbol
    return symbol+'.'+suffix

def recordStatus(index,attributes):
    now = TL.getNowStr()
    
    status = -1
    status = -2 if attributes == 1 else status
    status = 1 if attributes >= AttributesAvailable else status
    status = 2 if attributes >= AttributesPrice else status
    status = 3 if attributes >= AttributesFinance else status

    if status >= 2:
        TL.DFInfo.at[index,'succeeded'] = now
        addCount(index,'successes')
    elif status <= 1:
        TL.DFInfo.at[index,'failed'] = now
        addCount(index,'failures')
        
    TL.DFInfo.loc[index,['attributes','status','time']] = [attributes,status,now]
    
    return status

def addCount(index,field):
    text = TL.DFInfo.at[index,field]
    count = 1 if pd.isna(text) else text+1
    TL.DFInfo.at[index,field] = count

def fillInfo(index,symbol):
    #print(symbol)
    try:
        
        dictInfo = TL.getInfo(symbol)
        validAttributes = [x for x in dictInfo.keys() if x not in TL.AvoidedAttributes]
        #TL.DFInfo.loc[index,validColumns] = [None] * len(validColumns)
        for column in validColumns:
            TL.DFInfo.at[index,column] = None

        for attribute in validAttributes:
            if attribute not in validColumns:
                print(symbol+' missed: '+attribute)
                TL.DFInfo[attribute] = None
                validColumns.append(attribute)
            TL.DFInfo.at[index,attribute] = processValue(attribute,dictInfo[attribute],symbol)

        attributes = len(dictInfo)
        if recordStatus(index,attributes) <= 1:
            #print(symbol+' is incomplete. Attributes:'+str(attributes))
            return False
                
        return True
        
    except Exception as error:
        print(symbol+' error:',error)
        recordStatus(index,0)
        return False

def loadExchange(row):
    rowExchange = TL.DFExchanges[TL.DFExchanges.code == row.mic_code]
    lSuffix = str(rowExchange.suffix.values[0]).split('|')
    index = row.Index
    for i in range(len(lSuffix)):
        suffix = lSuffix[i]
        fullSymbol = renameSymbol(row.symbol,suffix)
        
        TL.DFInfo.at[index,'suffix'] = suffix
        TL.DFInfo.at[index,'fullSymbol'] = fullSymbol
        
        if fillInfo(index,fullSymbol):
            break

def updateSymbol(row):
    fullSymbol = row.fullSymbol
    if pd.isna(fullSymbol):
        print('New symbol:'+row.symbol+','+row.mic_code + ' ')
        loadExchange(row)
    else:
        fillInfo(row.Index,fullSymbol)

    global Num
    Num+=1
    if Num % PrintInterval == 0:
        print(str(Num) + ' ' + TL.getNowStr())
    if Num % SaveInterval == 0:
        TL.saveInfo(False)

def getTickers(df):
    threads = []
    for row in df.itertuples():
        t = threading.Thread(target=updateSymbol, args=(row,))
        t.start()
        threads.append(t)
        if len(threads) % ThreadsNumber == 0:
            for t in threads:
                t.join()
            threads = []
            time.sleep(SleepTime)        

    if len(threads) > 0:
        for t in threads:
            t.join()    

startTime = pd.Timestamp.now()
timeDelta = timedelta(seconds=UpdateInterval)
outdatedTime = startTime - timeDelta

##exchanges = []
##for world in ExchangeWorlds:
##    exchanges.extend(TL.getExchanges(world))

avoidedColumns = TL.AvoidedAttributes.union(TL.AddedColumns)
validColumns = [x for x in TL.DFInfo.columns if x not in avoidedColumns]

for world in ExchangeWorlds:
    exchanges = TL.getExchanges(world)
    dfOKInfo = TL.DFInfo.loc[
        TL.DFInfo.issue.isna()
        & TL.DFInfo.mic_code.isin(exchanges)
        & TL.DFInfo.status.isin(GettingStatus)
        & (
            pd.isna(TL.DFInfo.time)
            | (pd.to_datetime(TL.DFInfo.time) < outdatedTime) 
          )
        ]
    print('World '+str(world)+' is running, there are '+str(len(dfOKInfo))+' tickers. '+TL.getNowStr())
    getTickers(dfOKInfo)
    print('World '+str(world)+' is finished. '+TL.getNowStr())
    
for i in range(MaxUpdatingCount):
    dfError = TL.DFInfo.loc[TL.DFInfo.status == -1]
    ct = len(dfError)
    if ct == 0:
        break
    print('Error updating No.'+str(i+1)+' is running, there are ' + str(ct) + ' errors. ' + TL.getNowStr())
    getTickers(dfError)
    
if ct == 0:
    msg = 'All errors have been cleared. '
else:
    msg = 'There are '+str(ct)+' errors left. '
print(msg + TL.getNowStr())
                
TL.saveInfo()
