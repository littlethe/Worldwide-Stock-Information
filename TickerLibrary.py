import yfinance as yf
import csv
from datetime import datetime,timedelta
import pandas as pd
import os
import winsound
import time
import re
from dateutil.relativedelta import relativedelta
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

#unicode_escape utf-8-sig utf-16 ascii
AttributesPath       = 'TickerAttributes.csv'
InfoPath        = 'TickerInfo.csv'
InfoOldPath     = 'TickerInfo20241220.csv'
ExchangePath    = '12data_exchanges.csv'
Exchange12dataPath = '12data_stock_exchanges.csv'
StockPath       = '12data_stocks.csv'
ETFPath         = '12data_etf.csv'
UTFPath         = 'UTF.csv'
ScorePath       = 'TickerScore.csv'

CleanSorts         = {'world','kind1','kind2','area','field','leverage','pay','start','end'}

InvalidSorts    = [     #參數1為交易所代號，參數2為開頭(空白為全部)，參數3為股號長度(0為所有長度)，參數4為無效種類
     ['XLON','0',0,4]   #IB沒有英股外幣
    ,['XSHG','9',0,4]   #上海B股不能買
    ,['XSHE','2',0,4]   #深圳B股不能買
    ,['XWBO','AT0000',0,4]  #奧地利特殊股號不能買
    ,['XSAU','',6,4]    #沙烏地股號帶.SABE的讀不了資料也不能買
    ,['XWAR','',4,4]    #波蘭股號長度4的無法買或查不到
    ,['','',12,2]       #代號長度到達12以上的，暫不考慮
    ]

TypeSorts = [
     [{'Common Stock'},['Stock','Common']]
    ,[{'Preferred Stock'},['Stock','Preferred']]
    ,[{'Depositary Receipt','American Depositary Receipt','Global Depositary Receipt'},['Stock','DR']]
    ,[{'REIT'},['Stock','REIT']]
    ,[{'ETF',None},['Fund',None]]
    ,[{'Exchange-Traded Note'},['Note',None]]
    ,[{'Warrent'},['Warrent',None]]
    ]

K1K2Sorts = [
     [' ETN| Note |EMTN','',['Note',None]]
    ,[' ETC | ETC$','',['Note','Commodity']]
    ,['WARR ','',['Warrent',None]]
    ,['ETF |ETF-|ETFS| ETF$| fund$| fund | founds | lof|WisdomTree ','Fund Investment|Fund Inc',['Fund',None]]
    ]

K2Sorts = [
     ['dividend','','Dividend']
    ,[' Call | Call$| Option | Option$| Opt | Opt$','','Option']
    ,[' bond |bond$| BD |BD$| bill |bill$| treasury|BONDS |^EB |^RCB |\.BD | Debt | interest |fixed in|fixed-in'
      ,'Double Bond|Bond Exchange Holdings|fixed interest|Bond Group|Bond Income','Bond']
    ,[' cash|-cash|interest savings','cash pay','Cash']
    ,[' REIT|-REIT','','REIT']
    ]

SymbolSorts = [
     ['TWD','B',6,'kind2','Bond']
     ,['USD','.PR.|-P',0,'kind2','Preferred']
     ,['CAD','.PR.|-P',0,'kind2','Preferred']
    ]

FixedSorts = [
    ['Stock','Bond','Fund','Bond']
    ]

NameSorts = [ #用名字來分類的規則矩陣，第1值為要分類的欄位，第2值為是否要包含stock，第3值為分類規則
##    ['kind2',
##        [['dividend','','Dividend']
##        ,[' Call | Call$| Option | Option$| Opt | Opt$','','Option']
##        ]
##    ]
     ['area', #由大區域(洲)到小區域(國)
        [[' glob|world| international ','China International|Global China|China A International|ChinaAMC Global|Huaan Global','Global'] #概念地區
        #,['Europe|UK|France|Italy|DAX|German','Sino European','Europe'] #洲
        #,['Asia|Pacific','iShares Asia Trust|Cathay Pacific','Asia']
        #,['Southeast Asia','','Southeast Asia']
        ,['USA|S&P|NASDAQ|Dow Jones|DJI|Russell| US |Dow30|U.S. |NYSE| S+P | S-P ','S&P China|ex US','USA']
        ,['china|Chinese|SSE | CSI |CSI300| CNI |hongkong|hong kong|Hang Seng|HSI','ChinaAMC|China Asset Management|China Merchants Fund|China Southern|ex China','China']
        ,['russia','BORUSSIA','Russia']
        ,['taiwan','','Taiwan']
        #,['japan|TOPIX|Nikkei|TSE | J-| JPX ','Ex Japan','Japan']
        #,['hongkong|hong kong|Hang Seng|HSI','','Kong Kong'] #國家
        ,['Singapore|israel|Australia| ATX |Canada|Korea|KODEX|UK|France|Italy|DAX|German|japan|TOPIX|Nikkei|MIB|FTSE 250|FTSE 100| J-| JPX |Developed|Europe|North America','Ex Japan|Sino European|Eastern Europe','High']
        ,['Malaysia|Argentina|Turkey|Chile|Colombia|Arabi|Latin America|Mexico|Brazil|Poland| emerging | emerging$|developing|Asia|Pacific|Eastern Europe','Emerging Industry|Emerging Tech','Middle']
        ,['viet|thailand|Indonesia|Philippines|Nigeria|Pakistan|Egypt|india|Nifty|Africa','','Low']
        ]
    ]
    ,['field', #由大行業到小行業，再抓原物料，可以處理Information Technology或Solar Energy的寫法
        [[' tech|computer','','Technology'],['energy','','Energy'] #行業大類
        ,['Food|Livestock|Agrclt|Agriculture','','Food']
        ,['Material','','Material']
        ,['medicine|medical|biomedic|Biomedic|Biotech|Health|Heth|Vaccine| Med | Bio ','','Health']
        ,['Semiconductor| Chip ','','Semiconductor'] #行業小類
        ,['Information|Big data|software|Fintech|Cloud|Online','','Software']
        ,['solar|Photovoltaic','','Solar']
        ,[' Clean | Green | Climate |New Energy| Water | Sustainable| Renewable|ESG','','Green']
        ,['battery','','Battery']
        ,['Anime|Comic|Game|Media','','Media']
        ,['car ','','Car']
        ,['Robot','','Robot']
        ,[' AI |Artificial Intelligence','','AI']
        ,[' oil ','','Oil']
        ,[' bank','','Bank']
        ,['financ','','Financial']
        ,['metal','','Metal'] #原物料
        ,[' gold ','','Gold']
        ,['crypto|bitcoin','','Crypto']
        ,['Treasury','','Treasury']
        ,['AAA| AA |-AA','','Rating A']
        ,['BBB| BB |-BB','','Rating B']         
        ,['High Yield| TST ','','High Yield']
        ]
    ]
    ,['pay',
        [['distributing| d | d$| dist | dist$| dis | dis$','','Distributing']
        ,[' acc |accumulati| acc$|capitalisation| c | c$','','Accumulating']
        ,['monthly','','Monthly']
        ,['6 month','','Half-yearly']
        ,['quarterly|Seasons ','','Quarterly']
        ]
    ]
    ,['leverage',
        [[' short | short$| Inverse| inv ','INV INC|INV TRUST','-1']
        ,[' long short','','0']
        ,[' Ultra | Ultra$| Leveraged','Ultra Clean|Ultra Deep','2']
        ,['UltraPro Short','','-3']
        ,['UltraShort| Ultra Short','','-2']
        ]
    ]
]

TestSymbol = 'MSFT'
#TestSymbol = 'MCD'

AddedColumns  = [
                'world'
                ,'fullSymbol'               
                ,'suffix'
                ,'attributes'
                ,'status'
                ,'time'
                ,'succeeded'
                ,'successes'
                ,'failed'
                ,'failures'
                ,'issue'
                ,'kind1'
                ,'kind2'             
                ,'area'
                ,'field'
                ,'leverage'
                ,'pay'
                ,'start'
                ,'end'
                 ]

#yfinance不讀入或不清空的欄位
AvoidedAttributes             = {
                            'symbol'
                             ,'name'
                             ,'currency'
                             ,'exchange'
                            ,'mic_code'
                             ,'country'
                             ,'type'
                             ,'suffix'
                             ,'maxAge'
                             ,'address1'
                             ,'state'
                             ,'zip'
                             ,'phone'
                             ,'address2'
                             ,'address3'
                             ,'fax'
                             ,'companyOfficers'
                             ,'regularMarketPreviousClose'
                             ,'regularMarketOpen'
                             ,'regularMarketDayLow'
                             ,'regularMarketDayHigh'
                             ,'regularMarketVolume'
                             ,'averageDailyVolume10Day'
                             ,'industryKey'
                             ,'industryDisp'
                             ,'sectorKey'
                             ,'sectorDisp'
                             ,'uuid'
                             ,'messageBoardId'
                             ,'annualReportExpenseRatio'
                             ,'industrySymbol'
                             ,'longBusinessSummary'
                            ,'irWebsite'
                            ,'shortName'
                            ,'longName'
                            ,'timeZoneFullName'
                            ,'gmtOffSetMilliseconds'
                            ,'underlyingSymbol'
                            ,'SandP52WeekChange'
                             }

ExchangeWorlds = {0:'valid',1:'East',2:'Middle',3:'West',-1:'test'}
ExchangeNewColumns = ['suffix','valid','East','Middle','West','price','test']
WorldSorts          = {'East','Middle','West'}

#UTFAttributes   = {'name','city','industry'}
UTFAttributes   = {'city','fundFamily','industry'}

TickerIndex = ['symbol','mic_code']

CopyAttributes = ['fullSymbol','suffix','industry','sector']

TimeFormat      = '%Y/%m/%d %H:%M:%S'

SoundRepeatSuccessed             = 5 #讀完資料後，提示聲要響多少次
SoundIntervalSuccessed           = 5 #多久響一次(秒)
SoundRepeatFailed                 = 5 #讀完資料後，提示聲要響多少次
SoundIntervalFailed               = 1 #多久響一次(秒)

FileCode        = 'ascii'

#issue說明:2是可以買但不考慮，3是廢棄，4是查得到但無法買
Issues = {2:'Not Considered',3:'Abandoned',4:'Unable To Buy'}

NAValues = ['','none','#N/A', '#N/A N/A', '#NA', '-1.#IND','-1.#QNAN', '-NaN', '-nan'
            , '1.#IND','1.#QNAN', 'N/A','n/a','nan']

##NAValues = ['', '#N/A', '#N/A N/A', '#NA', '-1.#IND','-1.#QNAN', '-NaN', '-nan'
##            , '1.#IND','1.#QNAN', 'N/A', 'NULL', 'NaN','n/a', 'nan', 'null']

def test():
    print('TEST')

def renameSymbol(symbol,suffix):
    
    symbol  = symbol.replace('.PR.', '-P')    
    symbol  = symbol.replace('.', '-')
    symbol  = symbol.replace(' ', '-')
    
    if suffix == '-':
        return symbol
    return symbol+'.'+suffix

def replaceUTF(s):
    if pd.isna(s):
        return s
    for c in s:
        code = ord(c)
        if code>127:
            newChar = str(DictUTF[code])
            s = s.replace(c,newChar)
    return s

def getInfo(symbol):
    ticker  = yf.Ticker(symbol)
    return ticker.info

def saveInfo(final=True):
    try:
        DFInfo.to_csv(InfoPath, encoding = FileCode,index = False)
        print('Saved.'+getNowStr())
        if final:
            playSound()
            print('Finished.')
    except Exception as error:
        print('error:',error)
        print('Can not save.'+getNowStr())
        playSound(0)

def playSound(no=1):
    repeat = SoundRepeatSuccessed if no == 1 else SoundRepeatFailed
    interval = SoundIntervalSuccessed if no == 1 else SoundIntervalFailed
    
    for i in range(repeat):
        winsound.PlaySound("SystemAsterisk", winsound.SND_ASYNC)
        time.sleep(interval)

def getExchanges(target):
    return DFExchanges.loc[DFExchanges[ExchangeWorlds[target]] == 'v'].code.tolist()

def sortLeverage(df,index,name,s1,s2,value):
    if checkString(name,s1,'') & checkString(name,s2,''):
        setRow(df,index,'leverage',value)

def sortMultiple(df,index,name,x):
    if x % 1 == 0:
        x=int(x)
    sX = ' '+str(x)
    minus = 0 - x
    sMinus =str(minus)
    sortLeverage(df,index,name,sX+'X','',x)
    sortLeverage(df,index,name,sMinus+'X','',x)
    sortLeverage(df,index,name,sX+'X','bear|inv',minus)

def beInt(s):
    return re.sub("[^0-9]", "", s)

def cutBySpace(s,index,backward=True):
    length = 4
    sub = s[index-length:index] if backward else s[index+1:index+length+1]
    index2 = sub.rfind(' ') if backward else sub.find(' ')
    if index2>-1:
        sub = sub[index2+1:] if backward else sub[:index2]
    return sub

def checkString(string,including,excluding):      
    return True if re.search(including,re.sub(excluding,'',string,flags=re.IGNORECASE),flags=re.IGNORECASE) else False

def setRow(df,index,column,value):
    df.at[index,column] = value

def setK1K2(df,index,k1,k2):
    setRow(df,index,'kind1',k1)
    setRow(df,index,'kind2',k2)

def sortTickers(df,sortIssue):

    if sortIssue:
        df['issue'] = None
    for s in CleanSorts: #分類前先清空
        df[s] = None
    
    worlds = {}
    for s in WorldSorts:
        worlds[s] = DFExchanges.loc[DFExchanges[s] == 'v'].code.tolist()

    exchanges = getExchanges(0)
    #df = DFInfo.loc[DFInfo.mic_code.isin(exchanges)]
    for row in df.loc[df.mic_code.isin(exchanges)].itertuples():
        ind = row.Index
        name = str(row.name)
        mic = str(row.mic_code)
        currency = str(row.currency)

        for key in worlds.keys(): #用交易所來決定屬於那個世界的
            if mic in worlds[key]:
                setRow(df,ind,'world',key)

        if sortIssue: 
            #無效狀況
            symbol = str(row.symbol)
            hasIssue = False          
            if name.split('.')[0] == symbol:  #名字和股號相同為被廢棄的股票
                setRow(df,ind,'issue',3)
                hasIssue = True

            for p in InvalidSorts: #用股號開頭和股號長度來判斷是不是無效的股票
                headL = len(p[1])
                if (
                    (p[0] == '') | (mic == p[0])
                ) & (
                    (p[2] == 0) | (len(symbol) >= p[2])
                ) & (
                    (p[1] == '') | (symbol[:headL] == p[1])             
                ):
                    setRow(df,ind,'issue',p[3])
                    hasIssue = True
                    
            if hasIssue:
                continue
        else:
            symbol = str(row.fullSymbol).split(".")[0]

        #用原始資料自帶的分類(Type)來進行新的分類(Kind1和Kind2)
        sType = None if pd.isna(row.type) else str(row.type)
        noKind = True
        for p in TypeSorts:
            if sType in p[0]:
                setK1K2(df,ind,p[1][0],p[1][1])
                noKind = False

        if noKind:
            setK1K2(df,ind,'Other',sType)      
        
        #用名字來處理ETF分類，column為那個分類，p的第1值為包含的字，第2值為不包含的字，第3值為為分類的值
        for p in K1K2Sorts:
            if checkString(name,p[0],p[1]):
                setK1K2(df,ind,p[2][0],p[2][1])

        for p in K2Sorts:
            if checkString(name,p[0],p[1]):
                setRow(df,ind,'kind2',p[2])

        for p in SymbolSorts: #用股號來分類
            if (
                (currency == p[0])
                & checkString(symbol,p[1],'')
                & (
                    (p[2] == 0)
                    | (p[2] == len(symbol))
                )
            ):
                setRow(df,ind,p[3],p[4])

        for p in FixedSorts: #分類修正
            if (df.at[ind,'kind1'] == p[0]) & (df.at[ind,'kind2'] == p[1]):
                setK1K2(df,ind,p[2],p[3])

        if df.at[ind,'kind1'] == 'Stock':
            continue
        
        for sort in NameSorts:
            for p in sort[1]:
                if checkString(name,p[0],p[1]):
                    setRow(df,ind,sort[0],p[2])

        for i in range(4,6): #抓槓桿(整數)
            sortMultiple(df,ind,name,i)
            
        for i in range(1,13): #抓槓桿(含小數數)
            sortMultiple(df,ind,name,i*0.25)

        #處理name有幾年到幾年的描述
        pos = name.rfind('+')
        if pos > 0:
            sub = cutBySpace(name,pos)
            if sub.isnumeric():
                setRow(df,ind,'start',sub)

        pos = name.rfind('-')
        if pos > 0:
            sub1 = cutBySpace(name,pos)
            sub2 = cutBySpace(name,pos,False)
            if sub1.isnumeric() & sub2.isnumeric():
                setRow(df,ind,'start',sub1)
                setRow(df,ind,'end',sub2)
                


def sortAllTickers():
    print('Sorting is starting.' + getNowStr())
    sortTickers(DFInfo,True)
    if os.path.isfile(ScorePath):
        dfScore = pd.read_csv(ScorePath,encoding = FileCode,low_memory=False)
        sortTickers(dfScore,False)
        try:
            dfScore.to_csv(ScorePath, encoding = FileCode,index = False,float_format='%.8f') #讓價錢顯示正常不會出現E
        except Exception as error:
            print('error:',error)
            print('Score can not be saved.')
    print('Sorting has been finished.' + getNowStr())
    
def getNowStr():
    return pd.Timestamp.now().strftime(TimeFormat)

DFUTF = pd.read_csv(UTFPath,encoding = 'utf-8',low_memory=False)
DictUTF = DFUTF.set_index('code').to_dict('dict')['replacement']

if os.path.isfile(ExchangePath):
    DFExchanges     = pd.read_csv(ExchangePath,encoding = FileCode)
else:
    DFExchanges = pd.read_csv(Exchange12dataPath,sep = ';',encoding = FileCode)
    dfNewColumns = pd.DataFrame(columns=ExchangeNewColumns)
    DFExchanges = pd.concat([DFExchanges,dfNewColumns], axis=1)
    DFExchanges.to_csv(ExchangePath, encoding = FileCode,index = False)

if os.path.isfile(InfoPath):
    DFInfo = pd.read_csv(InfoPath,encoding = FileCode,low_memory=False,na_values = NAValues,keep_default_na=False)
    print('Loaded Ticker Info.'+getNowStr())
else:
    dfStocks        = pd.read_csv(StockPath,sep = ';',encoding = 'utf-8',na_values = NAValues,keep_default_na=False)
    dfETFs          = pd.read_csv(ETFPath,sep = ';',encoding = 'utf-8',na_values = NAValues,keep_default_na=False)
    dfAttributes    = pd.read_csv(AttributesPath,encoding = FileCode)
    dfETFs.type     = 'ETF'
    
    allowedAttributes = dfAttributes.attribute.tolist()
    allowedAttributes = [x for x in allowedAttributes if x not in AvoidedAttributes]
    DFInfo = pd.concat([dfStocks,dfETFs],ignore_index=True)
    dfAddedColumns = pd.DataFrame(columns = AddedColumns)
    dfAllowedAttributes = pd.DataFrame(columns = allowedAttributes)
    DFInfo = pd.concat([DFInfo,dfAddedColumns,dfAllowedAttributes], axis=1)
    
    DFInfo.attributes    = 0
    DFInfo.status        = 0
    
    DFInfo.name = DFInfo.name.apply(lambda x: replaceUTF(x))

    sortAllTickers()

    dfOld = pd.read_csv(InfoOldPath,encoding = FileCode,low_memory=False,na_values = NAValues,keep_default_na=False)
    dfOld = dfOld.set_index(TickerIndex)
    dictOld = dfOld.to_dict('dict')
    oldKeys = dictOld['fullSymbol'].keys()
    for row in DFInfo.itertuples():
        key = (row.symbol,row.mic_code)
        if (key in oldKeys):
            for attribute in CopyAttributes:
                DFInfo.at[row.Index,attribute] = dictOld[attribute][key]

    print('Created Ticker Info.'+getNowStr())
