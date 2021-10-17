from datetime import datetime
import sys, getopt
import sqlite3
import numpy as np
import UploadStockData
from openpyxl import Workbook
from openpyxl import cell

gConn = sqlite3.connect("myStock.db")

class TechAnalysis:
    def __init__(self, stockCode, analydate) :
        self.stockCode = stockCode
        self.dateToAnalyze = datetime.strptime(analydate,"%d%b%Y")

    # Load Data from DB for given stock code
    def loadData(self):
        print("Loading Data for " + self.stockCode)
        query = "SELECT * FROM DAILY_NSE_STK_DATA WHERE SYMBOLE = '{0}' AND DATE <= '{1}' \
            ORDER BY DATE ASC".format(self.stockCode,self.dateToAnalyze.strftime("%Y-%m-%d"))
        curr = gConn.cursor()
        stkData = curr.execute(query)       
        numrows = curr.rowcount            
        stockDataDef = [('symbole','S20'),('open','f8'),('high','f8'),('low','f8'),('close','f8'),('volume','i8'),('date','datetime64[s]')]
        self.stockData = np.fromiter(stkData.fetchall(),dtype=stockDataDef,count=numrows)        
        #print(self.stockData['volume'])
    
    # Check if today's volume is > thne 1.5 times last 4 day volume
    def checkVolume(self):
        print("Checking volumne movement ")
        volarr = self.stockData['volume'][-5:-1]        
        todayVol = self.stockData['volume'][-1] 
        if (todayVol > (np.average(volarr)*1.5)):
            self.volIndicator = True
            print("High Tradding Volume ...")
        else:
            self.volIndicator = False   

    # Check for bulish engulfing, morning star or hammer
    def checkCandleType(self):        
        (day4, dayb4yday, yday ,today) = self.stockData[-4:] 
        
        #Bulish Engulfing - big green candle covering prev day red candle
        tdayCandleGreen = False
        ydayCandleRed = False
        if (today['close'] > today['open']):
            tdayCandleGreen = True
        if (yday['open'] > yday['close']):
            ydayCandleRed = True
        if (today['open'] > yday['open'] and today['close'] < yday['close']) and tdayCandleGreen and ydayCandleRed:
            self.bulishEngulf = True
        else:
            self.bulishEngulf = False
        
        #Morning Star
        ## Check for Big red candle on day-2
        if ((dayb4yday['open'] - dayb4yday['close']) > day4['close']*0.01):
            bigRedCandle = True
        else:
            bigRedCandle = False
        ## Check for Doji on day-1
        if (abs(yday['open'] - yday['close']) <= dayb4yday['close']*0.001):
            doji = True
        else:
            doji = False
        ## Check for green candle on day0
        if (today['close'] - today['open'] > yday['close']*0.01):
            bigGreenCandle= True
        else:
            bigGreenCandle= False
        ## Final check for last 3 day candle to determine morning star
        if (bigRedCandle and doji and bigGreenCandle):
            self.morningStar = True
        else:
            self.morningStar = False
        
        # Hammer - Wick should be 2 times body, open and close should be more or less same
        if (today['open'] > today['close']):
            loweredge = today['close']
        else :
            loweredge = today['open']
        if ( (abs(today['high'] - today['open']) < yday['close']*0.001) and \
              abs(today['low'] - loweredge) > abs(today['close'] - today['open'])*2):
            self.hammer = True
        else:
            self.hammer = False

    def checkAverages(self):
        print("Computing exp moving averages")
        # Compute moving averages as of today
        self.ema_5 = np.average(self.stockData['close'][-5:],weights=np.arange(5)+1)
        self.ema_13 = np.average(self.stockData['close'][-13:],weights=np.arange(13)+1)                
        self.ema_26 = np.average(self.stockData['close'][-26:],weights=np.arange(26)+1)
        if (self.ema_5 >= self.ema_13):
            self.avg5_above13 =True
        else:
            self.avg5_above13 =False
        if (self.ema_13 >= self.ema_26):
            self.avg13_above26 =True
        else:
            self.avg13_above26 =False
        # Compute moving averages as of yesrerday
        yday_ema_5 = np.average(self.stockData['close'][-6:-1],weights=np.arange(5)+1)
        yday_ema_13 = np.average(self.stockData['close'][-14:-1],weights=np.arange(13)+1)                
        yday_ema_26 = np.average(self.stockData['close'][-27:-1],weights=np.arange(26)+1)
        if((yday_ema_5 < yday_ema_13) and self.avg5_above13):
            self.avg5_cross13 = True
        else:
            self.avg5_cross13 = False
        if((yday_ema_13 < yday_ema_26) and self.avg13_above26):
            self.avg13_cross26 = True
        else:
            self.avg13_cross26 = False

    # Deriv polynomial of the form ax + y 
    def checkTrendDirection(self):
        print("Checking trend - Up \ Down \ sideways")
        y = self.stockData['close']
        x = np.arange(0,len(y))
        z = np.polyfit(x,y,1)
        self.trend = z[0]
        #print("{0}x+{1}".format(*z))        
    
    def dumpAnalysis(self):    
        to_db = self.stockData[-1].tolist()
        to_db = list(to_db)
        to_db[6] = to_db[6].strftime("%Y-%m-%d")
        to_db = to_db + [self.volIndicator,self.bulishEngulf,self.morningStar,self.hammer,\
                round(self.ema_5,2),round(self.ema_13,2),round(self.ema_26,2),round(self.avg5_above13,2),\
                self.avg13_above26,self.avg5_cross13,self.avg13_cross26,round(self.trend,2)]                
        curr = gConn.cursor()
        curr.execute("INSERT INTO DAILY_TECH_ANALYSIS (SYMBOLE,OPEN,HIGH, LOW,CLOSE,VOLUME,DATE,VOLUMEIND,BULISHENGULF,MORNINGSTAR,HAMMER,EMA5 ,EMA13 ,EMA26 ,EMA5_ABOVE13,EMA13_ABOVE26,EMA5_CROSS13,EMA13_CROSS26 ,TREND)"\
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",to_db)
        gConn.commit()
        print(to_db)  

    def cleanup(self):        
        query = "DELETE FROM DAILY_TECH_ANALYSIS WHERE SYMBOLE = '{0}' AND DATE <= '{1}'".\
            format(self.stockCode,self.dateToAnalyze.strftime("%Y-%m-%d"))    
        curr = gConn.cursor()
        curr.execute(query)
        gConn.commit()


    # Main Function to analyze stock data
    def analyze(self):
        print("Initiating Analysis for " + self.stockCode)
        self.cleanup()
        self.loadData()        
        self.checkVolume()
        self.checkCandleType()
        self.checkAverages()
        self.checkTrendDirection()
        self.dumpAnalysis()
    
# Extract data to Excel 
def extract(tDate):
    print("Extracting data from DB to excel")
    extDate = datetime.strptime(tDate,"%d%b%Y")
    gWb = Workbook()
    sht = gWb.active
    query = "SELECT * FROM DAILY_TECH_ANALYSIS WHERE (VOLUMEIND != 0 OR BULISHENGULF != 0 OR MORNINGSTAR != 0 OR HAMMER != 0 \
        OR EMA5_ABOVE13 != 0 OR EMA13_ABOVE26 != 0 OR EMA5_CROSS13 !=0 OR EMA13_CROSS26 !=0) AND DATE = '{0}'".format(extDate.strftime("%Y-%m-%d"))
    curr = gConn.cursor()
    print (query)
    result = curr.execute(query)
    colhdr = [i[0] for i in curr.description]
    sht.append(colhdr)
    for row in result:
        sht.append(row)
    gWb.save("TodayTechAnalysis.xlsx")


def analyze(tDate):
    # Analyze whose data is at least more then 27 days
    query = "SELECT DISTINCT SYMBOLE FROM DAILY_NSE_STK_DATA group by SYMBOLE HAVING COUNT(1) >= 27"
    curr1 = gConn.cursor()
    stklist = curr1.execute(query)       
    tstk = ""     
    for stk in stklist:        
        tstk = TechAnalysis(stk[0],tDate)
        tstk.analyze()
    

def main(argv):
    print("Initiating Technical Analysis....")
    
    tDate = '10-OCT-2021'
    tNoDays = 1
    doAnalysis = False
    try:
        opts,args = getopt.getopt(argv,"hd:n:a:",["load"])
        for opt,arg in opts:
            if opt == "-h":
                print("TechnicalAnalysis.py -d <date - DDMONYYYY> -n <no of days>")
                print("TechnicalAnalysis.py -a <date - DDMONYYYY> ")
                exit(0)
            elif opt in ("-a"):
                tDate = arg
                doAnalysis = True
                print("Analysing for " + tDate)
            elif opt in ("-d"):
                tDate = arg
                print("Date " + tDate)
            elif opt in ("-n","--load"):
                tNoDays = arg           
                print("Loading Stock Data for last {0} days ".format(tNoDays))     
    except getopt.GetoptError:
        print("TechnicalAnalysis.py -d <date - DDMONYYYY> -n <no of days>")
        print("TechnicalAnalysis.py -a <date - DDMONYYYY> ")
    if (doAnalysis):
        #analyze(tDate)
        extract(tDate)          
    else:
        handle = UploadStockData.UploadStockData (tDate,tNoDays)
        handle.upload()

    gReportDate = tDate
    

if __name__ == "__main__":
    main(sys.argv[1:])
