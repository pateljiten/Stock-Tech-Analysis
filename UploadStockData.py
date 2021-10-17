import sys
import urllib3,zipfile
import sqlite3,csv,datetime


class UploadStockData:
    def __init__(self,t_date, t_noOfDays):
        self.gReportDate = self.gDate = t_date
        self.gNoDays = t_noOfDays
        self.gFileName = "cm14AUG2020bhav.csv"
        self.gHolidayList = {"26JAN2021","11MAR2021","29MAR2021","02APR2021","14APR2021","21APR2021","13MAY2021","21JUL2021","19AUG2021","10SEP2021","15OCT2021","04NOV2021","05NOV2021","19NOV2021"}
        #gHolidayList = {"21FEB2020","10MAR2020","02APR2020","06APR2020","10APR2020","14APR2020","01MAY2020","25MAY2020","02OCT2020","16NOV2020","30NOV2020","25DEC2020"}
        self.gConn = sqlite3.connect("myStock.db")

    def downLoadStockData(self):
        baseURL = "https://archives.nseindia.com/content/historical/EQUITIES/"
        print("Downloading Stock data from NSE \n")
        year = self.gDate[-4:]
        month = self.gDate[2:5]  
        fileName = "cm"+ self.gDate + "bhav.csv.zip"
        URL = baseURL + year + "/" + month + "/" + fileName    
        http = urllib3.PoolManager()
        print("URL : " + URL)
        hdr = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.190 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*,q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-IN,en;q=0.9,en-GB;q=0.8,en-US;q=0.7,hi;q=0.6',
        'Connection': 'keep-alive','Host':'www1.nseindia.com',
        'Cache-Control':'max-age=0',
        'Host':'www1.nseindia.com',
        'Referer':'https://www1.nseindia.com/products/content/derivatives/equities/fo.htm',
        } 
        request = http.request('GET',URL,preload_content = False,headers=hdr)
        chunk_size = 100
        try:
            with open(fileName,'wb') as out:
                while True:
                    data = request.read(chunk_size)
                    if not data:
                        print(data)
                        break
                    out.write(data)
            out.close()    
        except:
            print("Error Downloading file")
        #Unzip file    
        with zipfile.ZipFile(fileName,'r') as zip_ref:
            zip_ref.extractall()
        self.gFileName = fileName[:-4]   
    
    # Upload Daily Stock data in DB
    def uploadToDB(self):
        print("Uploading daily data in DAILY_NSE_STK_DATA ...")
        dtObj = datetime.datetime.strptime(self.gDate,"%d%b%Y")
        tmpDate = dtObj.strftime("%Y-%m-%d")
        dcur = self.gConn.cursor()    
        query = "DELETE FROM DAILY_NSE_STK_DATA WHERE DATE = '" + tmpDate + "'"
        dcur.execute(query)
        self.gConn.commit()    
        cur = self.gConn.cursor()
        reader = csv.reader(open(self.gFileName,'r')) # remove "".zip" ext    
        for row in reader:
            if row[1] != 'EQ':
                continue
            to_db = [row[0],row[2],row[3],row[4],row[5],row[8],tmpDate]
            cur.execute("INSERT INTO DAILY_NSE_STK_DATA (SYMBOLE ,OPEN,HIGH,LOW,CLOSE,VOLUME,DATE) VALUES (?,?,?,?,?,?,?)",to_db)
            #print ("Inserting data for " + row[0])
        self.gConn.commit()

    #return next working day
    def getNextDate(self,tmpDate):            
        workingDay = False
        while not workingDay:
            dtObj = datetime.datetime.strptime(tmpDate,"%d%b%Y")
            days = datetime.timedelta(1)
            dtObj = dtObj - days
            dayno = dtObj.weekday()    
            tmpDate = dtObj.strftime("%d%b%Y").upper()          
            if (dayno >= 5) or (tmpDate in self.gHolidayList):  # check for holiday or week end
                workingDay = False
            else:
                workingDay = True                            
        print("returnning " + tmpDate)
        return tmpDate 

    def upload(self):
        for i in range(0,int(self.gNoDays)):  
            print("Pulling Stock Data for " , self.gDate)
            self.downLoadStockData()
            self.uploadToDB()
            self.gDate = self.getNextDate(self.gDate)  