# -*- coding: utf-8 -*-


from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import requests
import re
import mysql.connector 

def main():
    getCrowdData()
    uploadtoDB(historical)

def getCrowdData():
    from bs4 import BeautifulSoup
    import pandas as pd
    import numpy as np
    import requests
    import re 
    global historical
    historical=pd.DataFrame(columns=['date','dateString','crowdLevel','parkHours'])
    datetime=re.compile(r'\<time\sdatetime\=(.*)\>\<strong\>(.*)\<\/strong\>\<\/time')
    parkhours=re.compile(r'\s+([\d\:\sam\-pm]+)\s\<br')
    crowdLevels=re.compile(r'\>\s+([A-Za-z\s\d\(\)]+)\s*\<')
    url="http://touringplans.com/disneyland"
    
    page = requests.get(url)
    soup = BeautifulSoup(page.text, 'html5lib')
    
    park_info=soup.find_all('li')
    crowds=[]
    date=[]
    dateString=[]
    crowdLevel=[]
    parkHours=[]
    
    for i in park_info:
        if 'time datetime' in str(i):
            crowds.append(str(i))
#            print(i)
        else:
            pass
    for c in crowds:
    #    for m in re.finditer(parkhours, c):
        for m in re.finditer(datetime,c):
            date.append(m.group(1))
            dateString.append(m.group(2))
            print("%s\n%s" %(m.group(1),m.group(2)))
        for m in re.finditer(crowdLevels,c):
            p=m.group(1).strip('\n ')
            if 'out' in p:
                crowdLevel.append(p)
            print(m.group(1))
        for m in re.finditer(parkhours,c):
            parkHours.append(m.group(1).strip('\n '))
            print(m.group(1))
    zippedList =  list(zip(date,dateString,crowdLevel,parkHours))
    historical=pd.DataFrame(zippedList,columns=['date','dateString','crowdLevel','parkHours'])

def uploadtoDB(historical):
    import pandas as pd
    import mysql.connector
    
    conn = mysql.connector.connect(user='carena', password='',
                                  host='127.0.0.1',
                                  database='projects')
    
    c = conn.cursor()
    statement="""INSERT INTO crowds (date, dateString, crowdLevel, parkHours) VALUES(%s, %s, %s, %s)"""
    
    for i, row in historical.iterrows():
        d=row[0]
        dS=row[1]
        cL=row[2]
        pH=row[3]
        statement=("INSERT INTO crowd" "(date, dateString, crowdLevel, parkHours) " "VALUES(%s, %s, %s, %s)")
        
        data=(d,dS,cL,pH)
        c.execute(statement,data)
        print(data)
    conn.commit()

if __name__=="__main__"
    main()
