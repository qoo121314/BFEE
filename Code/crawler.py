import numpy as np
import pandas as pd
import requests
import datetime
import re
import time
from fake_useragent import UserAgent
import random
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

today = datetime.date.today()

try:
    with open('../QuotaData_Setting.txt',mode='r', encoding='utf-8') as f:
        quota_setting = f.readlines()
except:
    print('未定義QuotaData_Setting    由系統建立預設值設定檔')
    last_month_date = (today - pd.tseries.offsets.MonthEnd(1)).replace(day=1).date()
    with open("../QuotaData_Setting.txt", "a") as f:
        f.write(f"START:{last_month_date.strftime('%Y-%m-%d')}\nCREDIT_TABLE:credit_data_table.pickle\nSYSTEM_TABLE:system_table.pickle\nCONVERT_CSV:")
    with open('../QuotaData_Setting.txt',mode='r') as f:
        quota_setting = f.readlines()
    quota_table = pd.DataFrame({},columns=["日期",'股票代號', 
                                           '股票名稱', '前日餘額', '賣出', '買進', '現券',
                                           '今日餘額', '限額', '前日餘額', '當日賣出',
                                           '當日還券', '當日調整', '當日餘額', 
                                           '次一營業日可限額', '備註'])
    system_table = pd.DataFrame({},columns=["日期", '證券代號',
                                             '證券名稱', '前日借券餘額(1)股', 
                                            '本日異動股借券(2)', '本日異動股還券(3)',
                                            '本日借券餘額股(4)(1)+(2)-(3)', '本日收盤價(5)單位：元', 
                                            '借券餘額市值單位：元(6)(4)*(5)', '市場別'])
    
    quota_table.to_pickle('credit_data_table.pickle')
    system_table.to_pickle('system_table.pickle')

def set_header_user_agent():
    user_agent = UserAgent()
    return user_agent.random

def get_quote(date):
    parse_date = date.strftime(format='%Y%m%d')
    date = date.strftime(format='%Y-%m-%d')
    user_agent = set_header_user_agent()
    res = requests.get(f'https://www.twse.com.tw/exchangeReport/TWT93U?response=csv&date={parse_date}',
                      headers={ 'user-agent': user_agent})
    try:   
        a = res.text.split('\r\n')[1:-6] #原始文字清洗，清除首行及表尾說明語句
        b = [a.replace('=', "") for a in a]  #非個股其證券代號名稱前會有等號，予以刪除
        c = [c.split('","') for c in b] #每一列分割欄位
        df = pd.DataFrame(c[2:], columns=c[1])
        df = df.dropna()
        df.columns = [i.strip('",') for i in df.columns]
        df = df.applymap(lambda x:x.strip('",').replace(',',""))
        df = df[df.股票名稱!='合計']
        df.insert(0,'日期', date)
        return df
    except:
        return None


def get_system(date):
    parse_date = date.strftime(format='%Y%m%d')
    date = date.strftime(format='%Y-%m-%d')
    user_agent = set_header_user_agent()
    res = requests.get(f'https://www.twse.com.tw/exchangeReport/TWT72U?response=csv&date={parse_date}&selectType=SLBNLB',
                      headers={ 'user-agent': user_agent})
    try:
        a = res.text.split('\r\n')[1:-6] #原始文字清洗，清除首行及表尾說明語句
        b = [a.replace('=', "") for a in a]  #非個股其證券代號名稱前會有等號，予以刪除
        c = [c.split('","') for c in b] #每一列分割欄位
        df = pd.DataFrame(c[1:], columns=c[0])
        df.columns = [i.strip('",') for i in df.columns]
        df = df.applymap(lambda x:x.strip('",').replace(',',""))
        df = df[df.證券代號!='合計'] 
        df.insert(0,'日期', date)
        return df
    except:
        pass


weekday_dict={
    0:'星期一',
    1:'星期二',
    2:'星期三',
    3:'星期四',
    4:'星期五',
    5:'星期六',
    6:'星期日'
}

quota_setting = [i.strip('\n').split(':')[1] for i in quota_setting]
quota_table = pd.read_pickle(quota_setting[1])
system_table = pd.read_pickle(quota_setting[2])
convert = quota_setting[3]

if convert in ['True', 'T', 'Y']:
    convert=True
else:
    convert=False

quota_table = pd.read_pickle(quota_setting[1])
system_table = pd.read_pickle(quota_setting[2])

initial = False
if (len(quota_table)==0) & (len(quota_table)==0) & (convert==False):
    initial = True

if initial ==True:
    print(f'-------------初次下載  由 {last_month_date.strftime(format="%Y-%m-%d")} 開始下載--------------')
    parse_date = last_month_date
    while(parse_date<=today):
        print(f'-----------{parse_date.strftime(format="%Y-%m-%d")}  ({weekday_dict[parse_date.weekday()]})------------')
        if parse_date.weekday()>=5:
            parse_date= parse_date+datetime.timedelta(1)
            continue
            
        temp = get_quote(parse_date)
        if type(temp) == pd.core.frame.DataFrame:
            quota_table = quota_table.append(temp)
            print(f'---成功下載 : quota_table----')
        else:
            print(f'---無法下載 : quota_table----')
            
        time.sleep(random.randint(2, 5))
        
        temp = get_system(parse_date)
        if (type(temp) == pd.core.frame.DataFrame) & (len(temp)>0):
            system_table = system_table.append(temp)
            print(f'---成功下載 : system_table----')
        else:
            print(f'---無法下載 : system_table----')
        
        time.sleep(random.randint(3, 6))
        parse_date= parse_date+datetime.timedelta(1)

elif(convert==False):
    quota_lastest_date = pd.to_datetime(quota_table.日期).max().date()
    system_lastest_date = pd.to_datetime(system_table.日期).max().date()
    
    parse_date = min(quota_lastest_date, system_lastest_date)
    print(f'-------------接續前次紀錄下載  由 {parse_date.strftime(format="%Y-%m-%d")} 開始下載--------------')
    
    while(parse_date<=today):
        print(f'-----------{parse_date.strftime(format="%Y-%m-%d")}  ({weekday_dict[parse_date.weekday()]})------------')
        if parse_date.weekday()>=5:
            parse_date= parse_date+datetime.timedelta(1)
            continue
            
        temp = get_quote(parse_date)
        if type(temp) == pd.core.frame.DataFrame:
            quota_table = quota_table.append(temp)
            print(f'---成功下載 : quota_table----')
        else:
            print(f'---無法下載 : quota_table----')
            
        time.sleep(random.randint(2, 5))
        
        temp = get_system(parse_date)
        if (type(temp) == pd.core.frame.DataFrame):
            if (len(temp)>0):
                system_table = system_table.append(temp)
                print(f'---成功下載 : system_table----')
            else:
                print(f'---無法下載 : system_table--有抓到但無資料--')
        else:
            print(f'---無法下載 : system_table----')
        
        time.sleep(random.randint(3, 6))
        parse_date= parse_date+datetime.timedelta(1)
    
    
quota_table = quota_table.drop_duplicates()
system_table = system_table.drop_duplicates()


quota_table.to_pickle('credit_data_table.pickle')
system_table.to_pickle('system_table.pickle')
print('-----------儲存資料----------')

if (convert==True):
    print('----------轉存成CSV-------------')
    quota_table.to_csv('信用額度總量管制餘額表.csv', encoding='utf-8_sig', index=False)
    system_table.to_csv('證交所借券系統與證商營業處所借券餘額合計表.csv',encoding='utf-8_sig', index=False)


revised_start_date = pd.to_datetime(quota_table.日期).min().date().strftime(format='%Y-%m-%d')
print(f'修改設定檔的Start Date 為 第一個交易日{revised_start_date}')
with open('../QuotaData_Setting.txt',mode='r', encoding='utf-8') as f:
    quota_setting = f.readlines()
    quota_setting = [i.strip('\n').split(':')[1] for i in quota_setting]
with open("../QuotaData_Setting.txt", "w") as f:
    f.write(f"START:{revised_start_date}\nCREDIT_TABLE:{quota_setting[1]}\nSYSTEM_TABLE:{quota_setting[2]}\nCONVERT_CSV:{quota_setting[3]}")