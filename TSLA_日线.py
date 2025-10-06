import requests
import time
import pandas as pd


#股票代码
symbol='TSLA'
#开始时间
begin=1277740800
#结束时间
end=int(time.time()*1000)
result=[['date','volume','open','close','high','low','fluctuation']]
url=f'https://stock.xueqiu.com/v5/stock/chart/kline.json?symbol={symbol}&begin={begin}&end={end}&period=day&type=before&indicator=kline'
headers={
    'cookie':'s=bz17w0dxtu; device_id=67063e46ec7eb3c4c220613ab2a7833c; bid=4ac66462adfc6f43f8255597f052bc8f_lfajmx3p; xq_a_token=173d1b86b97861e4a0ecbe2d031fbd057d337248; xqat=173d1b86b97861e4a0ecbe2d031fbd057d337248; xq_r_token=ee8e80a187bf70af8a22704223d871770297dd64; xq_id_token=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJ1aWQiOi0xLCJpc3MiOiJ1YyIsImV4cCI6MTY4Mzg1MTMyOSwiY3RtIjoxNjgyMzA4NjY1NTA5LCJjaWQiOiJkOWQwbjRBWnVwIn0.UtHkMaZlO3bEKxGK-pky9jihLybHX34CAFp9LM1cPtkrx4UOdF6A5rQRbfCslJ6VBXMEQkfAw8nY0dUD9i1T0kTuByaGLvoAdH0udnpgJSHHZk1EWtMybAofYTwKWLgWFQ5B61mLrHYdGuLg2-ZARjWvgtLb_HDx2ecbhrw0CC94B1ycctDiWRiTLsIp84zjjz_Reh4X4o2aTp_6q1dS6fE5-Tl9XnHqvcg1VmenFCrgFypFoaQ_za5NVU2ZiX2XXaLb09QzgJ1x4GQ3JmRiYDviBa0rhxe_qbdwiWeQBhp2AUfKLzSMiuwMsn-J-FjpjTrWIc4vzQqdK_uAOizyWw; u=441682308685230; Hm_lvt_1db88642e346389874251b5a1eded6e3=1682308689; is_overseas=0; Hm_lpvt_1db88642e346389874251b5a1eded6e3=1682308697',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'}
resp=requests.get(url=url,headers=headers).json()['data']['item']
for r in resp:
    date=time.strftime('%Y-%m-%d', time.localtime(r[0]/1000))
    open=r[2]
    close=r[5]
    high=r[3]
    low=r[4]
    volume=r[1]
    fluctuation=r[7]
    result.append([date,volume,open,close,high,low,fluctuation])
df=pd.DataFrame(result)
df.to_csv(f'{symbol}.csv',index=False,header=None)