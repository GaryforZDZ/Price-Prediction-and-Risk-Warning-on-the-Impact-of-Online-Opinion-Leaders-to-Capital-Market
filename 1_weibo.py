import requests
from bs4 import BeautifulSoup
import time
import pandas as pd
from lxml import etree
import csv
import datetime
import math


with open('cookie.txt', 'r') as f:
    cookie = f.read()


def main():
    #关键词
    tags = ['马斯克','刘强东']
    ##日期段
    t_list = [['2017-01-01','2023-01-01'],['2017-01-01','2023-01-01']]
    #遍历关键词
    for tag, t in zip(tags, t_list):
        start_date = t[0]
        end_date = t[1]
        date_list = pd.date_range(start_date, end_date, freq='D').values
        for k in range(len(date_list)-1):
            #获取第n天日期,爬取该日数据
            d1 = str(date_list[k])[:10]
            print(f'正在获取{tag},{d1}数据')
            #爬取每日的数据,每日最多1页,遍历每页
            for i in range(1, 2, 1):
                try:
                    #构建url
                    url = f'https://s.weibo.com/weibo?q={tag}&vip=1&xsort=hot&suball=1&timescope=custom%3A{d1}-0%3A{d1}-23&Refer=g&page={i}'
                    headers = {'cookie': cookie,
                               'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'}
                    res = requests.get(url=url, headers=headers, timeout=10)
                    soup = BeautifulSoup(res.text, 'html.parser')
                    html = etree.HTML(res.text)
                    try:
                        #如果页面出现'以下是您可能感兴趣的微博',代表没有数据了,跳出循环
                        if soup.find('div', attrs={'class': 'm-error'}).text == '以下是您可能感兴趣的微博':
                            break
                    except:
                        pass
                    #获取所有微博的id,根据id获取详情
                    div_list = html.xpath(
                        '//div[@action-type="feed_list_item"]')
                    #遍历id
                    for d in div_list:
                        #对链接切片,获取id
                        wb_id = ''.join(
                            d.xpath('.//div[@class="from"]/a/@href')[0]).split('/')[-1].split('?')[0]
                        try:
                            #获取微博信息详情
                            get_info(tag,wb_id)
                        except:
                            pass
                except:
                    pass


def get_info(tag,id):
    try:
        #访问详情接口,获得json
        url = f'https://weibo.com/ajax/statuses/show?id={id}'
        headers = {
            'cookie': cookie,
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36'}
        response = requests.get(url=url, headers=headers).json()
        #点赞
        attitudes_count = response['attitudes_count']
        #评论
        comments_count = response['comments_count']
        #转发
        reposts_count = response['reposts_count']

        #mid
        mid = response['mid']
        #微博英文id
        mblogid = response['mblogid']

        #日期
        created_at = response['created_at']
        #用户id
        uid = response['user']['id']
        #微博链接
        short_url = f'https://weibo.com/{uid}/{mblogid}'
        #昵称
        screen_name = response['user']['screen_name']
        #微博发布地
        try:
            region_name=response['region_name'].replace('发布于 ','')
        except:
            region_name =''
        #将日期转换格式
        comment_time = ' '.join(
            created_at.replace(' +0800', '').split(' ')[1:])
        comment_time = datetime.datetime.strptime(
            comment_time, '%b %d %H:%M:%S %Y')
        #获取微博原文,如是长文,获得长文,否获取文本
        url1 = f'https://weibo.com/ajax/statuses/longtext?id={id}'
        #博主链接
        user_link=f'https://weibo.com/u/{uid}'
        #图片数量
        pic_num=response['pic_num']
        #是否有视频
        video = '否'
        try:
            v_list=response['url_struct']
            for v in v_list:
                if 'video' in v['long_url']:
                    video = '是'
        except:
            video ='否'
        try:
            longTextContent = requests.get(url=url1, headers=headers).json()[
                'data']['longTextContent']
        except:
            longTextContent =response['text_raw']
        print(longTextContent)
        print(screen_name,longTextContent,uid,reposts_count, comments_count,attitudes_count,comment_time,region_name,short_url,pic_num, video,user_link)
        #将数据写入csv
        with open(f'{tag}.csv', 'a', encoding='utf-8', newline='') as f:
            w = csv.writer(f)
            t = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
            w.writerow([screen_name,longTextContent,uid,reposts_count, comments_count,attitudes_count,comment_time,region_name,short_url,pic_num, video,user_link,t])
    except:
        pass

if __name__ == '__main__':
    main()
