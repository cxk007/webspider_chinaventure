# -*- coding: utf-8 -*-
import csv
import requests
import random
import threading
import simplejson
import multiprocessing

industry_list=['-1','00001','00002','00003','00004','00005','00006','00007','00008','00009','00010','00011','00012','00013',
               '00014','00015','00016','00017','00018','00019','00020','00021']
stage_list=['-1','0','1','2','3','4','5','6','7','8','9']
year_list=['-1','2016','2015','2014','2013','2012','2011','2010','2009','2008','2007','2006','2005','2004','2003','2002','2001','2000']
place_list=['-1','安徽','北京','重庆','福建','甘肃','广东','广西','贵州','海南','河北','河南','黑龙江','湖北','湖南','吉林','江苏','江西',
'内蒙古','四川','辽宁','宁夏','青海','山东','陕西','山西','上海','天津','西藏','新疆','云南','浙江']
money_list=['-1','1','2','3','4','5','6']
page_num_list=[1,2,3,4,5,6,7,8,9,10]


with open ('chinaadventure_page_details_v4.csv','w',encoding='utf-8',newline="") as header_write:
    field_header=['url','stork_right','amount','happenedDate','industry_id','industry_name','cnName','targetEnterprise_id','shortCnName','products','location','currencyType',
                            'happenedDateStr','estimatedType','estimated','investRound','title',
                                  'amountStr','id','enterpriseVal','investRoundStr',
                                'institutions_id','institutions_shortCnName','institutions_cnName']
    f_writer = csv.writer(header_write,dialect='excel',delimiter='|')
    f_writer.writerow(field_header)

browser_user_agent_list = [
'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:47.0) Gecko/20100101 Firefox/47.0 ',
'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:47.0) Gecko/20100101 Firefox/47.0',
'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36',
'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10; rv:33.0) Gecko/20100101 Firefox/33.0',
'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36 ',
'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1667.0 Safari/537.36',
'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET4.0C; .NET4.0E) ',
'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:47.0) Gecko/20100101 Firefox/47.0',
'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.124 Safari/537.36 ',
'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36',
'Mozilla/5.0 (Linux; U; Android 4.1.1; zh-cn; vivo X1 Build/JRO03C) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30 MicroMessenger/4.3.219 ',
'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.152 Safari/537.36',
'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1667.0 Safari/537.36',
'Mozilla/5.0 (iPhone; CPU iPhone OS 8_3 like Mac OS X) AppleWebKit/600.1.4 (KHTML, like Gecko) Mobile/12F70 MicroMessenger/6.3.13 NetType/WIFI Language/zh_CN',
'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET4.0C; .NET4.0E) ',
'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0); 360Spider',
'Mozilla/5.0 (Linux; U; Android 4.1.1; zh-cn; TCL S820 Build/JRO03C) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.1 Mobile Safari/534.30 MicroMessenger/4.2.192',
'Mozilla/5.0 (Linux; U; Android 5.1.1; zh-CN; G750-T01 Build/LMY49G) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 UCBrowser/10.10.3.810 U3/0.8.0 Mobile Safari/534.30',
'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36 ',
'Mozilla/5.0 (Linux; U; Android 4.1.1; zh-cn; SCH-N719 Build/JRO03C) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30 MicroMessenger/5.0.2.352',
'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:46.0) Gecko/20100101 Firefox/46.0',
]

# generate random header
def gen_rand_header():
    rand_ua = random.choice(browser_user_agent_list)
    rand_header = {'User-Agent':rand_ua,
                   'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                   'Accept-Language':'en-US,en;q=0.5',
                   'Accept-Encoding':'gzip, deflate, sdch',
                   'Connection':'keep-alive',
                   'Referer':'http://www.kuaidaili.com/proxylist/1/'}
    return rand_header

def post_data_to_main_page(url,retry_num = 3):
    try:
        req=requests.session()
        req=req.request('get',url,gen_rand_header(),timeout=5)
        return(req.text)
    except Exception as e:
        if retry_num >0:
            print('retry num is %d' %(retry_num-1))
            return post_data_to_main_page(url,retry_num-1)
        else:
            print('connection issue',e)
            with open('chinaventure_error_v4.csv','a',encoding='utf-16',newline='') as error_csv:
                error_writer = csv.writer(error_csv,dialect='excel',delimiter = '|')
                error_writer.writerow([url,e])
            return''


def cnventure_page_details_get(url_target):
    try:
        json_data = simplejson.loads(post_data_to_main_page(url_target))

    except Exception as e:
        print('this url does not return anything',e)
        # continue
    raw_data = json_data['data']
    if raw_data == []:
        print('really nothing here')
        return
    for company_item in raw_data:
        try:
            stork_right=company_item['storkRight']
            amount=company_item['amount']
            happenedDate=company_item['happenedDate']
            industry_id=company_item['targetEnterprise']['industry']['id']
            industry_name=company_item['targetEnterprise']['industry']['name']
            cnName=company_item['targetEnterprise']['cnName']
            targetEnterprise_id=company_item['targetEnterprise']['id']
            shortCnName=company_item['targetEnterprise']['shortCnName']
            products=company_item['targetEnterprise']['products']
            location=company_item['targetEnterprise']['location']
            currencyType=company_item['currencyType']
            happenedDateStr=company_item['happenedDateStr']
            estimatedType=company_item['estimatedType']
            estimated=company_item['estimated']
            investRound=company_item['investRound']
            title=company_item['title']
            amountStr=company_item['amountStr']
            id=company_item['id']
            enterpriseVal=company_item['enterpriseVal']
            investRoundStr=company_item['investRoundStr']
            institutions=company_item['institutions']
            institutions_id=''
            institutions_shortCnName=''
            institutions_cnName=''
            for institutions_list in institutions:
                institutions_id=institutions_list['id']
                institutions_shortCnName=institutions_list['shortCnName']
                institutions_cnName=institutions_list['cnName']
                # duplicate_item=str(str(cnName)+str(institutions_cnName)+str(investRound))
                with open ('chinaadventure_page_details_v4.csv','a',encoding='utf-8',newline="") as data_write:
                    data_writer = csv.writer(data_write,dialect='excel',delimiter = '|')
                    white_item=[url_target,stork_right,amount,happenedDate,industry_id,industry_name,
                                cnName,targetEnterprise_id,shortCnName,products,location,currencyType,happenedDateStr,
                                estimatedType,estimated,investRound,title,amountStr,id,enterpriseVal,investRoundStr,
                                institutions_id,institutions_shortCnName,institutions_cnName]
                    data_writer.writerow(white_item)
                data_write.close()
        except Exception as e:
            print('出问题了',e)
            pass

if __name__ == "__main__":
    pool = multiprocessing.Pool(processes = 1)
    # j = 0
    # for year in year_list:
    #     for stage in stage_list:
    #         for industry in industry_list:
    #             for place in place_list:
    #                 for money in money_list:
    #                     url = 'http://www.chinaventure.com.cn/event/searchInvestList/' + industry + '/' + stage + '/' + year + '/' + year + '/' + place + '/' + money + '/'
    #                     j+=1
    #                     with open ('chinaadventure_url_v4.csv','a',encoding='utf-8',newline="") as header_write:
    #                         f_writer = csv.writer(header_write,dialect='excel')
    #                         f_writer.writerow([url])
    # print('total url list is %d'%j)
    # header_write.close()

    i = 0
    with open ('chinaadventure_url_v4.csv','r',encoding='utf-8',newline='') as csvfile:
        f_reader = csv.reader(csvfile)
        for url_item in f_reader:
            url_target = url_item[0].strip().replace('﻿','')
            i+=1
            # # print('there is only %d records left'%(j-i))
            # for page_num in page_num_list:
            # url_target = url_target_part + str((page_num - 1) * 15) + '-16.shtml'
            print(url_target)
            pool.apply_async(cnventure_page_details_get, args=(url_target,))
    pool.close()
    pool.join()

