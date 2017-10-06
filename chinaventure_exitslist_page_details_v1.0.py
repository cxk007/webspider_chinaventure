# -*- coding: utf-8 -*-
import csv
import requests
import random
import threading
import simplejson
import multiprocessing

industry_list=['-1','00001','00002','00003','00004','00005','00006','00007','00008','00009','00010','00011','00012','00013',
               '00014','00015','00016','00017','00018','00019','00020','00021']
year_list=[ '-1','2016','2015','2014','2013','2012','2011','2010','2009','2008','2007','2006','2005','2004','2003','2002','2001','2000']
place_list=['-1','安徽','北京','重庆','福建','甘肃','广东','广西','贵州','海南','河北','河南','黑龙江','湖北','湖南','吉林','江苏','江西',
'内蒙古','四川','辽宁','宁夏','青海','山东','陕西','山西','上海','天津','西藏','新疆','云南','浙江']
money_list=['-1','1','2','3','4','5','6']
page_num_list=[1,2,3,4,5,6,7,8,9,10]

#http://www.chinaventure.com.cn/event/searchExitsList/-1/-1/-1/上海/-1/0-16.shtml
#第一个是行业字段，
#第二三个是起始和终止字段
#第四个是place
#第五个是钱的字段
#第六个是页码字段

##此网站的退出机构跟其他两种页面不一样，只有一个主体，
##机构字段是空的比如shortCnName和cnName，但是似乎id一定会有值，但是如果其他两个没有，那么值就是0

with open ('chinaventure_exits_list_page_details.csv','w',encoding='utf-8',newline="") as header_write:
    field_header=["url","id","title","happenedDate","exitsType","totalAmount","totalCurrencyType","amount",
                    "currencyType","returnRate","targetEnterprise_id","targetEnterprise_shortCnName",
                    "targetEnterprise_cnName","targetEnterprise_products","targetEnterprise_location",
                    "targetEnterprise_industry_id","targetEnterprise_industry_name","exitsInstitution_id",
                    "exitsInstitution_shortCnName","exitsInstitution_cnName","happenedDateStr","amountStr",
                    "totalAmountStr","exitsTypeStr"]
    f_writer = csv.writer(header_write,dialect='excel',delimiter='|')
    f_writer.writerow(field_header)
header_write.close()

with open('chinaventure_exits_list_error_.csv','w',encoding='utf-8',newline='') as error_csv:
    error_writer = csv.writer(error_csv,dialect='excel',delimiter = '|')
    error_writer.writerow(['url','error_message'])
error_csv.close()

with open ('chinaventure_Exits_list_url.csv','w',encoding='utf-8',newline="") as url_header_write:
    f_writer = csv.writer(url_header_write,dialect='excel')
    f_writer.writerow(['url'])
url_header_write.close()

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
            with open('chinaventure_exits_list_error_.csv','a',encoding='utf-8',newline='') as error_csv:
                error_writer = csv.writer(error_csv,dialect='excel',delimiter = '|')
                error_writer.writerow([url,e])
            error_csv.close()
            return''


def cnventure_page_details_get(url_target_part):
    for page_num in page_num_list:
        url_target = url_target_part + str((page_num - 1) * 15) + '-16.shtml'
        print(url_target)
        try:
            json_data = simplejson.loads(post_data_to_main_page(url_target))
        except Exception as e:
            print('you need to jump in %s'% url_target,e)
            with open('chinaventure_exits_list_error_.csv','a',encoding='utf-8',newline='') as error_csv:
                error_writer = csv.writer(error_csv,dialect='excel',delimiter = '|')
                error_writer.writerow([url_target,e])
            error_csv.close()
            continue
        raw_data = json_data['data']
        if raw_data == []:
            print('no content here in %s' % url_target)
            return
        else:
            pass
        for company_item in raw_data:
            try:
                id=company_item['id']
                title=company_item['title']
                happenedDate=company_item['happenedDate']
                exitsType=company_item['exitsType']
                totalAmount=company_item['totalAmount']
                totalCurrencyType=company_item['totalCurrencyType']
                amount=company_item['amount']
                currencyType=company_item['currencyType']
                returnRate=company_item['returnRate']
                targetEnterprise_id=company_item['targetEnterprise']['id']
                targetEnterprise_shortCnName=company_item['targetEnterprise']['shortCnName']
                targetEnterprise_cnName=company_item['targetEnterprise']['cnName']
                targetEnterprise_products=company_item['targetEnterprise']['products']
                targetEnterprise_location=company_item['targetEnterprise']['location']
                targetEnterprise_industry_id=company_item['targetEnterprise']['industry']['id']
                targetEnterprise_industry_name=company_item['targetEnterprise']['industry']['name']
                try:
                    exitsInstitution_id=company_item['exitsInstitution']['id']
                except:
                    exitsInstitution_id=''
                if exitsInstitution_id==0:
                    exitsInstitution_id=''
                try:
                    exitsInstitution_shortCnName=company_item['exitsInstitution']['shortCnName']
                except:
                    exitsInstitution_shortCnName=''
                try:
                    exitsInstitution_cnName=company_item['exitsInstitution']['cnName']
                except:
                    exitsInstitution_cnName=''
                happenedDateStr=company_item['happenedDateStr']
                amountStr=company_item['amountStr']
                totalAmountStr=company_item['totalAmountStr']
                exitsTypeStr=company_item['exitsTypeStr']
                with open ('chinaventure_exits_list_page_details.csv','a',encoding='utf-8',newline="") as data_write:
                    data_writer = csv.writer(data_write,dialect='excel',delimiter = '|')
                    white_item=[url_target,id,title,happenedDate,exitsType,totalAmount,totalCurrencyType,amount,
                                currencyType,returnRate,targetEnterprise_id,targetEnterprise_shortCnName,
                                targetEnterprise_cnName,targetEnterprise_products,targetEnterprise_location,
                                targetEnterprise_industry_id,targetEnterprise_industry_name,exitsInstitution_id,
                                exitsInstitution_shortCnName,exitsInstitution_cnName,happenedDateStr,amountStr,
                                totalAmountStr,exitsTypeStr]

                    data_writer.writerow(white_item)
                data_write.close()
            except Exception as e:
                print('some data columns have problems',e)
                #即使一个链接的某一条记录出了问题，那么整条链接都会被记录下来
                with open('chinaventure_exits_list_error_.csv','a',encoding='utf-8',newline='') as error_csv:
                    error_writer = csv.writer(error_csv,dialect='excel',delimiter = '|')
                    error_writer.writerow([url_target,e])
                error_csv.close()
                pass

def cnventure_error_url_reget(url_target):
    try:
        json_data = simplejson.loads(post_data_to_main_page(url_target))
    except Exception as e:
        print('you need to jump in %s'% url_target,e)
        return
    raw_data = json_data['data']
    if raw_data == []:
        print('no content here in %s' % url_target)
        return
    else:
        pass
    for company_item in raw_data:
        try:
            id=company_item['id']
            title=company_item['title']
            happenedDate=company_item['happenedDate']
            exitsType=company_item['exitsType']
            totalAmount=company_item['totalAmount']
            totalCurrencyType=company_item['totalCurrencyType']
            amount=company_item['amount']
            currencyType=company_item['currencyType']
            returnRate=company_item['returnRate']
            targetEnterprise_id=company_item['targetEnterprise']['id']
            targetEnterprise_shortCnName=company_item['targetEnterprise']['shortCnName']
            targetEnterprise_cnName=company_item['targetEnterprise']['cnName']
            targetEnterprise_products=company_item['targetEnterprise']['products']
            targetEnterprise_location=company_item['targetEnterprise']['location']
            targetEnterprise_industry_id=company_item['targetEnterprise']['industry']['id']
            targetEnterprise_industry_name=company_item['targetEnterprise']['industry']['name']
            try:
                exitsInstitution_id=company_item['exitsInstitution']['id']
            except:
                exitsInstitution_id=''
            if exitsInstitution_id==0:
                exitsInstitution_id=''
            try:
                exitsInstitution_shortCnName=company_item['exitsInstitution']['shortCnName']
            except:
                exitsInstitution_shortCnName=''
            try:
                exitsInstitution_cnName=company_item['exitsInstitution']['cnName']
            except:
                exitsInstitution_cnName=''
            happenedDateStr=company_item['happenedDateStr']
            amountStr=company_item['amountStr']
            totalAmountStr=company_item['totalAmountStr']
            exitsTypeStr=company_item['exitsTypeStr']
            white_item=[url_target,id,title,happenedDate,exitsType,totalAmount,totalCurrencyType,amount,
                            currencyType,returnRate,targetEnterprise_id,targetEnterprise_shortCnName,
                            targetEnterprise_cnName,targetEnterprise_products,targetEnterprise_location,
                            targetEnterprise_industry_id,targetEnterprise_industry_name,exitsInstitution_id,
                            exitsInstitution_shortCnName,exitsInstitution_cnName,happenedDateStr,amountStr,
                            totalAmountStr,exitsTypeStr]
            print(white_item)
            with open ('chinaventure_exits_list_page_details.csv','a',encoding='utf-8',newline="") as data_write:
                data_writer = csv.writer(data_write,dialect='excel',delimiter = '|')
                white_item=[url_target,id,title,happenedDate,exitsType,totalAmount,totalCurrencyType,amount,
                            currencyType,returnRate,targetEnterprise_id,targetEnterprise_shortCnName,
                            targetEnterprise_cnName,targetEnterprise_products,targetEnterprise_location,
                            targetEnterprise_industry_id,targetEnterprise_industry_name,exitsInstitution_id,
                            exitsInstitution_shortCnName,exitsInstitution_cnName,happenedDateStr,amountStr,
                            totalAmountStr,exitsTypeStr]
                data_writer.writerow(white_item)
            data_write.close()
        except Exception as e:
            print('some data columns have problems',e)
            with open('chinaventure_exits_list_error_again.csv','a',encoding='utf-8',newline='') as error_csv:
                error_writer = csv.writer(error_csv,dialect='excel',delimiter = '|')
                error_writer.writerow([url_target,e])
            error_csv.close()
            pass

if __name__ == "__main__":
    j = 0
    for year in year_list:
        for industry in industry_list:
            for place in place_list:
                for money in money_list:
                    url = 'http://www.chinaventure.com.cn/event/searchExitsList' + '/'+ industry + '/' + year + '/' + year + '/' + place + '/' + money + '/'
                    j+=1
                    with open ('chinaventure_Exits_list_url.csv','a',encoding='utf-8',newline="") as header_write:
                        f_writer = csv.writer(header_write,dialect='excel')
                        f_writer.writerow([url])
    print('total url list is %d'%j)
    header_write.close()

    pool = multiprocessing.Pool(processes=4)
    i = 0
    with open ('chinaventure_Exits_list_url.csv','r',encoding='utf-8',newline='') as csvfile:
        f_reader = csv.DictReader(csvfile)
        for url_item in f_reader:
            url_target_part = url_item['url']
            print('url 链接是：',url_target_part)
            pool.apply_async(cnventure_page_details_get,args=(url_target_part,))
            # cnventure_page_details_get(url_target_part)
            i += 1
    pool.close()
    pool.join()

    csvfile.close()

    with open('chinaventure_exits_list_error_.csv','r',encoding='utf-8',newline='') as error_read_csv:
        error_writer = csv.DictReader(error_read_csv)
        for error_url in error_writer:
            target_url=error_url['url']
            print(target_url)
            cnventure_error_url_reget(target_url)
            # pool.apply_async(cnventure_error_url_reget,args=(target_url,))
    error_read_csv.close()

    # pool.close()
    # pool.join()