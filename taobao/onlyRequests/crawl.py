import re,requests,json,gevent
import pymongo,redis
import urllib.parse
import time
from pyquery import PyQuery as pq
from gevent import monkey
from settings import *

#把IO设为非阻塞
monkey.patch_all()
#建立一个redis连接池
pool = redis.ConnectionPool(host=REDIS_URI, port=REDIS_PORT,password = REDIS_PASSWD)


headers = {
    'authority':'s.taobao.com',
    'method':'GET',
    'path':'/search?q=%E6%AF%9B%E6%AF%AF&imgfile=&commend=all&ssid=s5-e&search_type=item&sourceId=tb.index&spm=a21bo.2017.201856-taobao-item.1&ie=utf8&initiative_id=tbindexz_20170306',
    'scheme':'https',
    'accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'accept-encoding':'gzip, deflate, br',
    'accept-language':'zh-CN,zh;q=0.9',
    'cache-control':'max-age=0',
    'referer':'https://www.taobao.com/',
    'upgrade-insecure-requests':'1',
    'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36'
}

def get_id(start_url,key_word,page):
    print('getting id')
    url = start_url = start_url.format(keyword = key_word,page = page)
    print(url)
    response = requests.get(url, headers=headers)
    if response.status_code ==200:
        html = response.text
        print(response.status_code)
        #采用正则解析每个网页中的所商品信息
        titles = re.findall('raw_title":"(.*?)"',html)
        detail_urls = re.findall('detail_url":"(.*?)"',html)
        price = re.findall('view_price":"(.*?)"',html)
        location = re.findall('item_loc":"(.*?)"',html)
        sales = re.findall('view_sales":"(.*?)人',html)
        comment_count = re.findall('comment_count":"(.*?)"',html)
        #判断获取到不同信息的数量是否一致，如果相同，则信息正确，再用zip把商品信息组合起来；如果不同，则可能出错，返回空
        if len(titles)==len(detail_urls)==len(price)==len(location)==len(sales)==len(comment_count):
            print('length of titles',len(titles))
            results = zip(titles,detail_urls,price,location,sales,comment_count)
            return results
        else:
            print('该页面抓取到的item数目不一致',url)
            return None
    else:
        print('error',response.status_code)
        return None

def parse_details(result):
    #解析再起始页中每个商品的信息
    item1 = {}
    title, detail_url, price, area, sales, comment_count = result
    item1['title'] = title
    item1['price'] = price
    item1['area'] = area
    item1['receiveGoodsCount'] = sales
    item1['comment_count'] = comment_count
    print('crawling detail_url',detail_url)
    # 如果该商品是淘宝上的商品，则到淘宝的天猫详情页获取信息
    if str(detail_url).startswith('//detail.tmall.com'):
        id = detail_url.split('\\')[1][5:]
        if check_repetition(id):
            item1['id'] = id
            detail_item = parse_tmall(id)
            print('tmall_detail_item',detail_item)
            if detail_item:
                item1.update(detail_item)
                print('test1')
                return item1
        else:
            print('重复的id',id)
        return None

    #如果该商品是淘宝上的商品，则到淘宝的商品详情页获取信息
    elif str(detail_url).startswith('//item.taobao.com'):
        id = detail_url.split('\\')[1][5:]
        if check_repetition(id):
            item1['id'] = id
            detail_item = parse_taobao(id)
            print('taobao_detail_item',detail_item)
            item1.update(detail_item)
            return item1
        else:
            print('重复的id',id)
        return None


def parse_taobao(id):
    # 通过id来获取淘宝详情页的商品信息
    item = {}
    url = 'https://item.taobao.com/item.htm?id={}'
    html = requests.get(url.format(id), headers=headers).text
    doc = pq(html)
    item['shop'] = doc(
        '#J_ShopInfo > div.tb-shop-info-wrap > div.tb-shop-info-hd > div.tb-shop-name > dl > dd > strong > a').text()
    item['shop_description'] = doc(
        '#J_ShopInfo > div.tb-shop-info-wrap > div.tb-shop-info-bd > div > dl:nth-child(1) > dd > a').text()
    item['shop_service'] = doc(
        '#J_ShopInfo > div.tb-shop-info-wrap > div.tb-shop-info-bd > div > dl:nth-child(2) > dd > a').text()
    item['shop_transportion'] = doc(
        '#J_ShopInfo > div.tb-shop-info-wrap > div.tb-shop-info-bd > div > dl:nth-child(3) > dd > a').text()
    return item

def parse_tmall(id):
    # 通过id来获取天猫详情页的商品信息
    item = {}
    url = 'https://detail.tmall.com/item.htm?&id={}'.format(id)
    html = requests.get(url, headers=headers).text
    doc = pq(html)
    try:
        item['shop'] = doc('.slogo .slogo-shopname').text()
        item['shop_description'] = doc('.main-info .shopdsr-score').text().split()[0]
        item['shop_service'] = doc('.main-info .shopdsr-score').text().split()[1]
        item['shop_transportion'] = doc('.main-info .shopdsr-score').text().split()[2]
        return item
    except IndexError:
        pass
        return None


def save_to_mongo(item):
    #报存到mongo
    client = pymongo.MongoClient(MONGO_URI,password= MONGO_PASSWD)
    db = client[MONGO_DB]
    if db[MONGO_TB].insert(item):
        print('saved to mongo:',item)
    client.close()

def check_repetition(id):
    #通过redis的set来保存商品id号，b并以添加的返回结果来去重，也方便扩展成分布式
    conn = redis.Redis(connection_pool=pool)
    return conn.sadd(SET_KEY,id)

def main(start_url):
    # 把keyword编译为url中的编码
    key_word = urllib.parse.quote(KEYWORD)
    for page in range(100):
        #通过不同的start_url来采集不同的排序方式的商品
        results = get_id(start_url,key_word,page * 44)
        # print(results)
        if results:
            for result in results:
                item = parse_details(result)
                if item:
                    # print(item)
                    save_to_mongo(item)
                else:
                    print('error,item is None')
        else:
            print('error,results is None')



if __name__ == '__main__':
        ##采集按不同的排序方式的前100页的商品，通过协程来加快采集速度
    gevent.joinall(
        [gevent.spawn(main,'https://s.taobao.com/search?q={keyword}&sort=sale-desc&s={page}'),
         gevent.spawn(main,'https://s.taobao.com/search?q={keyword}&sort=renqi-desc&s={page}'),
         gevent.spawn(main,'https://s.taobao.com/search?q={keyword}&sort=default&s={page}'),
         gevent.spawn(main, 'https://s.taobao.com/search?q={keyword}&sort=credit-desc&s={page}'),
         gevent.spawn(main, 'https://s.taobao.com/search?q={keyword}&sort=price-asct&s={page}'),
         gevent.spawn(main, 'https://s.taobao.com/search?q={keyword}&sort=price-desc&s={page}'),
         gevent.spawn(main, 'https://s.taobao.com/search?q={keyword}&sort=total-asc&s={page}'),
         gevent.spawn(main, 'https://s.taobao.com/search?q={keyword}&sort=total-desc&s={page}')
        ],
    )