# auth: c_tob

import requests
from requests import RequestException
import json
from bs4 import BeautifulSoup
import re
import pymongo
from config import *
import os
from hashlib import md5
from multiprocessing import Pool

client = pymongo.MongoClient(MONGO_URL, connect=False)
db = client[MONGO_DB]


def get_page_index(offset, KEYWORD):
    url = 'http://www.toutiao.com/search_content/'
    params = {
        'offset': offset,
        'format': 'json',
        'keyword': KEYWORD,
        'autoload': 'true',
        'count': 20,
        'cur_tab': 3
    }
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        r = requests.get(url, params=params, headers=headers)
        if r.status_code == 200: return r.text
        return None
    except RequestException:
        print('请求索引页出错')
        return None


def parse_page_index(html):
    data = json.loads(html)
    if data and 'data' in data.keys():
        for item in data.get('data'):
            yield item.get('article_url')


def get_page_detail(url):
    try:
        r = requests.get(url)
        if r.status_code == 200:
            return r.text
        return None
    except RequestException:
        print('请求详情页出错', url)
        return None


def parse_page_detail(html, url):
    soup = BeautifulSoup(html, 'lxml')
    title = soup.select('title')[0].get_text()
    print(title)
    pattern = re.compile('gallery: (.*?)siblingList', re.S)
    result = re.search(pattern, html)
    if result:
        results = result.group(1).strip()
        content = results.rstrip(',')
        data = json.loads(content)
        if data and 'sub_images' in data.keys():
            sub_images = data['sub_images']
            images = [item.get('url') for item in sub_images]
            for image in images: download_image(image, title)
            return {
                'title': title,
                'url': url,
                'url_list': images
                }


def download_image(url, title):
    print('正在下载', url)
    try:
        r = requests.get(url)
        if r.status_code == 200:
            save_image(r.content, title)
        return None
    except RequestException:
        print('请求图片出错', url)
        return None


def save_image(content, title):
    file_path = '{0}/{1}.{2}'.format(os.getcwd(), md5(content).hexdigest(), 'jpg')
    path = '{0}/{1}'.format(r'E:\my spider\jinritoutiao',title)
    if not os.path.exists(path):
        os.makedirs(path)
        os.chdir(path)
    if not os.path.exists(file_path):
        with open(file_path, 'wb') as f:
            f.write(content)


def save_to_mongo(result):
    if db[MONGO_TABLE].insert(result):
        print('存储到MongoDB成功', result)
        return True
    return False


def main(offset):
    html = get_page_index(offset, KEYWORD)
    for url in parse_page_index(html):
        detail_html = get_page_detail(url)
        if detail_html:
            result = parse_page_detail(detail_html, url)
            if result:
                save_to_mongo(result)


if __name__ == '__main__':
    groups = [i*20 for i in range(GROUP_START, GROUP_END+1)]
    pool = Pool()
    pool.map(main, groups)