import requests 
from fake_useragent import UserAgent
from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd
import re
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import Select
import time
import os
from random import randint
import ast
from multiprocessing import Pool
from functools import partial
import concurrent.futures

start_time = time.time()
print('start')
    
result = ['Название запчасти:', 'Фирма-производитель:', 'Модель:', 'Номер кузова:', 'Номер оптики:', 'Цена:',
         'Номер двигателя:', 'Расположение:', 'Примечание:']


url = 'https://autojapan.japancar.ru/?code=parts&mode=old'
simple_url = 'https://autojapan.japancar.ru'

def subprice(price):
    if ('указана' in price) or ('не' in price):
        print(('значение цены не float, а:', price))
        return price
    price = float(re.sub(' ', '', price))
    if price < 200:
        return price + 300
    elif 200 <= price < 1000:
        return price + 400
    elif 1000 <= price < 11000:
        return price * 1.4
    elif 11000 <= price < 20000:
        return price * 1.3
    elif 20000 <= price < 30000:
        return price * 1.25
    else:
        return price * 1.2

def get_detail(url, split_date = None):
    page = requests.get(url, headers = {'User-Agent': UserAgent().random}, timeout = 6)
    soup = BeautifulSoup(page.content, 'html.parser')
    if split_date is not None:
        date = re.findall('\d{2}.\d{2}.\d{2}', soup.find('span', attrs = ('base_small')).text)[0]
        date = datetime.strptime(date, '%d.%m.%y')
        if (split_date - date).days > 0:
            return 0
    name = soup.findAll('td', attrs = {'class': 'table_row_dark'})
    value = soup.findAll('td', attrs = {'class': 'table_row_light'})
    img = soup.findAll('img')
    table = [x for x in [(name.text, value.text) for name, value in zip(name, value)] if x[0] in result]
    table = dict(table)
    table.update({'Фотография:': ', '.join([im.get('src') for im in img])})
    table['Цена:'] = subprice(table['Цена:'])
    table1 = {c: '' for c in [col for col in result if col not in list(table.keys())]} 
    table.update(table1)
    return table


def post_list(url, marka, model = None, page = None):
    if marka is not None:
        url += '&cl_marka={}'.format(marka)
    if model is not None:
        url += '&cl_model={}'.format(model)
    if page is not None:
        url += '&page={}'.format(page)
    page = requests.get(url, headers = {'User-Agent': UserAgent().random}, timeout = 6)
    soup = BeautifulSoup(page.content, 'html.parser')
    return set([href.get('href') for href in soup.findAll('a', attrs = {'class': 'mini'})])


def get_marks(url):
    stop_words = ['выбрать', '<выбрать>', '-', '']
    page = requests.get(url, headers = {'User-Agent': UserAgent().random}, timeout = 6)
    soup = BeautifulSoup(page.content, 'html.parser')
    return list([x for x in [elem.text for elem in soup.find('select', attrs = {'id': 'f_firm'}).findAll('option')] if x not in stop_words])


def get_models(url, marka, driver):
    stop_words = ['выбрать', '<выбрать>', '-', '']
    driver.get(url)
    select = Select(driver.find_element_by_xpath("//select[@id='f_firm']"))
    select.select_by_visible_text(marka)
    time.sleep(10)
    select = Select(driver.find_element_by_xpath("//select[@id='div_mark_combo']"))
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    return list([x for x in [elem.text for elem in soup.find('select', attrs = {'id': 'div_mark_combo'}).findAll('option')] if x not in stop_words])


def change_excel(data):
    if len(data) == 0:
        return data
    def search(s):
        global idx
        if not isinstance(s, str):
            return s
        if re.search('склад', s.lower()):
            s = s[: re.search('склад', s.lower()).span(0)[0]]
        if re.search('самовывоз', s.lower()):
            s = s[: re.search('самовывоз', s.lower()).span(0)[0]:]
        if s != '':
            s += '.'
        ind = randint(10 ** 8, 10 ** 9 - 1)
        while ind in idx:
            ind = randint(10 ** 8, 10 ** 9 - 1)
        idx.append(ind)
        return s + 'A{}'.format(ind)
        
    data = data.rename(columns = {col: re.sub(':', '', col) for col in data.columns})
    data['Примечание'] = data['Примечание'].apply(lambda s: search(s))
    data = data[['Название запчасти', 'Фирма-производитель', 'Модель', 'Номер кузова', 'Номер оптики',  'Номер двигателя', 'Расположение', 'Примечание', 'Фотография', 'Цена']]
    return data

def save_excel(data, name):
    writer = pd.ExcelWriter(name, engine='xlsxwriter',options = {'strings_to_urls': False})
    data.to_excel(writer, index = False)
    writer.close()


def read_excel(path, sheet_name):
    data = pd.read_excel(path, sheet_name = sheet_name)
    return data


def parse_all(marks, models, num_page, date = None):
    data = pd.DataFrame()
    for marka in marks:
        for model in models.get(marka):
            i = 1
            while i < num_page:
                print(('-------', marka, model, i))
                try:
                    posts = post_list(url, marka = marka, model = model, page = i)
                    if len(posts) == 0:
                        break
                except (requests.exceptions.Timeout, requests.exceptions.ConnectionError, requests.exceptions.RetryError) as exp:
                    print('sleep 80')
                    time.sleep(80)
                    posts = post_list(url, marka = marka, model = model, page = i)
                t = True
                while t:
                    try:
                        #with Pool(processes = 2) as p:
                        #    detail = p.map(partial(get_detail, split_date = date), [simple_url + url for url in posts])
                        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                            detail = executor.map(partial(get_detail, split_date = date), [simple_url + url for url in posts])
                        t = False
                        data = data.append([d for d in detail if d != 0], ignore_index = True)
                    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError, requests.exceptions.RetryError) as exp:
                        print('sleep 50')
                        time.sleep(50)
                i += 1
    data = change_excel(data)
    print((data.shape))
    return data


if __name__ == '__main__':
    with open('settings.spec', 'r') as f:
	    mode = ast.literal_eval(f.read())
	
    print(('mode: ', mode))
    if len(mode['marks']) != 0:
        marks = mode['marks']
        models = mode['models']
    else:
        driver = webdriver.PhantomJS()
        marks = get_marks(url)
        models = dict()
        for marka in marks:
            models[marka] = get_models(url, marka, driver)
        driver.quit()

    page = int(mode['page'])
    if mode['date'] != '':
	    date = datetime.strptime(mode['date'], '%d.%m.%y')
    else:
	    date = None

	
    print('-------------------')
    print(marks)
    print('-------------------')
    print(models)
    print('-------------------')

    if mode['mode'] == 'all':
	    data = pd.DataFrame()
	    idx = []
	    data = data.append(parse_all(marks, models, page, date), ignore_index = True)
    else:
	    data = read_excel(mode['load_path'], mode['sheet_name'])
	    idx = [int(s[-9:]) for s in data['Примечание']]
	    data = data.append(parse_all(marks, models, page, date), ignore_index = True)

    save_excel(data, mode['name'])
    print(("%s секунд" % (time.time() - start_time)))
    eval(input('нажмите enter чтобы закончить'))
