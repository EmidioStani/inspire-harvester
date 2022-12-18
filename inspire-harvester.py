import json
import os
import re
import socket
import ssl
import sys
import threading
import time
# import urllib.request
from datetime import datetime
from http.client import IncompleteRead
from multiprocessing.pool import Pool
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import Request, urlopen

import requests
import yaml
from bs4 import BeautifulSoup
from requests.utils import requote_uri
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait
from tqdm import tqdm


def get_config():
    my_path = Path(__file__).resolve()  # resolve to get rid of any symlinks
    config_path = my_path.parent / 'config.yaml'
    with config_path.open() as config_file:
        config = yaml.load(config_file, Loader=yaml.FullLoader)
    return config

def harvester(config):
    start_time = datetime.now()

    dir_path = os.path.dirname(os.path.realpath(__file__)).replace("\\", "/")
    config = get_config()
    portal = config['portalall']
    # print(portal)

    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    browser = webdriver.Chrome(options=options)

    browser.get(portal)
    timeouttable = config['selenium']['timeouttable']
    table = config['selenium']['selectors']['table']
    timeoutmessage = config['selenium']['timeoutmessage']
    metadata_pages = []
    try:
        WebDriverWait(browser, timeouttable).until(
            EC.visibility_of_element_located((By.CSS_SELECTOR, table))

        )
    except TimeoutException:
        print(timeoutmessage)
    
    # rows = browser.find_elements(By.XPATH, "//table[@id='resultsDataTable']/tbody/tr")
    # len method is used to get the size of that list
    # print(len(rows))
    select = Select(browser.find_element(By.CSS_SELECTOR, config['selenium']['selectors']['select']))
    select.select_by_value(config['selenium']['datasetperpage'])
    time.sleep(config['selenium']['timeoutrefresh'])
    mytable = browser.find_element(By.CSS_SELECTOR, table)
    total = config['selenium']['selectors']['total']
    total_pages = int(browser.find_element(By.XPATH, total).text)
    # print(total_pages)
    link_selector = config['selenium']['selectors']['metadatapage']
    tablerow = config['selenium']['selectors']['tablerow']
    metadatapagehref = config['selenium']['selectors']['metadatapagehref']
    # print(link_selector)
    for row in mytable.find_elements(By.CSS_SELECTOR, tablerow):
        for metadatalink in row.find_elements(By.CSS_SELECTOR, link_selector):
            link = metadatalink.get_attribute(metadatapagehref)
            # print(link)
            metadata_pages.append(link)
            # print(len(metadata_pages))
    next_page_selector = config['selenium']['selectors']['nextpagebutton']
    

    total_pages = total_pages - 1
    print(total_pages)
    file_bar = tqdm(range(total_pages), desc=config['listbar']['description'], colour=config['listbar']['colour'], leave=config['listbar']['leave'])
    for i in file_bar:
        #while next_page_button:
        next_page_button = browser.find_element(By.CSS_SELECTOR, next_page_selector)
        next_page_button.click()
        time.sleep(config['selenium']['timeoutnextpage'])
        mytable = browser.find_element(By.CSS_SELECTOR, table)
        for row in mytable.find_elements(By.CSS_SELECTOR, tablerow):
            for metadatalink in row.find_elements(By.CSS_SELECTOR, link_selector):
                link = metadatalink.get_attribute(metadatapagehref)
                # print(link)
                metadata_pages.append(link)
                # print(len(metadata_pages))
        # try:
        #     next_page_button = browser.find_element(By.CSS_SELECTOR, next_page_selector)
        # except NoSuchElementException:
        #    print("no next button")
        #break

    browser.close()
    savetofile = config['output']['list']
    with open(savetofile, 'w') as f:
        for item in metadata_pages:
            f.write("%s\n" % item)
        # json.dump(metadata_pages, filehandle)

def downloader(metadata_pages):
    config = get_config()
    metadata_download_selector = config['selenium']['selectors']['metadatadownload']
    metadata_country_selector = config['selenium']['selectors']['metadatacountry']
    timeoutwaitfordownload = config['selenium']['timeoutwaitfordownload']
    timeoutmessage = config['selenium']['timeoutmessage']
    dir_path = os.path.dirname(os.path.realpath(__file__)).replace("\\", "/")

    options = webdriver.ChromeOptions()
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    # options.add_argument("--headless")
    browser = webdriver.Chrome(options=options)

    file_bar = tqdm(metadata_pages, desc=config['bar']['description'], colour=config['bar']['colour'], leave=config['bar']['leave'])
    for index, metadata_page in enumerate(file_bar):
        browser.get(metadata_page)
        time.sleep(timeoutwaitfordownload)
        try:
            WebDriverWait(browser, timeoutwaitfordownload).until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, metadata_download_selector))

            )
        except TimeoutException:
            print(timeoutmessage)

        
        metadata_link = browser.find_element(By.CSS_SELECTOR, metadata_download_selector)
        metadata_href = metadata_link.get_attribute(config['selenium']['selectors']['metadatapagedownloadhref'])
        metadata_country = browser.find_element(By.CSS_SELECTOR, metadata_country_selector)
        # print("download:" + metadata_href)
        response = requests.get(metadata_href)

        file_name = str(index) + "-" + metadata_country.text + ".xml"
        completeName = os.path.join(dir_path + "/" + config['output']['folder'], file_name)         
        file = open(completeName, "wb")
        file.write(response.content)
        file.close()
        # time.sleep(config['selenium']['timeoutnextrequest'])

    #to close the browser
    browser.close()

def compare(listfile, donefile, todofile, errorfile):
    '''
    with open(listfile,'r') as f:
        # listlines = set(f.readlines())
        listlines = json.load(f)
        # listlines = set(listlines)
        # print("list len: " + str(len(listlines)))
    '''
    listlines = open(listfile,'r').read().splitlines()

    donelines = open(donefile,'r').read().splitlines()
    # print("done len: " + str(len(donelines)))

    errorlines = open(errorfile,'r').read().splitlines()

    open(todofile,'w').close()

    with open(todofile,'a') as f:
        # todolist = list(listlines - donelines)
        todolist = [x for x in listlines if ((x not in donelines) and (x not in errorlines))]
        # print("todo len: " + str(len(todolist)))
        for todoline in todolist:
            f.write(todoline +'\n')


def getdriver():
    options = webdriver.ChromeOptions()
    # options.add_experimental_option('excludeSwitches', ['enable-logging'])
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    browser = webdriver.Chrome(options=options)
    # browser.set_page_load_timeout (10)
    # browser.set_script_timeout (10) #Both settings are effective
    return browser

def scraper(metadata_page):
    config = get_config()
    metadata_download_selector = config['selenium']['selectors']['metadatadownload']
    metadata_country_selector = config['selenium']['selectors']['metadatacountry']
    timeoutwaitfordownload = config['selenium']['timeoutwaitfordownload']
    timeoutmessage = config['selenium']['timeoutmessage']
    dir_path = os.path.dirname(os.path.realpath(__file__)).replace("\\", "/")

    while os.path.exists("pause.txt"):
        time.sleep(10) 
    browser = getdriver()
    # print("opening page:" + metadata_page)
    try: 
        browser.get(metadata_page)
    except TimeoutException:
        print(timeoutmessage)
        file = open("error.txt", "a")
        file.write(metadata_page + "\n")
        file.close()
        return
    time.sleep(timeoutwaitfordownload)
    try:
        WebDriverWait(browser, timeoutwaitfordownload).until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, metadata_download_selector))

            )
    except TimeoutException:
        # print(timeoutmessage)
        file = open("error.txt", "a")
        file.write(metadata_page + "\n")
        file.close()
        return
        # browser.close()

    # print("got the page")
    metadata_link = browser.find_element(By.CSS_SELECTOR, metadata_download_selector)
    metadata_href = metadata_link.get_attribute(config['selenium']['selectors']['metadatapagedownloadhref'])
    metadata_country = browser.find_element(By.CSS_SELECTOR, metadata_country_selector)
    # print("download:" + metadata_href)
    # print("got the link")
    try:
        splitted = metadata_href.split('/')
        #print(splitted)
        index = splitted[-2].replace("-", "_") + '_'+ splitted[-1].replace(".xml", "")
        file_name = str(index) + "-" + metadata_country.text + ".xml"
        completeName = os.path.join(dir_path + "/" + config['output']['folder'], file_name)       

        if not (os.path.exists(completeName)):
            try:
                #print("downloading")
                response = requests.get(metadata_href)     
                file = open(completeName, "wb")
                file.write(response.content)
                file.close()
                # time.sleep(config['selenium']['timeoutnextrequest'])
                file = open("done.txt", "a")
                file.write(metadata_page + "\n")
                file.close()
            except:
                file = open("error.txt", "a")
                file.write(metadata_page + "\n")
                file.close()
                pass
        else:
            file = open("done.txt", "a")
            file.write(metadata_page + "\n")
            file.close()
    except:
        file = open("error.txt", "a")
        file.write(metadata_page + "\n")
        file.close()
        pass
    #to close the browser
    # print("closing")
    browser.close()

    # downloader(config)

def multip():
    
    config = get_config()
    # harvester(config)
    compare("list2.json","done.txt","todo.txt", "error.txt")
    filetoread = "todo.txt"
    # filetoread = config['output']['list']

    metadata_pages = open(filetoread,'r').read().splitlines()

    # with open(filetoread) as filehandle:
        # metadata_pages = json.load(filehandle)
    #    metadata_pages=set(filehandle.readlines())
        # print(len(metadata_pages))
    # harvester(config)
    # downloader(metadata_pages)
    # time.sleep(20)
    
    with Pool(processes=8) as pool, tqdm(total=len(metadata_pages), desc=config['listbar']['description'], colour=config['listbar']['colour'], leave=config['listbar']['leave']) as pbar: # create Pool of processes (only 2 in this example) and tqdm Progress bar                                                     # into this list I will store the urls returned from parse() function
        for data in pool.imap_unordered(scraper, metadata_pages):                   # send urls from all_urls list to parse() function (it will be done concurently in process pool). The results returned will be unordered (returned when they are available, without waiting for other processes)
            pbar.update() 
    
    pool.close()
    pool.join()                                                  
    '''
    pbar = tqdm.tqdm(metadata_pages, desc=config['listbar']['description'], colour=config['listbar']['colour'], leave=config['listbar']['leave'])
    for metadata_page in pbar:
        try:
            scraper(metadata_page)
        except :
            continue
    '''
    #
    # print(all_data)
    
if __name__ == '__main__':
     multip()
