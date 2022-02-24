import json
import os
import re
import socket
import ssl
import time
# import urllib.request
from datetime import datetime
from http.client import IncompleteRead
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import Request, urlopen

import requests
import yaml
from requests.utils import requote_uri
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait


def get_config():
    my_path = Path(__file__).resolve()  # resolve to get rid of any symlinks
    config_path = my_path.parent / 'config.yaml'
    with config_path.open() as config_file:
        config = yaml.load(config_file, Loader=yaml.FullLoader)
    return config

def harvester():
    start_time = datetime.now()

    dir_path = os.path.dirname(os.path.realpath(__file__)).replace("\\", "/")
    config = get_config()
    portal = config['portal']
    print(portal)

    options = webdriver.ChromeOptions()
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
    link_selector = config['selenium']['selectors']['metadatapage']
    tablerow = config['selenium']['selectors']['tablerow']
    metadatapagehref = config['selenium']['selectors']['metadatapagehref']
    print(link_selector)
    for row in mytable.find_elements(By.CSS_SELECTOR, tablerow):
        for metadatalink in row.find_elements(By.CSS_SELECTOR, link_selector):
            link = metadatalink.get_attribute(metadatapagehref)
            print(link)
            metadata_pages.append(link)
            print(len(metadata_pages))
    next_page_selector = config['selenium']['selectors']['nextpagebutton']
    next_page_button = browser.find_element(By.CSS_SELECTOR, next_page_selector)
    while next_page_button:
        next_page_button.click()
        time.sleep(config['selenium']['timeoutnextpage'])
        mytable = browser.find_element(By.CSS_SELECTOR, table)
        for row in mytable.find_elements(By.CSS_SELECTOR, tablerow):
            for metadatalink in row.find_elements(By.CSS_SELECTOR, link_selector):
                link = metadatalink.get_attribute(metadatapagehref)
                print(link)
                metadata_pages.append(link)
                print(len(metadata_pages))
        try:
            next_page_button = browser.find_element(By.CSS_SELECTOR, next_page_selector)
        except NoSuchElementException:
           break 
    
    metadata_download_selector = config['selenium']['selectors']['metadatadownload']
    metadata_country_selector = config['selenium']['selectors']['metadatacountry']
    timeoutwaitfordownload = config['selenium']['timeoutwaitfordownload']
    for index, metadata_page in enumerate(metadata_pages):
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
        print("download:" + metadata_href)
        response = requests.get(metadata_href)

        file_name = str(index) + "-" + metadata_country.text + ".xml"
        completeName = os.path.join(dir_path + "/" + config['output']['folder'], file_name)         
        file = open(completeName, "wb")
        file.write(response.content)
        file.close()
        time.sleep(config['selenium']['timeoutnextrequest'])

    #to close the browser
    browser.close()

harvester()
