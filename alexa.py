from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
from multiprocessing import Pool
import multiprocessing as mp
import datetime
import time
import random


def make_options():
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
#    chrome_options.add_experimental_option( "prefs",{'profile.managed_default_content_settings.javascript': 2})
    chrome_options.add_argument("--log-level=3")
    chrome_options.add_argument('disable-gpu')
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.67 Safari/537.36')
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument('--ignore-certificate-errors-spki-list')
    chrome_options.add_argument("--ignore-ssl-errors")
    chrome_options.add_argument("--ignore-certificate-error")
    chrome_options.add_argument("start-maximized") 
    chrome_options.add_argument("disable-infobars") 
    chrome_options.add_argument("--disable-extensions") 
   
    return chrome_options

#Chrome Driver 생성
def make_driver():
    chrome_options = make_options()
    driver = webdriver.Chrome(executable_path = './chromedriver.exe',options = chrome_options)   
    return driver


def replace_url(url):
    url = url.split(',')
    url[0] = url[0].replace("[", "")
    url[0] = url[0].replace("https://", "")
    url[0] = url[0].replace("http://", "")
    url[0] = url[0].replace("www.", "")
    url[0] = url[0].replace("]", "")
    url = url[0].replace("'", "")
    url = 'https://www.alexa.com/siteinfo/' + url
    return url


def get_rank(data):
    if data['valid_url'] == 0:
        return -1
    driver = make_driver()
    try:
        url = replace_url(data['urls.website'])         #기존 CSV파일에 있는 url을 정제하여 https://www.scamadviser/url 형식으로.
        driver.implicitly_wait(4)                      #안정성을 위해서 5초 wait
        driver.get(url)
        driver.find_elements_by_id
        time.sleep( random.uniform(0,2) )
        driver.find_elements_by_class_name('rankmini-rank')
        rank = driver.find_element_by_xpath("//*[@id=\"card_mini_trafficMetrics\"]/div[3]/div[2]/div[1]").text
        print("url : {} , rank {} : ".format(url,rank))
        return rank
        
    except:
        try: 
            rank = driver.find_element_by_xpath("//*[@id=\"card_mini_trafficMetrics\"]/div[2]/div[2]/div[1]").text
            print("url : {} , rank {} : ".format(url,rank))
            return rank
        except:    
            rank = 'Err'
            print("Error ! url : {} , rank {} : ".format(url,rank))
            return rank
    finally:
#        driver.close()
       driver.quit()

def split_csv(total_csv):
    rows = pd.read_csv(total_csv,chunksize=50)
    file_count = 0
    for i, chuck in enumerate(rows):
        chuck.to_csv('out{}.csv'.format(i))
        file_count = file_count+1
    return file_count


if __name__=='__main__':
    file_count = split_csv('total_v1.1.csv')

    for i in range(file_count):
        file_name = 'out{}.csv'.format(i+8)
        data = pd.read_csv(file_name,encoding ='CP949')
        data = data.to_dict('records')
        try:
            pool = Pool(processes=1)
            result = pool.map(get_rank,data)
            print("------------------------------------------")
            print(file_name + " is completed")
            print("-------------------------------------------")
            for i in range(len(data)):
                data[i]['alexa_rank'] = result[i]
            list_df = pd.DataFrame(data)
            list_df.to_csv('./result/f' + file_name ,index=False)
        except Exception as e:
            print(e)

                                      



