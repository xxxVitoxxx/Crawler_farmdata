from selenium import webdriver
from bs4 import BeautifulSoup
from selenium.webdriver.support.ui import Select
from tqdm import tqdm
import csv
import time
import os
from sys import argv
import change_data

try:
    '''
    Try to connect to the web 
    '''
    driver_position = os.path.join('.','chromedriver')
    driver = webdriver.Chrome(executable_path = driver_position) 
    driver.implicitly_wait(20) 
    driver.get(url='https://m.coa.gov.tw/FarmProductStat')
except:
    print('Can\'t Connect')

'''
fixed values 
'''
# 原本透過driver.find_element就可以定位到元素
# 但這網頁有些定位不到，所以用加上select去定位
Select(driver.find_element_by_name("YearStart")).select_by_value('109') 
Select(driver.find_element_by_name("MonStart")).select_by_value('01') 
Select(driver.find_element_by_name("YearEnd")).select_by_value('110')
Select(driver.find_element_by_name("MonEnd")).select_by_value('12') 
driver.find_element_by_xpath('/html/body/div/main/div[2]/div/section[1]/div/div/div/div/div[8]/div/span/div/button').click() #作物分類
driver.find_element_by_xpath('/html/body/div/main/div[2]/div/section[1]/div/div/div/div/div[8]/div/span/div/ul/li[4]/a/label').click() #作物分類
Select(driver.find_element_by_name("CropCat2")).select_by_visible_text('請選擇') # 次分類

def main(variety,i_e):
    '''
    web crawler
    '''
    Select(driver.find_element_by_name("TradeType")).select_by_value(i_e) # 進出口別
    driver.find_element_by_xpath('//*[@id="ByKeyword"]').send_keys(variety) # 關鍵字
    button = driver.find_element_by_xpath('//*[@id="btn-search"]')
    driver.execute_script("arguments[0].click();", button)
    time.sleep(3)

    types = driver.find_element_by_xpath('/html/body/div/main/div[2]/div/section[2]/div[2]/div/div/select[2]') #頁數的下拉式選單 
    lis = types.find_elements_by_tag_name('option') #抓選單內的option
    max_c = 0
    for i in lis: 
        max_c = i.get_attribute('value') # 將option內的頁數加到list

    file_position = os.path.join('.','Reptile_Data','09901_11012_'+variety + '_' + i_e)
    with open(file_position+'.csv','a',newline='') as file: # 自定義抬頭
        w = csv.writer(file) 
        w.writerow(['Date','Import and export','Name','Country','Weight','USD','NT'])
    
    pbar = tqdm(range(int(max_c))) # 設定讀取條的區間
    for i in pbar:
        soup = BeautifulSoup(driver.page_source, 'html.parser') 
        table = soup.find_all('table',{'class':'table table-hover text-nowrap LCGD'}) # 抓取class名為table table-hover text-nowrap LCGD的table
        td = table[0].find_all('td')
        td_len = len(td)
        td_list = []

        for j in range(8,td_len): # 只需要td內第八位移值後的資料
            td_list.append(td[j].text)

        with open(file_position+'.csv','a',newline='') as file: 
            w = csv.writer(file)
            w.writerows(zip(*[iter(td_list)]*8)) 

        driver.find_element_by_xpath('/html/body/div/main/div[2]/div/section[2]/div[2]/div/div/a[2]').click() #下一頁
        time.sleep(3)
        pbar.set_description('{}的{}資料共{}頁---Processing {}'.format(variety,i_e,max_c,i+1)) # 顯示跑條狀態

va = ['香蕉','芒果','鳳梨']
ie = ['出口','進口']

if __name__ == '__main__':
    
    for i in va:
        for j in ie:
            main(i,j)
            driver.find_element_by_id('ByKeyword').clear() # 清空關鍵字，不然搜尋的關鍵字會一直堆加上去
            print('{}的{}資料已完成下載...'.format(i,j))

    change_data.change_data() 
        