from selenium import webdriver
from bs4 import BeautifulSoup
from selenium.webdriver.support.ui import Select
from tqdm import tqdm
import csv
import time


#driver = webdriver.Chrome(ChromeDriverManager().install())
driver = webdriver.Chrome(executable_path = '/Users/vito/Desktop/Farmdata/chromedriver')
driver.implicitly_wait(20)
driver.get(url='https://m.coa.gov.tw/FarmProductStat')

Select(driver.find_element_by_name("YearStart")).select_by_value('099')

Select(driver.find_element_by_name("MonStart")).select_by_value('01') #可以不用選擇月份

Select(driver.find_element_by_name("YearEnd")).select_by_value('110')

Select(driver.find_element_by_name("MonEnd")).select_by_value('12') #可以不用選擇月份

driver.find_element_by_xpath('/html/body/div/main/div[2]/div/section[1]/div/div/div/div/div[8]/div/span/div/button').click() #作物分類
driver.find_element_by_xpath('/html/body/div/main/div[2]/div/section[1]/div/div/div/div/div[8]/div/span/div/ul/li[4]/a/label').click() #作物分類

Select(driver.find_element_by_name("CropCat2")).select_by_visible_text('請選擇')


def main(variety,i_e):
    Select(driver.find_element_by_name("TradeType")).select_by_value(i_e) 
    driver.find_element_by_xpath('//*[@id="ByKeyword"]').send_keys(variety)

    button = driver.find_element_by_xpath('//*[@id="btn-search"]')
    driver.execute_script("arguments[0].click();", button)

    time.sleep(5)
    ####
    types = driver.find_element_by_xpath('/html/body/div/main/div[2]/div/section[2]/div[2]/div/div/select[2]') #關鍵字xpath 
    lis = types.find_elements_by_tag_name('option') #select option
    max_c = 0
    for i in lis: 
        max_c = i.get_attribute('value')

    print('共{}頁資料'.format(max_c))

    with open('./未命名檔案夾/09901_11012_' + variety + '_' + i_e +'.csv','a') as file:
        w = csv.writer(file) 
        w.writerow(['Date','Import and export','Name','Country','Weight','USD','NT'])
    
    
    pbar = tqdm(range(int(max_c)))
    for i in pbar:
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        #print('第{}次'.format(i+1))
        table = soup.find_all('table',{'class':'table table-hover text-nowrap LCGD'})
        td = table[0].find_all('td')
        td_len = len(td)
        td_list = []
    
    
        for j in range(8,td_len):
            td_list.append(td[j].text)



        with open('./未命名檔案夾/09901_11012_' + variety + '_' + i_e +'.csv','a',encoding='utf8') as file:
                w = csv.writer(file) 
                w.writerows(zip(*[iter(td_list)]*8)) 
        driver.find_element_by_xpath('/html/body/div/main/div[2]/div/section[2]/div[2]/div/div/a[2]').click() #下一頁
        time.sleep(5)
        pbar.set_description("Processing %s"%i)

va = ['鳳梨','香蕉','芒果']
ie = ['出口','進口']


if __name__ == '__main__':
    for i in va:
        for j in ie:
            main(i,j)
            driver.find_element_by_id('ByKeyword').clear()
            print('{}的{}資料已完成下載...'.format(i,j))






