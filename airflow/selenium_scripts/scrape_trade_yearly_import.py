import os
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select
from datetime import datetime, timedelta


def get_df( driver, download_dir, countrycode, hslevelcode, currency):
    url = 'https://commerce-app.gov.in/eidb/icntcomq.asp'      #Url need to scrape data from
    driver.get(url)                                                 #Initializing driver with the url
    driver.implicitly_wait(10)                                      #Waiting for 10 sec until page to load
    
    year_list = [str(i) for i in range(1997,datetime.today().year)]
    
    imports_df = pd.DataFrame()
    for year in year_list:
        
        # create a new Firefox session
        select_year = Select(driver.find_element_by_id('select2'))      #  Select each financial year
        select_year.select_by_value(year)

        select_country = Select(driver.find_element_by_id("select3"))   # Select each country 
        select_country.select_by_value(countrycode)

        select_hsdigit = driver.find_element_by_id("hslevel")             #Select HS Code level 
        select_hsdigit.send_keys(hslevelcode)

        radio_all = driver.find_element_by_id("radioDAll")              #Select all commodities (Not top 100)
        radio_all.click()

        if currency == 'USD':                                           #Select either in 'rupee' or 'usd'
            curr = driver.find_element_by_id("radiousd")                
        elif currency == 'RS':
            curr = driver.find_element_by_id("radioval")
        curr.click()


        submit = driver.find_element_by_id("button1")              # Select Submit form button
        submit.click()
        driver.implicitly_wait(10)                                 # Wait for result to load     
        
        if 'Error' in driver.page_source or 'No record found' in driver.page_source: 
            driver.back()
        else:
            
            tmp_df = pd.read_html(driver.page_source)[0]
            temp_df = pd.DataFrame(tmp_df)
            temp_df.drop(temp_df.tail(3).index,inplace=True)
            temp_df.drop(temp_df.columns[[0,3,5]],axis=1,inplace=True)
            
            temp_df['HSName'] = temp_df['HSCode'].astype(int).astype(str)+'_'+temp_df['Commodity']
            temp_df = temp_df.drop(columns=['HSCode','Commodity'])
            temp_df = temp_df.set_index('HSName')

            imports_df = temp_df.join(imports_df,how='outer')
            driver.back()
    print(imports_df)    
    driver.close()
    
    # return imports_df
    pkl_filename = "import.pkl"
    path = os.path.join(download_dir, pkl_filename)
    imports_df.to_pickle(path)
