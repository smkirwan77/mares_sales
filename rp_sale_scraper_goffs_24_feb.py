##this is a new rp scraper, it seems i can get data in json form like you would for the sales. I will start tring to scrape that now. Its likley I will get data quicker and easier and prob more data to relations as its easier get. 
#Will also hopefully get a database of sire urls so i can then do an analysis on them too. 

##also want to scrape the entries parts. there is a section on the RP sales page with this but i cant see it picking up prog and siblings, it will be one or other I think


from bs4 import BeautifulSoup as bs
from bs4 import Comment
import requests
import numpy as np
import pandas as pd
# import headerProxy
import time
import re
import requests
import datetime
import itertools

import selenium
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

url = 'https://www.racingpost.com/bloodstock/sales/catalogues/3/2024-02-07/data.json'
min_lot = 314
max_lot = 430 #just throwing in a number here to avoid erros sale stops at 431 
#not aresed with lot 431 and she was withdrawn anyway. 

#the fuckin thing has supplments now so will have to think of a way to filter them out. the column has gone from int to str
#just do supplemented entries manually. there is only a handful of them that are sutiable in feb anyway. can often be stallions as well as yearlings etc.

#starts by scraping the lot info from the catologue page on rp. it also returns a dams url df which will be used to pass urls in functions to scrape more data
def scrape_lot_info(url, min_lot, max_lot):
    page_metadata = requests.get(url).json()
    # page_metadata = requests.get('https://www.racingpost.com/bloodstock/sales/catalogues/3/2023-11-24/data.json').json() ##trying goffs november now

    totalPages = page_metadata['pagination']['totalPages']
    
    
    full_data = []
    for page in range(1, totalPages + 1):
        response = requests.get(url, params={
        # response = requests.get('https://www.racingpost.com/bloodstock/sales/catalogues/3/2023-11-24/data.json', params={
            'page': str(page),
        })
        data: list[dict] = response.json()['rows']
        # print(data)
        for i in data:
            full_data.append(i)

    df = pd.DataFrame(full_data)

    df = df.loc[(df['lot_letter'] == " ") & (df['lot_no'] >= min_lot) & (df['lot_no'] <= max_lot)]    


    df['horse_uid'] = df['horse_uid'].fillna(0).astype(int).astype(str)
    df['dam_uid'] = df['dam_uid'].fillna(0).astype(int).astype(str)
    df['sire_uid'] = df['sire_uid'].fillna(0).astype(int).astype(str)
    df['sire_of_dam_uid'] = df['sire_of_dam_uid'].fillna(0).astype(int).astype(str)

    
    df['url_name'] = df['horse_style_name'].str.replace(" ", "-").str.replace("'","").str.lower()   
    df['url'] = 'https://www.racingpost.com/profile/horse/' + df['horse_uid'] + '/' + df['url_name'] 

    df['url_dam_name'] = df['dam_style_name'].str.replace(" ", "-").str.replace("'","").str.lower()
    df['dam_url'] = 'https://www.racingpost.com/profile/horse/' + df['dam_uid'] + '/' + df['url_dam_name']
    
    df['url_sire_name'] = df['sire_style_name'].str.replace(" ", "-").str.replace("'","").str.lower()
    df['sire_url'] = 'https://www.racingpost.com/profile/horse/' + df['sire_uid'] + '/' + df['url_sire_name']

    df['url_sire_of_dam_name'] = df['sire_of_dam_style_name'].str.replace(" ", "-").str.replace("'","").str.lower()
    df['sire_of_dam_url'] = 'https://www.racingpost.com/profile/horse/' + df['sire_of_dam_uid'] + '/' + df['url_sire_of_dam_name']


    df = df.rename(columns = {'lot_no': 'sale_lot_no'}) ##chanign lot_no to sale_lot_no as there will be plenty of df's with lot_no in them that may confuse me. The sale_lot_no represents what lot in the sale we are looking at that this data is relevent too.
    dam_urls = df[['dam_url', 'sale_lot_no']]
    # sire_urls = df[['sire_url', 'sale_lot_no']]
    # dam_sire_urls = df[['sire_of_dam_url', 'sale_lot_no']]

    
    df['horse_uid'] = df['horse_uid'].astype(int) ## easier for merging later i think

    ##there was a bug in the rp system where an unraced dam from the aga khan who had stock in the sales but it seems nothing out of the mare has ever ran and this could be the first public sale out of the mare in question. as a result the dams link is different as a racing post webpage changes if a dam is classed as a dam. very unusual case. wont spend time on it.
    def temp_2nd_dam_urls(dam_urls):
        options = webdriver.ChromeOptions()
        options.add_argument("headless")
        driver = webdriver.Chrome(options=options )
        driver.maximize_window()
        # visit your target site
        
        scnd_dam_urls = []
        for lot_no, dam_url in zip(dam_urls['sale_lot_no'],dam_urls['dam_url']):
            driver.get(dam_url)  
            scnd_dam_url = driver.find_elements(By.CLASS_NAME, 'hp-details__section')[0].find_elements(By.TAG_NAME, 'a')[1].get_attribute("href") 
            scnd_dam_name = driver.find_elements(By.CLASS_NAME, 'hp-details__section')[0].find_elements(By.TAG_NAME, 'a')[1].text.strip('right').strip()

            if 'owner' in scnd_dam_url:
                scnd_dam_url = driver.find_elements(By.CLASS_NAME, 'hp-details__section')[1].find_elements(By.TAG_NAME, 'a')[1].get_attribute("href")
                scnd_dam_name = driver.find_elements(By.CLASS_NAME, 'hp-details__section')[1].find_elements(By.TAG_NAME, 'a')[1].text.strip('right').strip()
                
            # print(scnd_dam_url, lot_no)
            scnd_dam_urls.append([lot_no, scnd_dam_url, scnd_dam_name])
            
            #if i can get the json handy from the window.PRELOADED_STATE part then its easy. if no reply by tomorrow I will just do it manually myself, will have the donkey work done in previous scraper
       
        scnd_dam_urls_df = pd.DataFrame(scnd_dam_urls, columns = ['sale_lot_no', '2nd_dam_url', '2nd_dam_name'])
        return  scnd_dam_urls_df

    scnd_dam_urls_df = temp_2nd_dam_urls(dam_urls.loc[(dam_urls['sale_lot_no'] >= min_lot) & (dam_urls['sale_lot_no'] <= max_lot)])####get rid of the .loc here. will try to think of something new but not the biggest inconvience from sale to sale, just with it being early in the code i could loose time scraping if not right for the next sale 
        
    df = df.merge(scnd_dam_urls_df, on = 'sale_lot_no', how = 'left')
    
    ##to help reduce the useless suplements will remove the lots that are not female. 
    #the sales with suppleem==ments at this stage are very annoying and removing one lot of data wont hurt when looking back. just be sure to look at these lots manually next feb. 
    #the supplmented lots make it so painful and the df gets merged all wrong and starts expanding. its fixable but not worth it for me.
    
    
    return df, dam_urls, scnd_dam_urls_df#, sire_urls, dam_sire_urls
    # ihave the sire and dams_sires df's there ready to go if needed but since the urls are in the sale_df there isnt a need, i can format the urls in one line so far in the functions that i have built. they are there ready to uncomment if required going forward
sale_df, dam_urls, scnd_dam_urls_df = scrape_lot_info(url, min_lot, max_lot) #dont mind scraping every time as its so quick. will get to the stage where file is only ran once anyway when i have the data in a form that i would like it
# sale_df, dam_urls, scnd_dam_urls_df, sire_urls, dam_sire_urls = scrape_lot_info(url) #dont mind scraping every time as its so quick. will get to the stage where file is only ran once anyway when i have the data in a form that i would like it


##for the goffs febuary sale in 2024 the sale is 2 days and mixes everything in. This year the mares are end of day 2 and there isnt a huge pile of them. 
##put in a lot catch mares are lots 314 onwards it seems. there is also potential fillies the first day. in the hit part. theres young mares in there. (lots 1 ->18 )
# sale_df = sale_df.loc[(sale_df['sale_lot_no'] >= min_lot) & (sale_df['sale_lot_no'] <= max_lot)] 
# dam_urls = dam_urls.loc[(dam_urls['sale_lot_no'] >= min_lot)  & (sale_df['sale_lot_no'] <= max_lot)] 
# scnd_dam_urls_df = scnd_dam_urls_df.loc[(scnd_dam_urls_df['sale_lot_no'] >= min_lot)  & (sale_df['sale_lot_no'] <= max_lot)]  
# sire_urls = sire_urls.loc[sire_urls['sale_lot_no'] >= 314]  
# dam_sire_urls = dam_sire_urls.loc[dam_sire_urls['sale_lot_no'] >= 314]  

# sale_df.to_excel('G:/My Drive/horse_racing/sale_work/2023_tat_dec_mares/rp_sale.xlsx', index = False)   


##this function will get the damsire stats for the sires of the mares on offer as well as there dams. which is more applicable is based on the profile of the mare in question
def dam_sire_stats_scrape(sale_df):    
    #getting the dam sire stats for the sires of the lots on offer
    sale_df['prog_dam_sire_url'] = sale_df['sire_url'].str.replace('/horse', '/tab/horse') + '/progeny-dams-sire'
    #getting the dam sire stats for the sires of the dams of offer
    sale_df['dam_prog_dam_sire_url'] = sale_df['sire_of_dam_url'].str.replace('/horse', '/tab/horse') + '/progeny-dams-sire'

    #the damsire df is quite good and worth a look
    def scrape_mean_max_rating(sire_url,j, sire):
        print('here')
        try:
            response = requests.get(sire_url).json()
            
            dam_sire_data = response['progenyHorsesData']['data']['damSireProgenyHorses']
            dam_sire_df = pd.DataFrame(dam_sire_data)
            print(dam_sire_df.shape, j)
    
            ##looking at the dataframe there is a much higher instance of 0 for or rating then racing post. i will use rp stats as a result
            mean_max_rating = {}
            if (dam_sire_df.shape[0] > 0) & (response['progenyHorsesData']['data']['seasonInfo']['raceType'] == 'flat'):
                mean_max_rating['dam_sire_mean_rating'] =  round(dam_sire_df.loc[dam_sire_df['rpPostmark'] > 0]['rpPostmark'].mean(),2)
                mean_max_rating['sire'] = sire
                mean_max_rating['sale_lot_no'] = lot_no
                mean_max_rating['count_dam_sire'] = dam_sire_df.shape[0]
                
                print(sire, dam_sire_df.loc[dam_sire_df['rpPostmark'] > 0]['rpPostmark'].mean())
                
            print(mean_max_rating)
        except ValueError:  # includes simplejson.decoder.JSONDecodeError:
            print('Decoding JSON has failed')           
        return mean_max_rating
        
    #sires dam sire stats
    dam_sire_stats = []
    for lot_no,url,dam_sire in zip(sale_df['sale_lot_no'], sale_df['prog_dam_sire_url'], sale_df['sire_style_name']):
        dam_sire_stats.append(scrape_mean_max_rating(url, lot_no, dam_sire))
    dam_sire_stats = pd.DataFrame(dam_sire_stats)

    #sires of the mares on offer dam_sire stats 
    sire_dam_sire_stats = []
    for lot_no,url,sire in zip(sale_df['sale_lot_no'], sale_df['dam_prog_dam_sire_url'], sale_df['sire_of_dam_style_name']):
        sire_dam_sire_stats.append(scrape_mean_max_rating(url, lot_no, sire))
    sire_dam_sire_stats = pd.DataFrame(sire_dam_sire_stats)

    return dam_sire_stats, sire_dam_sire_stats.rename(columns = {'sire': 'dam_sire', 'dam_sire_mean_rating': 'dams_dam_sire_mean_rating'})

dam_sire_stats, sire_dam_sire_stats = dam_sire_stats_scrape(sale_df)
dam_sire_stats = dam_sire_stats.merge(sire_dam_sire_stats, on = 'sale_lot_no', how = 'left').dropna()


def prog_form_basic(sale_df):
    sale_df['progeny_form_basic_url'] = sale_df['url'].str.replace('/horse', '/tab/horse') + '/progeny'
    # sale_df = sale_df.loc[sale_df['horse_uid'] != 0]
    full_data = []
    for lot_no,url in zip(sale_df['sale_lot_no'], sale_df['progeny_form_basic_url']):
        response = requests.get(url)
        # print(response.json())
        if 'progenyResults' in response.json().keys() and len(response.json()['progenyResults']) > 0 and 'FLAT' in response.json()['progenyResults'].keys():#was one unusual example of lot 321 in goffs feb 2024 where the lots only sibling that race was NH. Very rare so dont worry about getting rid of the last part of the and statement here. 
            print(lot_no, url, response.json().keys()) 
            data: list[dict] = response.json()['progenyResults']['FLAT']
            data1 = {}
            if len(response.json()['progenyResults']) == 2:
                data1: list[dict] = response.json()['progenyResults']['JUMPS']
            #have to put in a catch here as there isnt an empty dictionary returned when there is not progeny for form. for sales you get a enmpty dictionary which is great. will be a simple fix but thats why there is a catch here and not in sales        
            if data != None:
                for i in data:
                    i['sale_lot_no'] = lot_no ##have to stick in a way to merge here as there is no dam data in the basic form section, this mightn be neccesary when we get to the fuller form and it migth be easy to merge full form as that will surely have a dam id
                    i['relationship'] = 'prog_basic_form'
                    full_data.append(i)
                for i in data1:
                    i['sale_lot_no'] = lot_no
                    i['relationship'] = 'prog_basic_form'
                    full_data.append(i)
                    
    prog_form_basic = pd.DataFrame(full_data)
    # prog_form_basic = prog_form_basic.merge(sale_df['sale_lot_no'], on = 'sale_lot_no', how = 'left') #this will only work for mering sale_lot_no with progeny of the mares on sale. will have to create a new function for each relation i.e siblings, cousins etc.
    return prog_form_basic

prog_form_basic_df = prog_form_basic(sale_df.loc[sale_df['horse_uid'] != 0] )
prog_form_basic_df['relationship'] = 'prog_basic_form'
    
own_sib_form_basic_df = prog_form_basic(dam_urls.rename(columns = {'dam_url': 'url'}))    
own_sib_form_basic_df['relationship'] = 'sib_basic_form'

dams_aunt_uncles_form_basic_df = prog_form_basic(scnd_dam_urls_df.rename(columns = {'2nd_dam_url': 'url'}))    
dams_aunt_uncles_form_basic_df['relationship'] = 'aunt_uncles_basic_form'

##doing a bit here to get a suitable url df for nieces and nephews. the url that will be passed is called sibling urls. representing a df of urls of femail siblings to the lot on offer.
own_sibling_urls = own_sib_form_basic_df.loc[own_sib_form_basic_df['horseSexCode'].isin(['M','F'])]
own_sibling_urls['url_name'] = own_sibling_urls['styleName'].str.replace(" ", "-").str.replace("'","").str.lower()   
own_sibling_urls['url'] = 'https://www.racingpost.com/profile/horse/' + own_sibling_urls['horseUid'].astype(str) + '/' + own_sibling_urls['url_name']

prog_niece_nephew_form_basic_df = prog_form_basic(own_sibling_urls) ##there is going to be a 
prog_niece_nephew_form_basic_df['relationship'] = 'niece_nephew_basic_form'

##gets the data for the sales of the progeny of the urls passed in here.
def prog_sales(sale_df):    
    sale_df['prog_sales_url'] = sale_df['url'].str.replace('/horse', '/tab/horse') + '/progeny-sales'
    # sale_df = sale_df.loc[sale_df['horse_uid'] != 0]
    full_data = []
    for lot_no,url in zip(sale_df['sale_lot_no'], sale_df['prog_sales_url']):
        print(lot_no,url) 
        response = requests.get(url)
        data: list[dict] = response.json()['progenySales']
        # print(data)
        # print(data)
        for i in data:
            i['sale_lot_no'] = lot_no
            full_data.append(i)
    
    df = pd.DataFrame(full_data)
    
    if df.shape[0] > 0:
        df['saleDate'] = df['saleDate'].str[0:10]
    return df

prog_sales_df = prog_sales(sale_df.loc[sale_df['horse_uid'] != 0]) #doesnt add in ones that have no registered name as they will have no progeny data
# prog_sales_df = prog_sales_df.merge(sale_df[['sale_lot_no', 'horse_uid']], left_on = 'damUid', right_on = 'horse_uid') #this will only work for mering sale_lot_no with progeny of the mares on sale. will have to create a new function for each relation i.e siblings, cousins etc.
#the above merge will have changed, think i can get the lot no into the function for all relationships now
# prog_sales_df = prog_sales_df.drop(columns = {'horse_uid'})
prog_sales_df['relationship'] = 'prog_sales'

own_sib_sales_df = prog_sales(dam_urls.rename(columns = { 'dam_url': 'url'}))
own_sib_sales_df['relationship'] = 'sib_sales'

dams_aunt_uncles_sales_df = prog_sales(scnd_dam_urls_df.rename(columns = { '2nd_dam_url': 'url'}))
dams_aunt_uncles_sales_df['relationship'] = 'aunt_uncle_sales'

prog_niece_nephew_sales_df = prog_sales(own_sibling_urls)
prog_niece_nephew_sales_df['relationship'] = 'niece_nephew_sales'


#########################################################################################
##they dont have the fecking trainers in here. prob not a big deal. shame about that. they dont have another thing as well. this function is redundent for now. have left a note below it outlining what to do for next sale
##this was scraping the json formatted form full for a horse. the issue here is there is no sign of trainer. and it doesnt cover nr's etc. Im not sure where RP store that data before loading into the form tab on the site. 
# I will know go down the full scrape which is very slow but returns all the data I want I think.
def prog_form_full_json(prog_form_basic_df):
    prog_form_basic_df['url_name'] = prog_form_basic_df['horseName'].str.replace(" ", "-").str.replace("'","").str.lower()   
    prog_form_basic_df['url'] = 'https://www.racingpost.com/profile/tab/horse/' + prog_form_basic_df['horseUid'].astype(str) + '/' + prog_form_basic_df['url_name'] + '/form'


    full_data = []
    for lot_no,url in zip(prog_form_basic_df['sale_lot_no'], prog_form_basic_df['url']):
        print(lot_no, url) 

        response = requests.get(url)
        # if 'progenyResults' in response.json().keys() and len(response.json()['progenyResults']) > 0:
        # print(lot_no, url, response.json().keys()) 
        data: list[dict] = response.json()['form']
        # data1 = {}
        # if len(response.json()['progenyResults']) == 2:
        #     data1: list[dict] = response.json()['progenyResults']['JUMPS']
        #have to put in a catch here as there isnt an empty dictionary returned when there is not progeny for form. for sales you get a enmpty dictionary which is great. will be a simple fix but thats why there is a catch here and not in sales        
        if data != None:
            for i in data.keys():
                data[i]['sale_lot_no'] = lot_no ##have to stick in a way to merge here as there is no dam data in the basic form section, this mightn be neccesary when we get to the fuller form and it migth be easy to merge full form as that will surely have a dam id
                data[i]['relationship'] = 'prog_full_form'
                full_data.append(data[i])
            # for i in data1:
            #     i['sale_lot_no'] = lot_no
            #     i['relationship'] = 'prog_basic_form'
            #     full_data.append(i)
                    
    prog_form_full = pd.DataFrame(full_data)
    prog_form_full = prog_form_full.merge(sale_df['sale_lot_no'], on = 'sale_lot_no', how = 'left') #this will only work for mering sale_lot_no with progeny of the mares on sale. will have to create a new function for each relation i.e siblings, cousins etc.

    ##race records df has decent data but overall we can work it out from the full form i suspect. but bear in mind it migth be worth relooking at the race records of the form json tab.    
    return prog_form_full
##a bit disapointingly the data produced below isnt great. I think that i will have to take elements of the original scripts to get things like trainer etc, the below is even hard to merge as there is no record of the horses name or ID (there would be a work around by inserting the 2 columns in the above function)
##for feb 2024 I am happy just to get the basic data and not waste more time on the below. there is about 118 lots on offer so data isnt 100% neccessary really. Id be better bulding models etc with what I have anyway and then try get in the other details 
#the form_scrape().form_full() function would be best to get in extra detail. it wont take long to merge that function above im just moving on for now.

# prog_form_full_df = prog_form_full_json(prog_form_basic_df)
# sib_form_full_df = prog_form_full_json(sib_form_basic_df)
# aunt_uncles_form_full_df = prog_form_full_json(aunt_uncles_form_basic_df)

#########################################################################

def form_scrape(df):
    df['url_name'] = df['horseName'].str.replace(" ", "-").str.replace("'","").str.lower()   
    df['url'] = 'https://www.racingpost.com/profile/horse/' + df['horseUid'].astype(str) + '/' + df['url_name']

    def form_full(url, lot):
    # initialize an instance of the chrome driver (browser)
        options = webdriver.ChromeOptions()
        options.add_argument("headless")
        driver = webdriver.Chrome(options=options )
        driver.maximize_window()
        # visit your target site
        driver.get(f"{url}/form")  


        run_data = [] ##moved outside the try bit as we need to return a list, weather empty or not           
        prog_form_index_errors = []
        try:

            name = driver.find_element(By.CLASS_NAME, 'hp-nameRow__name').text 
            print(name)
            dob = driver.find_element(By.CLASS_NAME, 'hp-details__info').text.split(" ")[0].strip("(")
            sex = driver.find_element(By.CLASS_NAME, 'hp-details__info').text.split(" ")[2].strip(")")
            
            #different web page for dams and not dams (can inclued geldings/colts or young mares or mares that havnt produced anything publicaly (sale or track) yet)
            if len(driver.find_elements(By.CLASS_NAME, 'hp-nameRow__progenyIcon')) > 0:
                sire = driver.find_elements(By.CLASS_NAME, 'hp-details__section')[0].find_elements(By.TAG_NAME, 'a')[0].text.strip('right').strip()
                sire_url = driver.find_elements(By.CLASS_NAME, 'hp-details__section')[0].find_elements(By.TAG_NAME, 'a')[0].get_attribute("href")           
                dam = driver.find_elements(By.CLASS_NAME, 'hp-details__section')[0].find_elements(By.TAG_NAME, 'a')[1].text.strip('right').strip()
                dam_url = driver.find_elements(By.CLASS_NAME, 'hp-details__section')[0].find_elements(By.TAG_NAME, 'a')[1].get_attribute("href")           
                dam_sire = driver.find_elements(By.CLASS_NAME, 'hp-details__section')[0].find_elements(By.TAG_NAME, 'a')[2].text.strip('right').strip()
                dam_sire_url = driver.find_elements(By.CLASS_NAME, 'hp-details__section')[0].find_elements(By.TAG_NAME, 'a')[2].get_attribute("href") 
                trainer = driver.find_elements(By.CLASS_NAME, 'hp-details__section')[1].find_element(By.TAG_NAME, 'a').text.strip('right').strip()
    
            if len(driver.find_elements(By.CLASS_NAME, 'hp-nameRow__progenyIcon')) == 0:
                sire = driver.find_elements(By.CLASS_NAME, 'hp-details__section')[1].find_elements(By.TAG_NAME, 'a')[0].text.strip('right').strip()
                sire_url = driver.find_elements(By.CLASS_NAME, 'hp-details__section')[1].find_elements(By.TAG_NAME, 'a')[0].get_attribute("href")   
                dam = driver.find_elements(By.CLASS_NAME, 'hp-details__section')[1].find_elements(By.TAG_NAME, 'a')[1].text.strip('right').strip()
                dam_url = driver.find_elements(By.CLASS_NAME, 'hp-details__section')[1].find_elements(By.TAG_NAME, 'a')[1].get_attribute("href")   
                dam_sire = driver.find_elements(By.CLASS_NAME, 'hp-details__section')[1].find_elements(By.TAG_NAME, 'a')[2].text.strip('right').strip()
                dam_sire_url = driver.find_elements(By.CLASS_NAME, 'hp-details__section')[1].find_elements(By.TAG_NAME, 'a')[2].get_attribute("href") 
                trainer = driver.find_elements(By.CLASS_NAME, 'hp-details__section')[0].find_element(By.TAG_NAME, 'a').text.strip('right').strip()
            
            
            x = driver.find_elements(By.CLASS_NAME, 'ui-table__body')[1]    
            for i in x.find_elements(By.CLASS_NAME, 'ui-table__row'):
                print(i.text, '/n New row here')
                if ' btn ' in i.text or ' by ' in i.text and 'Appleby' not in i.text: ## have fuckin wierd instances of tim easterBY switching to someone else. will have to keep eye out for other wierd ones.
                ##an alternative to above will come in useful if there is plenty of trainers with by or btn in there name
                # if 'notification' not in i.text or 'break' not in i.text or 'Wind' not in i.text:
                
                    run_data.append({
                                    'date': i.find_element(By.CLASS_NAME, 'hp-formTable__dateWrapper').text.split("\n")[-1],
                                    'course': i.find_element(By.CLASS_NAME, 'hidden-lg-down').text,
                                    'distance': i.find_elements(By.CLASS_NAME, 'ui-table__cell')[2].text,
                                    'going': i.find_elements(By.CLASS_NAME, 'ui-table__cell')[3].text,
                                    'or': i.find_elements(By.CLASS_NAME, 'ui-table__cell')[8].text,
                                    'rpr': i.find_elements(By.CLASS_NAME, 'ui-table__cell')[10].text,
                                    'race_details': i.find_elements(By.CLASS_NAME, 'ui-table__cell')[1].text, ##split up later
                                    'horse_name': name,
                                    'dob': dob, ##convert to datetime later
                                    'sire': sire,
                                    'sire_url': sire_url,
                                    'trainer': trainer,
                                    # 'jockey' : i.find_elements(By.CLASS_NAME, 'ui-link ui-link_table js-popupLink').text,  #not too pushed                                                                      
                                    'dam': dam,
                                    'dam_url': dam_url,
                                    'dam_sire': dam_sire,
                                    'dam_sire_url': dam_sire_url,
                                    'sex': sex,
                                    'lot_no': lot
                                    #'age_date_of_sale': age                            
                                    }
                                    )
                ##can defo get more colums in there like race class or bt or not        
            driver.quit()
        
        except IndexError:
            prog_form_index_errors.append([name, lot])
            
        return run_data, prog_form_index_errors

    form = []     
    for index, rows in df.iterrows():
        #putting in a catch for mares with no Racing post profile here. some mares wont have a page due to not racign and no foals
        print("sale_lot_no: ", rows['sale_lot_no'])
        data, errors = form_full(rows['url'], rows['sale_lot_no'])
        
        for i in data:
            form.append(i)

    form = pd.DataFrame(form).rename(columns = {'lot_no': 'sale_lot_no'})
    
    dam_urls = form[['sale_lot_no', 'dam_url']].drop_duplicates()
    return form, dam_urls, errors        

##going to get dams form via this data. dont want to use the ratings function anymore, there is nothing in there that isnt here, except the ratings one only has one entry per horse. this will have multiple ones and that could look messy 
prog_form, dam_urls, prog_full_form_errors = form_scrape(prog_form_basic_df) # dam_urls is not needed, i used the original one from the top, this pulls the form for the progeny of the lots on offer
own_sibling_form, snd_dams_url, own_sib_full_form_errors= form_scrape(own_sib_form_basic_df) #dont have to change column names as this is a different tier of function, this pulls the form for the siblins for the lots on offer
##I think the above can be streamlined by removing the prog bit, we can get that in the niece/nephew thier.

#can use the below to get the prog_niece_nephew_form
prog_niece_nephew_form, unsure, prog_niece_nephew_full_form_errors = form_scrape(prog_niece_nephew_form_basic_df) #dont have to change column names as this is a different tier of function, this pulls the form for the siblins for the lots on offer
###^^ the above data will need to be grouped by dam when making stats. and then we can see lots that have good dam siblings.

#the below pulls the dams and then aunts/uncles of the lots on offer. they are the same generation makes it hard to label the variable.
#the aunts_uncles_form_basic_df variable that is passed below contains the form for the dams on offer as well.
dams_aunts_uncles_form, trd_dams_url, dams_aunts_uncles_full_form_errors = form_scrape(dams_aunt_uncles_form_basic_df) ## this pulls the form for the dams of the lots on offer, i would imagine uncle and aunties are included as well. 
#above might need a column change. looking at it after scraping goffs feb 24 it might be alright actually. will check as i go to make sure anyway. 

#spliting the df's up 
dam_names = sale_df['dam_style_name'].to_list()
dams_form = dams_aunts_uncles_form.loc[dams_aunts_uncles_form['horse_name'].isin(dam_names)]
aunt_uncles_form = dams_aunts_uncles_form.loc[~dams_aunts_uncles_form['horse_name'].isin(dam_names)]
##wont use the dfs in the few lines above for now. will leave in the generations that they are in now.

###looking at df's created I could get rid of the prog bit as that form is captured in the niece/nephew part

# G:\My Drive\horse_racing\sale_work\2024_gfs_feb\
def push():    
    with pd.ExcelWriter("G:/My Drive\horse_racing/sale_work/2024_gfs_feb/raw.xlsx") as writer:
        sale_df.to_excel(writer, sheet_name = "sale_df", index=False)
        dam_sire_stats.to_excel(writer, sheet_name="dam_sire_stats", index=False)
        
        ##prog looks to be double captured below
        prog_form_basic_df.to_excel(writer, sheet_name="prog_form_basic", index=False)
        prog_niece_nephew_form_basic_df.to_excel(writer, sheet_name="prog_niece_nephew_form_basic", index=False)
        own_sib_form_basic_df.to_excel(writer, sheet_name="own_sib_form_basic", index=False)
        dams_aunt_uncles_form_basic_df.to_excel(writer, sheet_name="dams_aunt_uncles_form_basic", index=False)

        #again prog double captured. not to worried as this function wasnt expensive to run. 
        prog_sales_df.to_excel(writer, sheet_name="prog_sales", index=False)
        prog_niece_nephew_sales_df.to_excel(writer, sheet_name="prog_niece_nephew_sales", index=False)
        own_sib_sales_df.to_excel(writer, sheet_name="own_sib_sales", index=False)
        dams_aunt_uncles_sales_df.to_excel(writer, sheet_name="dams_aunt_uncles_sales", index=False)

        #prog is double captured this is an expensive function so I would like to reduce that down
        prog_form.to_excel(writer, sheet_name="prog_form", index=False)
        prog_niece_nephew_form.to_excel(writer, sheet_name="prog_niece_nephew_form", index=False)
        own_sibling_form.to_excel(writer, sheet_name="own_sibling_form", index=False)
        dams_aunts_uncles_form.to_excel(writer, sheet_name="dams_aunts_uncles_form", index=False)

        # dams_form.to_excel(writer, sheet_name="dam_form", index=False)
        
## there is enough data there to get going on the next file which will clean it and then come up with metrics and a good view

#########Potential next steps
# changed trainers in full form.  
# get sp maybe. 




#other sales. have created a new bulk folder. issue is that this file will change a bit in the meantime and migth overwrite the other one.
# sales.append(['https://www.racingpost.com/bloodstock/sales/catalogues/3/2024-02-07/data.json', 314,1000]) #goffs_feb_24
# sales.append(['https://www.racingpost.com/bloodstock/sales/catalogues/5/2023-12-04/data.json', 1,1200]) #tatts_dec_23
# sales.append(['https://www.racingpost.com/bloodstock/sales/catalogues/3/2023-11-24/data.json', 1051, 1600]) #goffs_nov_23
# sales.append(['https://www.racingpost.com/bloodstock/sales/catalogues/3/2023-02-08/data.json', 355, 500 ]) #goffs_feb_23
# sales.append(['https://www.racingpost.com/bloodstock/sales/catalogues/5/2022-11-28/data.json', 1373, 2400 ]) #tatts_dec_22
# sales.append(['https://www.racingpost.com/bloodstock/sales/catalogues/3/2022-11-18/data.json', 1051, 1550 ]) #goffs_nov_22
