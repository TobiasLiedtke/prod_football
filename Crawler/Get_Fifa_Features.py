import os
os.chdir('D:/Projects/Football/PROD/Crawler')

#packages and modules
import pandas as pd
from selenium import webdriver
import time

#import other files 
import Tools_Crawler as t
import Read_Load_Database as db

from selenium.webdriver.chrome.service import Service
    
def get_fifa_features_bundesliga(spieltag, url, saison):
    
    """
    gets club based numerical strength value assigned by fifa for the bundesliga
    Arguments:
        spieltag: an integer
        url: an string
        url: an saison (format: yyyy-yy)
    Returns:
        data frame with fifa feature values for given spieltag
    """        
    driver = webdriver.Chrome(service=Service('D:\Projects\Football\Database\Crawler_Code\Webdrivers\chromedriver.exe'))
    print(url)
    driver.get(url)
    time.sleep(2)
    clubs = driver.find_elements("xpath", "//td[@class='col-name-wide']")
    overall = driver.find_elements("xpath", "//td[@class='col col-oa']")
    attack = driver.find_elements("xpath", "//td[@class='col col-at']")
    mittelfeld = driver.find_elements("xpath", "//td[@class='col col-md']")
    abwehr = driver.find_elements("xpath", "//td[@class='col col-df']")

            
    f_ver = t.get_df(clubs)
    df_verein = f_ver.df() 
    
    f_over =  t.get_df(overall)
    df_over = f_over.df() 
    
    f_attackt =  t.get_df(attack)
    df_attack = f_attackt.df() 
    
    f_mittelfeld =  t.get_df(mittelfeld)
    df_mittelfeld = f_mittelfeld.df()  
    
    f_abwehr =  t.get_df(abwehr)
    df_abwehr = f_abwehr.df()         
    
    driver.quit()
    df_verein = df_verein.drop(1, axis = 1)
    df_verein.columns = ['Verein']
    df_over.columns = ['Gesamt']
    df_attack.columns = ['Angriff']
    df_mittelfeld.columns = ['Mittelfeld']
    df_abwehr.columns = ['Abwehr']
    
    df = pd.concat([df_verein, df_over, df_attack, df_mittelfeld, df_abwehr], axis = 1)
    
    df_v = db.get_table('bl1_data_vereine_spielplan') 
    df_v = df_v[df_v['Saison']==saison]
    df_v = df_v[df_v['Spieltag']==spieltag]
    df = df.replace({'SC Freiburg':'Sport-Club Freiburg', 'DSC Arminia Bielefeld':'DSC Arminia Bielefeld'
                     ,'VfL Bochum 1848':'VfL Bochum', 'TSG Hoffenheim':'TSG 1899 Hoffenheim'})
    df = df.merge(df_v, on = 'Verein', how = 'inner')
    df = df[['Vereins_ID', 'Verein', 'Spieltag', 'Gesamt', 'Angriff', 'Abwehr', 'Mittelfeld', 'Saison']]

    
    return df


    
def get_fifa_features_premierleague(saison, spieltag, url):
    
    """
    gets club based numerical strength value assigned by fifa for the bundesliga
    Arguments:
        spieltag: an integer
        url: an string
        url: an saison (format: yyyy-yy)
    Returns:
        data frame with fifa feature values for given spieltag
    """          
    
    driver = webdriver.Chrome(service=Service('D:\Projects\Football\Database\Crawler_Code\Webdrivers\chromedriver.exe'))
    driver.get(url)
    time.sleep(2)
    clubs = driver.find_elements("xpath", "//td[@class='col-name-wide']")
    overall = driver.find_elements("xpath", "//td[@class='col col-oa']")
    attack = driver.find_elements("xpath", "//td[@class='col col-at']")
    mittelfeld = driver.find_elements("xpath", "//td[@class='col col-md']")
    abwehr = driver.find_elements("xpath", "//td[@class='col col-df']")

            
    f_ver = t.get_df(clubs)
    df_verein = f_ver.df() 
    
    f_over =  t.get_df(overall)
    df_over = f_over.df() 
    
    f_attackt =  t.get_df(attack)
    df_attack = f_attackt.df() 
    
    f_mittelfeld =  t.get_df(mittelfeld)
    df_mittelfeld = f_mittelfeld.df()  
    
    f_abwehr =  t.get_df(abwehr)
    df_abwehr = f_abwehr.df()         
    
    driver.quit()
    df_verein = df_verein.drop(1, axis = 1)
    df_verein.columns = ['Verein']
    df_over.columns = ['Gesamt']
    df_attack.columns = ['Angriff']
    df_mittelfeld.columns = ['Mittelfeld']
    df_abwehr.columns = ['Abwehr']
    
    df = pd.concat([df_verein, df_over, df_attack, df_mittelfeld, df_abwehr], axis = 1)
    
    df_v = db.get_table('pl_data_vereine_spielplan')
    df_v = df_v[df_v['Saison']==saison]
    df_v = df_v[df_v['Spieltag']==spieltag]
    
    df = df.replace({'Chelsea':'FC Chelsea', 'Liverpool':'FC Liverpool', 'Arsenal':'FC Arsenal'
                     ,'Everton':'FC Everton', 'Southampton':'FC Southampton', 'Sunderland':'AFC Sunderland'
                     ,'Burnley':'FC Burnley', 'Watford':'FC Watford', 'Middlesbrough':'FC Middlesbrough',
                     'Fulham':'FC Fulham', 'Brentford':'FC Brentford', 'Nottingham Forest':'Nottingham Forest'
                     , 'AFC Bournemouth':'AFC Bournemouth', 'Fulham':'FC Fulham'})
    
    df = df.merge(df_v, on = 'Verein', how = 'inner')
    df = df[['Vereins_ID', 'Verein', 'Spieltag', 'Gesamt', 'Angriff', 'Abwehr', 'Mittelfeld', 'Saison']]
    print(df[['Verein']])
    return df

