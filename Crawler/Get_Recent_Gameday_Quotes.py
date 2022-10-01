import os
os.chdir('D:/Projects/Football/PROD/Crawler')

import pandas as pd
from selenium import webdriver
import Read_Load_Database as db
from selenium.webdriver.chrome.service import Service

def get_recent_odds(spieltag, saison):
    
    """
    get initial quotes for bundesliga matchday for bet365 bookmaker
    Arguments:
        spieltag: an integer
        saison: string (format: yyyy/yy)
    Returns:
        data frame
    """      
        
    url = 'https://www.bundesligatrend.de/aktuelle-bundesliga-quoten.html'
    driver = webdriver.Chrome(service=Service('D:\Projects\Football\Database\Crawler_Code\Webdrivers\chromedriver.exe'))
    driver.get(url)

    whole_site = driver.find_elements("xpath", "//div[@id='container']")
    
    all_text = whole_site[0].text
    index = all_text.find("Datum")
    index_ende = all_text.find("(Stand der Quoten")
    quoten = all_text[index:index_ende]
    
    
    list_quoten = quoten.split('\n')
    list_quoten = list_quoten[1:-1]
    list_quoten = [w.replace('Union Berlin', 'Union') for w in list_quoten]
    list_quoten = [w.replace('Hertha BSC Berlin', 'Hertha') for w in list_quoten]
    list_quoten = [w.replace('Bayern München', 'Bayern') for w in list_quoten]
    list_quoten = [w.replace('  ', '') for w in list_quoten]
    
    df_quoten = pd.DataFrame(list_quoten)
    df_quoten = df_quoten[0].str.split(' ', expand = True)
    df_quoten = df_quoten.drop([0,1,2,3,5], axis = 1)
    
    df_quoten.columns = ['Heimmannschaft' , 'Auswärtsmannschaft', 'B365H', 'B365D', 'B365A']
    
    
    #umbennen und saison +  spieltag hinzufügen und mergen
    
    df_quoten = df_quoten.replace({'Dortmund':'Borussia Dortmund', 'Leipzig':'RB Leipzig', 'Leverkusen':'Bayer 04 Leverkusen', 
                     'Freiburg':'Sport-Club Freiburg', 'Hertha':'Hertha BSC', 'Bayern':'FC Bayern München'
                     , 'Wolfsburg':'VfL Wolfsburg','Köln':'1. FC Köln', 'Augsburg':'FC Augsburg'
                     , 'Frankfurt':'Eintracht Frankfurt', 'Hoffenheim':'TSG 1899 Hoffenheim'
                     , 'Mainz':'1. FSV Mainz 05', 'Gladbach':'Borussia Mönchengladbach'
                     , 'Union':'1. FC Union Berlin', 'Fürth':'SpVgg Greuther Fürth', 'Bochum':'VfL Bochum'
                     ,'Stuttgart':'VfB Stuttgart', 'Bielefeld':'DSC Arminia Bielefeld'
                     , 'Bremen':'SV Werder Bremen', 'Schalke':'FC Schalke 04'})
    
    
    df = db.get_table('master_vereins_id')        
    
    df_quoten = df_quoten.merge(df, left_on = ['Heimmannschaft'], right_on = ['Verein'])
    df_quoten = df_quoten.rename(columns = {'Vereins_ID':'Heimmannschaft_ID'})
    df_quoten = df_quoten.drop('Verein', axis = 1)
    df_quoten = df_quoten.merge(df, left_on = ['Auswärtsmannschaft'], right_on = ['Verein'])
    df_quoten = df_quoten.rename(columns = {'Vereins_ID':'Auswärtsmannschaft_ID'})
    df_quoten = df_quoten.drop('Verein', axis = 1)
    df_quoten = df_quoten.assign(Spieltag = spieltag, Saison = saison)
    
    df_quoten = df_quoten[['Heimmannschaft_ID', 'Heimmannschaft', 'Auswärtsmannschaft_ID', 'Auswärtsmannschaft', 
                           'Saison', 'Spieltag', 'B365H', 'B365D', 'B365A']]
    df_quoten['B365H'] = df_quoten['B365H'].str.replace(",", ".").astype(float)
    df_quoten['B365D'] = df_quoten['B365D'].str.replace(",", ".").astype(float)
    df_quoten['B365A'] = df_quoten['B365A'].str.replace(",", ".").astype(float)
    
    return df_quoten

