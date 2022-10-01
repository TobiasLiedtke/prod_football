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
 


def get_results(spieltag, saison):
    
    """
    gets results of past bundesliga matchdays, data for "bl1_staging_ergebnisse"
    Arguments:
        spieltag: an integer indicating which matchday
        saison: an saison (format: yyyy/yy), indicating which saison
    Returns:
        data frame with resutls of given matchday and saison 
    """    
     
    season_entered = saison[0:4]+'-'+saison[5:7]
    
    #driver = webdriver.Firefox(executable_path=r'D:\Projects\Football\Database\Crawler_Code\Webdrivers\geckodriver')
    driver = webdriver.Chrome(service=Service('D:\Projects\Football\Database\Crawler_Code\Webdrivers\chromedriver.exe'))
    driver.get('https://www.kicker.de/1-bundesliga/spieltag/'+str(season_entered)+'/'+str(spieltag))
    
    time.sleep(5)
    
    clubs = driver.find_elements("xpath", "//div[@class='kick__v100-gameCell__team__name']")
    results = driver.find_elements("xpath", "//div[@class='kick__v100-scoreBoard__scoreHolder ']")
    halftime_results = driver.find_elements("xpath", "//div[@class='kick__v100-scoreBoard__scoreHolder kick__v100-scoreBoard__scoreHolder--subscore']")
    
    f_1 = t.get_df(results)
    f_2 = t.get_df(clubs)    
    f_3 = t.get_df(halftime_results) 
    
    df_clubs = f_2.df()
    df_clubs.columns = ['Verein']
    df_clubs = df_clubs.iloc[0:18]
    df_clubs = df_clubs.replace({'Bayern München': 'FC Bayern München', 'Bor. Mönchengladbach': 'Borussia Mönchengladbach', 'TSG Hoffenheim': 'TSG 1899 Hoffenheim',
                                 'SC Freiburg': 'Sport-Club Freiburg', 'Werder Bremen': 'SV Werder Bremen',
                                 'Arminia Bielefeld':'DSC Arminia Bielefeld'})
    df_id = db.get_table('master_vereins_id')
     
    df_clubs = df_clubs.merge(df_id, on = 'Verein', how = 'inner')

    df_clubs = df_clubs.assign(Auswärtsmannschaft = 0, Auswärtsmannschaft_ID = 0)
    
    for i in range(1, len(df_clubs)):
        df_clubs.iloc[i-1, 2] = df_clubs.iloc[i, 0]
        df_clubs.iloc[i-1, 3] = df_clubs.iloc[i, 1]  
       
    df_clubs = df_clubs.iloc[::2]  
    df_clubs.index = range(len(df_clubs))
    df_clubs = df_clubs.assign(Spieltag = spieltag, Saison = saison, Jahr = 2021)
    df_clubs = df_clubs.rename(columns={"Verein": "Heimmannschaft", "Vereins_ID": "Heimmannschaft_ID"})      
    df_halftime = f_3.df()
    df_ergebnisse = f_1.df()
    
    df_ergebnisse['Ergebnis'] = df_ergebnisse[0] + df_ergebnisse[1] + df_ergebnisse[2]
    df_halftime['HalbzeitErgebnis'] = df_halftime[0] + df_halftime[1] + df_halftime[2]
    
    df_halftime = df_halftime.drop([0,1,2], axis = 1)
    df_ergebnisse = df_ergebnisse.drop([0,1,2], axis = 1)
 

    df = pd.concat([df_clubs, df_ergebnisse, df_halftime], axis = 1)
   
    driver.quit()

    df = df[['Spieltag', 'Heimmannschaft_ID', 'Heimmannschaft', 'Ergebnis', 'HalbzeitErgebnis', 
                     'Auswärtsmannschaft_ID', 'Auswärtsmannschaft', 'Saison']]

    df = df.drop_duplicates()
    return df
    
              
def categorisation(saison, spieltag):  
    
    """
    categorize results for each bundesliga club result (1 = victory, 0 = draw, -1 = loss) "bl1_data_ergebnisse_kategorisiert"
    Arguments:
        spieltag: an integer indicating which matchday
        saison: an saison (format: yyyy/yy), indicating which saison
    Returns:
        data frame with resutls of given matchday
    """  

    df = db.get_table('bl1_staging_ergebnisse')
    
    df = df[df['Saison']==saison]
    df = df[df['Spieltag']==spieltag]
    
    df = df.drop('HalbzeitErgebnis', axis = 1)
    df_tore_heim = df['Ergebnis'].str.split(':', expand = True)
    df_tore_auswärts = df['Ergebnis'].str.split(':', expand = True)
    df_tore_heim.columns = ['Tore', 'Gegentore']
    df_tore_auswärts.columns = ['Gegentore', 'Tore']
    f1 = t.as_int(df_tore_heim,1)
    f2 = t.as_int(df_tore_auswärts,1)
    df_tore_heim = f1.columns_to_int()
    df_tore_auswärts = f2.columns_to_int()
    df_heim = pd.concat([df_tore_heim, df], axis = 1)
    df_auswärts = pd.concat([df_tore_auswärts, df], axis = 1)
     
    
    df_heim = df_heim.assign(Differenz = lambda x: x['Tore']-x['Gegentore'])
    df_heim['Spiel_Ausgang'] = df_heim['Differenz'].apply(lambda x: '1' if x > 0 else ('-1' if x < 0 else '0'))
    df_heim = df_heim.rename(columns = {'Heimmannschaft_ID':'Vereins_ID', 'Heimmannschaft':'Verein', 'Auswärtsmannschaft_ID':'Gegner_ID', 'Auswärtsmannschaft':'Gegner'})
    df_heim = df_heim.assign(Heim = 1)
    df_heim = df_heim[['Spieltag', 'Vereins_ID', 'Verein', 'Spiel_Ausgang', 'Heim', 'Tore', 'Gegentore', 'Gegner_ID', 'Gegner', 'Saison']]
    
    df_auswärts = df_auswärts.assign(Differenz = lambda x: x['Tore']-x['Gegentore'])
    df_auswärts['Spiel_Ausgang'] = df_auswärts['Differenz'].apply(lambda x: '1' if x > 0 else ('-1' if x < 0 else '0'))
    df_auswärts = df_auswärts.rename(columns = {'Auswärtsmannschaft_ID':'Vereins_ID', 'Auswärtsmannschaft':'Verein', 'Heimmannschaft_ID':'Gegner_ID', 'Heimmannschaft':'Gegner'})
    df_auswärts = df_auswärts.assign(Heim = 0)
    df_auswärts = df_auswärts[['Spieltag', 'Vereins_ID', 'Verein', 'Spiel_Ausgang', 'Heim', 'Tore', 'Gegentore', 'Gegner_ID', 'Gegner', 'Saison']]
        
    df_all = df_heim.append(df_auswärts)
    df_all = df_all[['Spieltag', 'Vereins_ID', 'Verein', 'Spiel_Ausgang', 'Heim', 'Tore', 'Gegentore', 'Gegner_ID', 'Gegner', 'Saison']]
    
    return df_all
 
 
def get_results_premierleague(saison, spieltag):

    """
    gets results of past premier league matchdays, data for "pl_staging_ergebnisse"
    Arguments:
        spieltag: an integer indicating which matchday
        saison: an saison (format: yyyy/yy), indicating which saison
    Returns:
        data frame with resutls of given matchday and saison 
    """  

    season_entered = saison[0:4]+'-'+saison[5:7]
    
    #driver = webdriver.Firefox(executable_path=r'D:\Projects\Football\Database\Crawler_Code\Webdrivers\geckodriver')
    driver = webdriver.Chrome(service=Service('D:\Projects\Football\Database\Crawler_Code\Webdrivers\chromedriver.exe'))

    url = 'https://www.kicker.de/premier-league/spieltag/'+str(season_entered)+'/'+str(spieltag)

    driver.get(url)
    time.sleep(5)
    
    clubs = driver.find_elements("xpath", "//div[@class='kick__v100-gameCell__team__name']")
    results = driver.find_elements("xpath", "//div[@class='kick__v100-scoreBoard__scoreHolder ']")
    halftime_results = driver.find_elements("xpath", "//div[@class='kick__v100-scoreBoard__scoreHolder kick__v100-scoreBoard__scoreHolder--subscore']")

    f_1 = t.get_df(results)
    f_2 = t.get_df(clubs)    
    f_3 = t.get_df(halftime_results) 
    
    df_clubs = f_2.df()
    df_clubs.columns = ['Verein']
    df_clubs = df_clubs.iloc[0:20]
    
    df_id = db.get_table('master_vereins_id')      
    df_clubs = df_clubs.merge(df_id, on = 'Verein', how = 'inner')

    df_clubs = df_clubs.assign(Auswärtsmannschaft = 0, Auswärtsmannschaft_ID = 0)
    
    for i in range(1, len(df_clubs)):
        df_clubs.iloc[i-1, 2] = df_clubs.iloc[i, 0]
        df_clubs.iloc[i-1, 3] = df_clubs.iloc[i, 1]  
        
    df_clubs = df_clubs.iloc[::2]  
    
    df_clubs.index = range(len(df_clubs))
    df_clubs = df_clubs.assign(Spieltag = spieltag, Saison = saison)
    df_clubs = df_clubs.rename(columns={"Verein": "Heimmannschaft", "Vereins_ID": "Heimmannschaft_ID"})    

    df_halftime = f_3.df()
    df_ergebnisse = f_1.df()
    
    df_ergebnisse = df_ergebnisse.iloc[0:10]
    df_ergebnisse['Ergebnis'] = df_ergebnisse[0] + df_ergebnisse[1] + df_ergebnisse[2]
    df_halftime['HalbzeitErgebnis'] = df_halftime[0] + df_halftime[1] + df_halftime[2]
    df_halftime = df_halftime.iloc[0:10]
    
    df_halftime = df_halftime.drop([0,1,2], axis = 1)
    df_ergebnisse = df_ergebnisse.drop([0,1,2], axis = 1)

    df = pd.concat([df_clubs, df_ergebnisse, df_halftime], axis = 1)

    driver.quit()

    df = df[['Spieltag', 'Heimmannschaft_ID', 'Heimmannschaft', 'Ergebnis', 'HalbzeitErgebnis', 
                     'Auswärtsmannschaft_ID', 'Auswärtsmannschaft', 'Saison']]

    df = df.drop_duplicates()

    
    return df
#df = get_results_premierleague('2022/23', 8)    
#df = df.dropna()
#db.upload_local_data_to_database(df, 'pl_staging_ergebnisse')
def categorisation_premier_league(saison, spieltag): 
    
    """
    categorize results for each premierleague club result (1 = victory, 0 = draw, -1 = loss) "bl1_data_ergebnisse_kategorisiert"
    Arguments:
        spieltag: an integer indicating which matchday
        saison: an saison (format: yyyy/yy), indicating which saison
    Returns:
        data frame with resutls of given matchday and saison
    """
                
    df = db.get_table('pl_staging_ergebnisse')
    df = df[df['Saison']==saison]
    df = df[df['Spieltag']==spieltag]
    
    df = df.drop('HalbzeitErgebnis', axis = 1)
    df_tore_heim = df['Ergebnis'].str.split(':', expand = True)
    df_tore_auswärts = df['Ergebnis'].str.split(':', expand = True)
    df_tore_heim.columns = ['Tore', 'Gegentore']
    df_tore_auswärts.columns = ['Gegentore', 'Tore']
    f1 = t.as_int(df_tore_heim, 1)
    f2 = t.as_int(df_tore_auswärts, 1)
    df_tore_heim = f1.columns_to_int()
    df_tore_auswärts = f2.columns_to_int()
    df_heim = pd.concat([df_tore_heim, df], axis = 1)
    df_auswärts = pd.concat([df_tore_auswärts, df], axis = 1)

    df_heim = df_heim.assign(Differenz = lambda x: x['Tore']-x['Gegentore'])
    df_heim['Spiel_Ausgang'] = df_heim['Differenz'].apply(lambda x: '1' if x > 0 else ('-1' if x < 0 else '0'))
    df_heim = df_heim.rename(columns = {'Heimmannschaft_ID':'Vereins_ID', 'Heimmannschaft':'Verein', 'Auswärtsmannschaft_ID':'Gegner_ID', 'Auswärtsmannschaft':'Gegner'})
    df_heim = df_heim.assign(Heim = 1)
    df_heim = df_heim[['Spieltag', 'Vereins_ID', 'Verein', 'Spiel_Ausgang', 'Heim', 'Tore', 'Gegentore', 'Gegner_ID', 'Gegner', 'Saison']]
    
    df_auswärts = df_auswärts.assign(Differenz = lambda x: x['Tore']-x['Gegentore'])
    df_auswärts['Spiel_Ausgang'] = df_auswärts['Differenz'].apply(lambda x: '1' if x > 0 else ('-1' if x < 0 else '0'))
    df_auswärts = df_auswärts.rename(columns = {'Auswärtsmannschaft_ID':'Vereins_ID', 'Auswärtsmannschaft':'Verein', 'Heimmannschaft_ID':'Gegner_ID', 'Heimmannschaft':'Gegner'})
    df_auswärts = df_auswärts.assign(Heim = 0)
    df_auswärts = df_auswärts[['Spieltag', 'Vereins_ID', 'Verein', 'Spiel_Ausgang', 'Heim', 'Tore', 'Gegentore', 'Gegner_ID', 'Gegner','Saison']]
        
    df_all = df_heim.append(df_auswärts)
    df_all = df_all[['Spieltag', 'Vereins_ID', 'Verein', 'Spiel_Ausgang', 'Heim', 'Tore', 'Gegentore', 'Gegner_ID', 'Gegner', 'Saison']]
    
    return df_all
    
#df = categorisation_premier_league('2021/22', 38)  



def get_opponent_statistic_forecast(spieltag, saison):
    
    """
    gets past statistic of wins and losses regarding the opponent of the matchday
    Arguments:
        spieltag: an integer indicating which matchday
        saison: an saison (format: yyyy/yy), indicating which saison
    Returns:
        data frame with opponent statistic
    """
    
    df_ergebnisse_kategorisiert = db.get_table('bl1_data_ergebnisse_kategorisiert')
    df_ergebnisse_kategorisiert = df_ergebnisse_kategorisiert[df_ergebnisse_kategorisiert['Saison']>='2014/15']
    df_ergebnisse_kategorisiert = df_ergebnisse_kategorisiert[['Vereins_ID', 'Gegner_ID', 'Saison', 'Spieltag', 'Spiel_Ausgang']]
    df_all = pd.DataFrame()
    
    for verein in df_ergebnisse_kategorisiert['Vereins_ID'].drop_duplicates():
        df_verein = df_ergebnisse_kategorisiert[df_ergebnisse_kategorisiert['Vereins_ID']==verein]
        
        for gegner in df_verein['Gegner_ID'].drop_duplicates():
            df_verein_gegner = df_verein[df_verein['Gegner_ID']==gegner]
            df_verein_gegner = df_verein_gegner.assign(Statistic_Feature = 1)
            for s in range(len(df_verein_gegner)):
                
                if s > 0:
                    df = df_verein_gegner.iloc[0:s+1, :] 
                    win = len(df[df['Spiel_Ausgang'] == 1])
                    loss = len(df[df['Spiel_Ausgang'] == -1])
                    statistic = round((1 + win)/(1 + loss), 2)
                    df_verein_gegner.iloc[s, 5] = statistic
                else:
                    if df_verein_gegner.iloc[s, 4] == 1:
                        df_verein_gegner.iloc[s, 5] = 2
                    if df_verein_gegner.iloc[s, 4] == -1:
                        df_verein_gegner.iloc[s, 5] = 0.5
                    if df_verein_gegner.iloc[s, 4] == 0:
                        df_verein_gegner.iloc[s, 5] = 0    
                        
            df_all = df_all.append(df_verein_gegner)
    df_all = df_all.drop('Spiel_Ausgang', axis = 1)   
    df_all = df_all[df_all['Saison'] == saison]       
    df_all = df_all[df_all['Spieltag'] == spieltag] 
    return df_all
        

 
def get_opponent_stat(spieltag, saison):

    """
    gets past statistic of wins and losses regarding the opponent of the matchday
    Arguments:
        spieltag: an integer indicating which matchday
        saison: an saison (format: yyyy/yy), indicating which saison
    Returns:
        data frame with opponent statistic
    """
    
    df_ergebnisse_kategorisiert = db.get_table('bl1_data_ergebnisse_kategorisiert')
    df_ergebnisse_kategorisiert = df_ergebnisse_kategorisiert[df_ergebnisse_kategorisiert['Saison']>='2014/15']
    df_ergebnisse_kategorisiert = df_ergebnisse_kategorisiert[['Vereins_ID', 'Gegner_ID', 'Saison', 'Spieltag', 'Spiel_Ausgang']]
    df_all = pd.DataFrame()
    
    for verein in df_ergebnisse_kategorisiert['Vereins_ID'].drop_duplicates():
        df_verein = df_ergebnisse_kategorisiert[df_ergebnisse_kategorisiert['Vereins_ID']==verein]
        
        for gegner in df_verein['Gegner_ID'].drop_duplicates():
            df_verein_gegner = df_verein[df_verein['Gegner_ID']==gegner]
            df_verein_gegner = df_verein_gegner.assign(Statistic_Feature = 1)
            for s in range(len(df_verein_gegner)):
                
                if s > 0:
                    df = df_verein_gegner.iloc[0:s, :] 
                    win = len(df[df['Spiel_Ausgang'] == 1])
                    loss = len(df[df['Spiel_Ausgang'] == -1])
                    statistic = round((1 + win)/(1 + loss), 2)
                    df_verein_gegner.iloc[s, 5] = statistic   
                        
            df_all = df_all.append(df_verein_gegner)
    df_all = df_all.drop('Spiel_Ausgang', axis = 1) 
    df_all = df_all[df_all['Saison'] == saison]       
    df_all = df_all[df_all['Spieltag'] == spieltag] 
    
    return df_all

def get_opponent_statistic_forecast_premierleague(spieltag, saison):

    """
    gets past statistic of wins and losses regarding the opponent of the matchday
    Arguments:
        spieltag: an integer indicating which matchday
        saison: an saison (format: yyyy/yy), indicating which saison
    Returns:
        data frame with opponent statistic
    """
    
    df_ergebnisse_kategorisiert = db.get_table('pl_data_ergebnisse_kategorisiert')
    df_ergebnisse_kategorisiert = df_ergebnisse_kategorisiert[df_ergebnisse_kategorisiert['Saison']>='2014/15']
    df_ergebnisse_kategorisiert = df_ergebnisse_kategorisiert[['Vereins_ID', 'Gegner_ID', 'Saison', 'Spieltag', 'Spiel_Ausgang']]
    df_all = pd.DataFrame()
    
    for verein in df_ergebnisse_kategorisiert['Vereins_ID'].drop_duplicates():
        df_verein = df_ergebnisse_kategorisiert[df_ergebnisse_kategorisiert['Vereins_ID']==verein]
        
        for gegner in df_verein['Gegner_ID'].drop_duplicates():
            df_verein_gegner = df_verein[df_verein['Gegner_ID']==gegner]
            df_verein_gegner = df_verein_gegner.assign(Statistic_Feature = 1)
            for s in range(len(df_verein_gegner)):
                
                if s > 0:
                    df = df_verein_gegner.iloc[0:s+1, :] 
                    win = len(df[df['Spiel_Ausgang'] == 1])
                    loss = len(df[df['Spiel_Ausgang'] == -1])
                    statistic = round((1 + win)/(1 + loss), 2)
                    df_verein_gegner.iloc[s, 5] = statistic
                else:
                    if df_verein_gegner.iloc[s, 4] == 1:
                        df_verein_gegner.iloc[s, 5] = 2
                    if df_verein_gegner.iloc[s, 4] == -1:
                        df_verein_gegner.iloc[s, 5] = 0.5
                    if df_verein_gegner.iloc[s, 4] == 0:
                        df_verein_gegner.iloc[s, 5] = 0    
                        
            df_all = df_all.append(df_verein_gegner)
    df_all = df_all.drop('Spiel_Ausgang', axis = 1)     
    df_all = df_all[df_all['Saison'] == saison]       
    df_all = df_all[df_all['Spieltag'] == spieltag] 
      
    return df_all
        

 
def get_opponent_stat_premierleague(spieltag, saison):

    """
    gets past statistic of wins and losses regarding the opponent of the matchday
    Arguments:
        spieltag: an integer indicating which matchday
        saison: an saison (format: yyyy/yy), indicating which saison
    Returns:
        data frame with opponent statistic
    """
    
    df_ergebnisse_kategorisiert = db.get_table('pl_data_ergebnisse_kategorisiert')
    df_ergebnisse_kategorisiert = df_ergebnisse_kategorisiert[df_ergebnisse_kategorisiert['Saison']>='2014/15']
    df_ergebnisse_kategorisiert = df_ergebnisse_kategorisiert[['Vereins_ID', 'Gegner_ID', 'Saison', 'Spieltag', 'Spiel_Ausgang']]
    df_all = pd.DataFrame()
    
    for verein in df_ergebnisse_kategorisiert['Vereins_ID'].drop_duplicates():
        df_verein = df_ergebnisse_kategorisiert[df_ergebnisse_kategorisiert['Vereins_ID']==verein]
        
        for gegner in df_verein['Gegner_ID'].drop_duplicates():
            df_verein_gegner = df_verein[df_verein['Gegner_ID']==gegner]
            df_verein_gegner = df_verein_gegner.assign(Statistic_Feature = 1)
            for s in range(len(df_verein_gegner)):
                
                if s > 0:
                    df = df_verein_gegner.iloc[0:s, :] 
                    win = len(df[df['Spiel_Ausgang'] == 1])
                    loss = len(df[df['Spiel_Ausgang'] == -1])
                    statistic = round((1 + win)/(1 + loss), 2)
                    df_verein_gegner.iloc[s, 5] = statistic   
                        
            df_all = df_all.append(df_verein_gegner)
    df_all = df_all.drop('Spiel_Ausgang', axis = 1)   
    df_all = df_all[df_all['Saison'] == saison]       
    df_all = df_all[df_all['Spieltag'] == spieltag] 
    
    return df_all