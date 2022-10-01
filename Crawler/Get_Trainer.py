import os
os.chdir('D:/Projects/Football/PROD/Crawler')

#packages and modules
import pandas as pd
from selenium import webdriver
import time


import Tools_Crawler as t
import Read_Load_Database as db




def get_trainer():

    """
    prepares trainer data 
    Returns:
        data frame
    """    
    
    df = db.get_table('bl1_mapping_trainer')
    #df = df[df['Vereins_ID']==17]
    #print(df)
    list_trainer = df['Webpage']
    list_verein = df['Verein']
    list_verein_id = df['Vereins_ID']
    
    all_length = len(list_trainer)
    df_all = pd.DataFrame()
    
    for i in range(all_length):
        
        v_t = list_trainer[i]
        v = list_verein[i]
        v_id = list_verein_id[i]
        
        driver = webdriver.Firefox(executable_path=r'D:\Projects\Football\Database\Crawler_Code\Webdrivers\geckodriver')
        driver.get(v_t)
        time.sleep(5)
    
        table = driver.find_elements_by_xpath("//table[@class='standard_tabelle']")
        f_t = t.get_df(table)
        df_table = f_t.df().T
        driver.quit()
        
        df_table = df_table[0].str.split(' ', expand = True)
        df_table['Von'] = pd.to_datetime(df_table[0].iloc[1:], dayfirst=True, infer_datetime_format=True)
        df_table['Bis'] = pd.to_datetime(df_table[2].iloc[1:], dayfirst=True, infer_datetime_format=True)
        df_table['Trainer'] = df_table[3].iloc[1:] +' '+ df_table[4].iloc[1:]
        df_table['Geburtstag'] = df_table[5].iloc[1:]
        df_trainer = df_table[['Trainer', 'Von', 'Bis', 'Geburtstag']].dropna()
        df_trainer = df_trainer[df_trainer['Bis']>'2019-08-01']
        df_trainer = df_trainer.assign(Vereins_ID = v_id, Verein = v)
        df_all = df_all.append(df_trainer)
        
    f_replace = t.unify_letters(df_all, 2)
    df_all = f_replace.replace_letters()
    
    return df_all

def get_trainer_premier_league(saison):
    
    """
    prepares trainer data 
    Returns:
        data frame
    """        
    
    df = db.get_table('bl1_mapping_trainer')
    df = df[df['Saison']==saison]

    
    #df = df[df['Vereins_ID']==59]
    #print(df)
    df.index = range(len(df))
    
    #print(df)
    list_trainer = df['Webpage']
    list_verein = df['Verein']
    list_verein_id = df['Vereins_ID']
    
    all_length = len(list_trainer)
    df_all = pd.DataFrame()
    
    for i in range(all_length):
        v_t = list_trainer[i]
        v = list_verein[i]
        print(v)
        v_id = list_verein_id[i]
        driver = webdriver.Firefox(executable_path=r'D:\Projects\Football\Database\Crawler_Code\Webdrivers\geckodriver')
        driver.get(v_t)
        time.sleep(5)
    
        table = driver.find_elements_by_xpath("//table[@class='standard_tabelle']")
        f_t = t.get_df(table)
        df_table = f_t.df().T
        driver.quit()
        
        df_table = df_table[0].str.split(' ', expand = True)
        df_table['Von'] = pd.to_datetime(df_table[0].iloc[1:], dayfirst=True, infer_datetime_format=True)
        df_table['Bis'] = pd.to_datetime(df_table[2].iloc[1:], dayfirst=True, infer_datetime_format=True)
        df_table['Trainer'] = df_table[3].iloc[1:] +' '+ df_table[4].iloc[1:]
        df_table['Geburtstag'] = df_table[5].iloc[1:]
        df_trainer = df_table[['Trainer', 'Von', 'Bis', 'Geburtstag']].dropna()
        df_trainer = df_trainer[df_trainer['Bis']>'2014-01-01']
        df_trainer = df_trainer.assign(Vereins_ID = v_id, Verein = v)
        df_all = df_all.append(df_trainer)
        
    f_replace = t.unify_letters(df_all, 2)
    df_all = f_replace.replace_letters()
    
    return df_all



def new_trainer(df):
    
    """
    checks if new trainer is employed for a club 
    Arguments:
        df: data frame
    Returns:
        data frame
    """        
    
    df_tid = db.get_table('master_trainer_id')

    f_replace = t.unify_letters(df, 2)
    df_trainer = f_replace.replace_letters()

    df_new_trainer = df_trainer.merge(df_tid, on = ['Trainer'], how = 'outer' ,indicator=True).loc[lambda x : x['_merge']=='left_only'][['Trainer']].drop_duplicates()
    df_new_trainer['Trainer_ID'] = range(df_tid.tail(1).iloc[0,0]+1,df_tid.tail(1).iloc[0,0]+len(df_new_trainer)+1)
    df_new_trainer = df_new_trainer[['Trainer_ID', 'Trainer']]
    
    return df_new_trainer



def get_plan(df, spieltag, saison):
    
    """
    assign to each matchday the trainer to the corresponding club
    Arguments:
        spieltag: an integer
        saison: string (format: yyyy/yy)
        df: data frame
    Returns:
        data frame
    """        
    
    df_spielplan = db.get_table('bl1_data_vereine_spielplan')
    df_spielplan = df_spielplan[df_spielplan['Saison']==saison]
    
    df_trainer_id = db.get_table('master_trainer_id')
    
    df_t = df.merge(df_trainer_id, on = 'Trainer', how = 'inner')

    
    df_all = pd.DataFrame()
    trainer = df_t['Trainer_ID'].drop_duplicates()

    
    for trai in trainer:
        df_i = df_t[df_t['Trainer_ID']==trai]
        vereine = df_i['Vereins_ID'].drop_duplicates()
        for v in vereine:
            #df_vt = df_i[df_i['Saison']=='2020/21']
            df_vt = df_i[df_i['Vereins_ID']==v]
            bis = df_vt['Bis'].iloc[0]
            von = df_vt['Von'].iloc[0]
            
            df_splan_verein = df_spielplan[df_spielplan['Vereins_ID']==v]
        
            df_spam = df_splan_verein[df_splan_verein['Datum']>=von]
            df_spam = df_spam[df_spam['Datum']<=bis]
            df_ready = df_spam.merge(df_vt, on = ['Vereins_ID', 'Verein'], how = 'inner')
            df_all = df_all.append(df_ready)
        
    df_all = df_all.sort_values(by = ['Vereins_ID', 'Saison', 'Spieltag'])
    df_all = df_all[['Trainer_ID', 'Trainer', 'Vereins_ID', 'Verein', 'Von', 'Bis', 'Saison', 'Spieltag', 'Jahr', 'Geburtstag']]
    df_all = df_all[df_all['Spieltag']==spieltag]
    df_all = df_all.drop_duplicates()

    return df_all

def get_plan_premier_league(df, spieltag, saison):
    
    """
    assign to each matchday the trainer to the corresponding club
    Arguments:
        spieltag: an integer
        saison: string (format: yyyy/yy)
        df: data frame
    Returns:
        data frame
    """     

    df_spielplan = db.get_table('pl_data_vereine_spielplan')
    df_spielplan = df_spielplan[df_spielplan['Saison']==saison]

    df_trainer_id = db.get_table('master_trainer_id')
    df_t = df.merge(df_trainer_id, on = 'Trainer', how = 'inner')

    df_all = pd.DataFrame()
    trainer = df_t['Trainer_ID'].drop_duplicates()

    
    for trai in trainer:
        df_i = df_t[df_t['Trainer_ID']==trai]
        vereine = df_i['Vereins_ID'].drop_duplicates()
        for v in vereine:

            df_vt = df_i[df_i['Vereins_ID']==v]
            bis = df_vt['Bis'].iloc[0]
            von = df_vt['Von'].iloc[0]

            df_splan_verein = df_spielplan[df_spielplan['Vereins_ID']==v]

            df_spam = df_splan_verein[df_splan_verein['Datum']>=von]
            df_spam = df_spam[df_spam['Datum']<=bis]
            df_ready = df_spam.merge(df_vt, on = ['Vereins_ID', 'Verein'], how = 'inner')

            df_all = df_all.append(df_ready)
            
    df_all = df_all.sort_values(by = ['Vereins_ID', 'Saison', 'Spieltag'])
    df_all = df_all[['Trainer_ID', 'Trainer', 'Vereins_ID', 'Verein', 'Von', 'Bis', 'Saison', 'Spieltag', 'Jahr', 'Geburtstag']]
    df_all = df_all[df_all['Spieltag']==spieltag]
    df_all = df_all[df_all['Saison']==saison]

    df_all = df_all.drop_duplicates()

    return df_all






