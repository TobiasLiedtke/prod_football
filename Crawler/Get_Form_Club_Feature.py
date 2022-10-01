import os
os.chdir('D:/Projects/Football/PROD/Crawler')

import pandas as pd


import Read_Load_Database as db



def get_form(saison, spieltag):
    
    """
    gets the last 5 games categorized outcome as possible feature
    Arguments:
        spieltag: an integer
        saison: string (format: yyyy/yy)
    Returns:
        data frame
    """    
    
    df = db.get_table('bl1_data_ergebnisse_kategorisiert')
    df_all = pd.DataFrame()
    df = df.sort_values(by = ['Saison', 'Spieltag'])      
    seasons = df['Saison'].drop_duplicates()
    
    for s in seasons:
        df_season = df[df['Saison']==s]
        vereine = df_season['Vereins_ID'].drop_duplicates()
        
        for v in vereine:
            df_club = df_season[df_season['Vereins_ID']==v]
            results = df_club['Spiel_Ausgang']
            df_club = df_club.assign(L1=results.values, L2=results.values, L3=results.values, L4 = results, L5 = results)
            df_club['L1'] = df_club['L1'].shift(1)
            df_club['L2'] = df_club['L2'].shift(2)
            df_club['L3'] = df_club['L3'].shift(3)
            df_club['L4'] = df_club['L4'].shift(4)
            df_club['L5'] = df_club['L5'].shift(5)
            
            df_club = df_club.fillna(0)
            
            df_all = df_all.append(df_club)
    df_all = df_all[['Spieltag', 'Vereins_ID', 'Verein', 'Heim', 'Saison', 'L1', 'L2', 'L3', 'L4', 'L5']]  
    df_all = df_all[df_all['Saison']==saison]  
    df_all = df_all[df_all['Spieltag']==spieltag] 
    df_all = df_all.drop_duplicates()
    
    return df_all


def get_form_forcast(saison, spieltag):
    
    """
    gets the last 5 games categorized outcome as possible feature with the knowledge at t
    Arguments:
        spieltag: an integer
        saison: string (format: yyyy/yy)
    Returns:
        data frame
    """        
    df = db.get_table('bl1_data_ergebnisse_kategorisiert')    
    df_all = pd.DataFrame()
    df = df.sort_values(by = ['Saison', 'Spieltag'])      
    seasons = df['Saison'].drop_duplicates()
    
    for s in seasons:
        df_season = df[df['Saison']==s]
        vereine = df_season['Vereins_ID'].drop_duplicates()
        
        for v in vereine:
            df_club = df_season[df_season['Vereins_ID']==v]
            results = df_club['Spiel_Ausgang']
            df_club = df_club.assign(L1_Forecast=results.values, L2_Forecast=results.values,
                                     L3_Forecast=results.values, L4_Forecast = results, L5_Forecast = results)

            df_club['L1_Forecast'] = df_club['L1_Forecast']
            df_club['L2_Forecast'] = df_club['L2_Forecast'].shift(1)
            df_club['L3_Forecast'] = df_club['L3_Forecast'].shift(2)
            df_club['L4_Forecast'] = df_club['L4_Forecast'].shift(3)
            df_club['L5_Forecast'] = df_club['L5_Forecast'].shift(4)
            df_club = df_club.fillna(0)
            df_club = df_club.assign(Spieltag = lambda x: x['Spieltag']+1)
            df_club = df_club[df_club['Spieltag']!=35]

            df_all = df_all.append(df_club)
            
    
    df_all = df_all[['Spieltag', 'Saison', 'Vereins_ID', 'Verein', 'L1_Forecast', 'L2_Forecast', 'L3_Forecast', 
                     'L4_Forecast', 'L5_Forecast']]
    
    df_all = df_all[df_all['Saison']==saison]  
    df_all = df_all[df_all['Spieltag']==spieltag] 
    df_all = df_all.drop_duplicates()
    
    return df_all


def first_gameday(saison, spieltag):
    
    """
    only for the first matchday as prior season is not relevant for form feature
    Arguments:
        spieltag: an integer
        saison: string (format: yyyy/yy)
    Returns:
        data frame
    """      
    
    df = db.get_table('bl1_data_vereine_spielplan') 
    df = df[df['Saison']==saison]

    vereine = df['Vereins_ID'].drop_duplicates()
    
    df_all = pd.DataFrame()
    
    for v in vereine:
        df_club = df[df['Vereins_ID']==v]
        df_club = df_club.assign(L1_Forecast = 0, L2_Forecast = 0, L3_Forecast = 0, L4_Forecast = 0, L5_Forecast = 0)
        df_all = df_all.append(df_club)
        
    df_all = df_all[['Spieltag', 'Saison', 'Vereins_ID', 'Verein', 'L1_Forecast', 'L2_Forecast', 'L3_Forecast', 
                     'L4_Forecast', 'L5_Forecast']]
    df_all = df_all[df_all['Spieltag']==1]
    return df_all



def get_form_pl(saison, spieltag):
    
    """
    gets the last 5 games categorized outcome as possible feature with the knowledge at t
    Arguments:
        spieltag: an integer
        saison: string (format: yyyy/yy)
    Returns:
        data frame
    """   
    
    df = db.get_table('pl_data_ergebnisse_kategorisiert')    
    df_all = pd.DataFrame()
    df = df.sort_values(by = ['Saison', 'Spieltag'])      
    seasons = df['Saison'].drop_duplicates()
    
    for s in seasons:
        df_season = df[df['Saison']==s]
        vereine = df_season['Vereins_ID'].drop_duplicates()
        
        for v in vereine:
            df_club = df_season[df_season['Vereins_ID']==v]
            results = df_club['Spiel_Ausgang']
            df_club = df_club.assign(L1=results.values, L2=results.values, L3=results.values, L4 = results, L5 = results)
            df_club['L1'] = df_club['L1'].shift(1)
            df_club['L2'] = df_club['L2'].shift(2)
            df_club['L3'] = df_club['L3'].shift(3)
            df_club['L4'] = df_club['L4'].shift(4)
            df_club['L5'] = df_club['L5'].shift(5)
            
            df_club = df_club.fillna(0)
            
            df_all = df_all.append(df_club)
    df_all = df_all[['Spieltag', 'Vereins_ID', 'Verein', 'Heim', 'Saison', 'L1', 'L2', 'L3', 'L4', 'L5']]  
    df_all = df_all[df_all['Saison']==saison]  
    df_all = df_all[df_all['Spieltag']==spieltag] 
    df_all = df_all.drop_duplicates()
    
    return df_all


def get_form_forcast_pl(saison, spieltag):

    """
    gets the last 5 games categorized outcome as possible feature
    Arguments:
        spieltag: an integer
        saison: string (format: yyyy/yy)
    Returns:
        data frame
    """    
    
    df = db.get_table('pl_data_ergebnisse_kategorisiert')       
    df_all = pd.DataFrame()
    df = df.sort_values(by = ['Saison', 'Spieltag'])      
    seasons = df['Saison'].drop_duplicates()
    
    for s in seasons:
        df_season = df[df['Saison']==s]
        vereine = df_season['Vereins_ID'].drop_duplicates()
        
        for v in vereine:
            df_club = df_season[df_season['Vereins_ID']==v]
            results = df_club['Spiel_Ausgang']
            df_club = df_club.assign(L1_Forecast=results.values, L2_Forecast=results.values,
                                     L3_Forecast=results.values, L4_Forecast = results, L5_Forecast = results)

            df_club['L1_Forecast'] = df_club['L1_Forecast']
            df_club['L2_Forecast'] = df_club['L2_Forecast'].shift(1)
            df_club['L3_Forecast'] = df_club['L3_Forecast'].shift(2)
            df_club['L4_Forecast'] = df_club['L4_Forecast'].shift(3)
            df_club['L5_Forecast'] = df_club['L5_Forecast'].shift(4)
            df_club = df_club.fillna(0)
            df_club = df_club.assign(Spieltag = lambda x: x['Spieltag']+1)
            df_club = df_club[df_club['Spieltag']!=39]

            df_all = df_all.append(df_club)
            
    
    df_all = df_all[['Spieltag', 'Saison', 'Vereins_ID', 'Verein', 'L1_Forecast', 'L2_Forecast', 'L3_Forecast', 
                     'L4_Forecast', 'L5_Forecast']]
    
    df_all = df_all[df_all['Saison']==saison]  
    df_all = df_all[df_all['Spieltag']==spieltag] 
    df_all = df_all.drop_duplicates()
    
    return df_all


def first_gameday_pl(saison, spieltag):
    
    """
    only for the first matchday as prior season is not relevant for form feature
    Arguments:
        spieltag: an integer
        saison: string (format: yyyy/yy)
    Returns:
        data frame
    """          
    
    df = db.get_table('pl_data_vereine_spielplan') 
    df = df[df['Saison']==saison]

    print(df['Vereins_ID'].drop_duplicates())
    df_all = pd.DataFrame()
    
    for v in df['Vereins_ID'].drop_duplicates():
        df_club = df[df['Vereins_ID']==v]
        df_club = df_club.assign(L1_Forecast = 0, L2_Forecast = 0, L3_Forecast = 0, L4_Forecast = 0, L5_Forecast = 0)
        df_all = df_all.append(df_club)
        
    df_all = df_all[['Spieltag', 'Saison', 'Vereins_ID', 'Verein', 'L1_Forecast', 'L2_Forecast', 'L3_Forecast', 
                     'L4_Forecast', 'L5_Forecast']]
    df_all = df_all[df_all['Spieltag']==1]
    return df_all























