import os
os.chdir('D:/Projects/Football/PROD/Crawler')

#packages and modules
import pandas as pd

#import other files 
import Read_Load_Database as db

def get_club_data_feature_home():
    
    """
    prepares football statisitc data for prediction (average of past 3 games)
    Returns:
        data frame
    """         
    
    df = db.get_table('bl1_data_vereine_data_gov')            
    df_all = pd.DataFrame()
    df = df.sort_values(by = ['Saison', 'Spieltag'])      
    seasons = df['Saison'].drop_duplicates()
    
    for s in seasons:
        df_season = df[df['Saison']==s]
        vereine = df_season['Heimmannschaft_ID'].drop_duplicates()
        
        for v in vereine:
            df_club = df_season[df_season['Heimmannschaft_ID']==v]
            df_club = df_club.assign(Home_Shot_Feature = 0, Home_Shot_On_Goal_Feature = 0, Home_Fouls_Feature = 0, 
                                     Home_Corner_Feature = 0, Home_Yellowcard_Feature = 0, Home_Redcard_Feature = 0)
            df_club = df_club.sort_values(by = ['Spieltag'])
            df_club.index = range(len(df_club))
            for s in range(1, len(df_club)):
    
                if s == 1:
                    df_club.at[df_club.index[s], 'Home_Shot_Feature'] = df_club.at[df_club.index[s-1], 'Schüsse_Heim']   
                    df_club.at[df_club.index[s], 'Home_Shot_On_Goal_Feature'] = df_club.at[df_club.index[s-1], 'Torschüsse_Heim'] 
                    df_club.at[df_club.index[s], 'Home_Fouls_Feature'] = df_club.at[df_club.index[s-1], 'Fouls_Heim'] 
                    df_club.at[df_club.index[s], 'Home_Corner_Feature'] = df_club.at[df_club.index[s-1], 'Ecken_Heim'] 
                    df_club.at[df_club.index[s], 'Home_Yellowcard_Feature'] = df_club.at[df_club.index[s-1], 'Gelbe_Karten_Heim'] 
                    df_club.at[df_club.index[s], 'Home_Redcard_Feature'] = df_club.at[df_club.index[s-1], 'Rote_Karten_Heim'] 
                if s == 2:
                    df_club.at[df_club.index[s], 'Home_Shot_Feature'] = round((df_club.at[df_club.index[s-2], 'Schüsse_Heim'] + df_club.at[df_club.index[s-1], 'Schüsse_Heim'])/2, 2)  
                    df_club.at[df_club.index[s], 'Home_Shot_On_Goal_Feature'] = round((df_club.at[df_club.index[s-2], 'Torschüsse_Heim'] + df_club.at[df_club.index[s-1], 'Torschüsse_Heim'])/2, 2)
                    df_club.at[df_club.index[s], 'Home_Fouls_Feature'] = round((df_club.at[df_club.index[s-2], 'Fouls_Heim'] + df_club.at[df_club.index[s-1], 'Fouls_Heim'])/2, 2)
                    df_club.at[df_club.index[s], 'Home_Corner_Feature'] = round((df_club.at[df_club.index[s-2], 'Ecken_Heim'] + df_club.at[df_club.index[s-1], 'Ecken_Heim'])/2, 2)
                    df_club.at[df_club.index[s], 'Home_Yellowcard_Feature'] = round((df_club.at[df_club.index[s-2], 'Gelbe_Karten_Heim'] + df_club.at[df_club.index[s-1], 'Gelbe_Karten_Heim'])/2, 2)
                    df_club.at[df_club.index[s], 'Home_Redcard_Feature'] = round((df_club.at[df_club.index[s-2], 'Rote_Karten_Heim'] + df_club.at[df_club.index[s-1], 'Rote_Karten_Heim'])/2, 2)
                if s > 2:
                    df_club.at[df_club.index[s], 'Home_Shot_Feature'] = round((df_club.at[df_club.index[s-3], 'Schüsse_Heim'] + df_club.at[df_club.index[s-2], 'Schüsse_Heim'] + df_club.at[df_club.index[s-1], 'Schüsse_Heim']) / 3, 2) 
                    df_club.at[df_club.index[s], 'Home_Shot_On_Goal_Feature'] = round((df_club.at[df_club.index[s-3], 'Torschüsse_Heim'] + df_club.at[df_club.index[s-2], 'Torschüsse_Heim'] + df_club.at[df_club.index[s-1], 'Torschüsse_Heim']) / 3, 2)
                    df_club.at[df_club.index[s], 'Home_Fouls_Feature'] = round((df_club.at[df_club.index[s-3], 'Fouls_Heim'] + df_club.at[df_club.index[s-2], 'Fouls_Heim'] + df_club.at[df_club.index[s-1], 'Fouls_Heim']) / 3, 2)
                    df_club.at[df_club.index[s], 'Home_Corner_Feature'] = round((df_club.at[df_club.index[s-3], 'Ecken_Heim'] + df_club.at[df_club.index[s-2], 'Ecken_Heim'] + df_club.at[df_club.index[s-1], 'Ecken_Heim']) / 3, 2)
                    df_club.at[df_club.index[s], 'Home_Yellowcard_Feature'] = round((df_club.at[df_club.index[s-3], 'Gelbe_Karten_Heim'] + df_club.at[df_club.index[s-2], 'Gelbe_Karten_Heim'] + df_club.at[df_club.index[s-1], 'Gelbe_Karten_Heim']) / 3, 2)
                    df_club.at[df_club.index[s], 'Home_Redcard_Feature'] = round((df_club.at[df_club.index[s-3], 'Rote_Karten_Heim'] + df_club.at[df_club.index[s-2], 'Rote_Karten_Heim'] + df_club.at[df_club.index[s-1], 'Rote_Karten_Heim']) / 3, 2)
                
            df_all = df_all.append(df_club)
            
    df_all = df_all.drop_duplicates()
    
    return df_all

def get_club_data_feature_away(df, saison, spieltag):
    
    """
    prepares football statisitc data for prediction (average of past 3 games)
    Returns:
        data frame
    """  
    
    df_all = pd.DataFrame()
    df = df.sort_values(by = ['Saison', 'Spieltag'])      
    seasons = df['Saison'].drop_duplicates()
    
    for s in seasons:
        df_season = df[df['Saison']==s]
        vereine = df_season['Auswärtsmannschaft_ID'].drop_duplicates()
        
        for v in vereine:
            df_club = df_season[df_season['Auswärtsmannschaft_ID']==v]
            df_club = df_club.sort_values(by = ['Spieltag'])
            
            df_club = df_club.assign(Away_Shot_Feature = 0, Away_Shot_On_Goal_Feature = 0, Away_Fouls_Feature = 0, 
                                     Away_Corner_Feature = 0, Away_Yellowcard_Feature = 0, Away_Redcard_Feature = 0)
            df_club.index = range(len(df_club))

            for s in range(1, len(df_club)):
    
                if s == 1:
                    df_club.at[df_club.index[s], 'Away_Shot_Feature'] = df_club.at[df_club.index[s-1], 'Schüsse_Auswärts']   
                    df_club.at[df_club.index[s], 'Away_Shot_On_Goal_Feature'] = df_club.at[df_club.index[s-1], 'Torschüsse_Auswärts'] 
                    df_club.at[df_club.index[s], 'Away_Fouls_Feature'] = df_club.at[df_club.index[s-1], 'Fouls_Auswärts'] 
                    df_club.at[df_club.index[s], 'Away_Corner_Feature'] = df_club.at[df_club.index[s-1], 'Ecken_Auswärts'] 
                    df_club.at[df_club.index[s], 'Away_Yellowcard_Feature'] = df_club.at[df_club.index[s-1], 'Gelbe_Karten_Auswärts'] 
                    df_club.at[df_club.index[s], 'Away_Redcard_Feature'] = df_club.at[df_club.index[s-1], 'Rote_Karten_Auswärts'] 
                if s == 2:
                    df_club.at[df_club.index[s], 'Away_Shot_Feature'] = round((df_club.at[df_club.index[s-2], 'Schüsse_Auswärts'] + df_club.at[df_club.index[s-1], 'Schüsse_Auswärts'])/2, 2) 
                    df_club.at[df_club.index[s], 'Away_Shot_On_Goal_Feature'] = round((df_club.at[df_club.index[s-2], 'Torschüsse_Auswärts'] + df_club.at[df_club.index[s-1], 'Torschüsse_Auswärts'])/2, 2)
                    df_club.at[df_club.index[s], 'Away_Fouls_Feature'] = round((df_club.at[df_club.index[s-2], 'Fouls_Auswärts'] + df_club.at[df_club.index[s-1], 'Fouls_Auswärts'])/2, 2)
                    df_club.at[df_club.index[s], 'Away_Corner_Feature'] = round((df_club.at[df_club.index[s-2], 'Ecken_Auswärts'] + df_club.at[df_club.index[s-1], 'Ecken_Auswärts'])/2, 2)
                    df_club.at[df_club.index[s], 'Away_Yellowcard_Feature'] = round((df_club.at[df_club.index[s-2], 'Gelbe_Karten_Auswärts'] + df_club.at[df_club.index[s-1], 'Gelbe_Karten_Auswärts'])/2, 2)
                    df_club.at[df_club.index[s], 'Away_Redcard_Feature'] = round((df_club.at[df_club.index[s-2], 'Rote_Karten_Auswärts'] + df_club.at[df_club.index[s-1], 'Rote_Karten_Auswärts'])/2, 2)
                if s > 2:
                    df_club.at[df_club.index[s], 'Away_Shot_Feature'] = round((df_club.at[df_club.index[s-3], 'Schüsse_Auswärts'] + df_club.at[df_club.index[s-2], 'Schüsse_Auswärts'] + df_club.at[df_club.index[s-1], 'Schüsse_Auswärts']) / 3, 2) 
                    df_club.at[df_club.index[s], 'Away_Shot_On_Goal_Feature'] = round((df_club.at[df_club.index[s-3], 'Torschüsse_Auswärts'] + df_club.at[df_club.index[s-2], 'Torschüsse_Auswärts'] + df_club.at[df_club.index[s-1], 'Torschüsse_Auswärts']) / 3, 2)
                    df_club.at[df_club.index[s], 'Away_Fouls_Feature'] = round((df_club.at[df_club.index[s-3], 'Fouls_Auswärts'] + df_club.at[df_club.index[s-2], 'Fouls_Auswärts'] + df_club.at[df_club.index[s-1], 'Fouls_Auswärts']) / 3, 2)
                    df_club.at[df_club.index[s], 'Away_Corner_Feature'] = round((df_club.at[df_club.index[s-3], 'Ecken_Auswärts'] + df_club.at[df_club.index[s-2], 'Ecken_Auswärts'] + df_club.at[df_club.index[s-1], 'Ecken_Auswärts']) / 3, 2)
                    df_club.at[df_club.index[s], 'Away_Yellowcard_Feature'] = round((df_club.at[df_club.index[s-3], 'Gelbe_Karten_Auswärts'] + df_club.at[df_club.index[s-2], 'Gelbe_Karten_Auswärts'] + df_club.at[df_club.index[s-1], 'Gelbe_Karten_Auswärts']) / 3, 2)
                    df_club.at[df_club.index[s], 'Away_Redcard_Feature'] = round((df_club.at[df_club.index[s-3], 'Rote_Karten_Auswärts'] + df_club.at[df_club.index[s-2], 'Rote_Karten_Auswärts'] + df_club.at[df_club.index[s-1], 'Rote_Karten_Auswärts']) / 3, 2)
                
            df_all = df_all.append(df_club)
            
    df_all = df_all.drop_duplicates()
    df_all = df_all[df_all['Saison'] == saison]
    df_all = df_all[df_all['Spieltag'] == spieltag]
    
    return df_all
  


def prepare_upload_home(df):

    """
    prepares football statisitc data for data base upload
    Arguments:
        df: dataframe
    Returns:
        data frame
    """  
    
    df_home = df[['Heimmannschaft_ID', 'Heimmannschaft', 'Saison', 'Spieltag','Home_Shot_Feature', 'Home_Shot_On_Goal_Feature'
                  ,'Home_Fouls_Feature', 'Home_Corner_Feature', 'Home_Yellowcard_Feature', 'Home_Redcard_Feature']]
    df_away = df[['Auswärtsmannschaft_ID', 'Auswärtsmannschaft', 'Saison', 'Spieltag','Away_Shot_Feature', 'Away_Shot_On_Goal_Feature'
                  ,'Away_Fouls_Feature', 'Away_Corner_Feature', 'Away_Yellowcard_Feature', 'Away_Redcard_Feature']]
    
    df_home = df_home.rename(columns={"Heimmannschaft_ID": "Vereins_ID", "Heimmannschaft": "Verein", "Home_Shot_Feature":"Shot_Feature",
                               "Home_Shot_On_Goal_Feature":"Shot_On_Goal_Feature", "Home_Fouls_Feature":"Fouls_Feature",
                               'Home_Corner_Feature':"Corner_Feature", "Home_Yellowcard_Feature":"Yellowcard_Feature", 
                               "Home_Redcard_Feature":"Redcard_Feature"})
    
    df_away = df_away.rename(columns={"Auswärtsmannschaft_ID": "Vereins_ID", "Auswärtsmannschaft": "Verein", "Away_Shot_Feature":"Shot_Feature",
                               "Away_Shot_On_Goal_Feature":"Shot_On_Goal_Feature", "Away_Fouls_Feature":"Fouls_Feature",
                               'Away_Corner_Feature':"Corner_Feature", "Away_Yellowcard_Feature":"Yellowcard_Feature", 
                               "Away_Redcard_Feature":"Redcard_Feature"})      
    df = df_home.append(df_away) 

    return df
      

def get_club_data_feature_home_forecast(saison):
    
    """
    prepares football statisitc data for prediction (average of past 3 games) known at t 
    Returns:
        data frame
    """      
    
    df = db.get_table('bl1_data_vereine_data_gov')

    df = df[df['Saison']==saison]
    df_all = pd.DataFrame()
    df = df.sort_values(by = ['Spieltag'])
    vereine = df['Heimmannschaft_ID'].drop_duplicates()
            
    for v in vereine:
        df_club = df[df['Heimmannschaft_ID']==v]
        df_club = df_club.assign(Home_Shot_Feature = 0, Home_Shot_On_Goal_Feature = 0, Home_Fouls_Feature = 0, 
                                 Home_Corner_Feature = 0, Home_Yellowcard_Feature = 0, Home_Redcard_Feature = 0)
        df_club = df_club.sort_values(by = ['Spieltag'])
        df_club.index = range(len(df_club))
   
        for s in range(1, len(df_club)):

            if s == 1:
                df_club.at[df_club.index[s], 'Home_Shot_Feature'] = df_club.at[df_club.index[s], 'Schüsse_Heim']   
                df_club.at[df_club.index[s], 'Home_Shot_On_Goal_Feature'] = df_club.at[df_club.index[s], 'Torschüsse_Heim'] 
                df_club.at[df_club.index[s], 'Home_Fouls_Feature'] = df_club.at[df_club.index[s], 'Fouls_Heim'] 
                df_club.at[df_club.index[s], 'Home_Corner_Feature'] = df_club.at[df_club.index[s], 'Ecken_Heim'] 
                df_club.at[df_club.index[s], 'Home_Yellowcard_Feature'] = df_club.at[df_club.index[s], 'Gelbe_Karten_Heim'] 
                df_club.at[df_club.index[s], 'Home_Redcard_Feature'] = df_club.at[df_club.index[s], 'Rote_Karten_Heim'] 
            if s == 2:
                df_club.at[df_club.index[s], 'Home_Shot_Feature'] = round((df_club.at[df_club.index[s], 'Schüsse_Heim'] + df_club.at[df_club.index[s-1], 'Schüsse_Heim'])/2, 2)  
                df_club.at[df_club.index[s], 'Home_Shot_On_Goal_Feature'] = round((df_club.at[df_club.index[s], 'Torschüsse_Heim'] + df_club.at[df_club.index[s-1], 'Torschüsse_Heim'])/2, 2)
                df_club.at[df_club.index[s], 'Home_Fouls_Feature'] = round((df_club.at[df_club.index[s], 'Fouls_Heim'] + df_club.at[df_club.index[s-1], 'Fouls_Heim'])/2, 2)
                df_club.at[df_club.index[s], 'Home_Corner_Feature'] = round((df_club.at[df_club.index[s], 'Ecken_Heim'] + df_club.at[df_club.index[s-1], 'Ecken_Heim'])/2, 2)
                df_club.at[df_club.index[s], 'Home_Yellowcard_Feature'] = round((df_club.at[df_club.index[s], 'Gelbe_Karten_Heim'] + df_club.at[df_club.index[s-1], 'Gelbe_Karten_Heim'])/2, 2)
                df_club.at[df_club.index[s], 'Home_Redcard_Feature'] = round((df_club.at[df_club.index[s], 'Rote_Karten_Heim'] + df_club.at[df_club.index[s-1], 'Rote_Karten_Heim'])/2, 2)
            if s > 2:
                df_club.at[df_club.index[s], 'Home_Shot_Feature'] = round((df_club.at[df_club.index[s], 'Schüsse_Heim'] + df_club.at[df_club.index[s-2], 'Schüsse_Heim'] + df_club.at[df_club.index[s-1], 'Schüsse_Heim']) / 3, 2) 
                df_club.at[df_club.index[s], 'Home_Shot_On_Goal_Feature'] = round((df_club.at[df_club.index[s], 'Torschüsse_Heim'] + df_club.at[df_club.index[s-2], 'Torschüsse_Heim'] + df_club.at[df_club.index[s-1], 'Torschüsse_Heim']) / 3, 2)
                df_club.at[df_club.index[s], 'Home_Fouls_Feature'] = round((df_club.at[df_club.index[s], 'Fouls_Heim'] + df_club.at[df_club.index[s-2], 'Fouls_Heim'] + df_club.at[df_club.index[s-1], 'Fouls_Heim']) / 3, 2)
                df_club.at[df_club.index[s], 'Home_Corner_Feature'] = round((df_club.at[df_club.index[s], 'Ecken_Heim'] + df_club.at[df_club.index[s-2], 'Ecken_Heim'] + df_club.at[df_club.index[s-1], 'Ecken_Heim']) / 3, 2)
                df_club.at[df_club.index[s], 'Home_Yellowcard_Feature'] = round((df_club.at[df_club.index[s], 'Gelbe_Karten_Heim'] + df_club.at[df_club.index[s-2], 'Gelbe_Karten_Heim'] + df_club.at[df_club.index[s-1], 'Gelbe_Karten_Heim']) / 3, 2)
                df_club.at[df_club.index[s], 'Home_Redcard_Feature'] = round((df_club.at[df_club.index[s], 'Rote_Karten_Heim'] + df_club.at[df_club.index[s-2], 'Rote_Karten_Heim'] + df_club.at[df_club.index[s-1], 'Rote_Karten_Heim']) / 3, 2)
        
        df_all = df_all.append(df_club)
  
    df_all = df_all.drop_duplicates()
    
    return df_all

def get_club_data_feature_away_forecast(df, saison, spieltag):

    """
    prepares football statisitc data for prediction (average of past 3 games) known at t 
    Returns:
        data frame
    """        
    
    df_all = pd.DataFrame()
    df = df.sort_values(by = ['Spieltag'])      

    vereine = df['Auswärtsmannschaft_ID'].drop_duplicates()
    
    for v in vereine:
        df_club = df[df['Auswärtsmannschaft_ID']==v]
        df_club = df_club.sort_values(by = ['Spieltag'])
        
        df_club = df_club.assign(Away_Shot_Feature = 0, Away_Shot_On_Goal_Feature = 0, Away_Fouls_Feature = 0, 
                                 Away_Corner_Feature = 0, Away_Yellowcard_Feature = 0, Away_Redcard_Feature = 0)
        df_club.index = range(len(df_club))

        for s in range(1, len(df_club)):

            if s == 1:
                df_club.at[df_club.index[s], 'Away_Shot_Feature'] = df_club.at[df_club.index[s], 'Schüsse_Auswärts']   
                df_club.at[df_club.index[s], 'Away_Shot_On_Goal_Feature'] = df_club.at[df_club.index[s], 'Torschüsse_Auswärts'] 
                df_club.at[df_club.index[s], 'Away_Fouls_Feature'] = df_club.at[df_club.index[s], 'Fouls_Auswärts'] 
                df_club.at[df_club.index[s], 'Away_Corner_Feature'] = df_club.at[df_club.index[s], 'Ecken_Auswärts'] 
                df_club.at[df_club.index[s], 'Away_Yellowcard_Feature'] = df_club.at[df_club.index[s], 'Gelbe_Karten_Auswärts'] 
                df_club.at[df_club.index[s], 'Away_Redcard_Feature'] = df_club.at[df_club.index[s], 'Rote_Karten_Auswärts'] 
            if s == 2:
                df_club.at[df_club.index[s], 'Away_Shot_Feature'] = round((df_club.at[df_club.index[s], 'Schüsse_Auswärts'] + df_club.at[df_club.index[s-1], 'Schüsse_Auswärts'])/2, 2) 
                df_club.at[df_club.index[s], 'Away_Shot_On_Goal_Feature'] = round((df_club.at[df_club.index[s], 'Torschüsse_Auswärts'] + df_club.at[df_club.index[s-1], 'Torschüsse_Auswärts'])/2, 2)
                df_club.at[df_club.index[s], 'Away_Fouls_Feature'] = round((df_club.at[df_club.index[s], 'Fouls_Auswärts'] + df_club.at[df_club.index[s-1], 'Fouls_Auswärts'])/2, 2)
                df_club.at[df_club.index[s], 'Away_Corner_Feature'] = round((df_club.at[df_club.index[s], 'Ecken_Auswärts'] + df_club.at[df_club.index[s-1], 'Ecken_Auswärts'])/2, 2)
                df_club.at[df_club.index[s], 'Away_Yellowcard_Feature'] = round((df_club.at[df_club.index[s], 'Gelbe_Karten_Auswärts'] + df_club.at[df_club.index[s-1], 'Gelbe_Karten_Auswärts'])/2, 2)
                df_club.at[df_club.index[s], 'Away_Redcard_Feature'] = round((df_club.at[df_club.index[s], 'Rote_Karten_Auswärts'] + df_club.at[df_club.index[s-1], 'Rote_Karten_Auswärts'])/2, 2)
            if s > 2:
                df_club.at[df_club.index[s], 'Away_Shot_Feature'] = round((df_club.at[df_club.index[s], 'Schüsse_Auswärts'] + df_club.at[df_club.index[s-2], 'Schüsse_Auswärts'] + df_club.at[df_club.index[s-1], 'Schüsse_Auswärts']) / 3, 2) 
                df_club.at[df_club.index[s], 'Away_Shot_On_Goal_Feature'] = round((df_club.at[df_club.index[s], 'Torschüsse_Auswärts'] + df_club.at[df_club.index[s-2], 'Torschüsse_Auswärts'] + df_club.at[df_club.index[s-1], 'Torschüsse_Auswärts']) / 3, 2)
                df_club.at[df_club.index[s], 'Away_Fouls_Feature'] = round((df_club.at[df_club.index[s], 'Fouls_Auswärts'] + df_club.at[df_club.index[s-2], 'Fouls_Auswärts'] + df_club.at[df_club.index[s-1], 'Fouls_Auswärts']) / 3, 2)
                df_club.at[df_club.index[s], 'Away_Corner_Feature'] = round((df_club.at[df_club.index[s], 'Ecken_Auswärts'] + df_club.at[df_club.index[s-2], 'Ecken_Auswärts'] + df_club.at[df_club.index[s-1], 'Ecken_Auswärts']) / 3, 2)
                df_club.at[df_club.index[s], 'Away_Yellowcard_Feature'] = round((df_club.at[df_club.index[s], 'Gelbe_Karten_Auswärts'] + df_club.at[df_club.index[s-2], 'Gelbe_Karten_Auswärts'] + df_club.at[df_club.index[s-1], 'Gelbe_Karten_Auswärts']) / 3, 2)
                df_club.at[df_club.index[s], 'Away_Redcard_Feature'] = round((df_club.at[df_club.index[s], 'Rote_Karten_Auswärts'] + df_club.at[df_club.index[s-2], 'Rote_Karten_Auswärts'] + df_club.at[df_club.index[s-1], 'Rote_Karten_Auswärts']) / 3, 2)
            
        df_all = df_all.append(df_club)
              
    df_all = df_all.drop_duplicates()
    df_all = df_all.assign(Spieltag = lambda x: x['Spieltag']+1)
    df_all = df_all[df_all['Spieltag']!=35]  
    df_all = df_all[df_all['Saison'] == saison]
    df_all = df_all[df_all['Spieltag'] == spieltag]
    
    return df_all
    



def get_club_data_feature_home_pl():
    
    """
    prepares football statisitc data for prediction (average of past 3 games)
    Returns:
        data frame
    """     
    
    df = db.get_table('pl_data_vereine_data_gov') 
            
    df_all = pd.DataFrame()
    df = df.sort_values(by = ['Saison', 'Spieltag'])      
    seasons = df['Saison'].drop_duplicates()
    
    for s in seasons:
        df_season = df[df['Saison']==s]
        vereine = df_season['Heimmannschaft_ID'].drop_duplicates()
        
        for v in vereine:
            df_club = df_season[df_season['Heimmannschaft_ID']==v]
            df_club = df_club.assign(Home_Shot_Feature = 0, Home_Shot_On_Goal_Feature = 0, Home_Fouls_Feature = 0, 
                                     Home_Corner_Feature = 0, Home_Yellowcard_Feature = 0, Home_Redcard_Feature = 0)
            df_club = df_club.sort_values(by = ['Spieltag'])
            df_club.index = range(len(df_club))
            for s in range(1, len(df_club)):
    
                if s == 1:
                    df_club.at[df_club.index[s], 'Home_Shot_Feature'] = df_club.at[df_club.index[s-1], 'Schüsse_Heim']   
                    df_club.at[df_club.index[s], 'Home_Shot_On_Goal_Feature'] = df_club.at[df_club.index[s-1], 'Torschüsse_Heim'] 
                    df_club.at[df_club.index[s], 'Home_Fouls_Feature'] = df_club.at[df_club.index[s-1], 'Fouls_Heim'] 
                    df_club.at[df_club.index[s], 'Home_Corner_Feature'] = df_club.at[df_club.index[s-1], 'Ecken_Heim'] 
                    df_club.at[df_club.index[s], 'Home_Yellowcard_Feature'] = df_club.at[df_club.index[s-1], 'Gelbe_Karten_Heim'] 
                    df_club.at[df_club.index[s], 'Home_Redcard_Feature'] = df_club.at[df_club.index[s-1], 'Rote_Karten_Heim'] 
                if s == 2:
                    df_club.at[df_club.index[s], 'Home_Shot_Feature'] = round((df_club.at[df_club.index[s-2], 'Schüsse_Heim'] + df_club.at[df_club.index[s-1], 'Schüsse_Heim'])/2, 2)  
                    df_club.at[df_club.index[s], 'Home_Shot_On_Goal_Feature'] = round((df_club.at[df_club.index[s-2], 'Torschüsse_Heim'] + df_club.at[df_club.index[s-1], 'Torschüsse_Heim'])/2, 2)
                    df_club.at[df_club.index[s], 'Home_Fouls_Feature'] = round((df_club.at[df_club.index[s-2], 'Fouls_Heim'] + df_club.at[df_club.index[s-1], 'Fouls_Heim'])/2, 2)
                    df_club.at[df_club.index[s], 'Home_Corner_Feature'] = round((df_club.at[df_club.index[s-2], 'Ecken_Heim'] + df_club.at[df_club.index[s-1], 'Ecken_Heim'])/2, 2)
                    df_club.at[df_club.index[s], 'Home_Yellowcard_Feature'] = round((df_club.at[df_club.index[s-2], 'Gelbe_Karten_Heim'] + df_club.at[df_club.index[s-1], 'Gelbe_Karten_Heim'])/2, 2)
                    df_club.at[df_club.index[s], 'Home_Redcard_Feature'] = round((df_club.at[df_club.index[s-2], 'Rote_Karten_Heim'] + df_club.at[df_club.index[s-1], 'Rote_Karten_Heim'])/2, 2)
                if s > 2:
                    df_club.at[df_club.index[s], 'Home_Shot_Feature'] = round((df_club.at[df_club.index[s-3], 'Schüsse_Heim'] + df_club.at[df_club.index[s-2], 'Schüsse_Heim'] + df_club.at[df_club.index[s-1], 'Schüsse_Heim']) / 3, 2) 
                    df_club.at[df_club.index[s], 'Home_Shot_On_Goal_Feature'] = round((df_club.at[df_club.index[s-3], 'Torschüsse_Heim'] + df_club.at[df_club.index[s-2], 'Torschüsse_Heim'] + df_club.at[df_club.index[s-1], 'Torschüsse_Heim']) / 3, 2)
                    df_club.at[df_club.index[s], 'Home_Fouls_Feature'] = round((df_club.at[df_club.index[s-3], 'Fouls_Heim'] + df_club.at[df_club.index[s-2], 'Fouls_Heim'] + df_club.at[df_club.index[s-1], 'Fouls_Heim']) / 3, 2)
                    df_club.at[df_club.index[s], 'Home_Corner_Feature'] = round((df_club.at[df_club.index[s-3], 'Ecken_Heim'] + df_club.at[df_club.index[s-2], 'Ecken_Heim'] + df_club.at[df_club.index[s-1], 'Ecken_Heim']) / 3, 2)
                    df_club.at[df_club.index[s], 'Home_Yellowcard_Feature'] = round((df_club.at[df_club.index[s-3], 'Gelbe_Karten_Heim'] + df_club.at[df_club.index[s-2], 'Gelbe_Karten_Heim'] + df_club.at[df_club.index[s-1], 'Gelbe_Karten_Heim']) / 3, 2)
                    df_club.at[df_club.index[s], 'Home_Redcard_Feature'] = round((df_club.at[df_club.index[s-3], 'Rote_Karten_Heim'] + df_club.at[df_club.index[s-2], 'Rote_Karten_Heim'] + df_club.at[df_club.index[s-1], 'Rote_Karten_Heim']) / 3, 2)
                
            df_all = df_all.append(df_club)
            
    df_all = df_all.drop_duplicates()
    
    return df_all

def get_club_data_feature_away_pl(df, saison, spieltag):
    
    """
    prepares football statisitc data for prediction (average of past 3 games)
    Returns:
        data frame
    """       
    
    df_all = pd.DataFrame()
    df = df.sort_values(by = ['Saison', 'Spieltag'])      
    seasons = df['Saison'].drop_duplicates()
    
    for s in seasons:
        df_season = df[df['Saison']==s]
        vereine = df_season['Auswärtsmannschaft_ID'].drop_duplicates()
        
        for v in vereine:
            df_club = df_season[df_season['Auswärtsmannschaft_ID']==v]
            df_club = df_club.sort_values(by = ['Spieltag'])
            
            df_club = df_club.assign(Away_Shot_Feature = 0, Away_Shot_On_Goal_Feature = 0, Away_Fouls_Feature = 0, 
                                     Away_Corner_Feature = 0, Away_Yellowcard_Feature = 0, Away_Redcard_Feature = 0)
            df_club.index = range(len(df_club))

            for s in range(1, len(df_club)):
    
                if s == 1:
                    df_club.at[df_club.index[s], 'Away_Shot_Feature'] = df_club.at[df_club.index[s-1], 'Schüsse_Auswärts']   
                    df_club.at[df_club.index[s], 'Away_Shot_On_Goal_Feature'] = df_club.at[df_club.index[s-1], 'Torschüsse_Auswärts'] 
                    df_club.at[df_club.index[s], 'Away_Fouls_Feature'] = df_club.at[df_club.index[s-1], 'Fouls_Auswärts'] 
                    df_club.at[df_club.index[s], 'Away_Corner_Feature'] = df_club.at[df_club.index[s-1], 'Ecken_Auswärts'] 
                    df_club.at[df_club.index[s], 'Away_Yellowcard_Feature'] = df_club.at[df_club.index[s-1], 'Gelbe_Karten_Auswärts'] 
                    df_club.at[df_club.index[s], 'Away_Redcard_Feature'] = df_club.at[df_club.index[s-1], 'Rote_Karten_Auswärts'] 
                if s == 2:
                    df_club.at[df_club.index[s], 'Away_Shot_Feature'] = round((df_club.at[df_club.index[s-2], 'Schüsse_Auswärts'] + df_club.at[df_club.index[s-1], 'Schüsse_Auswärts'])/2, 2) 
                    df_club.at[df_club.index[s], 'Away_Shot_On_Goal_Feature'] = round((df_club.at[df_club.index[s-2], 'Torschüsse_Auswärts'] + df_club.at[df_club.index[s-1], 'Torschüsse_Auswärts'])/2, 2)
                    df_club.at[df_club.index[s], 'Away_Fouls_Feature'] = round((df_club.at[df_club.index[s-2], 'Fouls_Auswärts'] + df_club.at[df_club.index[s-1], 'Fouls_Auswärts'])/2, 2)
                    df_club.at[df_club.index[s], 'Away_Corner_Feature'] = round((df_club.at[df_club.index[s-2], 'Ecken_Auswärts'] + df_club.at[df_club.index[s-1], 'Ecken_Auswärts'])/2, 2)
                    df_club.at[df_club.index[s], 'Away_Yellowcard_Feature'] = round((df_club.at[df_club.index[s-2], 'Gelbe_Karten_Auswärts'] + df_club.at[df_club.index[s-1], 'Gelbe_Karten_Auswärts'])/2, 2)
                    df_club.at[df_club.index[s], 'Away_Redcard_Feature'] = round((df_club.at[df_club.index[s-2], 'Rote_Karten_Auswärts'] + df_club.at[df_club.index[s-1], 'Rote_Karten_Auswärts'])/2, 2)
                if s > 2:
                    df_club.at[df_club.index[s], 'Away_Shot_Feature'] = round((df_club.at[df_club.index[s-3], 'Schüsse_Auswärts'] + df_club.at[df_club.index[s-2], 'Schüsse_Auswärts'] + df_club.at[df_club.index[s-1], 'Schüsse_Auswärts']) / 3, 2) 
                    df_club.at[df_club.index[s], 'Away_Shot_On_Goal_Feature'] = round((df_club.at[df_club.index[s-3], 'Torschüsse_Auswärts'] + df_club.at[df_club.index[s-2], 'Torschüsse_Auswärts'] + df_club.at[df_club.index[s-1], 'Torschüsse_Auswärts']) / 3, 2)
                    df_club.at[df_club.index[s], 'Away_Fouls_Feature'] = round((df_club.at[df_club.index[s-3], 'Fouls_Auswärts'] + df_club.at[df_club.index[s-2], 'Fouls_Auswärts'] + df_club.at[df_club.index[s-1], 'Fouls_Auswärts']) / 3, 2)
                    df_club.at[df_club.index[s], 'Away_Corner_Feature'] = round((df_club.at[df_club.index[s-3], 'Ecken_Auswärts'] + df_club.at[df_club.index[s-2], 'Ecken_Auswärts'] + df_club.at[df_club.index[s-1], 'Ecken_Auswärts']) / 3, 2)
                    df_club.at[df_club.index[s], 'Away_Yellowcard_Feature'] = round((df_club.at[df_club.index[s-3], 'Gelbe_Karten_Auswärts'] + df_club.at[df_club.index[s-2], 'Gelbe_Karten_Auswärts'] + df_club.at[df_club.index[s-1], 'Gelbe_Karten_Auswärts']) / 3, 2)
                    df_club.at[df_club.index[s], 'Away_Redcard_Feature'] = round((df_club.at[df_club.index[s-3], 'Rote_Karten_Auswärts'] + df_club.at[df_club.index[s-2], 'Rote_Karten_Auswärts'] + df_club.at[df_club.index[s-1], 'Rote_Karten_Auswärts']) / 3, 2)
                
            df_all = df_all.append(df_club)
            
    df_all = df_all.drop_duplicates()
    df_all = df_all[df_all['Saison'] == saison]
    df_all = df_all[df_all['Spieltag'] == spieltag]
    
    return df_all
  


def prepare_upload_home_pl(df):
    
    """
    prepares football statisitc data for data base upload
    Arguments:
        df: dataframe
    Returns:
        data frame
    """  
    
    df_home = df[['Heimmannschaft_ID', 'Heimmannschaft', 'Saison', 'Spieltag','Home_Shot_Feature', 'Home_Shot_On_Goal_Feature'
                  ,'Home_Fouls_Feature', 'Home_Corner_Feature', 'Home_Yellowcard_Feature', 'Home_Redcard_Feature']]
    df_away = df[['Auswärtsmannschaft_ID', 'Auswärtsmannschaft', 'Saison', 'Spieltag','Away_Shot_Feature', 'Away_Shot_On_Goal_Feature'
                  ,'Away_Fouls_Feature', 'Away_Corner_Feature', 'Away_Yellowcard_Feature', 'Away_Redcard_Feature']]
    
    df_home = df_home.rename(columns={"Heimmannschaft_ID": "Vereins_ID", "Heimmannschaft": "Verein", "Home_Shot_Feature":"Shot_Feature",
                               "Home_Shot_On_Goal_Feature":"Shot_On_Goal_Feature", "Home_Fouls_Feature":"Fouls_Feature",
                               'Home_Corner_Feature':"Corner_Feature", "Home_Yellowcard_Feature":"Yellowcard_Feature", 
                               "Home_Redcard_Feature":"Redcard_Feature"})
    
    df_away = df_away.rename(columns={"Auswärtsmannschaft_ID": "Vereins_ID", "Auswärtsmannschaft": "Verein", "Away_Shot_Feature":"Shot_Feature",
                               "Away_Shot_On_Goal_Feature":"Shot_On_Goal_Feature", "Away_Fouls_Feature":"Fouls_Feature",
                               'Away_Corner_Feature':"Corner_Feature", "Away_Yellowcard_Feature":"Yellowcard_Feature", 
                               "Away_Redcard_Feature":"Redcard_Feature"})      
    df = df_home.append(df_away)
    df = df.sort_values(by = ['Saison', 'Spieltag'])   

    return df
      


def get_club_data_feature_home_forecast_pl(saison):
    
    """
    prepares football statisitc data for prediction (average of past 3 games) known at t 
    Returns:
        data frame
    """     
    
    df = db.get_table('pl_data_vereine_data_gov')

    #df = df[df['Saison']==saison]
    df_all = pd.DataFrame()
    df = df.sort_values(by = ['Spieltag'])
    vereine = df['Heimmannschaft_ID'].drop_duplicates()
            
    for v in vereine:
        df_club = df[df['Heimmannschaft_ID']==v]
        df_club = df_club.assign(Home_Shot_Feature = 0, Home_Shot_On_Goal_Feature = 0, Home_Fouls_Feature = 0, 
                                 Home_Corner_Feature = 0, Home_Yellowcard_Feature = 0, Home_Redcard_Feature = 0)
        df_club = df_club.sort_values(by = ['Spieltag'])
        df_club.index = range(len(df_club))
   
        for s in range(1, len(df_club)):

            if s == 1:
                df_club.at[df_club.index[s], 'Home_Shot_Feature'] = df_club.at[df_club.index[s], 'Schüsse_Heim']   
                df_club.at[df_club.index[s], 'Home_Shot_On_Goal_Feature'] = df_club.at[df_club.index[s], 'Torschüsse_Heim'] 
                df_club.at[df_club.index[s], 'Home_Fouls_Feature'] = df_club.at[df_club.index[s], 'Fouls_Heim'] 
                df_club.at[df_club.index[s], 'Home_Corner_Feature'] = df_club.at[df_club.index[s], 'Ecken_Heim'] 
                df_club.at[df_club.index[s], 'Home_Yellowcard_Feature'] = df_club.at[df_club.index[s], 'Gelbe_Karten_Heim'] 
                df_club.at[df_club.index[s], 'Home_Redcard_Feature'] = df_club.at[df_club.index[s], 'Rote_Karten_Heim'] 
            if s == 2:
                df_club.at[df_club.index[s], 'Home_Shot_Feature'] = round((df_club.at[df_club.index[s], 'Schüsse_Heim'] + df_club.at[df_club.index[s-1], 'Schüsse_Heim'])/2, 2)  
                df_club.at[df_club.index[s], 'Home_Shot_On_Goal_Feature'] = round((df_club.at[df_club.index[s], 'Torschüsse_Heim'] + df_club.at[df_club.index[s-1], 'Torschüsse_Heim'])/2, 2)
                df_club.at[df_club.index[s], 'Home_Fouls_Feature'] = round((df_club.at[df_club.index[s], 'Fouls_Heim'] + df_club.at[df_club.index[s-1], 'Fouls_Heim'])/2, 2)
                df_club.at[df_club.index[s], 'Home_Corner_Feature'] = round((df_club.at[df_club.index[s], 'Ecken_Heim'] + df_club.at[df_club.index[s-1], 'Ecken_Heim'])/2, 2)
                df_club.at[df_club.index[s], 'Home_Yellowcard_Feature'] = round((df_club.at[df_club.index[s], 'Gelbe_Karten_Heim'] + df_club.at[df_club.index[s-1], 'Gelbe_Karten_Heim'])/2, 2)
                df_club.at[df_club.index[s], 'Home_Redcard_Feature'] = round((df_club.at[df_club.index[s], 'Rote_Karten_Heim'] + df_club.at[df_club.index[s-1], 'Rote_Karten_Heim'])/2, 2)
            if s > 2:
                df_club.at[df_club.index[s], 'Home_Shot_Feature'] = round((df_club.at[df_club.index[s], 'Schüsse_Heim'] + df_club.at[df_club.index[s-2], 'Schüsse_Heim'] + df_club.at[df_club.index[s-1], 'Schüsse_Heim']) / 3, 2) 
                df_club.at[df_club.index[s], 'Home_Shot_On_Goal_Feature'] = round((df_club.at[df_club.index[s], 'Torschüsse_Heim'] + df_club.at[df_club.index[s-2], 'Torschüsse_Heim'] + df_club.at[df_club.index[s-1], 'Torschüsse_Heim']) / 3, 2)
                df_club.at[df_club.index[s], 'Home_Fouls_Feature'] = round((df_club.at[df_club.index[s], 'Fouls_Heim'] + df_club.at[df_club.index[s-2], 'Fouls_Heim'] + df_club.at[df_club.index[s-1], 'Fouls_Heim']) / 3, 2)
                df_club.at[df_club.index[s], 'Home_Corner_Feature'] = round((df_club.at[df_club.index[s], 'Ecken_Heim'] + df_club.at[df_club.index[s-2], 'Ecken_Heim'] + df_club.at[df_club.index[s-1], 'Ecken_Heim']) / 3, 2)
                df_club.at[df_club.index[s], 'Home_Yellowcard_Feature'] = round((df_club.at[df_club.index[s], 'Gelbe_Karten_Heim'] + df_club.at[df_club.index[s-2], 'Gelbe_Karten_Heim'] + df_club.at[df_club.index[s-1], 'Gelbe_Karten_Heim']) / 3, 2)
                df_club.at[df_club.index[s], 'Home_Redcard_Feature'] = round((df_club.at[df_club.index[s], 'Rote_Karten_Heim'] + df_club.at[df_club.index[s-2], 'Rote_Karten_Heim'] + df_club.at[df_club.index[s-1], 'Rote_Karten_Heim']) / 3, 2)
        
        df_all = df_all.append(df_club)
  
    df_all = df_all.drop_duplicates()
    
    return df_all

def get_club_data_feature_away_forecast_pl(df, saison, spieltag):

    """
    prepares football statisitc data for data base upload
    Arguments:
        df: dataframe
    Returns:
        data frame
    """      
    
    df_all = pd.DataFrame()
    df = df.sort_values(by = ['Spieltag'])      

    vereine = df['Auswärtsmannschaft_ID'].drop_duplicates()
    
    for v in vereine:
        df_club = df[df['Auswärtsmannschaft_ID']==v]
        df_club = df_club.sort_values(by = ['Spieltag'])
        
        df_club = df_club.assign(Away_Shot_Feature = 0, Away_Shot_On_Goal_Feature = 0, Away_Fouls_Feature = 0, 
                                 Away_Corner_Feature = 0, Away_Yellowcard_Feature = 0, Away_Redcard_Feature = 0)
        df_club.index = range(len(df_club))

        for s in range(1, len(df_club)):

            if s == 1:
                df_club.at[df_club.index[s], 'Away_Shot_Feature'] = df_club.at[df_club.index[s], 'Schüsse_Auswärts']   
                df_club.at[df_club.index[s], 'Away_Shot_On_Goal_Feature'] = df_club.at[df_club.index[s], 'Torschüsse_Auswärts'] 
                df_club.at[df_club.index[s], 'Away_Fouls_Feature'] = df_club.at[df_club.index[s], 'Fouls_Auswärts'] 
                df_club.at[df_club.index[s], 'Away_Corner_Feature'] = df_club.at[df_club.index[s], 'Ecken_Auswärts'] 
                df_club.at[df_club.index[s], 'Away_Yellowcard_Feature'] = df_club.at[df_club.index[s], 'Gelbe_Karten_Auswärts'] 
                df_club.at[df_club.index[s], 'Away_Redcard_Feature'] = df_club.at[df_club.index[s], 'Rote_Karten_Auswärts'] 
            if s == 2:
                df_club.at[df_club.index[s], 'Away_Shot_Feature'] = round((df_club.at[df_club.index[s], 'Schüsse_Auswärts'] + df_club.at[df_club.index[s-1], 'Schüsse_Auswärts'])/2, 2) 
                df_club.at[df_club.index[s], 'Away_Shot_On_Goal_Feature'] = round((df_club.at[df_club.index[s], 'Torschüsse_Auswärts'] + df_club.at[df_club.index[s-1], 'Torschüsse_Auswärts'])/2, 2)
                df_club.at[df_club.index[s], 'Away_Fouls_Feature'] = round((df_club.at[df_club.index[s], 'Fouls_Auswärts'] + df_club.at[df_club.index[s-1], 'Fouls_Auswärts'])/2, 2)
                df_club.at[df_club.index[s], 'Away_Corner_Feature'] = round((df_club.at[df_club.index[s], 'Ecken_Auswärts'] + df_club.at[df_club.index[s-1], 'Ecken_Auswärts'])/2, 2)
                df_club.at[df_club.index[s], 'Away_Yellowcard_Feature'] = round((df_club.at[df_club.index[s], 'Gelbe_Karten_Auswärts'] + df_club.at[df_club.index[s-1], 'Gelbe_Karten_Auswärts'])/2, 2)
                df_club.at[df_club.index[s], 'Away_Redcard_Feature'] = round((df_club.at[df_club.index[s], 'Rote_Karten_Auswärts'] + df_club.at[df_club.index[s-1], 'Rote_Karten_Auswärts'])/2, 2)
            if s > 2:
                df_club.at[df_club.index[s], 'Away_Shot_Feature'] = round((df_club.at[df_club.index[s], 'Schüsse_Auswärts'] + df_club.at[df_club.index[s-2], 'Schüsse_Auswärts'] + df_club.at[df_club.index[s-1], 'Schüsse_Auswärts']) / 3, 2) 
                df_club.at[df_club.index[s], 'Away_Shot_On_Goal_Feature'] = round((df_club.at[df_club.index[s], 'Torschüsse_Auswärts'] + df_club.at[df_club.index[s-2], 'Torschüsse_Auswärts'] + df_club.at[df_club.index[s-1], 'Torschüsse_Auswärts']) / 3, 2)
                df_club.at[df_club.index[s], 'Away_Fouls_Feature'] = round((df_club.at[df_club.index[s], 'Fouls_Auswärts'] + df_club.at[df_club.index[s-2], 'Fouls_Auswärts'] + df_club.at[df_club.index[s-1], 'Fouls_Auswärts']) / 3, 2)
                df_club.at[df_club.index[s], 'Away_Corner_Feature'] = round((df_club.at[df_club.index[s], 'Ecken_Auswärts'] + df_club.at[df_club.index[s-2], 'Ecken_Auswärts'] + df_club.at[df_club.index[s-1], 'Ecken_Auswärts']) / 3, 2)
                df_club.at[df_club.index[s], 'Away_Yellowcard_Feature'] = round((df_club.at[df_club.index[s], 'Gelbe_Karten_Auswärts'] + df_club.at[df_club.index[s-2], 'Gelbe_Karten_Auswärts'] + df_club.at[df_club.index[s-1], 'Gelbe_Karten_Auswärts']) / 3, 2)
                df_club.at[df_club.index[s], 'Away_Redcard_Feature'] = round((df_club.at[df_club.index[s], 'Rote_Karten_Auswärts'] + df_club.at[df_club.index[s-2], 'Rote_Karten_Auswärts'] + df_club.at[df_club.index[s-1], 'Rote_Karten_Auswärts']) / 3, 2)
            
        df_all = df_all.append(df_club)
              
    df_all = df_all.drop_duplicates()
    df_all = df_all.assign(Spieltag = lambda x: x['Spieltag']+1)
    df_all = df_all[df_all['Spieltag']!=39]  
    df_all = df_all[df_all['Saison'] == saison]
    df_all = df_all[df_all['Spieltag'] == spieltag]
    
    return df_all
 
    
def first_gameday_club_data(saison, spieltag):
    
    """
    only for first matchday as statistic of past season are not considered
    Arguments:
        spieltag: an integer
        saison: string (format: yyyy/yy)
    Returns:
        data frame
    """  
    
    df = db.get_table('bl1_data_vereine_spielplan')
    df = df[df['Saison']==saison]
    print(df)
    vereine = df['Vereins_ID'].drop_duplicates()
    
    df_all = pd.DataFrame()
    #must be changed to variables
    for v in vereine:
        df_club = df[df['Vereins_ID']==v]
        df_club = df_club.assign(Shot_Feature = 0, Shot_On_Goal_Feature = 0, Fouls_Feature = 0, Corner_Feature = 0,
                                 Yellowcard_Feature = 0, Redcard_Feature = 0)
        df_all = df_all.append(df_club)
        
    df_all = df_all[['Spieltag', 'Saison', 'Vereins_ID', 'Verein', 'Shot_Feature', 'Shot_On_Goal_Feature', 'Fouls_Feature', 
                     'Corner_Feature', 'Yellowcard_Feature', 'Redcard_Feature']]
    df_all = df_all[df_all['Spieltag']==spieltag]
    return df_all   



def first_gameday_club_data_pl(saison, spieltag):
    
    """
    only for first matchday as statistic of past season are not considered
    Arguments:
        spieltag: an integer
        saison: string (format: yyyy/yy)
    Returns:
        data frame
    """  
    
    df = db.get_table('pl_data_vereine_spielplan')

    df = df[df['Saison']==saison]

    vereine = df['Vereins_ID'].drop_duplicates()
    
    df_all = pd.DataFrame()
    #must be changed to variables
    for v in vereine:
        df_club = df[df['Vereins_ID']==v]
        df_club = df_club.assign(Shot_Feature = 0, Shot_On_Goal_Feature = 0, Fouls_Feature = 0, Corner_Feature = 0,
                                 Yellowcard_Feature = 0, Redcard_Feature = 0)
        df_all = df_all.append(df_club)
        
    df_all = df_all[['Spieltag', 'Saison', 'Vereins_ID', 'Verein', 'Shot_Feature', 'Shot_On_Goal_Feature', 'Fouls_Feature', 
                     'Corner_Feature', 'Yellowcard_Feature', 'Redcard_Feature']]
    df_all = df_all[df_all['Spieltag']==spieltag]
    
    return df_all

