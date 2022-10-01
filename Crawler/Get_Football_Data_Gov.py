import os
os.chdir('D:/Projects/Football/PROD/Crawler')

#packages and modules
import pandas as pd


#import other files 
import Read_Load_Database as db


def get_staging_football(spieltag, saison):
    
    """
    gets club statistic data as shot on goal, corners, bookmaker odds 
    Arguments:
        spieltag: an integer
        saison: string (format: yyyy/yy)
    Returns:
        data frame with statistic data per each bundesliga club for a given matchday
    """        
    season_entered = saison[0:4]+'_'+saison[5:7]
    df = pd.read_csv('D:/Projects/Football/Database/'+season_entered+'.csv')
    df = df[['Date', 'HomeTeam', 'AwayTeam', 'FTHG', 'FTAG','HTHG', 'HTAG', 'HS', 
                           'AS', 'HST', 'AST', 'HF', 'AF', 'HC', 'AC', 'HY', 'AY', 'HR', 'AR', 'B365H', 'B365D', 'B365A',
                           'BWH', 'BWD', 'BWA', 'IWH', 'IWD', 'IWA']]
    df['Date'] = pd.to_datetime(df['Date'])
    
    df = df[['Date', 'HomeTeam', 'AwayTeam', 'FTHG', 'FTAG','HTHG', 'HTAG', 'HS', 
                           'AS', 'HST', 'AST', 'HF', 'AF', 'HC', 'AC', 'HY', 'AY', 'HR', 'AR', 'B365H', 'B365D', 'B365A',
                           'BWH', 'BWD', 'BWA', 'IWH', 'IWD', 'IWA']]
    df = df.replace({'Bayern Munich':'FC Bayern München', 'Dortmund': 'Borussia Dortmund'
                     ,'Ein Frankfurt':'Eintracht Frankfurt', 'FC Koln':'1. FC Köln', 'Hannover':'Hannover 96'
                     ,'Hertha': 'Hertha BSC', 'Hoffenheim':'TSG 1899 Hoffenheim', 'M\'gladbach':'Borussia Mönchengladbach'
                     ,'Paderborn':'SC Paderborn 07', 'Augsburg':'FC Augsburg', 'Hamburg':'Hamburger SV'
                     ,'Leverkusen':'Bayer 04 Leverkusen', 'Schalke 04':'FC Schalke 04','Stuttgart':'VfB Stuttgart'
                     ,'Werder Bremen':'SV Werder Bremen','Wolfsburg':'VfL Wolfsburg','Freiburg':'Sport-Club Freiburg'
                     ,'Mainz':'1. FSV Mainz 05','Darmstadt':'SV Darmstadt 98','Ingolstadt':'FC Ingolstadt 04'
                     ,'Fortuna Dusseldorf':'Fortuna Düsseldorf','Nurnberg':'1. FC Nürnberg'
                     ,'Union Berlin':'1. FC Union Berlin', 'Bielefeld':'DSC Arminia Bielefeld'
                     ,'Greuther Furth':'SpVgg Greuther Fürth', 'Bochum':'VfL Bochum'})
    
    df = df.rename(columns={'Date': 'Datum', 'HomeTeam': 'Heimmannschaft', 'AwayTeam':'Auswärtsmannschaft', 'FTHG':'Heimtore'
                            ,'FTAG':'Auswärtstore', 'HTHG':'Heimtore_Halbzeit', 'HTAG':'Auswärtstore_Halbzeit'
                            ,'HS':'Schüsse_Heim', 'AS':'Schüsse_Auswärts', 'HST':'Torschüsse_Heim'
                            , 'AST':'Torschüsse_Auswärts'
                            ,'HF':'Fouls_Heim','AF':'Fouls_Auswärts','HC':'Ecken_Heim','AC':'Ecken_Auswärts'
                            ,'HY':'Gelbe_Karten_Heim','AY':'Gelbe_Karten_Auswärts', 'HR':'Rote_Karten_Heim'
                            ,'AR':'Rote_Karten_Auswärts'})
    
    df = df.assign(Saison = saison)
    
    df['Datum'] = pd.to_datetime(df['Datum'])
    
    df_join = db.get_results('bl1_staging_ergebnisse')
    
    df_join = df_join[df_join['Saison']==saison]
    
    df_all = df.merge(df_join, on = ['Heimmannschaft', 'Auswärtsmannschaft', 'Saison'])
    
    df_all = df_all[df_all['Spieltag']==spieltag]
    print(df_all)
    return df_all

#df = get_staging_football(1, '2022/23')

def get_data(saison, spieltag):
    
    """
    prepares staging data for club statistic data table
    Arguments:
        spieltag: an integer
        saison: string (format: yyyy/yy)
    Returns:
        data frame with statistic data per each bundesliga club for a given matchday
    """      
    
    df_gov = db.get_table('bl1_staging_football_uk') 
    df = df_gov[['Heimmannschaft_ID', 'Heimmannschaft', 'Auswärtsmannschaft_ID', 'Auswärtsmannschaft', 
                           'Saison', 'Spieltag', 'Auswärtstore_Halbzeit', 'Schüsse_Heim', 'Schüsse_Auswärts', 
                           'Torschüsse_Heim', 'Torschüsse_Auswärts', 'Fouls_Heim', 'Fouls_Auswärts', 'Ecken_Heim',
                           'Ecken_Auswärts', 'Gelbe_Karten_Heim', 'Gelbe_Karten_Auswärts',
                           'Rote_Karten_Heim', 'Rote_Karten_Auswärts']]

    df = df[df['Saison']==saison]
    df = df[df['Spieltag']==spieltag]

    return df
    
def get_odds(saison, spieltag):
    
    """
    prepares staging data for club initial odds data 
    Arguments:
        spieltag: an integer
        saison: string (format: yyyy/yy)
    Returns:
        data frame with initial bet365 odds data per each bundesliga club for a given matchday
    """   
    
    df_gov = db.get_table('bl1_staging_football_uk') 
    
    df = df_gov[['Heimmannschaft_ID', 'Heimmannschaft', 'Auswärtsmannschaft_ID', 'Auswärtsmannschaft', 
                'Saison', 'Spieltag', 'B365H', 'B365D', 'B365A', 'BWH', 'BWD', 'BWA', 'IWH', 'IWD', 'IWA']]
    df = df[df['Saison']==saison]
    df = df[df['Spieltag']==spieltag]

    return df



def get_feature_odds(df):
    
    """
    prepares odd data features scheme
    Arguments:
        df: a dataframe
    Returns:
        data frame with initial bet365 odds data per each bundesliga club for a given matchday
    """       
    
    df = df[['Heimmannschaft_ID', 'Heimmannschaft', 'Auswärtsmannschaft_ID', 'Auswärtsmannschaft', 
            'Saison', 'Spieltag', 'B365H', 'B365D', 'B365A']] 
    
    return df




def get_staging_football_pl(spieltag, saison):
    
    """
    gets club statistic data as shot on goal, corners, bookmaker odds 
    Arguments:
        spieltag: an integer
        saison: string (format: yyyy/yy)
    Returns:
        data frame with statistic data per each premier league club for a given matchday
    """       
    season_entered = saison[0:4]+'_'+saison[5:7]
    df = pd.read_csv('D:/Projects/Football/Database/pl_'+season_entered+'.csv')
    df = df[['Date', 'HomeTeam', 'AwayTeam', 'FTHG', 'FTAG','HTHG', 'HTAG', 'HS', 
                           'AS', 'HST', 'AST', 'HF', 'AF', 'HC', 'AC', 'HY', 'AY', 'HR', 'AR', 'B365H', 'B365D', 'B365A',
                           'BWH', 'BWD', 'BWA', 'IWH', 'IWD', 'IWA']]
    
    df['Date'] = pd.to_datetime(df['Date'])
    print(df[['HomeTeam', 'AwayTeam']])
    
    df = df[['Date', 'HomeTeam', 'AwayTeam', 'FTHG', 'FTAG','HTHG', 'HTAG', 'HS', 
                           'AS', 'HST', 'AST', 'HF', 'AF', 'HC', 'AC', 'HY', 'AY', 'HR', 'AR', 'B365H', 'B365D', 'B365A',
                           'BWH', 'BWD', 'BWA', 'IWH', 'IWD', 'IWA']]
    df = df.replace({'Arsenal':'FC Arsenal', 'Leicester':'Leicester City', 'Everton':'FC Everton', 'Man United':'Manchester United'
                     ,'Swansea': 'Swansea City', 'QPR':'Queens Park Rangers', 'Hull':'Hull City', 'Sheffield United':'Sheffield United'
                     ,'Stoke':'Stoke City', 'West Brom':'West Bromwich Albion', 'Sunderland':'AFC Sunderland'
                     ,'West Ham':'West Ham United','Tottenham':'Tottenham Hotspur', 'Wolves':'Wolverhampton Wanderers'
                     ,'Liverpool':'FC Liverpool','Southampton':'FC Southampton','Newcastle':'Newcastle United'
                     ,'Man City':'Manchester City','Burnley':'FC Burnley', 'Huddersfield':'Huddersfield Town'
                     ,'Chelsea':'FC Chelsea', 'Bournemouth':'AFC Bournemouth', 'Brighton':'Brighton & Hove Albion'
                     ,'Norwich':'Norwich City', 'Watford':'FC Watford', 'Middlesbrough':'FC Middlesbrough'
                     ,'Cardiff':'Cardiff City', 'Fulham':'FC Fulham', 'Leeds':'Leeds United', 'Brentford':'FC Brentford'
                     ,'Fulham':'FC Fulham', 'Nott\'m Forest':'Nottingham Forest'})
    print(df[['HomeTeam', 'AwayTeam']])
    df = df.rename(columns={'Date': 'Datum', 'HomeTeam': 'Heimmannschaft', 'AwayTeam':'Auswärtsmannschaft', 'FTHG':'Heimtore'
                            ,'FTAG':'Auswärtstore', 'HTHG':'Heimtore_Halbzeit', 'HTAG':'Auswärtstore_Halbzeit'
                            ,'HS':'Schüsse_Heim', 'AS':'Schüsse_Auswärts', 'HST':'Torschüsse_Heim'
                            , 'AST':'Torschüsse_Auswärts'
                            ,'HF':'Fouls_Heim','AF':'Fouls_Auswärts','HC':'Ecken_Heim','AC':'Ecken_Auswärts'
                            ,'HY':'Gelbe_Karten_Heim','AY':'Gelbe_Karten_Auswärts', 'HR':'Rote_Karten_Heim'
                            ,'AR':'Rote_Karten_Auswärts'})
    df = df.assign(Saison = saison)
    
    df['Datum'] = pd.to_datetime(df['Datum'])
    
    df_join = db.get_results('pl_staging_ergebnisse')
    df_join = df_join[df_join['Saison']==saison]
    df_all = df.merge(df_join, on = ['Heimmannschaft', 'Auswärtsmannschaft', 'Saison'])
    print(df_all[['Heimmannschaft', 'Auswärtsmannschaft']])
    df_all = df_all[df_all['Spieltag']==spieltag]
    print(df_all[['Heimmannschaft', 'Auswärtsmannschaft']])
    return df_all
#df = get_staging_football_pl(1, '2022/23')

def get_data_pl(saison, spieltag):
    
    """
    prepares staging data for club statistic data table
    Arguments:
        spieltag: an integer
        saison: string (format: yyyy/yy)
    Returns:
        data frame with statistic data per each premier club for a given matchday
    """          
    
    df_gov = db.get_table('pl_staging_football_uk') 
    df = df_gov[['Heimmannschaft_ID', 'Heimmannschaft', 'Auswärtsmannschaft_ID', 'Auswärtsmannschaft', 
                           'Saison', 'Spieltag', 'Auswärtstore_Halbzeit', 'Schüsse_Heim', 'Schüsse_Auswärts', 
                           'Torschüsse_Heim', 'Torschüsse_Auswärts', 'Fouls_Heim', 'Fouls_Auswärts', 'Ecken_Heim',
                           'Ecken_Auswärts', 'Gelbe_Karten_Heim', 'Gelbe_Karten_Auswärts',
                           'Rote_Karten_Heim', 'Rote_Karten_Auswärts']]

    df = df[df['Saison']==saison]
    df = df[df['Spieltag']==spieltag]

    return df

    
def get_odds_pl(saison, spieltag):
    
    """
    prepares staging data for club initial odds data 
    Arguments:
        spieltag: an integer
        saison: string (format: yyyy/yy)
    Returns:
        data frame with initial bet365 odds data per each premierleague club for a given matchday
    """   
    
    df_gov = db.get_table('pl_staging_football_uk') 
    
    df = df_gov[['Heimmannschaft_ID', 'Heimmannschaft', 'Auswärtsmannschaft_ID', 'Auswärtsmannschaft', 
                'Saison', 'Spieltag', 'B365H', 'B365D', 'B365A', 'BWH', 'BWD', 'BWA', 'IWH', 'IWD', 'IWA']]
    df = df[df['Saison']==saison]
    df = df[df['Spieltag']==spieltag]

    return df



def get_feature_odds_pl(saison, spieltag):
    
    """
    prepares odd data features scheme
    Arguments:
        df: a dataframe
    Returns:
        data frame with initial bet365 odds data per each premierleague club for a given matchday
    """ 
    df = db.get_table('pl_data_vereine_bookmaker_odds') 
    df = df[df['Saison']==saison]
    df = df[df['Spieltag']==spieltag]
    df = df[['Heimmannschaft_ID', 'Heimmannschaft', 'Auswärtsmannschaft_ID', 'Auswärtsmannschaft', 
            'Saison', 'Spieltag', 'B365H', 'B365D', 'B365A']] 
    
    return df


saison = '2022/23'
s = 1
#df = get_feature_odds_pl(saison, s)
    
#db.delete_table_saison_spieltag('pl_features_odds', saison, s)
#db.upload_local_data_to_database(df, 'pl_features_odds')
# for s in range(18, 39):


#     df = get_feature_odds_pl(saison, s)
    
#     db.delete_table_saison_spieltag('pl_features_odds', saison, s)
#     db.upload_local_data_to_database(df, 'pl_features_odds')


# spieltag = 16
# df = get_feature_odds_pl(saison, spieltag)

# db.delete_table_saison_spieltag('pl_features_odds', saison, spieltag)
# db.upload_local_data_to_database(df, 'pl_features_odds')


# df = db.get_table('pl_data_vereine_bookmaker_odds') 
# df = df[df['Saison']==saison]
# df = df[df['Spieltag']==spieltag]
# df = df[['Heimmannschaft_ID', 'Heimmannschaft', 'Auswärtsmannschaft_ID', 'Auswärtsmannschaft', 
#         'Saison', 'Spieltag', 'B365H', 'B365D', 'B365A']] 