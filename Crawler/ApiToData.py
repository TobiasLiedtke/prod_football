import os
os.chdir('D:/Projects/Football/PROD/Crawler')

#packages and modules
import pandas as pd
import Tools_Crawler as t

#import other files 
import Read_Load_Database as db


def getKaderPremierLeague(df):
    
    df = df[['player.id', 'player.name', 'player.firstname', 'player.lastname','player.birth.date', 'player.height', 'club_id',
             'club', 'player.name',  'saison']]    
    df['Spieler'] = df['player.firstname']+ " " + df['player.lastname']
    f = t.unify_letters(df, 1)
    df = f.replace_letters()
    df = df.replace({'Arsenal':'FC Arsenal', 'Leicester':'Leicester City', 'Everton':'FC Everton'
                     ,'Swansea': 'Swansea City', 'QPR':'Queens Park Rangers', 'Sheffield Utd':'Sheffield United'
                     ,'West Brom':'West Bromwich Albion', 'Sunderland':'AFC Sunderland'
                     ,'West Ham':'West Ham United','Tottenham':'Tottenham Hotspur', 'Wolves':'Wolverhampton Wanderers'
                     ,'Liverpool':'FC Liverpool','Southampton':'FC Southampton','Newcastle':'Newcastle United'
                     ,'Burnley':'FC Burnley', 'Huddersfield':'Huddersfield Town'
                     ,'Chelsea':'FC Chelsea', 'Bournemouth':'AFC Bournemouth', 'Brighton':'Brighton & Hove Albion'
                     ,'Norwich':'Norwich City', 'Watford':'FC Watford', 'Middlesbrough':'FC Middlesbrough'
                     ,'Cardiff':'Cardiff City', 'Fulham':'FC Fulham', 'Leeds':'Leeds United', 'Brentford':'FC Brentford'
                     ,2014:'2014/15', 2015:'2015/16', 2016:'2016/17', 2017:'2017/18', 2018:'2018/19', 2019:'2019/20'
                     ,2020:'2020/21', 2021:'2021/22'})
    print(len(df))
    df = df.rename(columns = {'player.birth.date':'Geburtstag', 'player.height':'Spieler_Größe', 'club_id':'Vereins_ID_Api',
                              'saison':'Saison', 'club':'Verein', 'player.id':'Spieler_ID_Api'})   
    
    df_vereins_id = db.get_table('master_vereins_id')
    df_spieler_id = db.get_table('master_spieler_api')
    df = df.merge(df_vereins_id, on = 'Verein', how = 'inner')
    print(len(df))
    df = df.merge(df_spieler_id, on = 'Spieler_ID_Api', how = 'inner')
    print(len(df))
    df = df[['Verein', 'Vereins_ID', 'Spieler', 'Spieler_ID', 'Geburtstag', 'Spieler_Größe', 'Vereins_ID_Api',
             'Spieler_ID_Api', 'Saison']]
    return df

def getKaderBundesliga(df):
    
    df = df[['player.name', 'player.firstname', 'player.lastname','player.birth.date', 'player.height', 'club_id',
             'club', 'player.name',  'saison']]    
    df['Spieler'] = df['player.firstname']+ " " + df['player.lastname']
    f = t.unify_letters(df, 1)
    df = f.replace_letters()
    df = df.replace({'Bor. Dortmund': 'Borussia Dortmund', 'Bayer Leverkusen': 'Bayer 04 Leverkusen', 
                     'SC Freiburg':'Sport-Club Freiburg', 'Fortuna Dusseldorf':'Fortuna Düsseldorf', 
                     'Union Berlin':'1. FC Union Berlin', 'Bayern Munich':'FC Bayern München', 
                    'SC Paderborn':'SC Paderborn 07', '1.FSV Mainz':'1. FSV Mainz 05', 'FC Nurnberg':'1. FC Nürnberg', 
                    '1899 Hoffenheim':'TSG 1899 Hoffenheim',
                    'Hertha Berlin':'Hertha BSC', 'Werder Bremen':'SV Werder Bremen', 'FC Koln':'1. FC Köln',                    
                    'Borussia Monchengladbach':'Borussia Mönchengladbach', 'VfL BOCHUM':'VfL Bochum', 
                    'FSV Mainz 05':'1. FSV Mainz 05', '1.FC Union Berlin':'1. FC Union Berlin', 
                    'Arminia Bielefeld':'DSC Arminia Bielefeld', 'SpVgg Greuther Furth':'SpVgg Greuther Fürth'
                     ,2014:'2014/15', 2015:'2015/16', 2016:'2016/17', 2017:'2017/18', 2018:'2018/19', 2019:'2019/20'
                     ,2020:'2020/21', 2021:'2021/22'})
    print(len(df))
    df = df.rename(columns = {'player.birth.date':'Geburtstag', 'player.height':'Spieler_Größe', 'club_id':'Vereins_ID_Api',
                              'saison':'Saison', 'club':'Verein', 'player.name':'Spieler_Name_Api'})   
    
    df_vereins_id = db.get_table('master_vereins_id')
    df_spieler_id = db.get_table('master_spieler_api')
    df = df.merge(df_vereins_id, on = 'Verein', how = 'inner')
    print(len(df))
    df = df.merge(df_spieler_id, on = 'Spieler', how = 'inner')
    print(len(df))
    df = df[['Verein', 'Vereins_ID', 'Spieler', 'Spieler_ID', 'Nachname', 'Geburtstag', 'Spieler_Größe', 'Vereins_ID_Api',
             'Spieler_Name_Api', 'Saison']]
    return df

def getNewPlayer(df):
    df['Spieler'] = df['player.firstname']+ " " + df['player.lastname']
    dfPlayerID = df[['Spieler']] 
    f = t.unify_letters(dfPlayerID, 1)
    dfPlayerID = f.replace_letters()
    dfPlayerID = dfPlayerID.drop_duplicates()
    
    df_id = db.get_table('master_spieler_id')
    player_missing = t.get_new_player(dfPlayerID, df_id)  
    player_missing['Spieler_ID'] = range(df_id.tail(1).iloc[0,0]+1,df_id.tail(1).iloc[0,0]+len(player_missing)+1)
    player_missing = player_missing[['Spieler_ID', 'Spieler']]
        
    return player_missing



def getKommendeSpieltage(df, saison):
    
    df = df[df['Saison']==saison]
    df['Spieltag'] = df['Spieltag'].astype(int)
    df = df[['Heimmannschaft_ID', 'Heimmannschaft', 'Auswärtsmannschaft_ID', 'Auswärtsmannschaft', 'Spieltag', 'Saison']]
    
    return df


def getSchiedsrichterID(df):
    
    df['Schiedsrichter'] = df['Schiedsrichter'].str.split(",", expand = True)[0]
    df_schiedsrichter = db.get_table('master_schiedsrichter_id')
    df_new_schiedsrichter = df.merge(df_schiedsrichter, on = ['Schiedsrichter'], how = 'outer' ,indicator=True).loc[lambda x : x['_merge']=='left_only'][['Schiedsrichter']].drop_duplicates()
    df_new_schiedsrichter.index = range(len(df_new_schiedsrichter))
    df_new_schiedsrichter['Schiedsrichter_ID'] = range(df_schiedsrichter.tail(1).iloc[0, 0] + 1, df_schiedsrichter.tail(1).iloc[0, 0] + len(df_new_schiedsrichter) + 1)
    df_new_schiedsrichter.index = range(len(df_new_schiedsrichter))
    
    return df_new_schiedsrichter



def prepareDataUploadPremierLeague(df):

    df['Spieltag'] = df['Spieltag'].str.split("-", expand = True)[1]
    df['Schiedsrichter'] = df['Schiedsrichter'].str.split(",", expand = True)[0]
    df = df.replace({'Arsenal':'FC Arsenal', 'Leicester':'Leicester City', 'Everton':'FC Everton'
                     ,'Swansea': 'Swansea City', 'QPR':'Queens Park Rangers', 'Sheffield Utd':'Sheffield United'
                     ,'West Brom':'West Bromwich Albion', 'Sunderland':'AFC Sunderland'
                     ,'West Ham':'West Ham United','Tottenham':'Tottenham Hotspur', 'Wolves':'Wolverhampton Wanderers'
                     ,'Liverpool':'FC Liverpool','Southampton':'FC Southampton','Newcastle':'Newcastle United'
                     ,'Burnley':'FC Burnley', 'Huddersfield':'Huddersfield Town'
                     ,'Chelsea':'FC Chelsea', 'Bournemouth':'AFC Bournemouth', 'Brighton':'Brighton & Hove Albion'
                     ,'Norwich':'Norwich City', 'Watford':'FC Watford', 'Middlesbrough':'FC Middlesbrough'
                     ,'Cardiff':'Cardiff City', 'Fulham':'FC Fulham', 'Leeds':'Leeds United', 'Brentford':'FC Brentford'
                     ,2014:'2014/15', 2015:'2015/16', 2016:'2016/17', 2017:'2017/18', 2018:'2018/19', 2019:'2019/20'
                     ,2020:'2020/21', 2021:'2021/22', 2022:'2022/23'})

    df_id = db.get_table('master_vereins_id')
    
    df = df.merge(df_id, left_on = 'Heimmannschaft', right_on = 'Verein', how = 'inner')
    df = df.rename(columns = {'Vereins_ID':'Heimmannschaft_ID'})
    df = df.merge(df_id, left_on = 'Auswärtsmannschaft', right_on = 'Verein', how = 'inner')
    df = df.rename(columns = {'Vereins_ID':'Auswärtsmannschaft_ID'})
    df = df.drop(['Verein_x', 'Verein_y', 'Schiedsrichter'], axis = 1)
    df = df.drop_duplicates()
    return df



def prepareDataUploadBundesliga(df):

    df['Spieltag'] = df['Spieltag'].str.split("-", expand = True)[1]
    df['Schiedsrichter'] = df['Schiedsrichter'].str.split(",", expand = True)[0]
    df = df.replace({'Bor. Dortmund': 'Borussia Dortmund', 'Bayer Leverkusen': 'Bayer 04 Leverkusen', 
                     'SC Freiburg':'Sport-Club Freiburg', 'Fortuna Dusseldorf':'Fortuna Düsseldorf', 
                     'Union Berlin':'1. FC Union Berlin', 'Bayern Munich':'FC Bayern München', 
                    'SC Paderborn':'SC Paderborn 07', '1.FSV Mainz':'1. FSV Mainz 05', 'FC Nurnberg':'1. FC Nürnberg', 
                    '1899 Hoffenheim':'TSG 1899 Hoffenheim',
                    'Hertha Berlin':'Hertha BSC', 'Werder Bremen':'SV Werder Bremen', 'FC Koln':'1. FC Köln',                    
                    'Borussia Monchengladbach':'Borussia Mönchengladbach', 'VfL BOCHUM':'VfL Bochum', 
                    'FSV Mainz 05':'1. FSV Mainz 05', '1.FC Union Berlin':'1. FC Union Berlin', 
                    'Arminia Bielefeld':'DSC Arminia Bielefeld', 'SpVgg Greuther Furth':'SpVgg Greuther Fürth'
                     ,2014:'2014/15', 2015:'2015/16', 2016:'2016/17', 2017:'2017/18', 2018:'2018/19', 2019:'2019/20'
                     ,2020:'2020/21', 2021:'2021/22', 2022:'2022/23'})

    df_id = db.get_table('master_vereins_id')
    
    df = df.merge(df_id, left_on = 'Heimmannschaft', right_on = 'Verein', how = 'inner')
    df = df.rename(columns = {'Vereins_ID':'Heimmannschaft_ID'})
    df = df.merge(df_id, left_on = 'Auswärtsmannschaft', right_on = 'Verein', how = 'inner')
    df = df.rename(columns = {'Vereins_ID':'Auswärtsmannschaft_ID'})
    df = df.drop(['Verein_x', 'Verein_y', 'Schiedsrichter'], axis = 1)
    df = df.drop_duplicates()
    return df

def getDataInjuries():
    df = db.get_table('pl_staging_injuries_api')
    df = df.rename(columns = {'player.id':'Spieler_ID_Api', 'player.name':'Spieler_Name_Api', 'fixture.id':'Fixture_ID_Api',
                              'team.name':'Verein', 'league.season':'Saison'})
    df = df.replace({'Arsenal':'FC Arsenal', 'Leicester':'Leicester City', 'Everton':'FC Everton'
                     ,'Swansea': 'Swansea City', 'QPR':'Queens Park Rangers', 'Sheffield Utd':'Sheffield United'
                     ,'West Brom':'West Bromwich Albion', 'Sunderland':'AFC Sunderland'
                     ,'West Ham':'West Ham United','Tottenham':'Tottenham Hotspur', 'Wolves':'Wolverhampton Wanderers'
                     ,'Liverpool':'FC Liverpool','Southampton':'FC Southampton','Newcastle':'Newcastle United'
                     ,'Burnley':'FC Burnley', 'Huddersfield':'Huddersfield Town'
                     ,'Chelsea':'FC Chelsea', 'Bournemouth':'AFC Bournemouth', 'Brighton':'Brighton & Hove Albion'
                     ,'Norwich':'Norwich City', 'Watford':'FC Watford', 'Middlesbrough':'FC Middlesbrough'
                     ,'Cardiff':'Cardiff City', 'Fulham':'FC Fulham', 'Leeds':'Leeds United', 'Brentford':'FC Brentford',
                     2014:'2014/15', 2015:'2015/16', 2016:'2016/17', 2017:'2017/18', 2018:'2018/19', 2019:'2019/20'
                     ,2020:'2020/21', 2021:'2021/22'})
    df = df[['Spieler_ID_Api', 'Spieler_Name_Api', 'Fixture_ID_Api', 'Verein', 'Saison']]
    return df


def getGamePlan(df):

    dfHome = df.drop(['Auswärtsmannschaft', 'Auswärtsmannschaft_ID', 'Fixture_ID_Api'], axis = 1)
    dfAway = df.drop(['Heimmannschaft', 'Heimmannschaft_ID', 'Fixture_ID_Api'], axis = 1)
    dfHome = dfHome.rename(columns = {'Heimmannschaft':'Verein', 'Heimmannschaft_ID':'Vereins_ID', 'Date':'Datum'})
    dfAway = dfAway.rename(columns = {'Auswärtsmannschaft':'Verein', 'Auswärtsmannschaft_ID':'Vereins_ID', 'Date':'Datum'})
    dfAll = dfHome.append(dfAway)
    dfAll = dfAll[dfAll['Spieltag'].isnull()==False]
    dfAll['Spieltag'] = dfAll['Spieltag'].astype(int)
    dfAll = dfAll.sort_values(by=['Saison', 'Spieltag'])
    
    return dfAll

#f = db.get_data_db(72)
#dfPremierLeague = f.get_data()
#df_new_schiedsrichter = getSchiedsrichterID(df)

#db.upload_local_data_to_database(dfPremierLeague, "pl_data_fixtures_api")

dfPremierLeague = db.get_staging_api('pl_staging_fixtures_api')
dfPremierLeague = prepareDataUploadPremierLeague(dfPremierLeague)
dfKommendeSpieltagePremierLeague = getKommendeSpieltage(dfPremierLeague, '2022/23')
dfKommendeSpieltagePremierLeague['Heimmannschaft'].drop_duplicates()

dfGamePlanPremierLeague = getGamePlan(dfPremierLeague)
dfGamePlanPremierLeague = dfGamePlanPremierLeague[dfGamePlanPremierLeague['Saison']=='2022/23']
#db.upload_local_data_to_database(dfKommendeSpieltagePremierLeague, "pl_staging_vereine_kommende_spieltag")

#db.upload_local_data_to_database(dfGamePlanPremierLeague, "pl_data_vereine_spielplan")



dfBundesliga = db.get_staging_api('bl1_staging_fixtures_api')
dfBundesliga = prepareDataUploadBundesliga(dfBundesliga)
dfKommendeSpieltageBundesliga = getKommendeSpieltage(dfBundesliga, '2022/23')
dfKommendeSpieltageBundesliga['Heimmannschaft'].drop_duplicates()
#dfKommendeSpieltageBundesliga = dfKommendeSpieltageBundesliga[dfKommendeSpieltageBundesliga['Spieltag']>17]
dfGamePlanBundesliga = getGamePlan(dfBundesliga)
dfGamePlanBundesliga = dfGamePlanBundesliga[dfGamePlanBundesliga['Saison']=='2022/23']
#db.upload_local_data_to_database(dfKommendeSpieltageBundesliga, "bl1_staging_vereine_kommende_spieltag")
#db.upload_local_data_to_database(dfGamePlanBundesliga, "bl1_data_vereine_spielplan")
#dfSquadBundesliga = db.get_table('bl1_staging_squad_api')
#dfSquadBundesliga.columns
#dfNewPlayer = getNewPlayer(dfSquadBundesliga)
#dfKaderBundesliga = getKaderBundesliga(dfSquadBundesliga)

#db.upload_local_data_to_database(dfKaderBundesliga, "bl1_data_spieler_kader_api")

#df = db.get_table('pl_staging_squad_api')
#dfKaderPremierLeague = getKaderPremierLeague(df)
#dfNewPlayer = getNewPlayer(df)
#db.upload_local_data_to_database(dfNewPlayer, "master_spieler_id")
#db.upload_local_data_to_database(dfKaderPremierLeague, "pl_data_spieler_kader_api")



#dfDataInjuries = getDataInjuries()
#db.upload_local_data_to_database(dfDataInjuries, "pl_data_injuries_api")




def getMasterFixtureApi():
    df = db.get_table('pl_data_fixtures_api')
    df = df[['Saison', 'Spieltag', 'Fixture_ID_Api']]
    df = df.drop_duplicates()
    df = df.sort_values(['Saison', 'Spieltag'])
    return df

#dfMasterFixture = getMasterFixtureApi()
#db.upload_local_data_to_database(dfMasterFixture, "master_fixture_ID_Api")
#df = db.get_table('pl_data_injuries_api')
#df = df.drop_duplicates()
#db.upload_local_data_to_database(df, "pl_data_injuries_api")
