import os
os.chdir('D:/Projects/Football/PROD/Crawler')

import http.client
import pandas as pd
import json
from pandas import json_normalize 
import time
import Read_Load_Database as db
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

conn = http.client.HTTPSConnection("v3.football.api-sports.io")

headers = {'x-rapidapi-host': "v3.football.api-sports.io",
    'x-rapidapi-key': "268977abe51f319b2cc3544c793085e7"}


def getFixtures(season, league):
    conn.request("GET", "/fixtures?season="+str(season)+"&league="+str(league), headers=headers)
    res = conn.getresponse()
    data = res.read()
    data = json.loads(data)
    
    df = json_normalize(data['response'])
    df = df[['fixture.id', 'fixture.referee', 'fixture.date', 'fixture.timestamp', 'fixture.periods.first', 'fixture.periods.second',
           'league.id', 'league.season', 'league.round', 'teams.home.id', 'teams.home.name', 'teams.away.id', 'teams.away.name',
           'goals.home', 'goals.away', 'score.halftime.home', 'score.halftime.away', 'score.fulltime.home', 'score.fulltime.away',
           'score.extratime.home', 'score.extratime.away', 'score.penalty.home', 'score.penalty.away']]
    
    return df


def getTeams(season, league):
    searchParameter = "/teams?league="+str(league)+"&season="+str(season)
    conn.request("GET", searchParameter, headers=headers)
    res = conn.getresponse()
    data = res.read()
    data = json.loads(data)
    
    df = json_normalize(data['response'])
    df = df[['team.id', 'team.name']]
    print("done")
    return df



def getSquad(dfTeams, saison):
    
    df_all = pd.DataFrame()
    
    for t in range(len(dfTeams)):
        searchParameter = "/players/squads?team="+str(dfTeams.iloc[t,0])
        conn.request("GET", searchParameter, headers=headers)
        res = conn.getresponse()
        data = res.read()
        data = json.loads(data)
        df = json_normalize(data['response'])
        df = df.drop(['player.photo', 'statistics'], axis = 1)
        df = df.assign(club_id = dfTeams.iloc[t,0], club = dfTeams.iloc[t,1], saison = saison)
        df_all = df_all.append(df)
        print(dfTeams.iloc[t,0])
        time.sleep(10)
        
    return df_all


def getPlayerInfoSquad(dfTeams, saison):
    
    df_all = pd.DataFrame()
    
    for t in range(len(dfTeams)):
        searchParameter = "/players?season="+str(saison)+"&team="+str(dfTeams.iloc[t,0])
        conn.request("GET", searchParameter, headers=headers)
        res = conn.getresponse()
        data = res.read()
        data = json.loads(data)
        df = json_normalize(data['response'])
        df = df.drop(['player.photo', 'statistics'], axis = 1)
        df = df.assign(club_id = dfTeams.iloc[t,0], club = dfTeams.iloc[t,1], saison = saison)
        df_all = df_all.append(df)
        print(dfTeams.iloc[t,0])
        time.sleep(10)
        
    return df_all


def getInjuriesSeason(dfTeams, season):
    
    df_all = pd.DataFrame()
    
    for t in range(len(dfTeams)):
        
        searchParameter = "/players/squads?team="+str(dfTeams.iloc[t,0])+"&season="+str(season)
        conn.request("GET", searchParameter, headers=headers)
        
        res = conn.getresponse()
        data = res.read()
        data = json.loads(data)
        df = json_normalize(data['response'])
        df = df[['player.id', 'player.name', 'player.type', 'player.reason', 'team.id', 'team.name', 'fixture.id',
                 'fixture.date', 'fixture.timestamp', 'league.id', 'league.season']]
        df_all = df_all.append(df)
        print(dfTeams.iloc[t,0])
        time.sleep(10)
        
    return df_all




#db.upload_local_data_to_database(df, "pl_data_fixtures_api")


#dfTeamsPremierLeague = getTeams(2021, 39)
#dfTeamsBundesliga = getTeams(2021, 78)

#dfSquadPremierLeague = getPlayerInfoSquad(dfTeamsPremierLeague, 2021)
#dfSquadBundesliga = getPlayerInfoSquad(dfTeamsBundesliga, 2021)

#db.upload_local_data_to_database(dfSquadPremierLeague, "pl_staging_squad_api")
#db.upload_local_data_to_database(dfSquadBundesliga, "bl1_staging_squad_api")

#dfInjuriesPremierLeague = getInjuriesSeason(dfTeamsPremierLeague, 2021)
#dfInjuriesBundesliga = getInjuriesSeason(dfTeamsBundesliga, 2020)
#db.upload_local_data_to_database(dfInjuriesPremierLeague, "pl_staging_injuries_api")
#db.upload_local_data_to_database(dfInjuriesBundesliga, "bl1_staging_injuries_api")

dfFixturesPremierLeague = getFixtures(2022, 39)
dfFixturesBundesliga = getFixtures(2022, 78)
#db.upload_local_data_to_database(dfFixturesPremierLeague, "pl_staging_fixtures_api")
#db.upload_local_data_to_database(dfFixturesBundesliga, "bl1_staging_fixtures_api")

#db.upload_local_data_to_database(df_new_schiedsrichter, "master_schiedsrichter_id")

