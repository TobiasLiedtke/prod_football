from django.db import models
from sklearn.ensemble import RandomForestClassifier
import pandas as pd
from django.db import connection 
from numpy import array
from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import OneHotEncoder
from numpy import random
import numpy as np


def bundesliga_query_forecast_1():
    
    query = ''' 
        SELECT 
        	s.Saison AS Saison,
        	s.Spieltag AS Spieltag,
        	s.Heimmannschaft_ID AS Heimmannschaft_ID,
        	s.Heimmannschaft AS Heimmannschaft,
        	s.Auswärtsmannschaft_ID AS Gegner_ID,
        	s.Auswärtsmannschaft AS Gegner,
        	(kw.Kaderwert_Million - kw1.Kaderwert_Million) AS Kaderwert_Differenz,
        	(kw.Kaderwert_Per_Spieler_Million - kw1.Kaderwert_Per_Spieler_Million) AS Kaderwert_Per_Spieler_Differenz,
        	(ff.Abwehr - ff2.Abwehr) AS Abwehrdifferenz,
        	(ff.Gesamt - ff2.Gesamt) AS Gesamtdiffferenz,
        	(ff.Angriff - ff2.Angriff) AS Angriffdifferenz,
        	(ff.Mittelfeld - ff2.Mittelfeld) AS Mittelfelddifferenz,
        	(ff.Angriff - ff2.Abwehr) AS Heimangriff_Abwehr_Differenz,
        	(ff2.Angriff - ff.Abwehr) AS Auswärtsangriff_Abwehr_Differenz,
        	ts.Trainer_ID AS Trainer_ID,
        	ts1.Trainer_ID AS Gegner_Trainer_ID,
            o.Statistic_Feature
        FROM
        	bl1_staging_vereine_kommende_spieltag s
        	JOIN bl1_data_vereine_kader_wert kw ON s.Heimmannschaft_ID = kw.Vereins_ID
        		AND s.Saison =  kw.Saison 
        		AND s.Spieltag = kw.Spieltag
        	JOIN bl1_data_vereine_kader_wert kw1 ON s.Auswärtsmannschaft_ID = kw1.Vereins_ID
        		AND s.Saison =  kw1.Saison
        		AND s.Spieltag = kw1.Spieltag
        	JOIN bl1_staging_vereine_fifa_features ff ON s.Heimmannschaft_ID = ff.Vereins_ID
        		AND s.Saison = ff.Saison 
        		AND s.Spieltag = ff.Spieltag
        	JOIN bl1_staging_vereine_fifa_features ff2 ON s.Auswärtsmannschaft_ID = ff2.Vereins_ID
        		AND s.Saison = ff2.Saison
        		AND s.Spieltag = ff2.Spieltag
        	JOIN bl1_data_trainer_spiele ts ON s.Heimmannschaft_ID = ts.Vereins_ID
        		AND s.Saison = ts.Saison
        		AND s.Spieltag = ts.Spieltag
        	JOIN bl1_data_trainer_spiele ts1 ON s.Auswärtsmannschaft_ID = ts1.Vereins_ID
        		AND s.Saison = ts1.Saison
        		AND s.Spieltag = ts1.Spieltag
            inner join 
            	bl1_feature_opponent_statistic_forecast o on s.Heimmannschaft_ID = o.Vereins_ID  
                and s.Saison = o.Saison 
                and s.Spieltag = o.Spieltag+1;'''
    df = pd.read_sql(query, con = connection)               
    return df


def bundesliga_query_forecast_2():

    query = '''
        SELECT 
        	s.Saison AS Saison,
        	s.Spieltag AS Spieltag,
            s.Heimmannschaft,
        	s.Heimmannschaft_ID AS Heimmannschaft_ID,
            s.Auswärtsmannschaft,
        	s.Auswärtsmannschaft_ID AS Gegner_ID,
        	o.B365H,
        	o.B365A,
        	o.B365D,
        	ms.System_ID as HeimSystem,
        	ms1.System_ID as AuswärtsSystem,
        	cl1.Shot_Feature as Home_Shot_Feature,
        	cl1.Shot_On_Goal_Feature as Home_Shot_On_Goal_Feature,
        	cl1.Fouls_Feature as Home_Fouls_Feature,
        	cl1.Corner_Feature as Home_Corner_Feature,
        	cl1.Yellowcard_Feature as Home_Yellowcard_Feature,
        	cl2.Shot_Feature as Away_Shot_Feature,
        	cl2.Shot_On_Goal_Feature as Away_Shot_On_Goal_Feature,
        	cl2.Fouls_Feature as Away_Fouls_Feature,
        	cl2.Corner_Feature as Away_Corner_Feature,
        	cl2.Yellowcard_Feature as Away_Yellowcard_Feature
        FROM
        	bl1_staging_vereine_kommende_spieltag s
        	JOIN bl1_features_odds o ON s.Heimmannschaft_ID = o.Heimmannschaft_ID
        		AND s.Saison = o.Saison
        		AND s.Spieltag = o.Spieltag
        	JOIN bl1_data_vereine_spielsystem vss1 on s.Heimmannschaft_ID = vss1.Vereins_ID  
        		and s.Saison = vss1.Saison 
        		and s.Spieltag = vss1.Spieltag
        	JOIN bl1_data_vereine_spielsystem vss2 on s.Auswärtsmannschaft_ID = vss2.Vereins_ID  
        		and s.Saison = vss2.Saison 
        		and s.Spieltag = vss2.Spieltag
            left outer join 
        		master_system ms on ms.System = vss1.Spiel_System
        	left outer join 
            	master_system ms1 on ms1.System = vss2.Spiel_System
        	inner join 
        		bl1_features_forecast_club_data cl1 on s.Heimmannschaft_ID = cl1.Vereins_ID  and s.Saison = cl1.Saison and s.Spieltag = cl1.Spieltag
        	inner join 
        		bl1_features_forecast_club_data cl2 on s.Auswärtsmannschaft_ID = cl2.Vereins_ID  and s.Saison = cl2.Saison and s.Spieltag = cl2.Spieltag;''' 
    df = pd.read_sql(query, con = connection)   
    return df


def premierleague_query_forecast_1():

    query = ''' 
        SELECT 
        	s.Saison AS Saison,
        	s.Spieltag AS Spieltag,
        	s.Heimmannschaft_ID AS Heimmannschaft_ID,
        	s.Heimmannschaft AS Heimmannschaft,
            s.Auswärtsmannschaft  AS Gegner,
        	s.Auswärtsmannschaft_ID AS Gegner_ID,
        	(kw.Kaderwert_Million - kw1.Kaderwert_Million) AS Kaderwert_Differenz,
        	(kw.Kaderwert_Per_Spieler_Million - kw1.Kaderwert_Per_Spieler_Million) AS Kaderwert_Per_Spieler_Differenz,
        	(ff.Abwehr - ff2.Abwehr) AS Abwehrdifferenz,
        	(ff.Gesamt - ff2.Gesamt) AS Gesamtdiffferenz,
        	(ff.Angriff - ff2.Angriff) AS Angriffdifferenz,
        	(ff.Mittelfeld - ff2.Mittelfeld) AS Mittelfelddifferenz,
        	(ff.Angriff - ff2.Abwehr) AS Heimangriff_Abwehr_Differenz,
        	(ff2.Angriff - ff.Abwehr) AS Auswärtsangriff_Abwehr_Differenz,
        	ts.Trainer_ID AS Trainer_ID,
        	ts1.Trainer_ID AS Gegner_Trainer_ID
            #o.Statistic_Feature
        FROM
        	pl_staging_vereine_kommende_spieltag s
        	JOIN pl_data_vereine_kader_wert kw ON s.Heimmannschaft_ID = kw.Vereins_ID
        		AND s.Saison =  kw.Saison 
        		AND s.Spieltag = kw.Spieltag
        	JOIN pl_data_vereine_kader_wert kw1 ON s.Auswärtsmannschaft_ID = kw1.Vereins_ID
        		AND s.Saison =  kw1.Saison
        		AND s.Spieltag = kw1.Spieltag
        	JOIN pl_staging_vereine_fifa_features ff ON s.Heimmannschaft_ID = ff.Vereins_ID
        		AND s.Saison = ff.Saison 
        		AND s.Spieltag = ff.Spieltag
        	JOIN pl_staging_vereine_fifa_features ff2 ON s.Auswärtsmannschaft_ID = ff2.Vereins_ID
        		AND s.Saison = ff2.Saison
        		AND s.Spieltag = ff2.Spieltag
        	JOIN pl_data_trainer_spiele ts ON s.Heimmannschaft_ID = ts.Vereins_ID
        		AND s.Saison = ts.Saison
        		AND s.Spieltag = ts.Spieltag
        	JOIN pl_data_trainer_spiele ts1 ON s.Auswärtsmannschaft_ID = ts1.Vereins_ID
        		AND s.Saison = ts1.Saison
        		AND s.Spieltag = ts1.Spieltag
            #JOIN 
            #	pl_feature_opponent_statistic_forecast o on s.Heimmannschaft_ID = o.Vereins_ID  
            #    and s.Saison = o.Saison 
            #    and s.Spieltag = o.Spieltag+1
            ;'''    
    df = pd.read_sql(query, con = connection)          
    return df

def premierleague_query_forecast_2():

    query = ''' 
            SELECT 
            	s.Saison AS Saison,
            	s.Spieltag AS Spieltag,
                s.Heimmannschaft,
            	s.Heimmannschaft_ID AS Heimmannschaft_ID,
                s.Auswärtsmannschaft,
               	s.Auswärtsmannschaft_ID AS Gegner_ID,
            	o.B365H,
            	o.B365A,
            	o.B365D,
            	ms.System_ID as HeimSystem,
            	ms1.System_ID as AuswärtsSystem
            	#cl1.Shot_Feature as Home_Shot_Feature,
            	#cl1.Shot_On_Goal_Feature as Shot_On_Goal_Feature,
            	#cl1.Fouls_Feature as Home_Fouls_Feature,
            	#cl1.Corner_Feature as Home_Corner_Feature,
            	#cl1.Yellowcard_Feature as Home_Yellowcard_Feature,
            	#cl1.Redcard_Feature as Home_Redcard_Feature,
            	#cl2.Shot_Feature as Away_Shot_Feature,
            	#cl2.Shot_On_Goal_Feature as Gegner_Shot_On_Goal_Feature,
            	#cl2.Fouls_Feature as Away_Fouls_Feature,
            	#cl2.Corner_Feature as Away_Corner_Feature,
            	#cl2.Yellowcard_Feature as Away_Yellowcard_Feature,
            	#cl2.Redcard_Feature as Away_Redcard_Feature
            FROM
            	pl_staging_vereine_kommende_spieltag s
            	JOIN pl_features_odds o ON s.Heimmannschaft_ID = o.Heimmannschaft_ID
            		AND s.Saison = o.Saison
            		AND s.Spieltag = o.Spieltag
            	JOIN pl_data_vereine_spielsystem vss1 on s.Heimmannschaft_ID = vss1.Vereins_ID  
            		and s.Saison = vss1.Saison 
            		and s.Spieltag = vss1.Spieltag
            	JOIN pl_data_vereine_spielsystem vss2 on s.Auswärtsmannschaft_ID = vss2.Vereins_ID  
            		and s.Saison = vss2.Saison 
            		and s.Spieltag = vss2.Spieltag
            	left outer join 
            		master_system ms on ms.System = vss1.Spiel_System
            	left outer join 
                	master_system ms1 on ms1.System = vss2.Spiel_System
            	#inner join 
            		#pl_features_forecast_club_data cl1 on s.Heimmannschaft_ID = cl1.Vereins_ID 
                    #and s.Saison = cl1.Saison
                    #and s.Spieltag = cl1.Spieltag
            	#inner join 
            		#pl_features_forecast_club_data cl2 on s.Auswärtsmannschaft_ID = cl2.Vereins_ID  
                    #and s.Saison = cl2.Saison 
                    #and s.Spieltag = cl2.Spieltag
                    ;'''    
    df = pd.read_sql(query, con = connection)        
    return df




def bundesliga_query_data_1(): 

    query = ''' 
        select
        	e.Saison,
        	e.Spieltag, 
        	e.Heimmannschaft,
        	e.Heimmannschaft_ID, 
        	e.Auswärtsmannschaft_ID as Gegner_ID, 
            e.Auswärtsmannschaft AS Gegner,
        	k.Spiel_Ausgang, 
        	kw.Kaderwert_Million - kw1.Kaderwert_Million as Kaderwert_Differenz,
        	kw.Kaderwert_Per_Spieler_Million - kw1.Kaderwert_Per_Spieler_Million as Kaderwert_Per_Spieler_Differenz,
        	ff.Abwehr - ff2.Abwehr as Abwehrdifferenz,
        	ff.Gesamt - ff2.Gesamt as Gesamtdiffferenz,
        	ff.Angriff - ff2.Angriff as Angriffdifferenz,
        	ff.Mittelfeld - ff2.Mittelfeld as Mittelfelddifferenz,
        	ff.Angriff - ff2.Abwehr as Heimangriff_Abwehr_Differenz, 
        	ff2.Angriff - ff.Abwehr as Auswärtsangriff_Abwehr_Differenz,
        	ts.Trainer,
        	ts.Trainer_ID,
        	ts1.Trainer_ID as Gegner_Trainer_ID,
            o.Statistic_Feature
        from 
        	bl1_staging_ergebnisse e
        inner join 
        	bl1_data_ergebnisse_kategorisiert k on e.Heimmannschaft_ID = k.Vereins_ID 
        and 
            e.Saison = k.Saison 
        and 
            e.Spieltag = k.Spieltag
        inner join 
        	bl1_data_vereine_kader_wert kw on e.Heimmannschaft_ID = kw.Vereins_ID 
        and 
            e.Saison = kw.Saison 
        and 
            e.Spieltag = kw.Spieltag
        inner join 
        	bl1_data_vereine_kader_wert kw1 on e.Auswärtsmannschaft_ID = kw1.Vereins_ID 
        and 
            e.Saison = kw1.Saison 
        and 
            e.Spieltag = kw1.Spieltag
        inner join 
        	bl1_staging_vereine_fifa_features ff on e.Heimmannschaft_ID = ff.Vereins_ID 
        and 
            e.Saison = ff.Saison 
        and 
            e.Spieltag = ff.Spieltag
        inner join 
        	bl1_staging_vereine_fifa_features ff2 on e.Auswärtsmannschaft_ID = ff2.Vereins_ID 
        and 
            e.Saison = ff2.Saison 
        and 
            e.Spieltag = ff2.Spieltag
        inner join 
        	bl1_data_trainer_spiele ts on e.Heimmannschaft_ID = ts.Vereins_ID 
        and 
            e.Saison = ts.Saison 
        and 
            e.Spieltag = ts.Spieltag
        inner join 
        	bl1_data_trainer_spiele ts1 on e.Auswärtsmannschaft_ID = ts1.Vereins_ID 
        and 
            e.Saison = ts1.Saison and e.Spieltag = ts1.Spieltag
        JOIN 
            bl1_feature_opponent_statistic_forecast o on e.Heimmannschaft_ID = o.Vereins_ID  
            and e.Saison = o.Saison 
            and e.Spieltag = o.Spieltag
            ;'''
    df = pd.read_sql(query, con = connection)          
    return df 

def bundesliga_query_data_2():
    query = ''' 
        select
        	e.Saison,
        	e.Spieltag, 
        	e.Heimmannschaft,
        	e.Heimmannschaft_ID, 
            e.Auswärtsmannschaft,
        	e.Auswärtsmannschaft_ID AS Gegner_ID,
        	o.B365H,
        	o.B365D,
        	o.B365A,
        	ms.System_ID as HeimSystem,
        	ms1.System_ID as AuswärtsSystem,
        	cl1.Shot_Feature as Home_Shot_Feature,
        	cl1.Shot_On_Goal_Feature as Home_Shot_On_Goal_Feature,
        	cl1.Fouls_Feature as Home_Fouls_Feature,
        	cl1.Corner_Feature as Home_Corner_Feature,
        	cl1.Yellowcard_Feature as Home_Yellowcard_Feature,
        	cl1.Redcard_Feature as Home_Redcard_Feature,
        	cl2.Shot_Feature as Away_Shot_Feature,
        	cl2.Shot_On_Goal_Feature as Away_Shot_On_Goal_Feature,
        	cl2.Fouls_Feature as Away_Fouls_Feature,
        	cl2.Corner_Feature as Away_Corner_Feature,
        	cl2.Yellowcard_Feature as Away_Yellowcard_Feature
        from 
        	bl1_staging_ergebnisse e
        inner join 
        	bl1_features_odds o on e.Heimmannschaft_ID = o.Heimmannschaft_ID  
        and 
            e.Saison = o.Saison 
        and 
            e.Spieltag = o.Spieltag
        inner join 
        	bl1_data_vereine_spielsystem vss1 on e.Heimmannschaft_ID = vss1.Vereins_ID  
        and 
            e.Saison = vss1.Saison 
        and 
            e.Spieltag = vss1.Spieltag
        inner join 
        	bl1_data_vereine_spielsystem vss2 on e.Auswärtsmannschaft_ID = vss2.Vereins_ID 
        and 
            e.Saison = vss2.Saison 
        and 
            e.Spieltag = vss2.Spieltag
        left outer join 
        	master_system ms on ms.System = vss1.Spiel_System
        left outer join 
        	master_system ms1 on ms1.System = vss2.Spiel_System
        inner join 
        	bl1_features_club_data cl1 on e.Heimmannschaft_ID = cl1.Vereins_ID  
        and 
            e.Saison = cl1.Saison 
        and 
            e.Spieltag = cl1.Spieltag
        inner join 
        	bl1_features_club_data cl2 on e.Auswärtsmannschaft_ID = cl2.Vereins_ID 
        and 
            e.Saison = cl2.Saison 
        and 
            e.Spieltag = cl2.Spieltag
            ;'''
    df = pd.read_sql(query, con = connection)          
    return df 





def premierleague_query_data_1():

    query = ''' 
            select
            	e.Saison,
            	e.Spieltag, 
            	e.Heimmannschaft,
            	e.Heimmannschaft_ID, 
                e.Auswärtsmannschaft AS Gegner,
                e.Auswärtsmannschaft_ID AS Gegner_ID,
            	k.Spiel_Ausgang, 
            	kw.Kaderwert_Million - kw1.Kaderwert_Million as Kaderwert_Differenz,
            	kw.Kaderwert_Per_Spieler_Million - kw1.Kaderwert_Per_Spieler_Million as Kaderwert_Per_Spieler_Differenz,
            	ff.Abwehr - ff2.Abwehr as Abwehrdifferenz,
            	ff.Gesamt - ff2.Gesamt as Gesamtdiffferenz,
            	ff.Angriff - ff2.Angriff as Angriffdifferenz,
            	ff.Mittelfeld - ff2.Mittelfeld as Mittelfelddifferenz,
            	ff.Angriff - ff2.Abwehr as Heimangriff_Abwehr_Differenz, 
            	ff2.Angriff - ff.Abwehr as Auswärtsangriff_Abwehr_Differenz,
            	ts.Trainer,
            	ts.Trainer_ID,
            	ts1.Trainer_ID as Gegner_Trainer_ID
                #o.Statistic_Feature
            from 
            	pl_staging_ergebnisse e
            inner join 
            	pl_data_ergebnisse_kategorisiert k on e.Heimmannschaft_ID = k.Vereins_ID 
                and e.Saison = k.Saison 
                and e.Spieltag = k.Spieltag
            inner join 
            	pl_data_vereine_kader_wert kw on e.Heimmannschaft_ID = kw.Vereins_ID 
                and e.Saison = kw.Saison 
                and e.Spieltag = kw.Spieltag
            inner join 
            	pl_data_vereine_kader_wert kw1 on e.Auswärtsmannschaft_ID = kw1.Vereins_ID 
                and e.Saison = kw1.Saison 
                and e.Spieltag = kw1.Spieltag
            inner join 
            	pl_staging_vereine_fifa_features ff on e.Heimmannschaft_ID = ff.Vereins_ID 
            and 
                e.Saison = ff.Saison 
            and 
                e.Spieltag = ff.Spieltag
            inner join 
            	pl_staging_vereine_fifa_features ff2 on e.Auswärtsmannschaft_ID = ff2.Vereins_ID 
            and 
                e.Saison = ff2.Saison 
            and 
                e.Spieltag = ff2.Spieltag
            inner join 
            	pl_data_trainer_spiele ts on e.Heimmannschaft_ID = ts.Vereins_ID 
            and 
                e.Saison = ts.Saison 
            and 
                e.Spieltag = ts.Spieltag
            inner join 
            	pl_data_trainer_spiele ts1 on e.Auswärtsmannschaft_ID = ts1.Vereins_ID 
            and 
                e.Saison = ts1.Saison and e.Spieltag = ts1.Spieltag
       # JOIN 
       #     pl_feature_opponent_statistic_forecast o on e.Heimmannschaft_ID = o.Vereins_ID  
       #     and e.Saison = o.Saison 
       #     and e.Spieltag = o.Spieltag;'''    
    df = pd.read_sql(query, con = connection)      
    return df

def premierleague_query_data_2():

    query = ''' 
            select
            	e.Saison,
            	e.Spieltag, 
            	e.Heimmannschaft,
            	e.Heimmannschaft_ID, 
                e.Auswärtsmannschaft,
                e.Auswärtsmannschaft_ID AS Gegner_ID,
            	o.B365H,
            	o.B365D,
            	o.B365A,
            	ms.System_ID as HeimSystem,
            	ms1.System_ID as AuswärtsSystem,
            	cl1.Shot_Feature as Home_Shot_Feature,
            	cl1.Shot_On_Goal_Feature as Shot_On_Goal_Feature,
            	cl1.Fouls_Feature as Home_Fouls_Feature,
            	cl1.Corner_Feature as Home_Corner_Feature,
            	cl1.Yellowcard_Feature as Home_Yellowcard_Feature,
            	cl1.Redcard_Feature as Home_Redcard_Feature,
            	cl2.Shot_Feature as Away_Shot_Feature,
            	cl2.Shot_On_Goal_Feature as Gegner_Shot_On_Goal_Feature,
            	cl2.Fouls_Feature as Away_Fouls_Feature,
            	cl2.Corner_Feature as Away_Corner_Feature,
            	cl2.Yellowcard_Feature as Away_Yellowcard_Feature
            from 
            	pl_staging_ergebnisse e
            inner join 
            	pl_features_odds o on e.Heimmannschaft_ID = o.Heimmannschaft_ID  
            and 
                e.Saison = o.Saison 
            and 
                e.Spieltag = o.Spieltag
            inner join 
            	pl_data_vereine_spielsystem vss1 on e.Heimmannschaft_ID = vss1.Vereins_ID  
            and 
                e.Saison = vss1.Saison 
            and 
                e.Spieltag = vss1.Spieltag
            inner join 
            	pl_data_vereine_spielsystem vss2 on e.Auswärtsmannschaft_ID = vss2.Vereins_ID 
            and 
                e.Saison = vss2.Saison 
            and 
                e.Spieltag = vss2.Spieltag
            left outer join 
            	master_system ms on ms.System = vss1.Spiel_System
            left outer join 
            	master_system ms1 on ms1.System = vss2.Spiel_System
            inner join 
            	pl_features_club_data cl1 on e.Heimmannschaft_ID = cl1.Vereins_ID  
            and 
                e.Saison = cl1.Saison 
            and 
                e.Spieltag = cl1.Spieltag
            inner join 
            	pl_features_club_data cl2 on e.Auswärtsmannschaft_ID = cl2.Vereins_ID 
            and 
                e.Saison = cl2.Saison 
            and 
                e.Spieltag = cl2.Spieltag;'''    
    df = pd.read_sql(query, con = connection)
    return df

def get_bundesliga_training_data():
    
    df1 = bundesliga_query_data_1()
    df2 = bundesliga_query_data_2()
    df = df1.merge(df2, on = ['Saison', 'Spieltag', 'Heimmannschaft', 'Heimmannschaft_ID', 'Gegner_ID']) 
    df = df.sort_values(['Saison', 'Spieltag'])
    
    return df

def get_premierleague_training_data():
    
    df1 = premierleague_query_data_1()
    df2 = premierleague_query_data_2()
    df = df1.merge(df2, on = ['Saison', 'Spieltag', 'Heimmannschaft', 'Heimmannschaft_ID', 'Gegner_ID']) 
    df = df.sort_values(['Saison', 'Spieltag'])
    
    return df

def get_bundesliga_forecast_data():
    
    df1 = bundesliga_query_forecast_1()
    df2 = bundesliga_query_forecast_2()
    df = df1.merge(df2, on = ['Saison', 'Spieltag', 'Heimmannschaft', 'Heimmannschaft_ID', 'Gegner_ID']) 
    df = df.sort_values(['Saison', 'Spieltag'])
    
    return df

def get_premierleague_forecast_data():
    
    df1 = premierleague_query_forecast_1()
    df2 = premierleague_query_forecast_2()
    df = df1.merge(df2, on = ['Saison', 'Spieltag', 'Heimmannschaft', 'Heimmannschaft_ID', 'Gegner_ID']) 
    df = df.sort_values(['Saison', 'Spieltag'])
    
    return df

def prepare_data(df, saison, spieltag, variables_chosen):
    
    length_of_data_set = len(df)
    df = df.dropna()
    length_of_training_set = len(df)
    df_seasons_before = df[df['Saison'] < saison]
    df_season_chosen = df[df['Saison'] == saison]
    df_season_chosen = df_season_chosen[df_season_chosen['Spieltag'] < spieltag]
    df_analyse = df_seasons_before.append(df_season_chosen, ignore_index = True)
    df_analyse = df_analyse.sort_values(by = ['Saison', 'Spieltag'])        
    
    results = df_analyse[['Spiel_Ausgang']].values
    x_variables = df_analyse[variables_chosen]
    
    return results, x_variables, length_of_data_set, length_of_training_set

def prepare_forecast(df_forecast, saison, spieltag, vereins_id, variables_chosen):
    

    df_forecast = df_forecast[df_forecast['Saison']==saison]
    df_forecast = df_forecast[df_forecast['Spieltag']==spieltag]
    
    df_forecast = df_forecast[df_forecast['Heimmannschaft_ID']==vereins_id]     
     
    home_team = df_forecast['Heimmannschaft'].iloc[0]
    away_team = df_forecast['Auswärtsmannschaft'].iloc[0]
    forecast = df_forecast[variables_chosen]
    
    return home_team, away_team, forecast


def get_random_forest_proba(x, y, forecast):
    classifier = RandomForestClassifier(n_estimators = 2000, criterion = 'gini', random_state = 0)
    classifier.fit(x, y)
    y_proba = classifier.predict_proba(forecast)       
    
    return y_proba

def get_odds_random_forest(y_proba):
    odds_home = round(1/y_proba[0][2], 2)
    odds_draw = round(1/y_proba[0][1], 2)
    odds_away = round(1/y_proba[0][0], 2)
    return odds_home, odds_draw , odds_away

def encode_variable(df, variable):
    
    data = df[variable]
    df = df.drop([variable], axis = 1)
    values = array(data)
    label_encoder = LabelEncoder()
    integer_encoded = label_encoder.fit_transform(values)
    
    onehot_encoder = OneHotEncoder(sparse=False)
    integer_encoded = integer_encoded.reshape(len(integer_encoded), 1)
    onehot_encoded = onehot_encoder.fit_transform(integer_encoded)
    
    return df, onehot_encoded

def encode_variable_train_and_test(df_train, df_test, variable):
    
    data_train = df_train[variable]
    data_test = df_test[variable]
    
    df_train = df_train.drop([variable], axis = 1)
    df_test = df_test.drop([variable], axis = 1)
    
    values_train = array(data_train)
    values_test = array(data_test)    
    
    label_encoder = LabelEncoder()
    
    integer_encoded_train = label_encoder.fit_transform(values_train)
    integer_encoded_test = label_encoder.transform(values_test)
    
    onehot_encoder = OneHotEncoder(sparse=False)
    integer_encoded_train = integer_encoded_train.reshape(len(integer_encoded_train), 1)
    integer_encoded_test = integer_encoded_test.reshape(len(integer_encoded_test), 1)
    
    
    onehot_encoded_train = onehot_encoder.fit_transform(integer_encoded_train)
    onehot_encoded_test = onehot_encoder.transform(integer_encoded_test)
    
    return df_train, df_test, onehot_encoded_train, onehot_encoded_test

def encode_test_training_set(df_train, df_test, variables):
    
    if 'Heimmannschaft_ID' in variables:
        
        df_train, df_test, home_teams_encoded_train, home_teams_encoded_test = encode_variable_train_and_test(df_train, df_test, 'Heimmannschaft_ID')
        df_train, df_test, away_teams_encoded_train, away_teams_encoded_test = encode_variable_train_and_test(df_train, df_test, 'Gegner_ID')
        teams_encoded_train = np.concatenate((home_teams_encoded_train, away_teams_encoded_train), axis=1)
        teams_encoded_test = np.concatenate((home_teams_encoded_test, away_teams_encoded_test), axis=1)
        
    if 'Trainer_ID' in variables:
        
        df_train, df_test, home_trainer_encoded_train, home_trainer_encoded_test = encode_variable_train_and_test(df_train, df_test, 'Trainer_ID')
        df_train, df_test, away_trainer_encoded_train, away_trainer_encoded_test = encode_variable_train_and_test(df_train, df_test, 'Gegner_Trainer_ID')
        trainer_encoded_train = np.concatenate((home_trainer_encoded_train, away_trainer_encoded_train), axis=1)
        trainer_encoded_test = np.concatenate((home_trainer_encoded_test, away_trainer_encoded_test), axis=1)
        
    if 'HeimSystem' in variables:
        
        df_train, df_test, home_system_encoded_train, home_system_encoded_test = encode_variable_train_and_test(df_train, df_test, 'HeimSystem')
        df_train, df_test, away_system_encoded_train, away_system_encoded_test = encode_variable_train_and_test(df_train, df_test, 'AuswärtsSystem')
        system_encoded_train = np.concatenate((home_system_encoded_train, away_system_encoded_train), axis=1)
        system_encoded_test = np.concatenate((home_system_encoded_test, away_system_encoded_test), axis=1)
        
    if (('Heimmannschaft_ID' in variables) and ('Trainer_ID' in variables) and ('HeimSystem' in variables)):
        
        all_encoded_variables_train = np.concatenate((teams_encoded_train, trainer_encoded_train, system_encoded_train), axis=1)
        all_encoded_variables_test = np.concatenate((teams_encoded_test, trainer_encoded_test, system_encoded_test), axis=1)
        
    if (('Heimmannschaft_ID' in variables) and ('HeimSystem' in variables) and ('Trainer_ID' not in variables)):
        
        all_encoded_variables_train = np.concatenate((teams_encoded_train, system_encoded_train), axis=1)
        all_encoded_variables_test = np.concatenate((teams_encoded_test, system_encoded_test), axis=1)
        
    if (('Heimmannschaft_ID' in variables) and ('Trainer_ID' in variables) and ('HeimSystem' not in variables)):
        
        all_encoded_variables_train = np.concatenate((teams_encoded_train, trainer_encoded_train), axis=1)
        all_encoded_variables_test = np.concatenate((teams_encoded_test, trainer_encoded_test), axis=1)       
        
    if (('Heimmannschaft_ID' not in variables) and ('Trainer_ID' in variables) and ('HeimSystem' in variables)):
        
        all_encoded_variables_train = np.concatenate((system_encoded_train, trainer_encoded_train), axis=1)
        all_encoded_variables_test = np.concatenate((system_encoded_test, trainer_encoded_test), axis=1)          

    if (('Heimmannschaft_ID' in variables) and ('Trainer_ID' not in variables) and ('HeimSystem' not in variables)):
        
        all_encoded_variables_train = teams_encoded_train
        all_encoded_variables_test = teams_encoded_test    
        
    if (('Heimmannschaft_ID' not in variables) and ('Trainer_ID' in variables) and ('HeimSystem' not in variables)):
        
        all_encoded_variables_train = trainer_encoded_train
        all_encoded_variables_test = trainer_encoded_test
        
    if (('Heimmannschaft_ID' not in variables) and ('Trainer_ID' not in variables) and ('HeimSystem' in variables)):
        
        all_encoded_variables_train = system_encoded_train
        all_encoded_variables_test = system_encoded_test

          
    x_variables = df_train.values
    forecast = df_test.values
    
    x_variables = np.concatenate((x_variables, all_encoded_variables_train), axis=1)
    forecast = np.concatenate((forecast, all_encoded_variables_test), axis=1)
    
    return x_variables, forecast
    




