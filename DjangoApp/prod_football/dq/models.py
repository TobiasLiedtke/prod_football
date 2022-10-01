
import pandas as pd
from django.db import connection 

def get_bundesliga_all_saison_spieltag_check():

    query = ''' 
       select
        	e.Saison,
        	count(distinct e.Spieltag)
        from 
        	bl1_staging_ergebnisse e
        inner join 
        	bl1_data_ergebnisse_kategorisiert k on e.Heimmannschaft_ID = k.Vereins_ID 
			and e.Saison = k.Saison 
            and e.Spieltag = k.Spieltag
        inner join 
        	bl1_data_vereine_kader_wert kw on e.Heimmannschaft_ID = kw.Vereins_ID 
			and e.Saison = kw.Saison 
            and e.Spieltag = kw.Spieltag
        inner join 
        	bl1_data_vereine_kader_wert kw1 on e.Auswärtsmannschaft_ID = kw1.Vereins_ID 
            and e.Saison = kw1.Saison 
			and e.Spieltag = kw1.Spieltag
        inner join 
        	bl1_staging_vereine_fifa_features ff on e.Heimmannschaft_ID = ff.Vereins_ID 
			and e.Saison = ff.Saison 
			and e.Spieltag = ff.Spieltag
        inner join 
        	bl1_staging_vereine_fifa_features ff2 on e.Auswärtsmannschaft_ID = ff2.Vereins_ID 
			and e.Saison = ff2.Saison 
			and e.Spieltag = ff2.Spieltag
        inner join 
        	bl1_data_trainer_spiele ts on e.Heimmannschaft_ID = ts.Vereins_ID 
			and e.Saison = ts.Saison 
			and e.Spieltag = ts.Spieltag
        inner join 
        	bl1_data_trainer_spiele ts1 on e.Auswärtsmannschaft_ID = ts1.Vereins_ID 
			and e.Saison = ts1.Saison 
            and e.Spieltag = ts1.Spieltag
		JOIN 
            bl1_feature_opponent_statistic_forecast op on e.Heimmannschaft_ID = op.Vereins_ID  
            and e.Saison = op.Saison 
            and e.Spieltag = op.Spieltag
        inner join 
        	bl1_features_odds o on e.Heimmannschaft_ID = o.Heimmannschaft_ID  
			and e.Saison = o.Saison 
			and e.Spieltag = o.Spieltag
        inner join 
        	bl1_data_vereine_spielsystem vss1 on e.Heimmannschaft_ID = vss1.Vereins_ID  
            and e.Saison = vss1.Saison 
			and e.Spieltag = vss1.Spieltag
        inner join 
        	bl1_data_vereine_spielsystem vss2 on e.Auswärtsmannschaft_ID = vss2.Vereins_ID 
            and e.Saison = vss2.Saison 
			and e.Spieltag = vss2.Spieltag
        left outer join 
        	master_system ms on ms.System = vss1.Spiel_System
        left outer join 
        	master_system ms1 on ms1.System = vss2.Spiel_System
        inner join 
        	bl1_features_club_data cl1 on e.Heimmannschaft_ID = cl1.Vereins_ID  
			and e.Saison = cl1.Saison 
			and e.Spieltag = cl1.Spieltag
        inner join 
        	bl1_features_club_data cl2 on e.Auswärtsmannschaft_ID = cl2.Vereins_ID 
			and e.Saison = cl2.Saison 
			and e.Spieltag = cl2.Spieltag
		group by e.saison;'''
                
    df = pd.read_sql(query, con = connection)
        
    return df 


def get_bundesliga_all_club_count():

    query = ''' 
       select
        	e.Saison,
            e.Spieltag,
        	count(distinct e.Heimmannschaft_ID)
        from 
        	bl1_staging_ergebnisse e
        inner join 
        	bl1_data_ergebnisse_kategorisiert k on e.Heimmannschaft_ID = k.Vereins_ID 
			and e.Saison = k.Saison 
            and e.Spieltag = k.Spieltag
        inner join 
        	bl1_data_vereine_kader_wert kw on e.Heimmannschaft_ID = kw.Vereins_ID 
			and e.Saison = kw.Saison 
            and e.Spieltag = kw.Spieltag
        inner join 
        	bl1_data_vereine_kader_wert kw1 on e.Auswärtsmannschaft_ID = kw1.Vereins_ID 
            and e.Saison = kw1.Saison 
			and e.Spieltag = kw1.Spieltag
        inner join 
        	bl1_staging_vereine_fifa_features ff on e.Heimmannschaft_ID = ff.Vereins_ID 
			and e.Saison = ff.Saison 
			and e.Spieltag = ff.Spieltag
        inner join 
        	bl1_staging_vereine_fifa_features ff2 on e.Auswärtsmannschaft_ID = ff2.Vereins_ID 
			and e.Saison = ff2.Saison 
			and e.Spieltag = ff2.Spieltag
        inner join 
        	bl1_data_trainer_spiele ts on e.Heimmannschaft_ID = ts.Vereins_ID 
			and e.Saison = ts.Saison 
			and e.Spieltag = ts.Spieltag
        inner join 
        	bl1_data_trainer_spiele ts1 on e.Auswärtsmannschaft_ID = ts1.Vereins_ID 
			and e.Saison = ts1.Saison 
            and e.Spieltag = ts1.Spieltag
		JOIN 
            bl1_feature_opponent_statistic_forecast op on e.Heimmannschaft_ID = op.Vereins_ID  
            and e.Saison = op.Saison 
            and e.Spieltag = op.Spieltag
        inner join 
        	bl1_features_odds o on e.Heimmannschaft_ID = o.Heimmannschaft_ID  
			and e.Saison = o.Saison 
			and e.Spieltag = o.Spieltag
        inner join 
        	bl1_data_vereine_spielsystem vss1 on e.Heimmannschaft_ID = vss1.Vereins_ID  
            and e.Saison = vss1.Saison 
			and e.Spieltag = vss1.Spieltag
        inner join 
        	bl1_data_vereine_spielsystem vss2 on e.Auswärtsmannschaft_ID = vss2.Vereins_ID 
            and e.Saison = vss2.Saison 
			and e.Spieltag = vss2.Spieltag
        left outer join 
        	master_system ms on ms.System = vss1.Spiel_System
        left outer join 
        	master_system ms1 on ms1.System = vss2.Spiel_System
        inner join 
        	bl1_features_club_data cl1 on e.Heimmannschaft_ID = cl1.Vereins_ID  
			and e.Saison = cl1.Saison 
			and e.Spieltag = cl1.Spieltag
        inner join 
        	bl1_features_club_data cl2 on e.Auswärtsmannschaft_ID = cl2.Vereins_ID 
			and e.Saison = cl2.Saison 
			and e.Spieltag = cl2.Spieltag
		group by e.saison, e.spieltag
        having count(distinct e.Heimmannschaft_ID) != 9;'''
                
    df = pd.read_sql(query, con = connection)
        
    return df 

def get_premierleague_all_saison_spieltag_check():

    query = ''' 
       select
        	e.Saison,
        	count(distinct e.Spieltag)
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
			and e.Saison = ff.Saison 
			and e.Spieltag = ff.Spieltag
        inner join 
        	pl_staging_vereine_fifa_features ff2 on e.Auswärtsmannschaft_ID = ff2.Vereins_ID 
			and e.Saison = ff2.Saison 
			and e.Spieltag = ff2.Spieltag
        inner join 
        	pl_data_trainer_spiele ts on e.Heimmannschaft_ID = ts.Vereins_ID 
			and e.Saison = ts.Saison 
			and e.Spieltag = ts.Spieltag
        inner join 
        	pl_data_trainer_spiele ts1 on e.Auswärtsmannschaft_ID = ts1.Vereins_ID 
			and e.Saison = ts1.Saison 
            and e.Spieltag = ts1.Spieltag
		JOIN 
            pl_feature_opponent_statistic_forecast op on e.Heimmannschaft_ID = op.Vereins_ID  
            and e.Saison = op.Saison 
            and e.Spieltag = op.Spieltag
        inner join 
        	pl_features_odds o on e.Heimmannschaft_ID = o.Heimmannschaft_ID  
			and e.Saison = o.Saison 
			and e.Spieltag = o.Spieltag
        inner join 
        	pl_data_vereine_spielsystem vss1 on e.Heimmannschaft_ID = vss1.Vereins_ID  
            and e.Saison = vss1.Saison 
			and e.Spieltag = vss1.Spieltag
        inner join 
        	pl_data_vereine_spielsystem vss2 on e.Auswärtsmannschaft_ID = vss2.Vereins_ID 
            and e.Saison = vss2.Saison 
			and e.Spieltag = vss2.Spieltag
        left outer join 
        	master_system ms on ms.System = vss1.Spiel_System
        left outer join 
        	master_system ms1 on ms1.System = vss2.Spiel_System
        inner join 
        	pl_features_club_data cl1 on e.Heimmannschaft_ID = cl1.Vereins_ID  
			and e.Saison = cl1.Saison 
			and e.Spieltag = cl1.Spieltag
        inner join 
        	pl_features_club_data cl2 on e.Auswärtsmannschaft_ID = cl2.Vereins_ID 
			and e.Saison = cl2.Saison 
			and e.Spieltag = cl2.Spieltag
		group by e.saison
            ;'''
                
    df = pd.read_sql(query, con = connection)
        
    return df 


def get_premierleague_all_club_count():

    query = ''' 
       select
        	e.Saison,
            e.Spieltag,
        	count(distinct e.Heimmannschaft_ID)
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
			and e.Saison = ff.Saison 
			and e.Spieltag = ff.Spieltag
        inner join 
        	pl_staging_vereine_fifa_features ff2 on e.Auswärtsmannschaft_ID = ff2.Vereins_ID 
			and e.Saison = ff2.Saison 
			and e.Spieltag = ff2.Spieltag
        inner join 
        	pl_data_trainer_spiele ts on e.Heimmannschaft_ID = ts.Vereins_ID 
			and e.Saison = ts.Saison 
			and e.Spieltag = ts.Spieltag
        inner join 
        	pl_data_trainer_spiele ts1 on e.Auswärtsmannschaft_ID = ts1.Vereins_ID 
			and e.Saison = ts1.Saison 
            and e.Spieltag = ts1.Spieltag
		JOIN 
            pl_feature_opponent_statistic_forecast op on e.Heimmannschaft_ID = op.Vereins_ID  
            and e.Saison = op.Saison 
            and e.Spieltag = op.Spieltag
        inner join 
        	pl_features_odds o on e.Heimmannschaft_ID = o.Heimmannschaft_ID  
			and e.Saison = o.Saison 
			and e.Spieltag = o.Spieltag
        inner join 
        	pl_data_vereine_spielsystem vss1 on e.Heimmannschaft_ID = vss1.Vereins_ID  
            and e.Saison = vss1.Saison 
			and e.Spieltag = vss1.Spieltag
        inner join 
        	pl_data_vereine_spielsystem vss2 on e.Auswärtsmannschaft_ID = vss2.Vereins_ID 
            and e.Saison = vss2.Saison 
			and e.Spieltag = vss2.Spieltag
        left outer join 
        	master_system ms on ms.System = vss1.Spiel_System
        left outer join 
        	master_system ms1 on ms1.System = vss2.Spiel_System
        inner join 
        	pl_features_club_data cl1 on e.Heimmannschaft_ID = cl1.Vereins_ID  
			and e.Saison = cl1.Saison 
			and e.Spieltag = cl1.Spieltag
        inner join 
        	pl_features_club_data cl2 on e.Auswärtsmannschaft_ID = cl2.Vereins_ID 
			and e.Saison = cl2.Saison 
			and e.Spieltag = cl2.Spieltag
		group by e.saison, e.spieltag
        having count(distinct e.Heimmannschaft_ID) != 10
        ;'''
                
    df = pd.read_sql(query, con = connection)
        
    return df 


   
    
def get_table_list_bundesliga():
    table_list_bundesliga = ['bl1_staging_ergebnisse', 'bl1_staging_football_uk', 'bl1_staging_vereine_fifa_features', 
                       'bl1_staging_vereine_kommende_spieltag', 'bl1_data_ergebnisse_kategorisiert', 
                       'bl1_data_trainer_spiele', 'bl1_data_vereine_bookmaker_odds', 'bl1_data_vereine_data_gov',
                       'bl1_data_vereine_kader_wert', 'bl1_data_vereine_spielplan', 'bl1_data_vereine_spielsystem',
                       'bl1_features_club_data', 'bl1_features_club_form', 'bl1_features_forecast_club_data',
                       'bl1_features_forecast_club_form', 'bl1_features_odds', 'bl1_feature_opponent_statistic',
                       'bl1_feature_opponent_statistic_forecast']  
    
    return table_list_bundesliga

def get_table_list_premierleague():
    table_list_premierleague = ['pl_staging_ergebnisse', 'pl_staging_football_uk', 'pl_staging_vereine_fifa_features', 
                           'pl_staging_vereine_kommende_spieltag', 'pl_data_ergebnisse_kategorisiert', 'pl_data_trainer_spiele', 
                           'pl_data_vereine_bookmaker_odds', 'pl_data_vereine_data_gov', 'pl_data_vereine_kader_wert', 
                           'pl_data_vereine_spielplan', 'pl_data_vereine_spielsystem', 'pl_features_club_data', 
                           'pl_features_club_form', 'pl_features_forecast_club_data', 'pl_features_forecast_club_form',
                           'pl_features_odds', 'pl_feature_opponent_statistic', 'pl_feature_opponent_statistic_forecast']   
    
    return table_list_premierleague

def check_duplicates_bl_pl():
     
    table_list_gamedays_bundesliga = get_table_list_bundesliga()
    
    no_duplicates_output_bundesliga = list()
    duplicates_output_bundesliga = list()
    
    for table in table_list_gamedays_bundesliga:
        query = 'select * from ' + table
        
        df = pd.read_sql(query, con = connection) 

        saisons = df['Saison'].drop_duplicates()    
        for s in saisons:
            df_s = df[df['Saison']==s]
            if len(df_s.drop_duplicates())==len(df_s):
                no_duplicates_output_bundesliga.append(table)
                no_duplicates_output_bundesliga.append(s)
            else:
                duplicates_output_bundesliga.append(table)
                duplicates_output_bundesliga.append(s)
                
                
    table_list_gamedays_premierleage = get_table_list_premierleague()  
    
    no_duplicates_output_premierleage = list()
    duplicates_output_premierleage = list()
    for table in table_list_gamedays_premierleage:
        query = 'select * from ' + table
        
        df = pd.read_sql(query, con = connection) 
        saisons = df['Saison'].drop_duplicates()    
        for s in saisons:
            df_s = df[df['Saison']==s]
            if len(df_s.drop_duplicates())==len(df_s):
                no_duplicates_output_premierleage.append(table)
                no_duplicates_output_premierleage.append(s)
            else:
                duplicates_output_premierleage.append(table)
                duplicates_output_premierleage.append(s)        
                    
    return no_duplicates_output_bundesliga, duplicates_output_bundesliga, no_duplicates_output_premierleage, duplicates_output_premierleage
 
    
def check_matchdays_bl_pl():
       
    table_list_gamedays_bundesliga = get_table_list_bundesliga()
    
    df_all = pd.DataFrame()
    tables = list()
    for table in table_list_gamedays_bundesliga:
        query = 'select saison, count(distinct spieltag) as Spieltage from ' + table + ' group by saison having count(distinct spieltag) != 34'
        
        df = pd.read_sql(query, con = connection)   
        df = df.assign(Tables = table)
        df_all = df_all.append(df)

        
    saisons = list(df_all['saison']) 
    spieltage = list(df_all['Spieltage'])
    tables = list(df_all['Tables'])
    zipped_bundesliga = zip(saisons, spieltage, tables)
    
    table_list_gameday_premierleague = get_table_list_premierleague()
    
    df_all = pd.DataFrame()
    tables = list()
    for table in table_list_gameday_premierleague:
        query = 'select saison, count(distinct spieltag) as Spieltage from ' + table + ' group by saison having count(distinct spieltag) != 38'
        
        df = pd.read_sql(query, con = connection)   
        df = df.assign(Tables = table)
        df_all = df_all.append(df)

        
    saisons = list(df_all['saison']) 
    spieltage = list(df_all['Spieltage'])
    tables = list(df_all['Tables'])
    zipped_premier_league = zip(saisons, spieltage, tables)
        
    return zipped_bundesliga, zipped_premier_league



def check_clubs_premier():
    
    table_list_club_nbr_bundesliga = [ 'bl1_data_ergebnisse_kategorisiert', 'bl1_data_trainer_spiele',
                       'bl1_data_vereine_kader_wert', 'bl1_data_vereine_spielplan', 
                       'bl1_data_vereine_spielsystem', 'bl1_features_club_data', 'bl1_features_club_form',
                       'bl1_features_forecast_club_data', 'bl1_features_forecast_club_form',
                       'bl1_staging_vereine_fifa_features', 'bl1_feature_opponent_statistic',
                       'bl1_feature_opponent_statistic_forecast']    
    
    df_all = pd.DataFrame()

    for table in table_list_club_nbr_bundesliga:

        query = 'select saison, spieltag, count(distinct vereins_id) as Clubs_Nbr from '  + table + ' group by saison, spieltag having count(distinct vereins_id) != 18 and saison > ' + "'2013/14'" + ' order by saison, spieltag'
        
        df = pd.read_sql(query, con = connection)      
        df = df.assign(Tables = table)
        df_all = df_all.append(df)
        saisons = list(df_all['saison']) 
        spieltage = list(df_all['spieltag'])
        clubs = list(df_all['Clubs_Nbr'])
        tables = list(df_all['Tables'])
        zipped_bundesliga = zip(saisons, spieltage, tables, clubs)    
        
        
        
        
    table_list_club_nbr_premierleague = ['pl_data_ergebnisse_kategorisiert', 'pl_data_trainer_spiele',
                           'pl_data_vereine_kader_wert', 'pl_data_vereine_spielplan', 
                           'pl_data_vereine_spielsystem', 'pl_features_club_data', 'pl_features_club_form',
                           'pl_features_forecast_club_data', 'pl_features_forecast_club_form',
                           'pl_staging_vereine_fifa_features', 'pl_feature_opponent_statistic',
                           'pl_feature_opponent_statistic_forecast']    
    
    df_all = pd.DataFrame()
    for table in table_list_club_nbr_premierleague:

        query = 'select saison, spieltag, count(distinct vereins_id) as Clubs_Nbr from ' + table + ' group by saison, spieltag having count(distinct vereins_id) != 20 order by saison, spieltag'
        
        df = pd.read_sql(query, con = connection)      
        df = df.assign(Tables = table)
        df_all = df_all.append(df)
        saisons = list(df_all['saison']) 
        spieltage = list(df_all['spieltag'])
        clubs = list(df_all['Clubs_Nbr'])
        tables = list(df_all['Tables'])
        zipped_premierleague = zip(saisons, spieltage, tables, clubs)
        
    table_list_club_nbr_homeid_bundesliga = ['bl1_data_vereine_bookmaker_odds','bl1_data_vereine_data_gov', 'bl1_features_odds', 
                           'bl1_staging_ergebnisse', 'bl1_staging_football_uk', 
                           'bl1_staging_vereine_kommende_spieltag']    
    
    df_all = pd.DataFrame()
    for table in table_list_club_nbr_homeid_bundesliga:
        query = 'select saison, spieltag, count(distinct Heimmannschaft_ID) as Clubs_Nbr from ' + table + ' group by saison, spieltag having count(distinct Heimmannschaft_ID) != 9 and saison > ' + "'2013/14'" + ' order by saison, spieltag'
        
        df = pd.read_sql(query, con = connection)      
        df = df.assign(Tables = table)
        df_all = df_all.append(df)
        saisons = list(df_all['saison']) 
        spieltage = list(df_all['spieltag'])
        clubs = list(df_all['Clubs_Nbr'])
        tables = list(df_all['Tables'])
        zipped_homeid_bundesliga = zip(saisons, spieltage, tables, clubs) 
        
    table_list_club_nbr_homeid_premierlague = ['pl_data_vereine_bookmaker_odds','pl_data_vereine_data_gov', 'pl_features_odds', 
                           'pl_staging_ergebnisse', 'pl_staging_football_uk', 
                           'pl_staging_vereine_kommende_spieltag'] 
    
    df_all = pd.DataFrame()
    for table in table_list_club_nbr_homeid_premierlague:
        query = 'select saison, spieltag, count(distinct Heimmannschaft_ID) as Clubs_Nbr from ' + table + ' group by saison, spieltag having count(distinct Heimmannschaft_ID) != 10 and saison > ' + "'2013/14'" + ' order by saison, spieltag'
        
        df = pd.read_sql(query, con = connection)      
        df = df.assign(Tables = table)
        df_all = df_all.append(df)
        saisons = list(df_all['saison']) 
        spieltage = list(df_all['spieltag'])
        clubs = list(df_all['Clubs_Nbr'])
        tables = list(df_all['Tables'])
        zipped_homeid_premierleague = zip(saisons, spieltage, tables, clubs) 
            
    return zipped_bundesliga, zipped_homeid_bundesliga, zipped_premierleague, zipped_homeid_premierleague