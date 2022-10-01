import os
os.chdir('D:/Projects/Football/PROD/Crawler')

from sqlalchemy import create_engine

import Tools_Crawler as t


def get_table(table):
    query = 'select* from '+table   
    df = t.connection(query)
    return df

def delete_table_saison(table, saison):
    query = 'delete from '+table + ' where saison = ' +  saison 
    print(query)
    t.delete(query)

def delete_table_saison_spieltag(table, saison, spieltag):
    saison_qery = "'" + saison + "'"
    query = 'delete from '+ table + ' where saison = ' +  saison_qery + ' and spieltag = ' +  str(spieltag)
    print(query)
    t.delete(query)
    
    
def upload_local_data_to_database(df, table):
    engine = create_engine('mysql+mysqlconnector://root:Teleshop,1871@localhost:3306/prod_football', echo=False)
    df.to_sql(name=table, con=engine, if_exists = 'append', index=False)
    print("Data succesfully uploaded")
    
def upload_replace_local_data_to_database(df, table):
    engine = create_engine('mysql+mysqlconnector://root:Teleshop,1871@localhost:3306/prod_football', echo=False)
    df.to_sql(name=table, con=engine, if_exists = 'replace', index=False)
    print("Data succesfully uploaded")
    
    
def get_staging_api(table):    


    query = '''SELECT 
            	`league.season` as Saison, 
            	`fixture.id` as Fixture_ID_Api,
                `fixture.date` as Date,
                `league.round` as Spieltag,
                `teams.home.name` as Heimmannschaft,
                `teams.away.name` as Auswärtsmannschaft,
                `fixture.referee` as Schiedsrichter
            FROM ''' + table
    df = t.connection(query)
    
    return df

def get_results(table):    

    query = '''SELECT 
                Heimmannschaft_ID, 
                Heimmannschaft, 
                Auswärtsmannschaft_ID, 
                Auswärtsmannschaft, 
                Spieltag, 
                Saison
            FROM ''' + table + ''' where saison >= '2014/15' '''
    df = t.connection(query)
    
    return df
