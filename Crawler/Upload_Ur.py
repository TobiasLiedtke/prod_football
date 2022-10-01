import os
os.chdir('D:/Projects/Football/PROD/Crawler')

#packages and modules
import pandas as pd
import numpy as np
import os

import Tools_Crawler as t
import Read_Load_Database as db

df_trainer = db.get_table('bl1_data_trainer_spiele')
df_trainer['Saison'].drop_duplicates()
df_trainer = df_trainer[df_trainer['Saison']=='2022/23']
df_trainer = df_trainer[df_trainer['Spieltag'] == 7]

df_trainer = df_trainer.drop('Spieltag', axis = 1)
df_trainer = df_trainer.assign(Spieltag = 8)


#db.upload_local_data_to_database(df_trainer, 'bl1_data_trainer_spiele')

df_trainer = db.get_table('pl_data_trainer_spiele')
df_trainer = df_trainer[df_trainer['Saison']=='2022/23']
df_trainer = df_trainer.drop_duplicates()
df_trainer = df_trainer[df_trainer['Spieltag']==8]
df_trainer = df_trainer.drop(['Spieltag'], axis = 1)
df_trainer = df_trainer.assign(Spieltag = 9)
#db.upload_local_data_to_database(df_trainer, 'pl_data_trainer_spiele')


df_startelf = db.get_table('bl1_data_startelf')
df_startelf = df_startelf[df_startelf['Saison']=='2021/22']
df_startelf = df_startelf.drop_duplicates()
#db.upload_local_data_to_database(df_startelf, 'bl1_data_startelf')


df_feature_startelf = db.get_table('bl1_features_startelf')
df_feature_startelf = df_feature_startelf[df_feature_startelf['Saison']=='2021/22']
df_feature_startelf = df_feature_startelf.drop_duplicates()
#db.upload_local_data_to_database(df_feature_startelf, 'bl1_features_startelf')

system = db.get_table('pl_data_vereine_spielsystem')
system = system[system['Saison']=='2021/22']
system = system.drop_duplicates()
#db.delete_table_saison('pl_data_vereine_spielsystem', "'2021/22'")
#db.upload_local_data_to_database(system, 'pl_data_vereine_spielsystem')


club_data = db.get_table('bl1_features_club_data')
club_data = club_data[club_data['Saison']=='2021/22']
club_data = club_data.drop_duplicates()
#db.delete_table_saison('bl1_features_club_data', "'2021/22'")
#db.upload_local_data_to_database(club_data, 'bl1_features_club_data')


club_data_forecast = db.get_table('bl1_features_forecast_club_data')
club_data_forecast = club_data_forecast[club_data_forecast['Saison']=='2021/22']
club_data_forecast = club_data_forecast.drop_duplicates()
#db.upload_local_data_to_database(club_data_forecast, 'bl1_features_forecast_club_data')


pl_football_uk = db.get_table('pl_staging_football_uk')
pl_football_uk = pl_football_uk[pl_football_uk['Saison']=='2021/22']
pl_football_uk = pl_football_uk.drop_duplicates()
#db.upload_local_data_to_database(pl_football_uk, 'pl_staging_football_uk')

pl_fifa_features = db.get_table('pl_staging_vereine_fifa_features')
pl_fifa_features = pl_fifa_features[pl_fifa_features['Saison']=='2021/22']
pl_fifa_features = pl_fifa_features.drop_duplicates()
#db.upload_local_data_to_database(pl_fifa_features, 'pl_staging_vereine_fifa_features')


pl_bookmaker_odds = db.get_table('pl_data_vereine_bookmaker_odds')
pl_bookmaker_odds = pl_bookmaker_odds[pl_bookmaker_odds['Saison']=='2021/22']
pl_bookmaker_odds = pl_bookmaker_odds.drop_duplicates()
#db.upload_local_data_to_database(pl_bookmaker_odds, 'pl_data_vereine_bookmaker_odds')


pl_data_gov = db.get_table('pl_data_vereine_data_gov')
pl_data_gov = pl_data_gov[pl_data_gov['Saison']=='2021/22']
pl_data_gov = pl_data_gov.drop_duplicates()
#db.upload_local_data_to_database(pl_data_gov, 'pl_data_vereine_data_gov')


pl_kader_wert = db.get_table('pl_data_vereine_kader_wert')
pl_kader_wert = pl_kader_wert[pl_kader_wert['Saison']=='2021/22']
pl_kader_wert = pl_kader_wert.drop_duplicates()
#db.upload_local_data_to_database(pl_kader_wert, 'pl_data_vereine_kader_wert')



pl_club_data = db.get_table('pl_features_forecast_club_form')
pl_club_data = pl_club_data[pl_club_data['Saison']=='2021/22']
pl_club_data = pl_club_data.drop_duplicates()
#db.upload_local_data_to_database(pl_club_data, 'pl_features_forecast_club_form')