import os
os.chdir('D:/Projects/Football/PROD/Crawler')

#packages and modules
import pandas as pd
from selenium import webdriver
import time
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
#import other files 
import Tools_Crawler as t
import Read_Load_Database as db



def get_system(c, s, saison, url):

    df_date = db.get_table('bl1_staging_vereine_kommende_spieltag')
    df_date = df_date[df_date['Saison']==saison] 
    df_date = df_date[df_date['Spieltag']==s]
    df_date = df_date[df_date['Heimmannschaft_ID']==c]

    Heimannschaft = df_date['Heimmannschaft'].iloc[0]
    Auswärtsmannschaft = df_date['Auswärtsmannschaft'].iloc[0]
    away_id = df_date['Auswärtsmannschaft_ID'].iloc[0]
    
    #driver = webdriver.Chrome(service=Service('D:\Projects\Football\Database\Crawler_Code\Webdrivers\geckodriver'))
    driver = webdriver.Chrome(service=Service('D:\Projects\Football\Database\Crawler_Code\Webdrivers\chromedriver.exe'))

    driver.get(url)

    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH,'//button[@id="L2AGLb"]'))).click()
    time.sleep(5)
    l_system_h = []
    l_system_a = []
                                                           
    #driver.find_elements_by_xpath("//span[@class='lrvl-tvc lrvl-f']") 
    systeme = driver.find_elements("xpath", "//span[@class='lrvl-tvc lrvl-f']")
    l_system_h.append(systeme[0].text)
    l_system_a.append(systeme[1].text)
    
    driver.quit()
    
    df_system_h = pd.DataFrame(l_system_h)
    df_system_a = pd.DataFrame(l_system_a)
       
    system_h = str(df_system_h.iloc[0,0])
    system_a = str(df_system_a.iloc[0,0])
    df = pd.DataFrame( {'Vereins_ID': [c, away_id], 'Verein': [Heimannschaft, Auswärtsmannschaft], 'Spieltag' : [s, s],
                        'Spiel_System' : [system_h, system_a], 'Saison' : [saison, saison]})
    
    return df

url = 'https://www.google.de/search?q=bundesliga&sxsrf=ALiCzsbfBfc7aAGzqZL2zXWda-EeNjJFcQ%3A1662901298800&ei=MtwdY4rDMLOM9u8PlvGi8AE&ved=0ahUKEwiKpNiJ5oz6AhUzhv0HHZa4CB4Q4dUDCA4&uact=5&oq=bundesliga&gs_lcp=Cgdnd3Mtd2l6EAMyBwgjELADECcyBwgjELADECcyBwgjELADECcyCggAEEcQ1gQQsAMyCggAEEcQ1gQQsAMyCggAEEcQ1gQQsAMyCggAEEcQ1gQQsAMyCggAEEcQ1gQQsAMyCggAEEcQ1gQQsAMyCggAEEcQ1gQQsAMyCggAEEcQ1gQQsAMyDQgAEOQCENYEELADGAEyDQgAEOQCENYEELADGAEyDQgAEOQCENYEELADGAEyDAguEMgDELADEEMYAjIPCC4Q1AIQyAMQsAMQQxgCMgwILhDIAxCwAxBDGAIyDAguEMgDELADEEMYAkoECEEYAEoECEYYAVAAWABgiAJoAXABeACAAQCIAQCSAQCYAQDIARLAAQHaAQYIARABGAnaAQYIAhABGAg&sclient=gws-wiz#sie=m;/g/11s39n2b4f;2;/m/037169;dt;fp;1;;;'
saison = '2022/23'
spieltag = 6
club_id = 4

#df = get_system(club_id, spieltag, saison, url)
#db.upload_local_data_to_database(df, 'bl1_data_vereine_spielsystem')

def get_system_premier_league_url(c, s, saison, url):

    df_date = db.get_table('pl_staging_vereine_kommende_spieltag')
    df_date = df_date[df_date['Saison']==saison] 
    df_date = df_date[df_date['Spieltag']==s]
    df_date = df_date[df_date['Heimmannschaft_ID']==c]
    
    Heimannschaft = df_date['Heimmannschaft'].iloc[0]
    Auswärtsmannschaft = df_date['Auswärtsmannschaft'].iloc[0]
    away_id = df_date['Auswärtsmannschaft_ID'].iloc[0]
    
    driver = webdriver.Chrome(service=Service('D:\Projects\Football\Database\Crawler_Code\Webdrivers\chromedriver.exe'))
    
    driver.get(url)
    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH,'//button[@id="L2AGLb"]'))).click()
    time.sleep(5)

    l_system_h = []
    l_system_a = []
                                                           

    systeme = driver.find_elements("xpath", "//span[@class='lrvl-tvc lrvl-f']")
    l_system_h.append(systeme[0].text)
    l_system_a.append(systeme[1].text)
    
    driver.quit()
    
    df_system_h = pd.DataFrame(l_system_h)
    df_system_a = pd.DataFrame(l_system_a)
       
    system_h = str(df_system_h.iloc[0,0])
    system_a = str(df_system_a.iloc[0,0])
    df = pd.DataFrame( {'Vereins_ID': [c, away_id], 'Verein': [Heimannschaft, Auswärtsmannschaft], 'Spieltag' : [s, s],
                        'Spiel_System' : [system_h, system_a], 'Saison' : [saison, saison]})
    
    return df

url = 'https://www.google.de/search?q=premier+league&sxsrf=ALiCzsZmojUx2D0Wo-UHt_kTXB6I-H1gkw%3A1662296465265&ei=kaEUY7XoD7LBlAa7hL-IDA&ved=0ahUKEwj19MrymPv5AhWyIMUKHTvCD8EQ4dUDCA4&uact=5&oq=premier+league&gs_lcp=Cgdnd3Mtd2l6EAMyBwgjELADECcyBwgjELADECcyBwgjELADECcyCggAEEcQ1gQQsAMyCggAEEcQ1gQQsAMyCggAEEcQ1gQQsAMyCggAEEcQ1gQQsAMyCggAEEcQ1gQQsAMyCggAEEcQ1gQQsAMyCggAEEcQ1gQQsAMyCggAEEcQ1gQQsAMyDQgAEOQCENYEELADGAEyDQgAEOQCENYEELADGAEyDQgAEOQCENYEELADGAEyDAguEMgDELADEEMYAjIPCC4Q1AIQyAMQsAMQQxgCMgwILhDIAxCwAxBDGAIyDAguEMgDELADEEMYAkoECEEYAEoECEYYAVDBCFipGWCPGmgDcAF4AIABAIgBAJIBAJgBAKABAcgBEsABAdoBBggBEAEYCdoBBggCEAEYCA&sclient=gws-wiz#sie=m;/g/11t3cx6vf5;2;/m/02_tc;dt;fp;1;;;'
saison = '2022/23'
spieltag = 6
club_id = 39


#df = get_system_premier_league_url(club_id, spieltag, saison, url)
#db.upload_local_data_to_database(df, 'pl_data_vereine_spielsystem')





def get_system_aufstellung(c, s, saison):

    f1 = db.get_data_db(26)
    df_mapping_transfermarkt = f1.get_data()
    df_mapping_transfermarkt = df_mapping_transfermarkt[df_mapping_transfermarkt['Saison']==saison]
    df_mapping_transfermarkt = df_mapping_transfermarkt[df_mapping_transfermarkt['Spieltag']==s]
    id_Verein = df_mapping_transfermarkt['Vereins_ID']
    v1 = id_Verein.iloc[c-1]
    verein = df_mapping_transfermarkt['Verein'].iloc[c-1]
    print(verein)
    f1 = db.get_data_db(20)
    df_ergebnisse = f1.get_data()
    df_ergebnisse = df_ergebnisse[df_ergebnisse['Saison']==saison] 
    df_ergebnisse = df_ergebnisse[df_ergebnisse['Spieltag']==s] 
    df_ergebnisse.index = range(len(df_ergebnisse.index))

    f2 = db.get_data_db(49)
    df_spieler = f2.get_data()
    df_spieler = df_spieler[df_spieler['Saison']==saison] 
    df_spieler = df_spieler[df_spieler['Spieltag']==s]
    df_spieler['Nachname'] = df_spieler['Spieler'].str.split(' ', expand = True)[1]

    f3 = db.get_data_db(2)
    df_date = f3.get_data()
    df_date = df_date[df_date['Saison']==saison] 
    df_date = df_date[df_date['Spieltag']==s]
    
    print(v1)
    df_v = df_ergebnisse[df_ergebnisse['Heimmannschaft_ID']==v1]


    df_h = df_v[['Spieltag', 'Heimmannschaft_ID', 'Heimmannschaft','Saison']]
    df_a = df_v[['Spieltag', 'Auswärtsmannschaft_ID', 'Auswärtsmannschaft', 'Saison']]
    
    heimmannschaft_spieler = df_v['Heimmannschaft'].iloc[0]
    auswärtsmannschaft_spieler = df_v['Auswärtsmannschaft'].iloc[0]
    
              
    df_spieler_h = df_spieler[df_spieler['Verein']==heimmannschaft_spieler]
    df_spieler_a = df_spieler[df_spieler['Verein']==auswärtsmannschaft_spieler]
    
    df_h = df_h.rename(columns = {'Heimmannschaft_ID': 'Vereins_ID', 'Heimmannschaft':'Verein'})
    df_a = df_a.rename(columns = {'Auswärtsmannschaft_ID': 'Vereins_ID', 'Auswärtsmannschaft':'Verein'})
    
    driver = webdriver.Firefox(executable_path=r'D:\Projects\Football\Database\Crawler_Code\Webdrivers\geckodriver')
    driver.get('http://www.google.com')
    
    if v1 == 7:
        verein = 'Bayern'  
        
    #query = 'bundesliga ergebnisse ' + str(day) +'.'+str(month)+ ' FC Bayern München'
    query = 'bundesliga ergebnisse saison '  + str(saison) +' '+ str(verein) +' '  + str(s) +  ' Spieltag'    
    search = driver.find_element_by_name('q')
    search.send_keys(query)
    time.sleep(3)
    search.send_keys(Keys.RETURN)
    
    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH,'//button[@id="L2AGLb"]'))).click()
    wait = WebDriverWait(driver, 10)
    element = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, 'imso-hov')))
    element.click()
    time.sleep(5)
    
    element = driver.find_element_by_xpath("//li[@data-hveid='CAEQDQ']")
    element.click()
    #CAEQDQ
    time.sleep(5)
    start_aufstellung = driver.find_elements_by_xpath("//div[@class='lr-vl-ls']")
    l_start_aufstellung_h = []
    l_start_aufstellung_a = []
    l_start_auswechseln_h = []
    l_start_auswechseln_a = []
    l_system_h = []
    l_system_a = []
    
    l_start_aufstellung_h.append(start_aufstellung[0].text)
    l_start_aufstellung_a.append(start_aufstellung[1].text)
    
    
    auswechsler_heim = driver.find_elements_by_xpath("//td[@class='lr-imso-ls-ftnc lr-imso-ls-lt']")
    auswechsler_auswärts = driver.find_elements_by_xpath("//td[@class='lr-imso-ls-stnc lr-imso-ls-rt']")
    
    n_h = len(auswechsler_heim)
    
    for i in range(0,n_h):
        l_start_auswechseln_h.append(auswechsler_heim[i].text)
        
    n_a = len(auswechsler_auswärts)
    
    for x in range(0,n_a):
        l_start_auswechseln_a.append(auswechsler_auswärts[x].text)
    
           
    systeme = driver.find_elements_by_xpath("//span[@class='lrvl-tvc lrvl-f']") 
    
    l_system_h.append(systeme[0].text)
    l_system_a.append(systeme[1].text)
    
    driver.quit()
    
    df_start_h = pd.DataFrame(l_start_aufstellung_h)
    df_start_a = pd.DataFrame(l_start_aufstellung_a)
    df_auswechseln_h = pd.DataFrame(l_start_auswechseln_h)
    df_auswechseln_a = pd.DataFrame(l_start_auswechseln_a)
    df_system_h = pd.DataFrame(l_system_h)
    df_system_a = pd.DataFrame(l_system_a)
    
    end_1 = len(df_start_h[0].str.split('\n', expand = True).columns)-1
    end_2 = len(df_start_a[0].str.split('\n', expand = True).columns)-1
    
    start_elf_heim = df_start_h[0].str.split('\n', expand = True).T.drop([0,2,4,6,8,10,12,14,16,18, end_1-1], axis = 0)[0].str.split('.', expand = True)[1]        
    start_elf_auswärts = df_start_a[0].str.split('\n', expand = True).T.drop([0,2,4,6,8,10,12,14,16,18, end_2-1], axis = 0)[0].str.split('.', expand = True)[1]
    
    df_start_elf_heim = pd.DataFrame(start_elf_heim)
    df_start_elf_auswärts = pd.DataFrame(start_elf_auswärts)
    df_start_elf_heim.columns = ['Spieler']
    df_start_elf_auswärts.columns = ['Spieler']
    
    #print(df_start_elf_heim)
    #print(df_start_elf_auswärts)
    
    auswechseln_h = df_auswechseln_h[0].str.split(' ', expand = True)[1]
    auswechseln_a = df_auswechseln_a[0].str.split(' ', expand = True)[1]
    df_auswechseln_h = pd.DataFrame(auswechseln_h)
    df_auswechseln_a = pd.DataFrame(auswechseln_a)
    
    
    system_h = str(df_system_h.iloc[0,0])
    system_a = str(df_system_a.iloc[0,0])
    
    df_auswechseln_h.columns = ['Spieler']
    df_auswechseln_a.columns = ['Spieler']
    
    
    f4 = t.unify_letters(df_auswechseln_h, 1)
    f5 = t.unify_letters(df_auswechseln_a, 1)
    f6 = t.unify_letters(df_start_elf_heim, 1)
    f7 = t.unify_letters(df_start_elf_auswärts, 1)
    
    df_auswechseln_h = f4.replace_letters()
    df_auswechseln_a = f5.replace_letters()
    df_start_heim = f6.replace_letters()
    df_start_auswärts = f7.replace_letters()
    
    df_auswechseln_h = df_auswechseln_h.rename({'Spieler':'Nachname'}, axis = 1)
    df_auswechseln_a = df_auswechseln_a.rename({'Spieler':'Nachname'}, axis = 1)
    df_start_heim = df_start_heim.rename({'Spieler':'Nachname'}, axis = 1)
    df_start_auswärts = df_start_auswärts.rename({'Spieler':'Nachname'}, axis = 1)
    
    df_start_heim['Nachname'] = df_start_heim['Nachname'].str.strip()
    df_spieler_h['Nachname'] = df_spieler_h['Nachname'].str.strip()
    df_start_auswärts['Nachname'] = df_start_auswärts['Nachname'].str.strip()
    df_spieler_a['Nachname'] = df_spieler_a['Nachname'].str.strip()
    #print(df_auswechseln_h)
    #print(df_spieler_h)        

    #df_auswechseln_heim = df_auswechseln_h.merge(df_spieler_h, on = 'Spieler', how = 'inner')
    #df_auswechseln_auswärts = df_auswechseln_a.merge(df_spieler_a, on = 'Spieler', how = 'inner')
    df_auswechseln_auswärts = df_auswechseln_a.merge(df_spieler_a, on = ['Nachname'], how = 'inner')
    df_auswechseln_heim = df_auswechseln_h.merge(df_spieler_h, on = ['Nachname'], how = 'inner')
    
    df_start_heim = df_start_heim.merge(df_spieler_h, on = 'Nachname', how = 'inner')
    df_start_auswärts = df_start_auswärts.merge(df_spieler_a, on = 'Nachname', how = 'inner')   

 
    df_start_heim = df_start_heim.assign(Startelf = 1)
    df_start_auswärts = df_start_auswärts.assign(Startelf = 1)
    df_auswechseln_heim = df_auswechseln_heim.assign(Startelf = 0)
    df_auswechseln_auswärts = df_auswechseln_auswärts.assign(Startelf = 0)
    
    actually_played_h = df_start_heim.append(df_auswechseln_heim)
    actually_played_a = df_start_auswärts.append(df_auswechseln_auswärts)
    
    actually_played_h = actually_played_h.assign(System = system_h)
    actually_played_a = actually_played_a.assign(System = system_a)
    
    all_player = actually_played_h.append(actually_played_a)
    all_player = all_player.rename({'Nachname':'Spieler', 'System':'Spiel_System'}, axis = 1)
    
    all_player = all_player[['Vereins_ID', 'Verein', 'Spieler_ID', 'Spieler', 'Spieltag', 'Startelf', 'Spiel_System', 'Saison']]
    all_player['Spieler_ID'] = all_player['Spieler_ID'].astype(int)
    all_player = all_player.drop_duplicates()
    
    return all_player


#df = get_system_aufstellung(1, 1, '2020/21')


#db.upload_local_data_to_database(df, 'bl1_data_spieler_startelf_system')

def get_system_aufstellung_other_form(c, s, saison):
    
    f1 = db.get_data_db(26)
    df_mapping_transfermarkt = f1.get_data()
    df_mapping_transfermarkt = df_mapping_transfermarkt[df_mapping_transfermarkt['Saison']==saison]
    df_mapping_transfermarkt = df_mapping_transfermarkt[df_mapping_transfermarkt['Spieltag']==s]
    id_Verein = df_mapping_transfermarkt['Vereins_ID']
    v1 = id_Verein.iloc[c-1]
    verein = df_mapping_transfermarkt['Verein'].iloc[c-1]
    print(verein)
    f1 = db.get_data_db(20)
    df_ergebnisse = f1.get_data()
    df_ergebnisse = df_ergebnisse[df_ergebnisse['Saison']==saison] 
    df_ergebnisse = df_ergebnisse[df_ergebnisse['Spieltag']==s] 
    df_ergebnisse.index = range(len(df_ergebnisse.index))
    
    f2 = db.get_data_db(49)
    df_spieler = f2.get_data()
    df_spieler = df_spieler[df_spieler['Saison']==saison] 
    df_spieler = df_spieler[df_spieler['Spieltag']==s]
    df_spieler['Nachname'] = df_spieler['Spieler'].str.split(' ', expand = True)[1]
    
    f3 = db.get_data_db(2)
    df_date = f3.get_data()
    df_date = df_date[df_date['Saison']==saison] 
    df_date = df_date[df_date['Spieltag']==s]
    

    print(v1)
    df_v = df_ergebnisse[df_ergebnisse['Heimmannschaft_ID']==v1]
    
    
    df_h = df_v[['Spieltag', 'Heimmannschaft_ID', 'Heimmannschaft', 'Saison']]
    df_a = df_v[['Spieltag', 'Auswärtsmannschaft_ID', 'Auswärtsmannschaft',  'Saison']]
    
    heimmannschaft_spieler = df_v['Heimmannschaft'].iloc[0]
    auswärtsmannschaft_spieler = df_v['Auswärtsmannschaft'].iloc[0]
    
              
    df_spieler_h = df_spieler[df_spieler['Verein']==heimmannschaft_spieler]
    df_spieler_a = df_spieler[df_spieler['Verein']==auswärtsmannschaft_spieler]
    
    df_h = df_h.rename(columns = {'Heimmannschaft_ID': 'Vereins_ID', 'Heimmannschaft':'Verein'})
    df_a = df_a.rename(columns = {'Auswärtsmannschaft_ID': 'Vereins_ID', 'Auswärtsmannschaft':'Verein'})
    
    driver = webdriver.Firefox(executable_path=r'D:\Projects\Football\Database\Crawler_Code\Webdrivers\geckodriver')
    driver.get('http://www.google.com')
    
    if v1 == 7:
        verein = 'Bayern'  
        
    #query = 'bundesliga ergebnisse ' + str(day) +'.'+str(month)+ ' FC Bayern München'
    query = 'bundesliga ergebnisse saison '  + str(saison) +' '+ str(verein) +' '  + str(s) +  ' Spieltag'    
    search = driver.find_element_by_name('q')
    search.send_keys(query)
    time.sleep(3)
    search.send_keys(Keys.RETURN)
    
    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH,'//button[@id="L2AGLb"]'))).click()
    wait = WebDriverWait(driver, 10)
    element = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, 'imso-hov')))
    element.click()
    time.sleep(5)
    
    element = driver.find_element_by_xpath("//li[@data-hveid='CAEQDA']")
    element.click()
    
    time.sleep(5)
    aufstellung_auswärts = driver.find_elements_by_xpath("//td[@class='lr-imso-ls-stnc lr-imso-ls-rt']")
    aufstellung_heim = driver.find_elements_by_xpath("//td[@class='lr-imso-ls-ftnc lr-imso-ls-lt']")
    l_aufstellung_auswärts = []
    l_aufstellung_heim = []
    
    
    n_s = len(aufstellung_auswärts)
    for i in range(0,n_s):
        l_aufstellung_auswärts.append(aufstellung_auswärts[i].text)
        
    
    n_h = len(aufstellung_heim)
    
    for i in range(0,n_h):
        l_aufstellung_heim.append(aufstellung_heim[i].text)
        
    driver.quit()
    
    df_h = pd.DataFrame(l_aufstellung_heim)
    df_a = pd.DataFrame(l_aufstellung_auswärts)
    
    
    df_h.columns = ['Spieler']
    df_a.columns = ['Spieler']
    
    
    f4 = t.unify_letters(df_h, 1)
    f5 = t.unify_letters(df_a, 1)
    
    df_h = f4.replace_letters()
    df_a = f5.replace_letters()
    
    df_startelf_heim = df_h.iloc[0:11]
    df_startelf_auswärts = df_a.iloc[0:11]    
    df_auswechsler_heim = df_h.iloc[11:len(df_h)]
    df_auswechsler_auswärts = df_a.iloc[11:len(df_a)]
    
    
    df_auswechseln_heim = df_auswechsler_heim.merge(df_spieler_h, on = 'Spieler', how = 'inner')
    df_auswechseln_auswärts = df_auswechsler_auswärts.merge(df_spieler_a, on = 'Spieler', how = 'inner')
    
    df_start_heim = df_startelf_heim.merge(df_spieler_h, on = 'Spieler', how = 'inner')
    df_start_auswärts = df_startelf_auswärts.merge(df_spieler_a, on = 'Spieler', how = 'inner')   
    
     
    df_start_heim = df_start_heim.assign(Startelf = 1)
    df_start_auswärts = df_start_auswärts.assign(Startelf = 1)
    df_auswechseln_heim = df_auswechseln_heim.assign(Startelf = 0)
    df_auswechseln_auswärts = df_auswechseln_auswärts.assign(Startelf = 0)
    
    actually_played_h = df_start_heim.append(df_auswechseln_heim)
    actually_played_a = df_start_auswärts.append(df_auswechseln_auswärts)
    
    actually_played_h = actually_played_h.assign(Spiel_System = '-')
    actually_played_a = actually_played_a.assign(Spiel_System = '-')
    
    all_player = actually_played_h.append(actually_played_a)
    
    all_player = all_player[['Vereins_ID', 'Verein', 'Spieler_ID', 'Spieler', 'Spieltag', 'Startelf', 'Spiel_System', 'Saison']]
    all_player['Spieler_ID'] = all_player['Spieler_ID'].astype(int)
    all_player = all_player.drop_duplicates()

    return all_player

#saison = '2020/21'
#s = 1
#c = 1
#df = get_system_aufstellung_other_form(c, s, saison)
#db.upload_local_data_to_database(df, 'bl1_data_spieler_startelf_system')



def get_system_aufstellung_with_url(c, s, saison, url):

    f1 = db.get_data_db(26)
    df_mapping_transfermarkt = f1.get_data()
    df_mapping_transfermarkt = df_mapping_transfermarkt[df_mapping_transfermarkt['Saison']==saison]
    df_mapping_transfermarkt = df_mapping_transfermarkt[df_mapping_transfermarkt['Spieltag']==s]
    id_Verein = df_mapping_transfermarkt['Vereins_ID']
    v1 = id_Verein.iloc[c-1]
    verein = df_mapping_transfermarkt['Verein'].iloc[c-1]
    print(verein)
    f1 = db.get_data_db(20)
    df_ergebnisse = f1.get_data()
    df_ergebnisse = df_ergebnisse[df_ergebnisse['Saison']==saison] 
    df_ergebnisse = df_ergebnisse[df_ergebnisse['Spieltag']==s] 
    df_ergebnisse.index = range(len(df_ergebnisse.index))

    f2 = db.get_data_db(49)
    df_spieler = f2.get_data()
    df_spieler = df_spieler[df_spieler['Saison']==saison] 
    df_spieler = df_spieler[df_spieler['Spieltag']==s]
    df_spieler['Nachname'] = df_spieler['Spieler'].str.split(' ', expand = True)[1]

    f3 = db.get_data_db(2)
    df_date = f3.get_data()
    df_date = df_date[df_date['Saison']==saison] 
    df_date = df_date[df_date['Spieltag']==s]
    
    print(v1)
    df_v = df_ergebnisse[df_ergebnisse['Heimmannschaft_ID']==v1]


    df_h = df_v[['Spieltag', 'Heimmannschaft_ID', 'Heimmannschaft', 'Saison']]
    df_a = df_v[['Spieltag', 'Auswärtsmannschaft_ID', 'Auswärtsmannschaft', 'Saison']]
    
    heimmannschaft_spieler = df_v['Heimmannschaft'].iloc[0]
    auswärtsmannschaft_spieler = df_v['Auswärtsmannschaft'].iloc[0]
    
              
    df_spieler_h = df_spieler[df_spieler['Verein']==heimmannschaft_spieler]
    df_spieler_a = df_spieler[df_spieler['Verein']==auswärtsmannschaft_spieler]
    
    df_h = df_h.rename(columns = {'Heimmannschaft_ID': 'Vereins_ID', 'Heimmannschaft':'Verein'})
    df_a = df_a.rename(columns = {'Auswärtsmannschaft_ID': 'Vereins_ID', 'Auswärtsmannschaft':'Verein'})
    
    driver = webdriver.Firefox(executable_path=r'D:\Projects\Football\Database\Crawler_Code\Webdrivers\geckodriver')
    driver.get(url)

    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH,'//button[@id="L2AGLb"]'))).click()
    #CAEQDQ
    time.sleep(5)
    start_aufstellung = driver.find_elements_by_xpath("//div[@class='lr-vl-ls']")
    l_start_aufstellung_h = []
    l_start_aufstellung_a = []
    l_start_auswechseln_h = []
    l_start_auswechseln_a = []
    l_system_h = []
    l_system_a = []
    
    l_start_aufstellung_h.append(start_aufstellung[0].text)
    l_start_aufstellung_a.append(start_aufstellung[1].text)
    
    
    auswechsler_heim = driver.find_elements_by_xpath("//td[@class='lr-imso-ls-ftnc lr-imso-ls-lt']")
    auswechsler_auswärts = driver.find_elements_by_xpath("//td[@class='lr-imso-ls-stnc lr-imso-ls-rt']")
    
    n_h = len(auswechsler_heim)
    
    for i in range(0,n_h):
        l_start_auswechseln_h.append(auswechsler_heim[i].text)
        
    n_a = len(auswechsler_auswärts)
    
    for x in range(0,n_a):
        l_start_auswechseln_a.append(auswechsler_auswärts[x].text)
    
           
    systeme = driver.find_elements_by_xpath("//span[@class='lrvl-tvc lrvl-f']") 
    
    l_system_h.append(systeme[0].text)
    l_system_a.append(systeme[1].text)
    
    driver.quit()
    
    df_start_h = pd.DataFrame(l_start_aufstellung_h)
    df_start_a = pd.DataFrame(l_start_aufstellung_a)
    df_auswechseln_h = pd.DataFrame(l_start_auswechseln_h)
    df_auswechseln_a = pd.DataFrame(l_start_auswechseln_a)
    df_system_h = pd.DataFrame(l_system_h)
    df_system_a = pd.DataFrame(l_system_a)
    
    end_1 = len(df_start_h[0].str.split('\n', expand = True).columns)-1
    end_2 = len(df_start_a[0].str.split('\n', expand = True).columns)-1
    
    start_elf_heim = df_start_h[0].str.split('\n', expand = True).T.drop([0,2,4,6,8,10,12,14,16,18, end_1-1], axis = 0)[0].str.split('.', expand = True)[1]        
    start_elf_auswärts = df_start_a[0].str.split('\n', expand = True).T.drop([0,2,4,6,8,10,12,14,16,18, end_2-1], axis = 0)[0].str.split('.', expand = True)[1]
    
    df_start_elf_heim = pd.DataFrame(start_elf_heim)
    df_start_elf_auswärts = pd.DataFrame(start_elf_auswärts)
    df_start_elf_heim.columns = ['Spieler']
    df_start_elf_auswärts.columns = ['Spieler']
    
    #print(df_start_elf_heim)
    #print(df_start_elf_auswärts)
    
    auswechseln_h = df_auswechseln_h[0].str.split(' ', expand = True)[1]
    auswechseln_a = df_auswechseln_a[0].str.split(' ', expand = True)[1]
    df_auswechseln_h = pd.DataFrame(auswechseln_h)
    df_auswechseln_a = pd.DataFrame(auswechseln_a)
    
    
    system_h = str(df_system_h.iloc[0,0])
    system_a = str(df_system_a.iloc[0,0])
    
    df_auswechseln_h.columns = ['Spieler']
    df_auswechseln_a.columns = ['Spieler']
    
    
    f4 = t.unify_letters(df_auswechseln_h, 1)
    f5 = t.unify_letters(df_auswechseln_a, 1)
    f6 = t.unify_letters(df_start_elf_heim, 1)
    f7 = t.unify_letters(df_start_elf_auswärts, 1)
    
    df_auswechseln_h = f4.replace_letters()
    df_auswechseln_a = f5.replace_letters()
    df_start_heim = f6.replace_letters()
    df_start_auswärts = f7.replace_letters()
    
    df_auswechseln_h = df_auswechseln_h.rename({'Spieler':'Nachname'}, axis = 1)
    df_auswechseln_a = df_auswechseln_a.rename({'Spieler':'Nachname'}, axis = 1)
    df_start_heim = df_start_heim.rename({'Spieler':'Nachname'}, axis = 1)
    df_start_auswärts = df_start_auswärts.rename({'Spieler':'Nachname'}, axis = 1)
    
    df_start_heim['Nachname'] = df_start_heim['Nachname'].str.strip()
    df_spieler_h['Nachname'] = df_spieler_h['Nachname'].str.strip()
    df_start_auswärts['Nachname'] = df_start_auswärts['Nachname'].str.strip()
    df_spieler_a['Nachname'] = df_spieler_a['Nachname'].str.strip()
    #print(df_auswechseln_h)
    #print(df_spieler_h)        

    #df_auswechseln_heim = df_auswechseln_h.merge(df_spieler_h, on = 'Spieler', how = 'inner')
    #df_auswechseln_auswärts = df_auswechseln_a.merge(df_spieler_a, on = 'Spieler', how = 'inner')
    df_auswechseln_auswärts = df_auswechseln_a.merge(df_spieler_a, on = ['Nachname'], how = 'inner')
    df_auswechseln_heim = df_auswechseln_h.merge(df_spieler_h, on = ['Nachname'], how = 'inner')
    
    df_start_heim = df_start_heim.merge(df_spieler_h, on = 'Nachname', how = 'inner')
    df_start_auswärts = df_start_auswärts.merge(df_spieler_a, on = 'Nachname', how = 'inner')   

 
    df_start_heim = df_start_heim.assign(Startelf = 1)
    df_start_auswärts = df_start_auswärts.assign(Startelf = 1)
    df_auswechseln_heim = df_auswechseln_heim.assign(Startelf = 0)
    df_auswechseln_auswärts = df_auswechseln_auswärts.assign(Startelf = 0)
    
    actually_played_h = df_start_heim.append(df_auswechseln_heim)
    actually_played_a = df_start_auswärts.append(df_auswechseln_auswärts)
    
    actually_played_h = actually_played_h.assign(System = system_h)
    actually_played_a = actually_played_a.assign(System = system_a)
    
    all_player = actually_played_h.append(actually_played_a)
    all_player = all_player.rename({'Nachname':'Spieler', 'System':'Spiel_System'}, axis = 1)
    
    all_player = all_player[['Vereins_ID', 'Verein', 'Spieler_ID', 'Spieler', 'Spieltag', 'Startelf', 'Spiel_System', 'Saison']]
    all_player['Spieler_ID'] = all_player['Spieler_ID'].astype(int)
    all_player = all_player.drop_duplicates()
    
    return all_player


#url = 'https://www.google.de/search?q=premier+league&sxsrf=AOaemvJupYqx3HtXVsTxVtlSlYi8exKtIQ%3A1641928722854&source=hp&ei=EtjdYfuhMdGP9u8PgemQ-As&iflsig=ALs-wAMAAAAAYd3mIn_LlxC1BrfskOU9ltf5lRWgTApt&ved=0ahUKEwj78pOStar1AhXRh_0HHYE0BL8Q4dUDCAk&uact=5&oq=premier+league&gs_lcp=Cgdnd3Mtd2l6EAMyBAgjECcyBAgjECcyBAgjECcyCAguEIAEELEDMggIABCABBCxAzIFCAAQywEyCAgAEIAEELEDMgsIABCABBCxAxCDATIFCC4QywEyCAgAEIAEELEDOg4ILhCABBCxAxDHARDRAzoICC4QsQMQgwE6EQguEIAEELEDEIMBEMcBEKMCOgsILhCABBDHARCvAToOCC4QgAQQsQMQxwEQowI6CwguEIAEEMcBEKMCOgUILhCABDoFCAAQgARQAFj3C2DdDGgAcAB4AIABjAGIAdAKkgEDNy43mAEAoAEB&sclient=gws-wiz#sie=m;/g/11nnqtmr44;2;/m/02_tc;dt;fp;1;;'
#saison = '2021/22'
#s = 13
#c = 12
#df = get_system_aufstellung_with_url(c, s, saison, url)
#db.upload_local_data_to_database(df, 'bl1_data_spieler_startelf_system')

#saison = '2020/21'
#s = 19
#c = 8
#df = get_system_aufstellung(c, s, saison)

#df['Spieltag']








def get_system_premier_league(c, s, saison):

    f3 = db.get_data_db(63)
    df_date = f3.get_data()
    df_date = df_date[df_date['Saison']==saison] 
    df_date = df_date[df_date['Spieltag']==s]
    df_date = df_date[df_date['Heimmannschaft_ID']==c]

    Heimannschaft = df_date['Heimmannschaft'].iloc[0]
    Auswärtsmannschaft = df_date['Auswärtsmannschaft'].iloc[0]
    away_id = df_date['Auswärtsmannschaft_ID'].iloc[0]
    #driver = webdriver.Chrome(executable_path=r"D:\Projects\Football\Database\Crawler_Code\Webdrivers\chromedriver.exe")
    driver = webdriver.Firefox(executable_path=r'D:\Projects\Football\Database\Crawler_Code\Webdrivers\geckodriver')
    #driver = webdriver.Edge(r"D:\Projects\Football\Database\Crawler_Code\Webdrivers\msedgedriver.exe")
    driver.get('http://www.google.com')
    heimmannschaft_query = Heimannschaft
    #if c == 58:
     #   heimmannschaft_query = 'Watford'
    if c == 54:
        heimmannschaft_query = 'West Brom' 
    #if c == 57:
     #   heimmannschaft_query = Auswärtsmannschaft
    #if c == 46:
     #   heimmannschaft_query = 'Newcastle' 
    if c == 45:
        heimmannschaft_query = 'Southampton FC' 
    if c == 62:
        heimmannschaft_query = 'Brighton' 
    if c == 44:
        heimmannschaft_query = 'Everton FC'         
    query = 'premier league results '+ str(saison) + ' ' + str(heimmannschaft_query) +' ' + str(s) +  ' matchday'  
    #query = str(heimmannschaft_query) +' results season '+ str(saison) + ' ' + str(s) +  ' matchday' 
    driver.get('https://www.google.com/search?q={}'.format(query))
    time.sleep(3)
    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH,'//button[@id="L2AGLb"]'))).click()
    #WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH,'//div[@class="imso-loa imso_mh__mh-ed"]'))).click()
   
    wait = WebDriverWait(driver, 10)
    element = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, 'imso-hov')))
    element.click()
    time.sleep(5)
    
    element = driver.find_element_by_xpath("//li[@data-hveid='CAEQDQ']")
    element.click() 
#    CAEQDA
    l_system_h = []
    l_system_a = []
    time.sleep(5)                                                       
    systeme = driver.find_elements_by_xpath("//span[@class='lrvl-tvc lrvl-f']") 
    
    if len(systeme)!=0:
        print(systeme)
        l_system_h.append(systeme[0].text)
        l_system_a.append(systeme[1].text)
        print(l_system_h)
        print(l_system_a)
        driver.quit()
        
        df_system_h = pd.DataFrame(l_system_h)
        df_system_a = pd.DataFrame(l_system_a)
           
        system_h = str(df_system_h.iloc[0,0])
        system_a = str(df_system_a.iloc[0,0])
    else:
        system_h = '-'
        system_a = '-'
        driver.quit()
    df = pd.DataFrame( {'Vereins_ID': [c, away_id], 'Verein': [Heimannschaft, Auswärtsmannschaft], 'Spieltag' : [s, s],
                        'Spiel_System' : [system_h, system_a], 'Saison' : [saison, saison]})
    
    return df

def make_system_premier_league(c, s, saison, system_h, system_a):

    f3 = db.get_data_db(63)
    df_date = f3.get_data()
    df_date = df_date[df_date['Saison']==saison] 
    df_date = df_date[df_date['Spieltag']==s]
    df_date = df_date[df_date['Heimmannschaft_ID']==c]

    Heimannschaft = df_date['Heimmannschaft'].iloc[0]
    Auswärtsmannschaft = df_date['Auswärtsmannschaft'].iloc[0]
    away_id = df_date['Auswärtsmannschaft_ID'].iloc[0]
    
    df = pd.DataFrame( {'Vereins_ID': [c, away_id], 'Verein': [Heimannschaft, Auswärtsmannschaft], 'Spieltag' : [s, s],
                        'Spiel_System' : [system_h, system_a], 'Saison' : [saison, saison]})

    return df    
    
    
#df = get_system_premier_league(38, 13, '2021/22')
#db.upload_local_data_to_database(df, 'pl_data_vereine_spielsystem')

#df = make_system_premier_league(55, 20, '2017/18', '4-4-2', '3-4-2-1')

def make_system_premier_league_gameday(s, saison):

    f3 = db.get_data_db(63)
    df_date = f3.get_data()
    df_date = df_date[df_date['Saison']==saison] 
    df_date = df_date[df_date['Spieltag']==s]
    df_all = pd.DataFrame()
    
    for c in df_date['Heimmannschaft_ID']:
        print(c)
        df_d = df_date[df_date['Heimmannschaft_ID']==c]
        Heimannschaft = df_d['Heimmannschaft'].iloc[0]
        Auswärtsmannschaft = df_d['Auswärtsmannschaft'].iloc[0]
        away_id = df_d['Auswärtsmannschaft_ID'].iloc[0]
        
        driver = webdriver.Firefox(executable_path=r'D:\Projects\Football\Database\Crawler_Code\Webdrivers\geckodriver')
        driver.get('http://www.google.com')
        
        heimmannschaft_query = Heimannschaft
        if c == 57:
            heimmannschaft_query = Auswärtsmannschaft  
        if c == 54:
            heimmannschaft_query = Auswärtsmannschaft    
        if c == 49:
            heimmannschaft_query = 'West Ham'  
        if c == 41:
            heimmannschaft_query = 'Arsenal'     
        if c == 62:
            heimmannschaft_query = 'Brighton' 
        if c == 45:
            heimmannschaft_query = 'Southampton FC'   
        if c == 64:
            heimmannschaft_query = 'Fulham'   
        if c == 44:
            heimmannschaft_query = 'Everton'
        #if c == 57:
        #    heimmannschaft_query = Auswärtsmannschaft           
        query = 'premier league results '+ str(saison) + ' ' + str(heimmannschaft_query) +' ' + str(s) +  ' matchday'    
        driver.get('https://www.google.com/search?q={}'.format(query))
        time.sleep(3)
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH,'//button[@id="L2AGLb"]'))).click()
        #WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH,'//div[@class="imso-loa imso_mh__mh-ed"]'))).click()
        
        wait = WebDriverWait(driver, 10)
        element = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, 'imso-hov')))
        element.click()
        time.sleep(5)
        
        element = driver.find_element_by_xpath("//li[@data-hveid='CAEQDQ']")
        element.click() 
    #  CAEQDA
        l_system_h = []
        l_system_a = []
        time.sleep(5)                                                       
        systeme = driver.find_elements_by_xpath("//span[@class='lrvl-tvc lrvl-f']") 
        
        if len(systeme)!=0:
            print(systeme)
            l_system_h.append(systeme[0].text)
            l_system_a.append(systeme[1].text)
            print(l_system_h)
            print(l_system_a)
            driver.quit()
            
            df_system_h = pd.DataFrame(l_system_h)
            df_system_a = pd.DataFrame(l_system_a)
               
            system_h = str(df_system_h.iloc[0,0])
            system_a = str(df_system_a.iloc[0,0])
        else:
            system_h = '-'
            system_a = '-'
            driver.quit()
        df = pd.DataFrame( {'Vereins_ID': [c, away_id], 'Verein': [Heimannschaft, Auswärtsmannschaft], 'Spieltag' : [s, s],
                            'Spiel_System' : [system_h, system_a], 'Saison' : [saison, saison]})
        #db.upload_local_data_to_database(df, 'pl_data_vereine_spielsystem')
        df_all = df_all.append(df)

    return df_all    
    
    
#df = make_system_premier_league_gameday(16, '2021/22')
#db.upload_local_data_to_database(df, 'pl_data_vereine_spielsystem')


