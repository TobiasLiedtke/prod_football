import os
os.chdir('D:/Projects/Football/PROD/Crawler')


import tkinter as tk
import pandas as pd

#import files

import Read_Load_Database as db

import Get_System_Team as sys


#define dashboard method and size/height
root = tk.Tk(className = "Data Provider for club level, referees and trainer")
canvas = tk.Canvas(root, height = 900, width = 1200)
canvas.pack()



fifa_entry = tk.Entry()

fifa_label = tk.Label(root, text = 'Fifa Url', font=('calibre', 10, 'bold'))


#define button function 
def get_system_bundesliga():
    
    global df_system
    spieltag = int(entry_spieltag.get())
    saison = entry_saison.get()
    heimmannschaft_id = int(entry_heimmannschaft_id.get())
    url = url_entry.get()

    df_system = sys.get_system(heimmannschaft_id, spieltag, saison, url)
    print(df_system)
    return df_system

def upload_system_bundesliga():

    db.upload_local_data_to_database(df_system, 'bl1_data_vereine_spielsystem')
    print("Data successfuly uploaded")  
    
def get_system_premierleague():
    
    global df_system_pl
    spieltag = int(entry_spieltag.get())
    saison = entry_saison.get()
    heimmannschaft_id = int(entry_heimmannschaft_id.get())
    url = url_entry.get()

    df_system_pl = sys.get_system_premier_league_url(heimmannschaft_id, spieltag, saison, url)
    print(df_system_pl)
    return df_system_pl

def upload_system_premierleague():

    db.upload_local_data_to_database(df_system_pl, 'pl_data_vereine_spielsystem')
    print("Data successfuly uploaded")  
    
#define buttons, entries and labels    
entry_saison = tk.Entry()
entry_spieltag = tk.Entry()
entry_heimmannschaft_id = tk.Entry()
url_entry = tk.Entry()

headliner_label = tk.Label(root, text = 'System Upload', font=('calibre', 14, 'bold')) 
saison_label = tk.Label(root, text = 'Season', font=('calibre', 10, 'bold')) 
spieltag_label = tk.Label(root, text = 'Matchday', font=('calibre', 10, 'bold'))
homeid_label = tk.Label(root, text = 'Home ID', font=('calibre', 10, 'bold'))
system_label = tk.Label(root, text = 'System Url', font=('calibre', 10, 'bold'))

headliner_label.place(relx = 0, rely = 0, relwidth = 0.5, relheight = 0.1) 

saison_label.place(relx = 0.05, rely = 0.06, relwidth = 0.1, relheight = 0.1) 
spieltag_label.place(relx = 0.05, rely = 0.12, relwidth = 0.1, relheight = 0.1) 
homeid_label.place(relx = 0.05, rely = 0.18, relwidth = 0.1, relheight = 0.1) 

entry_saison.place(relx = 0.15, rely = 0.1, relwidth = 0.05, relheight = 0.02)
entry_spieltag.place(relx = 0.15, rely = 0.16, relwidth = 0.05, relheight = 0.02)
entry_heimmannschaft_id.place(relx = 0.15, rely = 0.22, relwidth = 0.05, relheight = 0.02)

system_label.place(relx = 0.2, rely = 0.12, relwidth = 0.1, relheight = 0.1)
url_entry.place(relx = 0.3, rely = 0.16, relwidth = 0.3, relheight = 0.02) 



#comments
system_label.place(relx = 0.2, rely = 0.12, relwidth = 0.1, relheight = 0.1)
url_entry.place(relx = 0.3, rely = 0.16, relwidth = 0.3, relheight = 0.02) 



button_system = tk.Button(root, text = 'System Team Bl1',command = get_system_bundesliga)
button_system.pack()
button_system_upload = tk.Button(root, text = 'Upload System Bl1',command = upload_system_bundesliga)
button_system_upload.pack()


button_system_pl = tk.Button(root, text = 'System Team PL',command = get_system_premierleague)
button_system_pl.pack()
button_system_upload_pl = tk.Button(root, text = 'Upload System PL',command = upload_system_premierleague)
button_system_upload_pl.pack()

button_system.place(relx = 0.05, rely = 0.3, relwidth = 0.15, relheight = 0.05)
button_system_upload.place(relx = 0.05, rely = 0.35, relwidth = 0.15, relheight = 0.05)

button_system_pl.place(relx = 0.25, rely = 0.3, relwidth = 0.15, relheight = 0.05)
button_system_upload_pl.place(relx = 0.25, rely = 0.35, relwidth = 0.15, relheight = 0.05)
root.mainloop()
