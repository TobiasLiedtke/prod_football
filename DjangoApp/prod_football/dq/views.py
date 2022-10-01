from django.shortcuts import render
from dq import models
import json
from django.http import HttpResponse

def main_page(request):
    return render(request, 'dq/main_page.html')

def dq(request):
    return render(request, 'dq/dq.html')


def bundesliga_all_check_matchday(request):

    if request.GET.get('bundesliga_all_matchdays') == 'bundesliga_all_matchdays':
    
        df = models.get_bundesliga_all_saison_spieltag_check()

        return HttpResponse(df.to_html())
    else:
       return render(request, 'dq/main_page.html')
   
def premierleague_all_check_matchday(request):

    if request.GET.get('premierleague_all_matchdays') == 'premierleague_all_matchdays':
    
        df = models.get_premierleague_all_saison_spieltag_check()

        return HttpResponse(df.to_html())
    else:
       return render(request, 'dq/main_page.html')   
   
def bundesliga_all_check_clubs(request):

    if request.GET.get('bundesliga_all_clubs') == 'bundesliga_all_clubs':
    
        df = models.get_bundesliga_all_club_count()

        return HttpResponse(df.to_html())
    else:
       return render(request, 'dq/main_page.html')
   
def premierleague_all_check_clubs(request):

    if request.GET.get('premierleague_all_clubs') == 'premierleague_all_clubs':
    
        df = models.get_premierleague_all_club_count()

        return HttpResponse(df.to_html())
    else:
       return render(request, 'dq/main_page.html')   



def duplicates_results(request):
    
    if request.GET.get('duplicates') == 'duplicates':
        
        no_duplicates_output_bundesliga, duplicates_output_bundesliga, no_duplicates_output_premierleage, duplicates_output_premierleage = models.check_duplicates_bl_pl()
    
        return render(request, 'dq/duplicates_results.html', {'no_duplicates_output_bundesliga':no_duplicates_output_bundesliga, 
                                                                              'duplicates_output_bundesliga':duplicates_output_bundesliga,
                                                                              'no_duplicates_output_premierleage':no_duplicates_output_premierleage,
                                                                              'duplicates_output_premierleage':duplicates_output_premierleage})
    else:
        return render(request, 'dq/main_page.html')  
    
def machtdays_results(request):
    
    if request.GET.get('machtdays') == 'machtdays':
        
        zipped_bundesliga, zipped_premier_league = models.check_matchdays_bl_pl()
    
        return render(request, 'dq/matchday_results.html', {'zipped_bundesliga':zipped_bundesliga, 
                                                             'zipped_premier_league':zipped_premier_league})
    else:
        return render(request, 'dq/main_page.html')  
    
def check_club_nbr(request):
    
    if request.GET.get('check_club') == 'check_club':
        zipped_bundesliga, zipped_homeid_bundesliga, zipped_premierleague, zipped_homeid_premierleague = models.check_clubs_premier()    

        return render(request, 'dq/club_nbr_check.html', {'zipped_bundesliga':zipped_bundesliga,
                                                                                        'zipped_homeid_bundesliga':zipped_homeid_bundesliga,
                                                                                        'zipped_premierleague':zipped_premierleague,
                                                                                        'zipped_homeid_premierleague':zipped_homeid_premierleague})
    else:
        return render(request, 'RandomForestApp/dq_bundesliga.html')