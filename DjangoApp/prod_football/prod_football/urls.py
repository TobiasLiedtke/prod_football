"""prod_football URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/4.0/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path
from dq import views as view_dq
from prediction import views as view_prediction



urlpatterns = [
    path('admin/', admin.site.urls),
    path('', view_dq.main_page),
    path('dq/', view_dq.dq),
    path('dq/duplicates_results', view_dq.duplicates_results, name='duplicates_results'), 
    path('dq/machtdays_results', view_dq.machtdays_results, name='machtdays_results'), 
    path('dq/club_nbr_check', view_dq.check_club_nbr, name='check_club_nbr'), 
    path('dq/bundesliga_all_check_matchday', view_dq.bundesliga_all_check_matchday, name='bundesliga_all_check_matchday'),
    path('dq/premierleague_all_check_matchday', view_dq.premierleague_all_check_matchday, name='premierleague_all_check_matchday'), 
    path('dq/bundesliga_all_check_clubs', view_dq.bundesliga_all_check_clubs, name='bundesliga_all_check_clubs'), 
    path('dq/premierleague_all_check_clubs', view_dq.premierleague_all_check_clubs, name='premierleague_all_check_clubs'), 
    
    path('predict/', view_prediction.algorithm_choice), 
    path('predict/random_forest', view_prediction.league_choice), 
    path('predict/random_forest/premierleague', view_prediction.random_forest_results_premierleague, name='random_forest_results_premierleague'),
    path('predict/random_forest/bundesliga', view_prediction.random_forest_results, name='random_forest_results'),
    

]
