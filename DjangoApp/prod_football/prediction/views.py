from django.shortcuts import render
from prediction import models

def algorithm_choice(request):
    return render(request, 'prediction/forecast_leagues.html')

def league_choice(request):
    return render(request, 'prediction/random_forest_league_choice.html')

#def random_forest_bundesliga(request):
#    return render(request, 'prediction/random_forest.html')

#def random_forest_premierleague(request):
#    return render(request, 'prediction/random_forest_premierleague.html')


def random_forest_results(request):

    if request.GET.get('randomforest') == 'randomforest':

        variables = request.GET.getlist('selectvariables')

        vereins_id = int(request.GET.get('vereins_id'))
        saison = request.GET.get('saison')
        spieltag = int(request.GET.get('spieltag'))
        
        df = models.get_bundesliga_training_data()                                            
        results, x_variables, length_of_data_set, length_of_training_set = models.prepare_data(df, saison, spieltag, variables)
        
        df_forecast = models.get_bundesliga_forecast_data()
        home_team, away_team, forecast = models.prepare_forecast(df_forecast, saison, spieltag, vereins_id, variables) 
        
        x_variables, forecast = models.encode_test_training_set(x_variables, forecast, variables)
        
        y_proba = models.get_random_forest_proba(x_variables, results.ravel(), forecast)
        
        odds_home, odds_draw, odds_away = models.get_odds_random_forest(y_proba)

        return render(request, 'prediction/random_forest_results.html', {'odds_home':odds_home, 'odds_draw':odds_draw, 'odds_away':odds_away,
                                                                              'home_team':home_team, 'away_team':away_team, 'variables':variables,
                                                                              'length_of_data_set':length_of_data_set, 
                                                                              'length_of_training_set':length_of_training_set})
    else:
       return render(request, 'prediction/random_forest.html')


def random_forest_results_premierleague(request):

    if request.GET.get('randomforest') == 'randomforest':

        variables = request.GET.getlist('selectvariables')

        vereins_id = int(request.GET.get('vereins_id'))
        saison = request.GET.get('saison')
        spieltag = int(request.GET.get('spieltag'))
        
        df = models.get_premierleague_training_data()                                            
        results, x_variables, length_of_data_set, length_of_training_set = models.prepare_data(df, saison, spieltag, variables)
        
        df_forecast = models.get_premierleague_forecast_data()
        home_team, away_team, forecast = models.prepare_forecast(df_forecast, saison, spieltag, vereins_id, variables) 
        
        x_variables, forecast = models.encode_test_training_set(x_variables, forecast)
        
        y_proba = models.get_random_forest_proba(x_variables, results.ravel(), forecast)
        
        odds_home, odds_draw, odds_away = models.get_odds_random_forest(y_proba)

        return render(request, 'prediction/random_forest_results.html', {'odds_home':odds_home, 'odds_draw':odds_draw, 'odds_away':odds_away,
                                                                              'home_team':home_team, 'away_team':away_team, 'variables':variables,
                                                                              'length_of_data_set':length_of_data_set, 
                                                                              'length_of_training_set':length_of_training_set})
    else:
       return render(request, 'prediction/random_forest_premierleague.html')