import pandas as pd
import pandas as pd 
from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import pickle
import lxml
from datetime import datetime
from datetime import date

def team_roster_base_u():
    team_roster_base = pd.read_csv("team_roster_base.csv")
    team_roster_base = team_roster_base[["Year", "Team", "team_dir"]]
    today = date.today()
    today_year, today_month, today_day= today.year, today.month, today.day
   
    if team_roster_base["Year"].max() < today_year:
        lag = today_year - team_roster_base["Year"].max()
        years = []
        for i in range(1, lag+1):
            years.append(team_roster_base["Year"].max()+i)

        # loop through each year
        for y in years:
            # NBA season to scrape- year is season end so 2015 is 2014-15 season 
            year = y
            url = f"https://www.basketball-reference.com/leagues/NBA_{year}_ratings.html"
            html = urlopen(url)
            soup = BeautifulSoup(html, features="lxml")
            table = soup.find('table', attrs={'id':'ratings'})
            teams = table.tbody.findAll("tr")
            for team in teams: #get team names and links - would get stats here as well
                team_name= team.td.string
                team_dir = team.td.a.get('href')
                team_year= {"Year": year, "Team": team_name, "team_dir": team_dir}
                team_roster_base = pd.concat([team_roster_base, pd.DataFrame([team_year])], ignore_index=True)
            time.sleep(5)
    team_roster_base = team_roster_base[["Year", "Team", "team_dir"]]
    team_roster_base.to_csv("team_roster_base.csv")
    return team_roster_base[["Year", "Team", "team_dir"]]

def get_rosters_u():
    today = date.today()
    today_year, today_month, today_day= today.year, today.month, today.day
    yearly_rosters = pd.read_csv('season_rosters.csv')[["Year","Team","team_dir","Player", "player_dir"]]
    team_roster_base = pd.read_csv("team_roster_base.csv")[["Year", "Team", "team_dir"]]
    
    if yearly_rosters["Year"].max() < today_year:
        lag = today_year - team_roster_base["Year"].max()
        years = []
        for i in range(1, lag+1):
            years.append(yearly_rosters["Year"].max()+i)
        
            
        team_roster_base = team_roster_base[team_roster_base["Year"].isin(years)]
    else:
        team_roster_base = team_roster_base[team_roster_base["Year"]==today_year]
        
    base_url = "https://www.basketball-reference.com"
    for index, row in team_roster_base.iterrows():
        print('\n', row, end = "")
        roster_url = base_url+str(row['team_dir'])
        time.sleep(random.randint(3, 7))
        html_team = urlopen(roster_url)
        soup = BeautifulSoup(html_team, features="lxml")
        roster_table = soup.find('table', attrs={'id':'roster'})
        players = roster_table.tbody.findAll("tr")
        year = [str(row['Year'])]
        team = [str(row['Team'])]
        team_dir = [str(row['team_dir'])]
        for player in players:
            print(".", end = "")
            player_name = [str(player.td.string)]
            player_dir = [str(player.td.a.get('href'))]
            team_year_player = pd.DataFrame({"Year": year,
                                             "Team": team, 
                                             "team_dir": team_dir, 
                                             "Player": player_name, 
                                             "player_dir": player_dir})
            yearly_rosters = pd.concat([yearly_rosters, team_year_player], ignore_index = True)
    yearly_rosters = yearly_rosters.drop_duplicates(subset=['Year', 'Team', 'Player'])
    yearly_rosters.to_csv('season_rosters.csv')
    return yearly_rosters.drop_duplicates(subset=['Year', 'Team', 'Player'])

def get_pers_u():
    #get player efficiency ratings 
    progress = 0 
    season_rosters = pd.read_csv('season_rosters.csv')[["Year","Team","team_dir","Player", "player_dir"]]
    per_table = pd.read_csv("player_ERs.csv")
    season_rosters['team_id'] = season_rosters['team_dir'].str.extract(r'/teams/(\w{3})/')
    season_rosters['season'] = (season_rosters['Year'] + -1).astype(str) + '-' + (season_rosters['Year']).astype(str).str.slice(2, 4)
    season_rosters.drop_duplicates(subset=['season', 'team_id', 'player_dir']).sort_values(by='player_dir')
    per_table = pd.read_csv("player_ERs.csv")
    per_table = per_table[per_table['team_id']!= 'TOT']
    per_table.drop_duplicates(subset=['season', 'team_id', 'player_dir']).sort_values(by='player_dir')
    merged_df = pd.merge(season_rosters, per_table, on=['season', 'team_id', 'player_dir'], how='outer', indicator=True)
    unmatched_rows = merged_df[merged_df['_merge'] == 'left_only'][["Year","Team","team_dir","Player", "player_dir"]]
    
    players_list = []
    
    for index, row in unmatched_rows.iterrows():
        progress+= 1
        if progress % len(unmatched_rows) == 0:
            print(round(progress/len(unmatched_rows), 4), "%", end='\t')
        season = str(int(row['Year']) - 1) + '-' + str(row['Year'])[2:]
        team = row['team_dir'][7:10]
        base_url = "https://www.basketball-reference.com"
        player_url = base_url+str(row['player_dir'])
        if row['player_dir'] in players_list:
            continue
        else:
            players_list.append(row['player_dir'])
        try:
            time.sleep(random.randint(3, 7))
            html_player = urlopen(player_url)
            player_seasons = []
            player_team_ids = []
            # player_team_dirs = []
            player_dirs = []
            player_pers = []
            soup = BeautifulSoup(html_player, features="lxml")
            adv_table = soup.find('table', attrs={'id':'advanced'})
            teams = adv_table.tbody.findAll("tr")
            for team in teams:
                # th is season td is all other stats 
                player_seasons.append(str(team.th.a.string))
                # print(player_seasons)
                other_stats = team.findAll('td')
                team_id = other_stats[1].string
                player_team_ids.append(str(team_id))
                # player_team_dirs.append(row['team_dir'])
                player_dirs.append(row['player_dir'])
                per_value = other_stats[6].string
                player_pers.append(per_value)
            this_player = pd .DataFrame({ "season": player_seasons,
                                         "team_id": player_team_ids, 
                                         "player_dir": player_dirs, 
                                         "per":player_pers})
            per_table = pd.concat([per_table, this_player], ignore_index = True)
        except:
            print(row)
        
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_parquet.html
    per_table.to_csv("player_ERs.csv")
    return per_table

def convert_date(date_string):
    date_object = datetime.strptime(date_string, "%a, %b %d, %Y")
    formated_date = date_object.strftime("%Y-%m-%d")
    return formated_date

def season_schedule_u():
    # Load initial season schedules
    season_schedules = pd.read_csv('season_schedules.csv')[['season', 'date', 'away_team_id', 'home_team_id', 'arena', 'link']]
    
    # Initialize errors DataFrame
    errors = pd.DataFrame(columns=["season", "month", "error"])
    
    # Get current date
    today_date = datetime.today()
    today_year, today_month = today_date.year, today_date.month
    
    months_array = ["may", "june", "july", "august", "september", "october", "november", "december", "january", "february", "march", "april"]
    
    # Loop over years and months
    for year in range(int(season_schedules['date'].max()[0:4]), today_year + 1):
        for month in months_array[(ss_month+7%12)+1:]: #loop over months
            print(month)
            month_schedule = f"https://www.basketball-reference.com/leagues/NBA_{year}_games-{month}.html"
            month_schedule_link = urlopen(month_schedule)
            soup = BeautifulSoup(month_schedule_link, features="lxml")
            schedule_table = soup.find('table', attrs={'id': 'schedule'})
            games = schedule_table.tbody.findAll("tr")
            season = []
            dates = []
            away_team_ids = []
            home_team_ids = []
            arenas = []
            links = []
            for game in games:
                if game.th.string == "Playoffs":
                    break
                date = game.th.a.string
                date_object =  datetime.strptime(date, "%a, %b %d, %Y")
                if "april" == date_object.strftime("%B").lower():
                    if int(date_object.strftime("%d")) > 14: #2023-24 regular season
                        season_month = pd.DataFrame({"season": season,
                                                     "date": dates,
                                                     "away_team_id": away_team_ids,
                                                     "home_team_id": home_team_ids,
                                                      "arena":arenas,
                                                     "link":links})
                        season_schedules = pd.concat([season_schedules, season_month], ignore_index=True)
                        season_schedules['date'] = season_schedules['date'].apply(lambda x: convert_date(x) if ',' in x else x)
                        season_schedules = season_schedules[['season', 'date', 'away_team_id', 'home_team_id', 'arena', 'link']].drop_duplicates(subset=['link'])
                        season_schedules.to_csv("season_schedules.csv", index=False)
                        break
                season.append(year)
                other_stats = game.findAll("td")
                dates.append(date)
                away_team = str(other_stats[1].a.get('href'))[7:10]
                away_team_ids.append(away_team)
                home_team = str(other_stats[3].a.get('href'))[7:10]
                home_team_ids.append(home_team)
                boxscore = other_stats[5].a.get('href')
                links.append(boxscore)
                arena = other_stats[8].string
                arenas.append(arena)

def game_stats_u():
    """
    gets individual game statistics given a seasons games as a pandas df
    """
    count = 1
    flag = 0
    season_schedule= pd.read_csv('season_schedules.csv')[['date', 'away_team_id', 'home_team_id', 'arena']]
    game_stats = pd.read_csv('combined_game_stats.csv').drop(columns=['Unnamed: 0'])[['date', 'home_team', 'away_team', 'arena']]
    
    # Filter rows that are in season_schedule but not in game_stats
    merged_df = pd.merge(season_schedule, game_stats, left_on=['date', 'away_team_id', 'home_team_id', 'arena'],
                         right_on=['date', 'away_team', 'home_team', 'arena'], how='left', indicator=True)
    
    # Filter rows that are in season_schedule but not in game_stats
    missing_rows = merged_df[merged_df['_merge'] == 'left_only'][season_schedule.columns]
    
    # merge season_schedule= pd.read_csv('season_schedules.csv') missing_rows
    
    # Show the missing rows
    season_schedule= pd.read_csv('season_schedules.csv')
    merged_missing_rows = pd.merge(missing_rows, season_schedule, on=['date', 'away_team_id', 'home_team_id', 'arena'], how='left')
    game_stats = pd.DataFrame(columns = ["date", "home_team", "home_fg", "home_fga", "home_fg_pct", "home_fg3", "home_fg3a", "home_fg3_pct", "home_ft",
                                          "home_fta", "home_ft_pct", "home_orb", "home_drb", "home_trb", "home_ast", "home_stl", "home_blk", "home_tov", 
                                          "home_pf", "home_pts", "away_team", "away_fg", "away_fga", "away_fg_pct", "away_fg3", "away_fg3a", "away_fg3_pct", 
                                          "away_ft", "away_fta", "away_ft_pct", "away_orb", "away_drb", "away_trb", "away_ast", "away_stl", "away_blk", 
                                          "away_tov", "away_pf", "away_pts", "arena"])
    
    print("getitng individual game stats", end = " ")

    for game in merged_missing_rows.iterrows():
        print('.', end="")
        game = game[1]
        home_team_id = game["home_team_id"]
        away_team_id = game["away_team_id"]
        arena = game["arena"]
        ext = game["link"]
        this_season = game["season"]
        try:
            box_score_url = f"https://www.basketball-reference.com{ext}"
            time.sleep(random.randint(5, 10))
            open_link = urlopen(box_score_url)
            soup = BeautifulSoup(open_link, features="lxml")
        except Exception as e:
            print(e)
            error_new = pd.DataFrame({"season": game["season"],
            "date": game["date"],
            "away_team_id": away_team_id,
            "home_team_id": home_team_id,
            "arena": arena,
            "link": ext,
            "error": ["404: invalid game link"]})
            try:
                errors = pd.concat([errors, error_new], ignore_index = True)
            except:
                errors = error_new
            continue
        try:
            home_stats = soup.find('table', attrs={'id':f"box-{home_team_id}-game-basic"}).tfoot.tr
            home_fg = home_stats.find('td', attrs = {'data-stat': 'fg'}).string
            home_fga = home_stats.find('td', attrs = {'data-stat': 'fga'}).string
            home_fg_pct = home_stats.find('td', attrs = {'data-stat': 'fg_pct'}).string
            home_fg3 = home_stats.find('td', attrs = {'data-stat': 'fg3'}).string
            home_fg3a = home_stats.find('td', attrs = {'data-stat': 'fg3a'}).string
            home_fg3_pct = home_stats.find('td', attrs = {'data-stat': 'fg3_pct'}).string
            home_ft = home_stats.find('td', attrs = {'data-stat': 'ft'}).string
            home_fta = home_stats.find('td', attrs = {'data-stat': 'fta'}).string
            home_ft_pct = home_stats.find('td', attrs = {'data-stat': 'ft_pct'}).string
            home_orb = home_stats.find('td', attrs = {'data-stat': 'orb'}).string
            home_drb = home_stats.find('td', attrs = {'data-stat': 'drb'}).string
            home_trb = home_stats.find('td', attrs = {'data-stat': 'trb'}).string
            home_ast = home_stats.find('td', attrs = {'data-stat': 'ast'}).string
            home_stl = home_stats.find('td', attrs = {'data-stat': 'stl'}).string
            home_blk = home_stats.find('td', attrs = {'data-stat': 'blk'}).string
            home_tov = home_stats.find('td', attrs = {'data-stat': 'tov'}).string
            home_pf = home_stats.find('td', attrs = {'data-stat': 'pf'}).string
            home_pts = home_stats.find('td', attrs = {'data-stat': 'pts'}).string

            away_stats = soup.find('table', attrs={'id':f"box-{away_team_id}-game-basic"}).tfoot.tr
            away_fg = away_stats.find('td', attrs = {'data-stat': 'fg'}).string
            away_fga = away_stats.find('td', attrs = {'data-stat': 'fga'}).string
            away_fg_pct = away_stats.find('td', attrs = {'data-stat': 'fg_pct'}).string
            away_fg3 = away_stats.find('td', attrs = {'data-stat': 'fg3'}).string
            away_fg3a = away_stats.find('td', attrs = {'data-stat': 'fg3a'}).string
            away_fg3_pct = away_stats.find('td', attrs = {'data-stat': 'fg3_pct'}).string
            away_ft = away_stats.find('td', attrs = {'data-stat': 'ft'}).string
            away_fta = away_stats.find('td', attrs = {'data-stat': 'fta'}).string
            away_ft_pct = away_stats.find('td', attrs = {'data-stat': 'ft_pct'}).string
            away_orb = away_stats.find('td', attrs = {'data-stat': 'orb'}).string
            away_drb = away_stats.find('td', attrs = {'data-stat': 'drb'}).string
            away_trb = away_stats.find('td', attrs = {'data-stat': 'trb'}).string
            away_ast = away_stats.find('td', attrs = {'data-stat': 'ast'}).string
            away_stl = away_stats.find('td', attrs = {'data-stat': 'stl'}).string
            away_blk = away_stats.find('td', attrs = {'data-stat': 'blk'}).string
            away_tov = away_stats.find('td', attrs = {'data-stat': 'tov'}).string
            away_pf = away_stats.find('td', attrs = {'data-stat': 'pf'}).string
            away_pts = away_stats.find('td', attrs = {'data-stat': 'pts'}).string

            this_game_stats = pd.DataFrame({
            "date": [game["date"]],
            "home_team":[home_team_id],
            "home_fg":[home_fg],
            "home_fga":[home_fga],
            "home_fg_pct":[home_fg_pct],
            "home_fg3":[home_fg3],
            "home_fg3a":[home_fg3a],
            "home_fg3_pct":[home_fg3_pct],
            "home_ft":[home_ft],
            "home_fta":[home_fta],
            "home_ft_pct":[home_ft_pct],
            "home_orb":[home_orb],
            "home_drb":[home_drb],
            "home_trb":[home_trb],
            "home_ast":[home_ast],
            "home_stl":[home_stl],
            "home_blk":[home_blk],
            "home_tov":[home_tov],
            "home_pf":[home_pf],
            "home_pts":[home_pts],
            "away_team":[away_team_id],
            "away_fg":[away_fg],
            "away_fga":[away_fga],
            "away_fg_pct":[away_fg_pct],
            "away_fg3":[away_fg3],
            "away_fg3a":[away_fg3a],
            "away_fg3_pct":[away_fg3_pct],
            "away_ft":[away_ft],
            "away_fta":[away_fta],
            "away_ft_pct":[away_ft_pct],
            "away_orb":[away_orb],
            "away_drb":[away_drb],
            "away_trb":[away_trb],
            "away_ast":[away_ast],
            "away_stl":[away_stl],
            "away_blk":[away_blk],
            "away_tov":[away_tov],
            "away_pf":[away_pf],
            "away_pts":[away_pts],
            "arena": [arena]})
            
            game_stats = pd.concat([game_stats, this_game_stats], ignore_index = True)
            
        except Exception as e:
            print(e)
            error_new = pd.DataFrame({"season": game["season"],
            "date": game["date"],
            "away_team_id": away_team_id,
            "home_team_id": home_team_id,
            "arena": arena,
            "link": ext,
            "error": ["error scraping table"]})
            flag = 1
            try:
                errors = pd.concat([errors, error_new], ignore_index = True)
            except Exception as e:
                print(e)
                errors = error_new
            continue

    if flag == 1:
        print("errors: ", errors)
    game_stats.to_csv("combined_game_stats.csv")
    return game_stats

def update_data(): 
    team_roster_base_u()
    get_rosters_u()
    get_pers_u()
    season_rosters = pd.read_csv("season_rosters.csv")[["Year","Team","team_dir","Player", "player_dir"]]
    season_rosters['team_id'] = season_rosters['team_dir'].str.extract(r'/teams/(\w{3})/')
    pers = pd.read_csv("player_ERs.csv")
    pers['Year'] = pers['season']
    pers['Year'] = pers['Year'].str[:2] + pers['Year'].str[-2:]
    pers['Year'] = pers['Year'].astype('int64')
    roster_pERs = season_rosters.merge(pers, how = "right", on=["Year", "player_dir", "team_id"])[['Year', 'Team', 'team_dir', 'Player', 'player_dir', 'team_id', 'season', 'per']]
    roster_pERs.to_csv("all_roster_pERs.csv")
    season_schedule_u()
    game_stats_u()