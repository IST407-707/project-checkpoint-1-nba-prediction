# required libraries
from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import pickle
import lxml

def get_season_schedule(filename, seasons = [2015]):
    '''
    Gets the season schedule (games played throughout the season) up until the playoffs.
    creates a dataframe to inform us of any potential errors and allows code to not break because of errors
    '''
    season_schedule_table = pd.DataFrame(columns = ["season", "date", "away_team_id", "home_team_id", "arena", "link"])
    months = ["october", "november", "december", "january", "february", "march", "april"]
    for year in seasons:
        print(f"getting {year} data....")
        for month in months:
            month_schedule = f"https://www.basketball-reference.com/leagues/NBA_{year}_games-{month}.html"
            time.sleep(random.randint(2, 6))
            #checks whether the link is valid and lets us know if any connections didnt work by saving to erros df. some errors here are good!
            try:
                month_schedule_link = urlopen(month_schedule)
                soup = BeautifulSoup(month_schedule_link, features="lxml")
            except:
                #the month may not have been a part of the season/year so each year will have some error months. this is really here for the covid seasons and high level error checking
                errors = pd.DataFrame({"season": [year], "month":[month], "error": ["invalid season link"]})
                try:
                    name_of_file = f"{filename}_error.parquet.gzip"
                    errors = pd.concat([pd.read_parquet(name_of_file), errors], ignore_index = True)
                    errors.to_parquet(name_of_file)
                except:
                    name_of_file = f"{filename}_error.parquet.gzip"
                    errors.to_parquet(name_of_file)
                continue
            try:
                schedule_table = soup.find('table', attrs={'id':'schedule'})
                games = schedule_table.tbody.findAll("tr")
                season = []
                dates = []
                away_team_ids = []
                home_team_ids = []
                arenas = []
                links = []
                for game in games:
                #avoinding playoffs schedule
                    if game.th.string == "Playoffs":
                        break
                    season.append(year)
                    other_stats = game.findAll("td")
                    date = game.th.a.string
                    dates.append(date)
                    away_team = other_stats[1].a.get('href')[7:10]
                    away_team_ids.append(away_team)
                    home_team = other_stats[3].a.get('href')[7:10]
                    home_team_ids.append(home_team)
                    boxscore = other_stats[5].a.get('href')
                    links.append(boxscore)
                    arena = other_stats[8].string
                    arenas.append(arena)
                season_month = pd.DataFrame({"season": season,
                                             "date": dates,
                                             "away_team_id": away_team_ids,
                                             "home_team_id": home_team_ids,
                                              "arena":arenas,
                                             "link":links
                                            })
            except:
                #table error most likely 2020 april, where games werent played
                errors = pd.DataFrame({"season": [year], "month":[month], "error": ["unable to scrape table"]})
                try:
                    name_of_file = f"{filename}_error.parquet.gzip"
                    errors = pd.concat([pd.read_parquet(name_of_file), errors], ignore_index = True)
                    errors.to_parquet(name_of_file)
                except:
                    name_of_file = f"{filename}_error.parquet.gzip"
                    errors.to_parquet(name_of_file)
                continue
            # try to append to an existing file, if not exist create it
            try:
                name_of_file = f"{filename}.parquet.gzip"
                season_games = pd.concat([pd.read_parquet(name_of_file), season_month], ignore_index = True)
                season_games.to_parquet(name_of_file)
            except:
                name_of_file = f"{filename}.parquet.gzip"
                season_month.to_parquet(name_of_file)
            print(month, "completed")
    return pd.read_parquet(name_of_file)

def get_game_stats(dataframe, filename):
    """
    gets individual game statistics given a seasons games as a pandas df
    """
    count = 1
    flag = 0

    game_stats = pd.DataFrame(columns = ["date",
            "home_team",
            "home_fg",
            "home_fga",
            "home_fg_pct",
            "home_fg3",
            "home_fg3a",
            "home_fg3_pct",
            "home_ft",
            "home_fta",
            "home_ft_pct",
            "home_orb",
            "home_drb",
            "home_trb",
            "home_ast",
            "home_stl",
            "home_blk",
            "home_tov",
            "home_pf",
            "home_pts",
            "away_team",
            "away_fg",
            "away_fga",
            "away_fg_pct",
            "away_fg3",
            "away_fg3a",
            "away_fg3_pct",
            "away_ft",
            "away_fta",
            "away_ft_pct",
            "away_orb",
            "away_drb",
            "away_trb",
            "away_ast",
            "away_stl",
            "away_blk",
            "away_tov",
            "away_pf",
            "away_pts",
            "arena"])
    
    print("getitng individual game stats", end = " ")

    for game in dataframe.iterrows():
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
            error = pd.DataFrame({"season": game["season"],
            "date": game["date"],
            "away_team_id": away_team_id,
            "home_team_id": home_team_id,
            "arena": arena,
            "link": ext,
            "error": ["404: invalid game link"]})
            try:
                name_of_file = f"{filename}_error.parquet.gzip"
                errors = pd.concat([pd.read_parquet(name_of_file), error], ignore_index = True)
                errors.to_parquet(name_of_file)
            except:
                name_of_file = f"{filename}_error.parquet.gzip"
                error.to_parquet(name_of_file)
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
            error = pd.DataFrame({"season": game["season"],
            "date": game["date"],
            "away_team_id": away_team_id,
            "home_team_id": home_team_id,
            "arena": arena,
            "link": ext,
            "error": ["error scraping table"]})
            flag = 1
            try:
                name_of_file = f"{filename}_error.parquet.gzip"
                errors = pd.concat([pd.read_parquet(name_of_file), error], ignore_index = True)
                errors.to_parquet(name_of_file)
            except Exception as e:
                print(e)
                name_of_file = f"{filename}_error.parquet.gzip"
                error.to_parquet(name_of_file)
            continue


        if count % 100 == 0:
            name_of_file = f"{filename}.parquet.gzip"
            try:
                existing_df = pd.read_parquet(name_of_file)
                combined_df = pd.concat([game_stats, existing_df], ignore_index = True)
                combined_df.to_parquet(name_of_file)
            except Exception as e:
                print(e)
                game_stats.to_parquet(name_of_file)
           
            scraped = len(pd.read_parquet(name_of_file))
            print(f"\nstats scraped from {scraped} of {len(dataframe)} in {this_season}")
            game_stats = pd.DataFrame(columns = game_stats.columns)

        
        if count == len(dataframe):
            name_of_file = f"{filename}.parquet.gzip"
            existing_df = pd.read_parquet(name_of_file)
            combined_df = pd.concat([game_stats, existing_df], ignore_index = True)
            combined_df.to_parquet(name_of_file)
        count+=1

    if flag == 1:
        print("errors: ", len(pd.read_parquet(f"{filename}_error.parquet.gzip")))
    return game_stats


def main():
    my_vol = int(input("volume number:"))
    my_vol-=1



    Volume_1 =  [2015, 2016]
    Volume_2 =  [2017, 2018]
    Volume_3 =  [2019, 2020]
    Volume_4 =  [2021, 2022]
    Volume_5 =  [2022, 2023, 2024]

    volumes = [Volume_1, Volume_2, Volume_3, Volume_4, Volume 5]
    print("getting seasons: ", str(volumes[my_vol]))


    #gets game schedule of a list of seasons
    # get_season_schedule(str(volumes[my_vol]), volumes[my_vol])
    name_of_file = f"{str(volumes[my_vol])}.parquet.gzip"
    season_games = pd.read_parquet(name_of_file)
    get_game_stats(season_games, f"{str(volumes[my_vol])}_game_stats")
    print("2-4 .parquet.gzip files should be saved in the current directory!")



if __name__ == "__main__":
    main()