import pandas as pd
from config import DATABASE_URL, DATABASE_KEY
from nba_api.stats.endpoints import leaguegamefinder
from datetime import date, timedelta, datetime
from nba_api.stats.endpoints import boxscoretraditionalv2
from supabase import create_client, Client
import sys

# Define logging
def log(message, level="INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{timestamp} [{level}]: {message}")

    # If the log level is ERROR, exit the program
    if level == "ERROR":
        sys.exit(1)

def section(message):
    print(f"-------- [{message}]: --------")


# Get schedule
def get_schedule():
    try:
        section("GETTING FULL SCHEDULE")
        # Create an instance of LeagueGameFinder
        game_finder = leaguegamefinder.LeagueGameFinder()

        # Retrieve all games (you can add parameters to filter for specific conditions)
        games = game_finder.get_data_frames()[0]

        season_games = games[games['SEASON_ID'] == '22023']

        # Extract unique GAME_IDs from the filtered games
        season_games.drop_duplicates(subset=['GAME_ID', 'GAME_DATE'], inplace=True)

        season_games = season_games[['GAME_ID', 'GAME_DATE']]

        log(f"Successfully fetched {len(season_games)} games")

        return season_games
    
    except Exception as e:
        log("An error occurred", "ERROR")

# Subset for yesterday's games
def reduce_schedule_to_yesterday(season_games):
    try:
        section("REDUCING SCHEDULE TO YESTERDAY")
        
        # Get yesterday's date
        yesterday = date.today() - timedelta(days=1)

        log(f"Yesterday identified as {yesterday}")

        # Filter the DataFrame for games that occurred yesterday
        yesterday_game_dates = season_games['GAME_DATE'] == str(yesterday)
        filtered_games = season_games[yesterday_game_dates]

        if len(filtered_games) > 0:
            log(f"{len(filtered_games)} games to be uploaded.")
            return filtered_games
        else:
            log("No games left in dataframe", "ERROR")
            
    except Exception as e:
        log("An error occurred", "ERROR")

# Get box scores for all games in schedule
def get_box_scores(schedule):
    try:
        section("GET BOX SCORES")
        # Create blank dataframe
        all_game_stats = pd.DataFrame()

        # Loop through each game, get box score, append
        for index, row in schedule.iterrows():
            try:
                # Get individual game stats
                boxscore = boxscoretraditionalv2.BoxScoreTraditionalV2(row['GAME_ID']).get_data_frames()[0]
                boxscore['GAME_DATE'] = row['GAME_DATE']
                # Append the game stats to the all_game_stats DataFrame
                all_game_stats = pd.concat([all_game_stats, boxscore])
            except Exception as e:
                print(f"Error fetching data for game ID {row['GAME_ID']}: {e}")
                log("Error retrieving at least one game, stopping", "ERROR")
        
        log(f"Successfully fetched {len(all_game_stats)} box scores")
        return all_game_stats
    except Exception as e:
        log("An error occurred", "ERROR")

def get_clean_box_scores(all_game_stats):

    try:
        section("CLEANING BOX SCORES")
        # Fill numeric column blanks with 0
        numeric_columns = ['MIN', 'FGM', 'FGA', 'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT', 'FTM', 'FTA',
                        'FT_PCT', 'OREB', 'DREB', 'REB', 'AST', 'STL', 'BLK', 'TO', 'PF', 'PTS',
                        'PLUS_MINUS']

        for col in numeric_columns:
            all_game_stats[col] = pd.to_numeric(all_game_stats[col], errors='coerce').fillna(0)

        # Convert other columns to string
        string_columns = list(set(all_game_stats.columns) - set(numeric_columns))
        for col in string_columns:
            all_game_stats[col] = all_game_stats[col].astype(str)

        # prompt: Convert all column names of all_game_stats to lower case, change TO to TOS
        all_game_stats.rename(columns={'TO': 'TOS'}, inplace=True)
        all_game_stats.columns = all_game_stats.columns.str.lower()

        # Convert most columns to integer
        numeric_columns = ['min', 'fgm', 'fga', 'fg_pct', 'fg3m', 'fg3a', 'ftm', 'fta', 'oreb', 'dreb', 'reb', 'ast', 'stl', 'blk', 'tos', 'pf', 'pts', 'plus_minus']

        for col in numeric_columns:
            if col not in ['fg3_pct', 'ft_pct', 'fg_pct']:
                all_game_stats[col] = all_game_stats[col].astype(int)

        all_game_stats['id'] = all_game_stats['player_id'].astype(str) + "_" + all_game_stats['game_id'].astype(str)

        log("Successfully cleaned box scores")

        return all_game_stats
    
    except Exception as e:
        log("An error occurred", "ERROR")

def upload_scores_to_supabase(all_game_stats):

    try:
        section("UPLOADING TO SUPABASE")
        # Supabase details
        url = DATABASE_URL
        key = DATABASE_KEY

        log("Retrieved database details correctly")

        # Create a Supabase client
        supabase: Client = create_client(url, key)

        # Upload each row to supabase
        table_name = 'player_game_stats'

        # Iterate through the DataFrame and upload each row
        for _, row in all_game_stats.iterrows():
            data = row.to_dict()
            # Insert the data to Supabase
            supabase.table(table_name).insert(data).execute()
        
        log(f"Successfully uploaded {len(all_game_stats)} rows to database. Finished!")
        return
    except Exception as e:
        log("An error occurred", "ERROR")

# Define main function
def main():
    schedule = get_schedule()
    yesterday_schedule = reduce_schedule_to_yesterday(schedule)
    box_scores = get_box_scores(yesterday_schedule)
    clean_box_scores = get_clean_box_scores(box_scores)
    upload_scores_to_supabase(clean_box_scores)


# Run it
if __name__ == "__main__":
    main()