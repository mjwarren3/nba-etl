import pandas as pd
from config import DATABASE_URL, DATABASE_KEY
from datetime import  datetime
from nba_api.stats.endpoints import leagueleaders
from supabase import create_client, Client
from nba_api.stats.endpoints import playernextngames
import pytz
import sys

def log(message, level="INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{timestamp} [{level}]: {message}")

    # If the log level is ERROR, exit the program
    if level == "ERROR":
        sys.exit(1)

def section(message):
    print(f"-------- [{message}]: --------")

def get_league_leaders_df():
    try:
        section("GET LEAGUE LEADERS")
        league_leaders = leagueleaders.LeagueLeaders().get_data_frames()
        leaders = league_leaders[0]
        log(f"Successfully fetched {len(leaders)} league leaders")
        return leaders
    except Exception as e:
        log("An error occurred", "ERROR")

def reduce_to_one_player_per_team(leaders):
    try:
        section("REDUCE TO ONE PLAYER PER TEAM")
        one_leaders = leaders.groupby('TEAM_ID').head(1)
        if len(one_leaders) == 30:
            log("All 30 teams are represented in matchups")
            return one_leaders
        else:
            log("Not all 30 teams are represented", "ERROR")
        
    except Exception as e:
        log("An error occurred", "ERROR")
   
def get_next_matchup_by_player(one_leaders):
    try:
        section("GET NEXT MATCHUPS")
        # Set up columns
        one_leaders['NEXT_MATCHUP_HOME'] = None
        one_leaders['NEXT_MATCHUP_AWAY'] = None
        one_leaders['NEXT_MATCHUP_DATE'] = None
        one_leaders['NEXT_MATCHUP_TIME'] = None

        for index, row in one_leaders.iterrows():
            try:
                player_id = row['PLAYER_ID']

                # Fetch the next game for this player
                next_game = playernextngames.PlayerNextNGames(number_of_games=1, player_id=player_id)
                next_game_data = next_game.get_data_frames()[0]

                if not next_game_data.empty:
                    # Assuming the first row contains the next game data
                    one_leaders.loc[index, 'NEXT_MATCHUP_HOME'] = next_game_data.iloc[0]['HOME_TEAM_ABBREVIATION']
                    one_leaders.loc[index, 'NEXT_MATCHUP_AWAY'] = next_game_data.iloc[0]['VISITOR_TEAM_ABBREVIATION']
                    one_leaders.loc[index, 'NEXT_MATCHUP_DATE'] = next_game_data.iloc[0]['GAME_DATE']
                    one_leaders.loc[index, 'NEXT_MATCHUP_TIME'] = next_game_data.iloc[0]['GAME_TIME']
            except Exception as e:
                print(f"An error occurred for player_id {player_id}: {e}")
                continue  # Skip to the next player

        # Keep only needed columns
        one_leaders = one_leaders[['TEAM_ID', 'NEXT_MATCHUP_HOME', 'NEXT_MATCHUP_AWAY', 'NEXT_MATCHUP_DATE', 'NEXT_MATCHUP_TIME']]

        return one_leaders
    except Exception as e:
        log("An error occurred", "ERROR")

def merge_next_matchups_with_league_leaders(leaders, one_leaders_with_next_matchups):
    try:
        section("MERGE MATCHUPS INTO LEADERS")
        # Merge with existing table
        leaders_ppg = pd.merge(leaders, one_leaders_with_next_matchups, on='TEAM_ID', how='left')

        # Check if duplicates occurred
        if len(leaders_ppg) != len(leaders):
            log("Duplicates occurred", "ERROR")
        else:
            log("Successfully merged matchups into leaders")
            return leaders_ppg
        
    except Exception as e:
        log("An error occurred", "ERROR")

def clean_players_df(leaders_ppg):
    try:
        section("CLEAN PLAYERS DF")
        # Calculate all per game metrics
        leaders_ppg['REB_PG'] = leaders_ppg['REB'] / leaders_ppg['GP']
        leaders_ppg['AST_PG'] = leaders_ppg['AST'] / leaders_ppg['GP']
        leaders_ppg['STL_PG'] = leaders_ppg['STL'] / leaders_ppg['GP']
        leaders_ppg['BLK_PG'] = leaders_ppg['BLK'] / leaders_ppg['GP']
        leaders_ppg['TOV_PG'] = leaders_ppg['TOV'] / leaders_ppg['GP']
        leaders_ppg['PTS_PG'] = leaders_ppg['PTS'] / leaders_ppg['GP']
        leaders_ppg['MIN_PG'] = leaders_ppg['MIN'] / leaders_ppg['GP']

        # Convert date and time to a datetime object
        leaders_ppg['MATCHUP_DATETIME'] = leaders_ppg['NEXT_MATCHUP_DATE'] + ' ' + leaders_ppg['NEXT_MATCHUP_TIME']
        leaders_ppg['MATCHUP_DATETIME'] = pd.to_datetime(leaders_ppg['MATCHUP_DATETIME'], format='%b %d, %Y %I:%M %p')

        # Convert from EST/EDT to UTC
        eastern = pytz.timezone('US/Eastern')
        leaders_ppg['MATCHUP_DATETIME'] = leaders_ppg['MATCHUP_DATETIME'].apply(lambda x: eastern.localize(x).astimezone(pytz.utc))

        # Convert datetime back to isoformat
        leaders_ppg['MATCHUP_DATETIME'] = leaders_ppg['MATCHUP_DATETIME'].apply(lambda x: x.isoformat())

        return leaders_ppg
    except Exception as e:
        log("An error occurred", "ERROR")

def add_opponent_column(clean_players_df):
    # Create a function to check who the opposing matchup is and apply it
    def find_opposing_matchup(row):
        if row['TEAM'] == row['NEXT_MATCHUP_HOME']:
            return row['NEXT_MATCHUP_AWAY']
        elif row['TEAM'] == row['NEXT_MATCHUP_AWAY']:
            return row['NEXT_MATCHUP_HOME']
        else:
            return None
    try:
        section("ADD OPPONENT COLUMN AND FILTER OUT ONLY NEEDED COLUMNS AND REMOVE IF NO OPPONENT")

        # Apply the function to create the "opposing matchup" column
        clean_players_df['OPP'] = clean_players_df.apply(find_opposing_matchup, axis=1)

        # Remove if no opponent set
        picks = clean_players_df.dropna(subset=['OPP'])

        # Reduce to only needed columns for database
        for_db = picks[['PLAYER_ID', 'PLAYER', 'TEAM_ID', 'TEAM', 'GP', 'MIN_PG', 'PTS_PG', 'REB_PG', 'AST_PG', 'STL_PG', 'BLK_PG', 'TOV_PG', 'MATCHUP_DATETIME', 'OPP']]

        # prompt: convert all in for_db to lowercase columns
        for_db.columns = for_db.columns.str.lower()

        # Ensure at least 400 rows are still in the table
        if len(for_db) < 400:
            log("Not enough rows in the table, players got removed.", "ERROR")
        else:
            log(f"Successfully saved updated database with {len(for_db)} rows.")
            return for_db
        
    except Exception as e:
        log("An error occurred", "ERROR")

def upload_players_to_supabase(for_db):

    try:
        section("UPLOADING PLAYERS")

        # Supabase details
        url = DATABASE_URL
        key = DATABASE_KEY

        log("Successfully retrieved URL and Key")

        # Create a Supabase client
        supabase: Client = create_client(url, key)

        # Upload to supabase
        table_name = 'player_picks'

        # Delete all existing rows in the table by specifying a filter that matches all rows
        supabase.table(table_name).delete().neq('player_id', 0).execute()

        log("Succesfully removed existing players")

        # Add back in the new rows
        for _, row in for_db.iterrows():
            data = row.to_dict()
            # Insert the data to Supabase
            supabase.table(table_name).insert(data).execute()

        log("Successfully uploaded players to database")
    except Exception as e:
        log("An error occurred", "ERROR")


# Define main function
def main():
    leaders = get_league_leaders_df()
    one_leaders = reduce_to_one_player_per_team(leaders)
    one_leaders_with_next_matchups = get_next_matchup_by_player(one_leaders)
    leaders_ppg = merge_next_matchups_with_league_leaders(leaders, one_leaders_with_next_matchups)
    cleaned_players_df = clean_players_df(leaders_ppg)
    for_db = add_opponent_column(cleaned_players_df)
    upload_players_to_supabase(for_db)

# Run it
if __name__ == "__main__":
    main()


