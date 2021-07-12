import psycopg2
import requests
import time
import re
from datetime import datetime

DB_USER = "wta"
DB_PASSWORD = "wta"
DB_NAME = "wta"
DB_HOST = "127.0.0.1"
DB_PORT = "5432"

URL_PART_1 = "https://api.wtatennis.com/tennis/players/"
URL_PART_2 = "/matches/?page="
URL_PART_3 = "&pageSize=100&type=S&sort=desc"
SERENA_WILLIAMS_URL = "230234"
START_YEAR = 2010

def transform_score(score, reason_code):
    # remove tie-break mini-score inside brackets
    score_modified = re.sub("\(.*?\)", "", score)
    # replace double-spaces with single space
    score_modified = " ".join(score_modified.split())
    # add (ret) to score if match was not played fully
    return score_modified + ' (ret)' if reason_code == 'R' else score_modified

def get_location(city, country):
    location = city if city else ''
    if location:
        location += ', '
    location += country if country else ''
    return location

def read_from_rest(connection, link_to_profile):
    page_number = 0
    matches = []
    matches_to_save = []
    player_data = None
    player_id = None
    cursor = connection.cursor()

    # Get all carreer matches for given player
    while True:
        url = URL_PART_1 + link_to_profile + URL_PART_2 + str(page_number) + URL_PART_3
        resp = requests.get(url)
        data = resp.json()
        matches_page = data['matches']
        if matches_page == []:
            player_data = data['player']
            break
        else:
            page_number += 1
            matches += matches_page
            time.sleep(3)

    # Get last match played date
    last_match_played = matches[0]['StartDate'][0:10]
    last_match_played_year = int(last_match_played[0:4])

    # If last match date was too far in the past, skip player and mark as done
    if last_match_played_year < START_YEAR:
        cursor.execute("UPDATE tp_player SET markedforupdate = false WHERE linktoprofile = %s", (link_to_profile, ))
        connection.commit()
    else:
        birth_date = datetime.strptime(player_data['dateOfBirth'], '%Y-%m-%d').date()
        last_match_played_date = datetime.strptime(last_match_played, '%Y-%m-%d').date()

        # check if player already exists in DB
        # if it does not exist - save the player in DB on the fly
        cursor.execute("SELECT id FROM tp_player WHERE linktoprofile = %s", (link_to_profile, ))
        player_tuple = cursor.fetchone()
        if player_tuple == None:
            cursor.execute("INSERT INTO tp_player (firstname, lastname, nationality, birthdate, lastmatchplayed, linktoprofile) VALUES (%s, %s, %s, %s, %s, %s) RETURNING id",
                (player_data['firstName'], player_data['lastName'], player_data['countryCode'], birth_date, last_match_played_date, link_to_profile))
        else:
            cursor.execute("UPDATE tp_player SET firstname = %s, lastname = %s, nationality = %s, birthdate = %s, lastmatchplayed = %s WHERE linktoprofile = %s RETURNING id",
                (player_data['firstName'], player_data['lastName'], player_data['countryCode'], birth_date, last_match_played_date, link_to_profile))
        player_id = cursor.fetchone()[0]  
        connection.commit()

        # Iterate matches
        for match in matches:
            opponent_id = None
            opponent = match['opponent']

            # check if player already exists in DB
            # if it does not exist - save the player in DB on the fly
            if opponent:
                cursor.execute("SELECT id FROM tp_player WHERE linktoprofile = %s", (str(opponent['id']), ))
                opponent_id_tuple = cursor.fetchone()
                if opponent_id_tuple != None:
                    opponent_id = opponent_id_tuple[0]
                else:
                    cursor.execute("INSERT INTO tp_player (linktoprofile, markedforupdate) VALUES (%s, %s) RETURNING id", (str(opponent['id']), str(True)))
                    opponent_id = cursor.fetchone()[0]
                    connection.commit()

                # check if tournament already exists in DB
                # if it does not exist - save in DB on the fly
                tournament_id = None
                tournament = match['tournament']
                start_date = datetime.strptime(tournament['startDate'], '%Y-%m-%d').date()
                end_date = datetime.strptime(tournament['endDate'], '%Y-%m-%d').date()
                cursor.execute("SELECT id FROM tp_tournament WHERE name = %s AND location = %s AND surface = %s AND indoor = %s AND startdate = %s AND enddate = %s",
                    (match['TournamentName'], get_location(tournament['city'], tournament['country']), tournament['surface'], tournament['inOutdoor'] == "I", start_date, end_date))
                tournament_id_tuple = cursor.fetchone()    
                if tournament_id_tuple != None:
                    tournament_id = tournament_id_tuple[0]
                else:
                    cursor.execute("INSERT INTO tp_tournament (rank, startdate, enddate, name, season, location, indoor, surface) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id",
                        (tournament['tournamentGroup']['level'], start_date, end_date, match['TournamentName'], tournament['year'], get_location(tournament['city'], tournament['country']), tournament['inOutdoor'] == "I", tournament['surface']))
                    tournament_id = cursor.fetchone()[0]
                    connection.commit()

                # check if match already exists in DB
                # if it does not exist - add to matches_to_save
                wonplayer_id = player_id if match['winner'] == 1 else opponent_id
                lostplayer_id = player_id if match['winner'] == 2 else opponent_id
                cursor.execute("SELECT id FROM tp_match WHERE tournament_id = %s AND round = %s AND wonplayer_id = %s AND lostplayer_id = %s",
                    (str(tournament_id), match['round_name'], str(wonplayer_id), str(lostplayer_id)))
                match_id_tuple = cursor.fetchone()
                if match_id_tuple == None:
                    new_match = {
                        "tournament_id": tournament_id,
                        "round": match['round_name'],
                        "wonplayerrank": match['rank_1'] if match['winner'] == 1 else match['rank_2'],
                        "lostplayerrank": match['rank_1'] if match['winner'] == 2 else match['rank_2'],
                        "wonplayer_id": wonplayer_id,
                        "lostplayer_id": lostplayer_id,
                        "score": transform_score(match['scores'], match['reason_code']) if match['scores'] else '',
                    }
                    matches_to_save.append(new_match)

        # save new matches (matches_to_save) into DB
        for match in matches_to_save:
            cursor.execute("INSERT INTO tp_match (tournament_id, round, wonplayerrank, lostplayerrank, wonplayer_id, lostplayer_id, score) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (match['tournament_id'], match['round'], match['wonplayerrank'], match['lostplayerrank'], match['wonplayer_id'], match['lostplayer_id'], match['score']))
        connection.commit()

        # markedforupdate set false for current player
        cursor.execute("UPDATE tp_player SET markedforupdate = false WHERE linktoprofile = %s", (link_to_profile, ))
        connection.commit()

    time.sleep(5)



def main():
    try:
        # Connect to the existing database
        connection = psycopg2.connect(user=DB_USER,
                                      password=DB_PASSWORD,
                                      host=DB_HOST,
                                      port=DB_PORT,
                                      database=DB_NAME)
        cursor = connection.cursor()

        # Get Serena Williams' data first
        # Then proceed to next players
        link_to_profile = SERENA_WILLIAMS_URL
        while link_to_profile != None:
            read_from_rest(connection, link_to_profile)
            cursor.execute("SELECT linktoprofile FROM tp_player WHERE markedforupdate IS true ORDER BY id ASC LIMIT 1")
            res = cursor.fetchone()
            if res != None:
                link_to_profile = res[0]
            else:
                break
        print("Parsing www.wta.com finished successfully!")

    except (psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

if __name__ == "__main__":
    main()
