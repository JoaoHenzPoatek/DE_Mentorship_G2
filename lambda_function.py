import datetime
import requests
import psycopg2
import json


def fetch_data_from_imdb_api(country_iso2: str = "US"):
    url = f"https://imdb188.p.rapidapi.com/api/v1/getWhatsStreaming?country={country_iso2}"

    headers = {
        "x-rapidapi-host": "imdb188.p.rapidapi.com",
        "x-rapidapi-key": "5c28490651mshaed820b25907425p134860jsn576183c3fcea",
        "x-rapidapi-ua": "RapidAPI-Playground",
    }

    response = requests.request("GET", url, headers=headers)
    data = response.json()

    return data["data"]


def count_highly_rated_movies(streaming_data, preferences):
    count = 0

    for provider in streaming_data:

        if provider["providerName"] in preferences["streaming_services"]:
            for movie in provider["edges"]:
                if (
                    movie["title"]["ratingsSummary"]["aggregateRating"]
                    > preferences["min_rating"]
                ):
                    count += 1

    return count


# "Rain", "Clouds", "Fog", "Clear", "Snow", "Thunderstorm", "Drizzle", "Mist", "Haze", "Smoke", "Dust", "Sand", "Ash", "Squall"
def fetch_weather_for_city(city: str = "vancouver"):
    url = f"https://open-weather13.p.rapidapi.com/city/{city}/EN"

    headers = {
        "x-rapidapi-host": "open-weather13.p.rapidapi.com",
        "x-rapidapi-key": "5c28490651mshaed820b25907425p134860jsn576183c3fcea",
        "x-rapidapi-ua": "RapidAPI-Playground",
    }

    response = requests.get(url, headers=headers)
    data = response.json()

    return data["weather"][0]["main"]


def fetch_preferences():
    conn = psycopg2.connect(
        "postgres://postgres:password@database-1.cxewmoesekym.us-east-2.rds.amazonaws.com:5432/initial_db"
    )
    cur = conn.cursor()

    cur.execute("SELECT * FROM public.moviepreferences LIMIT 10")
    column_names = [desc[0] for desc in cur.description]

    rows = cur.fetchall()

    # Convert the rows to a list of dictionaries
    preferences = [dict(zip(column_names, row)) for row in rows]

    conn.close()

    return preferences


def update_if_user_should_watch(preferences, should_watch_today):
    conn = psycopg2.connect(
        "postgres://postgres:password@database-1.cxewmoesekym.us-east-2.rds.amazonaws.com:5432/initial_db"
    )
    cur = conn.cursor()

    for preference in preferences:

        try:
            cur.execute(
                "INSERT INTO public.shouldwatch (moviepreferencesid, date, shouldwatch) VALUES (%s, %s, %s)",
                (
                    preference["id"],
                    datetime.datetime.now(),
                    should_watch_today[preference["id"]],
                ),
            )
            conn.commit()
        except Exception as e:
            conn.rollback()

            print("An error occurred:", e)


def check_preferences():

    preferences = fetch_preferences()
    should_watch_today = {}

    for preference in preferences:
        city = preference["city"]
        weather = fetch_weather_for_city(city)

        if weather != preference["favoriteclimate"]:
            should_watch_today[preference["id"]] = False
            continue

        streaming_data = fetch_data_from_imdb_api(preference["country_iso2"])
        num_movies_to_watch = count_highly_rated_movies(streaming_data, preference)
        print(num_movies_to_watch)

        if num_movies_to_watch > 0:
            should_watch_today[preference["id"]] = True

    update_if_user_should_watch(preferences, should_watch_today)

def lambda_handler(event, context):
    output_text = check_preferences()

    # Format response
    response = {
        "statusCode": 200,
        "body": json.dumps(output_text), 
        "headers": {
            "Content-Type": "application/json"
        }
    }

    return response
