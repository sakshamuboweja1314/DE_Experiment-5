pip install requests pymongo pandas
import os
from datetime import datetime, timezone
import requests
from pymongo import MongoClient, ASCENDING
from datetime import datetime, timezone
API_KEY = "cb3b7ff5870bf1836fcc688dc62dcb4f"
CITY = "Chengdu"
MONGO_URL = os.getenv("MONGO_URL","mongodb://127.0.0.1:27017")
DB_NAME = "sales_weatherinfo_db"
WEATHER_COL = "weather"
def midnight_utc_naive(dt_utc: datetime) -> datetime:
    dt_utc = dt_utc.astimezone(timezone.utc)
    return datetime(dt_utc.year,dt_utc.month,dt_utc.day,0,0,0,0)
def ensure_indexes(col):
    col.create_index([("weather_date",ASCENDING),("city",ASCENDING)], unique=True)
    col.create_index([("city",ASCENDING),("weather_date",ASCENDING)])
def upsert_weather(doc: dict, col):
    col.update_one(
        {"weather_date": doc["weather_date"], "city": doc["city"]},
        {"$set": doc},
        upsert = True
    )
def fetch_weather_live(api_key: str, city:str) -> dict:
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {"q":city, "appid":api_key.strip(), "units":"metric"}

    try:
        r = requests.get(url, params=params, timeout=20)
    except requests.RequestException as e:
        raise RuntimeError(f"Network error calling OpenWeather: {e}")

    if not r.ok:
        try:
            msg = r.json()
        except Exception:
            msg = r.text
        raise RuntimeError(f"OpenWeather error {r.status_code}: {msg} | url={r.url}")

    data = r.json()
    return{
        "weather_date":datetime.now(timezone.utc).replace(hour=0,minute=0,second=0,microsecond=0),
        "city": city,
        "temp_c": float(data["main"]["temp"]),
        "humidity": int(data["main"]["humidity"]),
        "description": str(data["weather"][0]["description"]),
        "fetched_at": datetime.now(timezone.utc),
        "source": "openweather"
    }
def main():
    if not API_KEY or len(API_KEY.strip()) != 32:
        raise ValueError("OpenWeather API key looks invalid. Except 32 chars.")

    client = MongoClient(MONGO_URL)
    col = client[DB_NAME][WEATHER_COL]

    ensure_indexes(col)
    doc = fetch_weather_live(API_KEY, CITY)
    upsert_weather(doc, col)

    print(f"Stored live weather for {doc['city']} on {doc['weather_date'].date()}.")
if __name__ == "__main__":
    main()
Stored live weather for Chengdu on 2025-09-13.
