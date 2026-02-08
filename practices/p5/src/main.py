import pandas as pd
import requests
from pymongo import MongoClient, ASCENDING
import os

MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:q@mongo:27017/?authSource=admin")
client = MongoClient(MONGO_URI)
db = client.weather

TOKEN = open("wather_key.txt").read().strip()

cities = {
    "Moscow": (55.7558, 37.6176),
    "Saint Petersburg": (59.9386, 30.3141),
    "Novosibirsk": (55.0084, 82.9357),
    "Yekaterinburg": (56.8389, 60.6057),
    "Kazan": (55.7961, 49.1064),
    "Nizhny Novgorod": (56.2965, 43.9361),
    "Chelyabinsk": (55.1540, 61.4291),
    "Samara": (53.1959, 50.1008),
    "Omsk": (54.9885, 73.3242),
    "Rostov-on-Don": (47.2357, 39.7015),
    "Ufa": (54.7388, 55.9721),
    "Krasnoyarsk": (56.0153, 92.8932),
    "Perm": (58.0105, 56.2502),
    "Voronezh": (51.6608, 39.2003),
    "Volgograd": (48.7080, 44.5133),
    "Krasnodar": (45.0355, 38.9753),
    "Saratov": (51.5336, 46.0343),
    "Tyumen": (57.1613, 65.5250),
    "Tolyatti": (53.5078, 49.4204),
    "Izhevsk": (56.8527, 53.2115),
    "Barnaul": (53.3474, 83.7783),
    "Ulyanovsk": (54.3142, 48.4031),
    "Irkutsk": (52.2869, 104.3050),
    "Khabarovsk": (48.4827, 135.0840),
    "Yaroslavl": (57.6261, 39.8845),
    "Vladivostok": (43.1155, 131.8855),
    "Makhachkala": (42.9831, 47.5047),
    "Tomsk": (56.4846, 84.9476),
    "Orenburg": (51.7682, 55.0968),
    "Kemerovo": (55.3552, 86.0868),
}

def parse_open_weather(data: dict, city_name: str) -> dict:
    df = pd.json_normalize(data["list"], sep="_")
    df["dt"] = pd.to_datetime(df["dt_txt"])

    df = df[df["dt"].dt.hour == 12].sort_values("dt").head(5).copy()

    df["weather_main"] = df["weather"].apply(lambda x: x[0]["main"] if x else None)

    coldest = df.loc[df["main_temp"].idxmin()]
    days = []
    for _, r in df.iterrows():
        days.append({
            "dt": r["dt"].to_pydatetime(), 
            "temp": float(r["main_temp"]),
            "feels_like": float(r["main_feels_like"]),
            "weather": r["weather_main"],
        })

    return {
        "city": {
            "name": city_name,
            "lat": data.get("city", {}).get("coord", {}).get("lat"),
            "lon": data.get("city", {}).get("coord", {}).get("lon"),
            "id": data.get("city", {}).get("id"),
            "country": data.get("city", {}).get("country"),
            "timezone": data.get("city", {}).get("timezone"),
        },
        "source": "openweather",
        "units": "metric",
        "fetched_at": pd.Timestamp.utcnow().to_pydatetime(),
        "days": days,
        "info": {
            "median_temp": float( df["main_temp"].median()),
            "snow_days": int((df["weather_main"] == "Snow").sum()),
            "clear_days": int((df["weather_main"] == "Clear").sum()),
            "rain_days": int( (df["weather_main"] == "Rain").sum()),
            "cloud_days": int((df["weather_main"] == "Clouds").sum()),
            "coldest_day": {
                "dt": coldest["dt"].to_pydatetime(),
                "temp": float(coldest["main_temp"]),
                "feels_like": float(coldest["main_feels_like"]),
                "weather": coldest["weather_main"],
            }
        }
    }

def main():
    for name, (lat, lon) in cities.items():
        query = f"https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&units=metric&&appid={TOKEN}"
        data = requests.get(query).json()
        doc = parse_open_weather(data, city_name=name)
        db.weather_forecast.update_one(
        {"city.name": doc["city"]["name"]},
        {"$set": doc},
         upsert=True
        )
        break

if __name__ == "__main__":
    main()
        
