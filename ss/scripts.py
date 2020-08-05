import re, requests
from time import sleep
from db_scripts import log_error, insert_db, get_data




def string_to_int(string):
    if type(string) == str:    
        temp = re.findall(r'\d+', string) 
        res = list(map(int, temp)) 
        return res
    else:
        return [0]

def geo_names(city,prod_id=""):

    try:

        local_city = get_data("cities", {"name": city.capitalize()})
        if local_city == None:
            city_data = requests.get(f"http://api.geonames.org/searchJSON?name_startsWith={city}&maxRows=1&username=giorgi0221").json()
            g_names = city_data["geonames"][0]
            insert_db("cities", {
                "name":g_names["name"],
                "geoname_id": g_names["geonameId"]
            })
            return g_names["geonameId"]

        return local_city["geoname_id"]

    except:
        log_error(prod_id, f"გეოლოკაციაში დაფიქსირდა შეცდომა, ლოკაცია: {city}", True)
        return ""


def exceptor(value, error, prod_id, saved=True):
    try:
        return value
    except:
        log_error(prod_id, error, saved) 
        return ""