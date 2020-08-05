import pymongo, time, requests, math, datetime, _print
from datedelta import MONTH, DAY
from bson import ObjectId
from scripts import string_to_int, geo_names, exceptor
from db_scripts import is_duplicate, log_error, remove_error


myclient = pymongo.MongoClient("mongodb://localhost:27017/")
database = myclient["scrapping"]
ids_db = database["myhome_product_id"]
real_estate_db = database["real_estate"]
users_db = database["myhome_users"]


links = {  
    "products_api": "https://api.myhome.ge/ka/products/",
    "product_details_api": "https://api.myhome.ge/en/products/GetProductDetails/",
    "img_links": "https://static.my.ge/myhome/photos/"
} 

PrTitles = {
    "1_1_1": "Newly finished apartment for sale",
    "1_1_2": "Apartment under construction for sale",
    "1_1_3": "Older finished apartment for sale",
    "1_2_17": "House for sale",
    "1_2_18": "Country house for sale",
    "1_4_4": "Commercial for office for sale",
    "1_4_5": "Commercial for retail for sale",
    "1_4_6": "Commercial for warehouse for sale",
    "1_4_7": "Commercial for production for sale",
    "1_4_8": "Commercial for sale",
    "1_4_9": "Commercial for special activities for sale",
    "1_4_10": "Commercial for food processing for sale",
    "1_4_11": "Commercial for hotel for sale",
    "1_4_12": "Commercial for parking for sale",
    "1_5_13": "Agricultural land for sale",
    "1_5_14": "Non agricultural land for sale",
    "1_5_15": "Commercial land for sale",
    "1_5_16": "Special use land for sale",
    "2_1_1": "Newly finished apartment under plege",
    "2_1_2": "Apartment under construction under plege",
    "2_1_3": "Older finished apartment under plege",
    "2_2_17": "House under plege",
    "2_2_18": "Country house under plege",
    "2_4_4": "Commercial for office under plege",
    "2_4_5": "Commercial for retail under plege",
    "2_4_6": "Commercial for warehouse under plege",
    "2_4_7": "Commercial for production under plege",
    "2_4_8": "Commercial under plege",
    "2_4_9": "Commercial for special activities under plege",
    "2_4_10": "Commercial for food processing unde plege",
    "2_4_11": "Commercial for hotel unde plege",
    "2_4_12": "Commercial for parking unde plege",
    "2_5_13": "Agricultural land unde plege",
    "2_5_14": "Non agricultural land unde plege",
    "2_5_15": "Commercial land unde plege",
    "2_5_16": "Special use land unde plege",
    "3_1_1": "Newly finished apartment for rent",
    "3_1_2": "Apartment under construction for rent",
    "3_1_3": "Older finished apartment for rent",
    "3_2_17": "House for rent",
    "3_2_18": "Country house for rent",
    "3_4_4": "Commercial for office for rent",
    "3_4_5": "Commercial for retail for rent",
    "3_4_6": "Commercial for warehouse for rent",
    "3_4_7": "Commercial for production for rent",
    "3_4_8": "Commercial for rent",
    "3_4_9": "Commercial for special activities for rent",
    "3_4_10": "Commercial for food processing for rent",
    "3_4_11": "Commercial for hotel for rent",
    "3_4_12": "Commercial for parking for rent",
    "3_5_13": "Agricultural land for rent",
    "3_5_14": "Non agricultural land for rent",
    "3_5_15": "Commercial land for rent",
    "3_5_16": "Special use land for rent",
    "7_1_1": "Newly finished apartment for daily rent",
    "7_1_2": "Apartment under construction for daily rent",
    "7_1_3": "Older finished apartment for daily rent",
    "7_2_17": "House for daily rent",
    "7_2_18": "Country house for daily rent",
    "7_4_4": "Commercial for office for daily rent",
    "7_4_5": "Commercial for retail for daily rent",
    "7_4_6": "Commercial for warehouse for daily rent",
    "7_4_7": "Commercial for production for daily rent",
    "7_4_8": "Commercial for daily rent",
    "7_4_9": "Commercial for special activities for daily rent",
    "7_4_10": "Commercial for food processing for daily rent",
    "7_4_11": "Commercial for hotel for daily rent",
    "7_4_12": "Commercial for parking for daily rent",
    "7_5_13": "Agricultural land for daily rent",
    "7_5_14": "Non agricultural land for daily rent",
    "7_5_15": "Commercial land for daily rent",
    "7_5_16": "Special use land for daily rent",
    "1_1_0": "Apartment for sale",
    "1_2_0": "House for Sale",
    "1_4_0": "Commercial for sale",
    "1_5_0": "Land for sale",
    "1_7_0": "Hotel for sale",
    "2_1_0": "Apartment for pledge",
    "2_2_0": "House for pledge",
    "2_4_0": "Commercial for pledge",
    "2_5_0": "Land for pledge",
    "2_7_0": "Hotel for pledge",
    "3_1_0": "Apartment for rent",
    "3_2_0": "House for rent",
    "3_4_0": "Commercial for rent",
    "3_5_0": "Land for rent",
    "3_7_0": "Hotel for rent",
    "7_1_0": "Apartment for daily rent",
    "7_2_0": "House for daily rent",
    "7_4_0": "Commercial for daily rent",
    "7_5_0": "ქირავდება დღიურად მიწის ნაკვეთი",
    "7_7_0": "Hotel for daily rent",
    "1_4_19": "Commercial basement for sale",
    "1_4_20": "Commercial semibasement for sale",
    "2_4_19": "Commercial basement under plege",
    "3_4_19": "Commercial basement for rent",
    "7_4_19": "Commercial basement for daily rent",
    "7_4_20": "Commercial semibasement for daily rent",
    "3_4_20": "Commercial semibasement for rent",
    "2_4_20": "Commercial semibasement unde pledge",
    "1_5_21": "Land for sale for construction",
    "2_5_21": "Land for pledge for construction",
    "3_5_21": "Land for rent for construction",
    "7_5_21": "ქირავდება დღიურად მიწის ნაკვეთი მშენებლობისთვის",
    "8_5_13": "Agricultural land for lease",
    "8_5_14": "Non agricultural land for lease",
    "8_5_15": "Commercial land for lease",
    "8_5_16": "Special designation land for lease",
    "8_5_21": "Building land for lease",
    "1_4_22": "Commercial Real Estate for sale (whole building)",
    "7_4_22": "Commercial Real Estate for daily rent (whole building)",
    "3_4_22": "Commercial Real Estate for rent (whole building)",
    "2_4_22": "Commercial Real Estate under pledge (whole building)",
    "1_4_23": "Car wash for sale",
    "2_4_23": "Car wash for lease",
    "3_4_23": "Car wash for rent",
    "7_4_23": "Car wash for daily rent",
    "8_5_0": "Land for lease"
}
city_osm = [
    {
        "name": "Tbilisi",
        "osm_id": 1996871
    },
    {
        "name": "Kutaisi",
        "osm_id": 8742174
    },
    {
        "name": "Rustavi",
        "osm_id": 5997314
    },
    {
        "name": "Batumi",
        "osm_id": 8742159
    },
    {
        "name": "Abastumani",
        "osm_id": 457440730
    },
    {
        "name": "Abasha",
        "osm_id": 33174341
    },
    {
        "name": "Agara",
        "osm_id": 265377360
    },
    {
        "name": "Adigeni",
        "osm_id": 457440822
    },
    {
        "name": "Ambrolauri",
        "osm_id": 34012095
    },
    {
        "name": "Anaklia",
        "osm_id": 2633905
    },
    {
        "name": "Aspindza",
        "osm_id": 415574362
    },
    {
        "name": "Akhalgori",
        "osm_id": 2537351
    },
    {
        "name": "New Athos",
        "osm_id": 2612381
    },
    {
        "name": "Akhalkalaki",
        "osm_id": 33836428
    },
    {
        "name": "Akhaltsikhe",
        "osm_id": 33728531
    },
    {
        "name": "Akhmeta",
        "osm_id": 33836710
    },
    {
        "name": "Bakuriani",
        "osm_id": 311913158
    },
    {
        "name": "Baghdati",
        "osm_id": 389644233
    },
    {
        "name": "Pitsunda",
        "osm_id": 2625159
    },
    {
        "name": "Bolnisi",
        "osm_id": 75732327
    },
    {
        "name": "Borjomi",
        "osm_id": 33728805
    },
    {
        "name": "Gagra",
        "osm_id": 1643098992
    },
    {
        "name": "Gali",
        "osm_id": 286418976
    },
    {
        "name": "Gardabani",
        "osm_id": 292825343
    },
    {
        "name": "Gori",
        "osm_id": 9195296
    },
    {
        "name": "Gudauta",
        "osm_id": 2624491
    },
    {
        "name": "Gudauri",
        "osm_id": 540059524
    },
    {
        "name": "Gulripsh",
        "osm_id": 2646065
    },
    {
        "name": "Gurjaani",
        "osm_id": 74688344
    },
    {
        "name": "Dedoplistskaro",
        "osm_id": 33880121
    },
    {
        "name": "Dmanisi",
        "osm_id": 34011404
    },
    {
        "name": "Dusheti",
        "osm_id": 33880279
    },
    {
        "name": "Vale",
        "osm_id": 34008861
    },
    {
        "name": "Vani",
        "osm_id": 389665587
    },
    {
        "name": "Zestaponi",
        "osm_id": 6255831
    },
    {
        "name": "Zugdidi",
        "osm_id": 26605136
    },
    {
        "name": "Tetritskaro",
        "osm_id": 25724519
    },
    {
        "name": "Telavi",
        "osm_id": 61843693
    },
    {
        "name": "Terjola",
        "osm_id": 389636290
    },
    {
        "name": "Tianeti",
        "osm_id": 95067936
    },
    {
        "name": "Kazreti",
        "osm_id": 75745987
    },
    {
        "name": "Kaspi",
        "osm_id": 25688527
    },
    {
        "name": "Kvaisa",
        "osm_id": 169310536
    },
    {
        "name": "Lagodekhi",
        "osm_id": 33880381
    },
    {
        "name": "Lanchkhuti",
        "osm_id": 628354549
    },
    {
        "name": "Lentekhi",
        "osm_id": 159047970
    },
    {
        "name": "Manglisi",
        "osm_id": 387157943
    },
    {
        "name": "Marneuli",
        "osm_id": 33728338
    },
    {
        "name": "Martvili",
        "osm_id": 338167531
    },
    {
        "name": "Mestia",
        "osm_id": 628347610
    },
    {
        "name": "Mtskheta",
        "osm_id": 122131492
    },
    {
        "name": "Ninotsminda",
        "osm_id": 33880592
    },
    {
        "name": "Ozurgeti",
        "osm_id": 8374107
    },
    {
        "name": "Oni",
        "osm_id": 34012039
    },
    {
        "name": "Ochamchira",
        "osm_id": 2646014
    },
    {
        "name": "Sagarejo",
        "osm_id": 26294040
    },
    {
        "name": "Sadakhlo",
        "osm_id": 457068541
    },
    {
        "name": "Samtredia",
        "osm_id": 628351266
    },
    {
        "name": "Sachkhere",
        "osm_id": 387159396
    },
    {
        "name": "Senaki",
        "osm_id": 30594351
    },
    {
        "name": "Sioni",
        "osm_id": 393023118
    },
    {
        "name": "Sighnaghi",
        "osm_id": 289105944
    },
    {
        "name": "Sokhumi",
        "osm_id": 790323861
    },
    {
        "name": "Stepantsminda",
        "osm_id": 95807189
    },
    {
        "name": "Surami",
        "osm_id": 33728182
    },
    {
        "name": "Tkibuli",
        "osm_id": 292823696
    },
    {
        "name": "Poti",
        "osm_id": 33728068
    },
    {
        "name": "Kareli",
        "osm_id": 280181619
    },
    {
        "name": "Keda",
        "osm_id": 218643951
    },
    {
        "name": "Kobuleti",
        "osm_id": 8738685
    },
    {
        "name": "Kvareli",
        "osm_id": 31970772
    },
    {
        "name": "Shuakhevi",
        "osm_id": 218704706
    },
    {
        "name": "Chakvi",
        "osm_id": 306086645
    },
    {
        "name": "Chokhatauri",
        "osm_id": 479780417
    },
    {
        "name": "Chkhorotsku",
        "osm_id": 375727966
    },
    {
        "name": "Tsageri",
        "osm_id": 387172805
    },
    {
        "name": "Tskhinvali",
        "osm_id": 2706851374
    },
    {
        "name": "Tsalenjikha",
        "osm_id": 28357831
    },
    {
        "name": "Tsalka",
        "osm_id": 34012533
    },
    {
        "name": "Tsnori",
        "osm_id": 26389317
    },
    {
        "name": "Tskaltubo",
        "osm_id": 291725496
    },
    {
        "name": "Chiatura",
        "osm_id": 291727016
    },
    {
        "name": "Kharagauli",
        "osm_id": 223666778
    },
    {
        "name": "Khashuri",
        "osm_id": 274202979
    },
    {
        "name": "Khelvachauri",
        "osm_id": 79605316
    },
    {
        "name": "Khelvachauri",
        "osm_id": 540555081
    },
    {
        "name": "Khobi",
        "osm_id": 389635184
    },
    {
        "name": "Khoni",
        "osm_id": 628349927
    },
    {
        "name": "Khulo",
        "osm_id": 540579150
    },
    {
        "name": "Java",
        "osm_id": 170196329
    },
    {
        "name": "Jvari",
        "osm_id": 34009508
    },
    {
        "name": "Abasha",
        "osm_id": 2016164
    },
    {
        "name": "Adigeni",
        "osm_id": 2013747
    },
    {
        "name": "Ambrolauri",
        "osm_id": 1997287
    },
    {
        "name": "Aspindza",
        "osm_id": 2013749
    },
    {
        "name": "Akhalgori",
        "osm_id": 2027320
    },
    {
        "name": "Akhalkalaki",
        "osm_id": 2013750
    },
    {
        "name": "Akhaltsikhe",
        "osm_id": 2013748
    },
    {
        "name": "Akhmeta",
        "osm_id": 2027328
    },
    {
        "name": "Baghdati",
        "osm_id": 2024539
    },
    {
        "name": "Bolnisi",
        "osm_id": 2014300
    },
    {
        "name": "Borjomi",
        "osm_id": 2013752
    },
    {
        "name": "Gardabani",
        "osm_id": 2014334
    },
    {
        "name": "Gori",
        "osm_id": 2025548
    },
    {
        "name": "Gurjaani",
        "osm_id": 2027330
    },
    {
        "name": "Dedoplistsqaro",
        "osm_id": 2027334
    },
    {
        "name": "Dmanisi",
        "osm_id": 2014299
    },
    {
        "name": "Dusheti",
        "osm_id": 2025552
    },
    {
        "name": "Vani",
        "osm_id": 2024538
    },
    {
        "name": "Zestaponi",
        "osm_id": 2024541
    },
    {
        "name": "Zugdidi",
        "osm_id": 2016161
    },
    {
        "name": "Tetritskaro",
        "osm_id": 2014301
    },
    {
        "name": "Telavi",
        "osm_id": 2027329
    },
    {
        "name": "Terjola",
        "osm_id": 2024542
    },
    {
        "name": "Tianeti",
        "osm_id": 2025551
    },
    {
        "name": "Kaspi",
        "osm_id": 2025549
    },
    {
        "name": "Lagodekhi",
        "osm_id": 2027332
    },
    {
        "name": "Lanchkhuti",
        "osm_id": 1995783
    },
    {
        "name": "Lentekhi",
        "osm_id": 1997285
    },
    {
        "name": "Marneuli",
        "osm_id": 1997312
    },
    {
        "name": "Martvili",
        "osm_id": 2016165
    },
    {
        "name": "Mestia",
        "osm_id": 2016168
    },
    {
        "name": "Mtskheta",
        "osm_id": 2025550
    },
    {
        "name": "Ninotsminda",
        "osm_id": 2013751
    },
    {
        "name": "Ozurgeti",
        "osm_id": 1995403
    },
    {
        "name": "Oni",
        "osm_id": 1997288
    },
    {
        "name": "Sagarejo",
        "osm_id": 2027335
    },
    {
        "name": "Samtredia",
        "osm_id": 2024537
    },
    {
        "name": "Sachkhere",
        "osm_id": 2024545
    },
    {
        "name": "Senaki",
        "osm_id": 2016163
    },
    {
        "name": "Signagi",
        "osm_id": 2027333
    },
    {
        "name": "Tqibuli",
        "osm_id": 2024546
    },
    {
        "name": "Kareli",
        "osm_id": 2025547
    },
    {
        "name": "Keda",
        "osm_id": 2009240
    },
    {
        "name": "Kobuleti",
        "osm_id": 2009239
    },
    {
        "name": "Kazbegi",
        "osm_id": 2025553
    },
    {
        "name": "Kvareli",
        "osm_id": 2027331
    },
    {
        "name": "Znauri",
        "osm_id": 2537350
    },
    {
        "name": "Shuakhevi",
        "osm_id": 2009241
    },
    {
        "name": "Chokhatauri",
        "osm_id": 1995969
    },
    {
        "name": "Chkhorotsqu",
        "osm_id": 2016166
    },
    {
        "name": "Tsageri",
        "osm_id": 1997286
    },
    {
        "name": "Tskhinvali",
        "osm_id": 2027318
    },
    {
        "name": "Tskhinvali",
        "osm_id": 2027319
    },
    {
        "name": "Tsalenjikha",
        "osm_id": 2016167
    },
    {
        "name": "Tsalka",
        "osm_id": 2014298
    },
    {
        "name": "Tskaltubo",
        "osm_id": 2024548
    },
    {
        "name": "Kharagauli",
        "osm_id": 2024540
    },
    {
        "name": "Khashuri",
        "osm_id": 2025546
    },
    {
        "name": "Khelvachauri",
        "osm_id": 2009238
    },
    {
        "name": "Khobi",
        "osm_id": 2016162
    },
    {
        "name": "Khoni",
        "osm_id": 2024536
    },
    {
        "name": "Khulo",
        "osm_id": 2009242
    },
    {
        "name": "Dzau",
        "osm_id": 2027316
    }
]

def get_product_type(p_type_id):
    try:
        switcher={
            "1": "appartaments",
            "2": "houses",
            "4": "any",
            "5": "any",
            "7": "any"
        }
        print("------------------------")
        return switcher.get(p_type_id,"any")
    except:
        _print.fail(f'line 784: get_product_type() - ს აქვს რაღაც ხარვეზი')
        return "any"

def get_deal_type(type_id):
    try: 
        switcher={
            "1": "sell",
            "2": "lease",
            "3": "rent",
            "7": "rent",
            "8":"lease"
        }
        return switcher.get(type_id,"")
    except:
        _print.fail(f'line 798: get_deal_type() - ს აქვს რაღაც ხარვეზი')
        return ""

def get_features(ar,_data):
    _loc_data = { 
        "indoor": [],
        "outdoor": [],
        "climat_control": []
    }
    try:
        if _data["elevator_1"] != "0": _loc_data["indoor"].append("lift")
        if _data["bathrooms"] != "0": _loc_data["indoor"].append("dishwasher")
        if _data["store_type_id"] != "0": _loc_data["indoor"].append("storage_room")
        
        
        if _data["balcony"] != "0": _loc_data["outdoor"].append("balcony")
        if _data["parking_id"] != "0": _loc_data["outdoor"].append("garage")
        if _data["yard_size"] != "0": _loc_data["outdoor"].append("outdoor_area")

        if _data["conditioner"] != "0": _loc_data["climat_control"].append("air_conditioning")
        if _data["water"] != "0": _loc_data["climat_control"].append("water_tank")
        if _data["hot_water_id"] != "0": _loc_data["climat_control"].append("solar_hot_water")
        if _data["hot_water_id"] == "6": _loc_data["climat_control"].append("zonal_heating")
        if _data["hot_water_id"] == "6": _loc_data["climat_control"].append("heat_pumps")
        return _loc_data[ar]
    except:
        _print.fail(f'line 824: get_features() - ს აქვს რაღაც ხარვეზი')
        return []

def get_property_type(_type_id):
    try:
        switcher={
            "1": "land",
            "2": "houses",
            "3": "appartments",
            "4": "commercial_properties",
            "5":"lease"
        }

        return switcher.get(_type_id, "any")
    except:
        _print.fail(f'line 839: get_property_type() - ს აქვს რაღაც ხარვეზი')
        return "any"

def get_images(_len,src,prod_id):
        _arr = []
    # try:
        for i in range(int(_len)):
            for_id = u"01234567890123456789"+prod_id+str(i)
            _arr.append({
                "id": ObjectId(for_id[len(for_id)-24:]),
                "name":prod_id+"_"+str(i+1)+".jpg",
                "mime":"image/jpg",
                "url":links["img_links"]+src+"/large/"+prod_id+"_"+str(i+1)+".jpg?v=1"
            })
    # except:
    #     _print.fail(f'line 849: get_images() - ს აქვს რაღაც ხარვეზი')

        return _arr 
                
def is_new_building(_estate_type):
    if _estate_type == "1": 
        return "new_building" 
    else: 
        return "any"

def is_old_data(date):
    try:
        _today = datetime.datetime.utcnow()
        _yesterday = datetime.datetime.utcnow() - DAY
       
        is_today = date.year == _today.year and date.month == _today.month and date.day == _today.day 
        is_yesterday = date.year == _yesterday.year and date.month == _yesterday.month and date.day == _yesterday.day
        if not is_today and not is_yesterday:
            return  True
        return False
    except:
        return False

# GET IDS 
def get_ids(daily=False):
    for _city in city_osm:
        try:
            for_page_len = requests.get(links["products_api"]+"?GID={}&cities={}".format(str(_city["osm_id"]),str(_city["osm_id"])))
            page_len_json = for_page_len.json()
        except:
            _print.fail(f'line 859: for_page_len - ს აქვს რაღაც ხარვეზი')

        try:
            page_len = math.ceil(int(page_len_json["Pagination"]["ContentCount"])/page_len_json["Pagination"]["PerPage"]) 
        except:
            _print.fail(f'line 864: page_len - ს აქვს რაღაც ხარვეზი')

        _print.value(_city['name'])

        for page_index in range(page_len):
            
            _print.value('Page '+str(page_index+1))

            try:
                product_pages_res = requests.get(links["products_api"]+"?Page="+str(page_index+1)+"&GID={}&cities={}".format(str(_city["osm_id"]),str(_city["osm_id"])))
                Products = product_pages_res.json()["Prs"]["Prs"]
                Users = product_pages_res.json()["Prs"]["Users"]["Data"]
              
                if is_old_data(datetime.datetime.strptime( Products[len(Products)-1]["order_date"], "%Y-%m-%d %H:%M:%S")) and daily:
                    break

                for prs in Products:
                    try:
                        if not is_duplicate(ids_db, "product_id", prs["product_id"]):
                            ids_db.insert({
                                "product_id": prs["product_id"],
                                "parsed": False,
                                "city": _city["name"]
                            })
                            _print.ok(f'{prs["product_id"]} პროდუქტი დაემატა')
                        else:
                            _print.warning(f'{prs["product_id"]} არის დუპლიკატი')
                    except:
                        _print.fail(f'line 884: {prs["product_id"]} პროდუქტზე არის რაღაც ხარვეზი')

            except:
                _print.fail(f'line 887: {page_index+1} გვერდზე არის რაღაც ხარვეზი')

# GET PRODUCTS
def get_products():

    for prod in ids_db.find({"parsed":False}):
        try:
            product_details_req = requests.get(links["product_details_api"]+prod['product_id']).json()
            product_details = product_details_req["PrData"]
            users_details = product_details_req["User"]
        except:
            _print.fail('პროდუქტის მოთხოვნა ვერ შესრულდა')
            continue

        if len(product_details)==0:
            ids_db.delete_one({"product_id":prod['product_id']})
            _print.fail('პროდუქტი არ მოიძებნა')
            continue

        try:
            if not is_duplicate(users_db, "user_id", users_details["user_id"]):
                user_object = users_db.insert_one({
                    "user_id":users_details["user_id"],
                    "name": (f'{users_details["user_name"]} {users_details["user_surname"]}'),
                    "gender": 0 if users_details['gender_id'] == "2" else 1,
                    "number_of_posts": users_details["pr_count"],
                    "phone": {
                        "country_code":int(product_details['client_phone'][0:3]),
                        "number": product_details['client_phone'][3:len(product_details['client_phone'])]
                    },
                    "email":[users_details['username']],
                    "created_at": datetime.datetime.utcnow(),
                    "social_media":[{
                        "provider": "skype",
                        "user": users_details['skype'],
                        "addres": ""
                    }] if users_details['skype'] != "" else []
                })
                user_object_id = user_object.inserted_id
                _print.ok(f'{users_details["user_id"]} მომხმარებელი დაემატა')
            else:

                try:
                    users_db.update_many({"user_id": users_details["user_id"]},{"$set": {"number_of_posts": users_details["pr_count"]}})
                except:
                    _print.fail(f'line 934: {users_details["user_id"]} მომხმარებელი ვერ განახლდა')

                user_object_id= ObjectId(users_db.find_one({"user_id":users_details["user_id"]})["_id"])
                _print.value(f'{users_details["user_id"]} მომხმარებელი განახლდა')

        except:
            _print.fail('line 944: მომხმარებელი არ დაემატა')

        try:
            order_date = product_details['order_date']
            prod_sdate = datetime.date(int(product_details["order_date"][0:4]), int(product_details["order_date"][5:7]), int(product_details["order_date"][8:10]))
            prod_edate = prod_sdate + MONTH

        except:
            _print.fail('line 954: თარიღზე დაფიქსირდა შეცდომა')
            log_error(prod['product_id'], "თარიღზე დაფიქსირდა შეცდომა")
            continue
    
        try:        
            real_estate_db.insert_one({
                "owner_id": user_object_id,
                "views": product_details_req['Views'],
                "is_company": True,
                "post_status": "active",
                "deal_type": get_deal_type(product_details["adtype_id"]),
                "type_of_property": [get_product_type(product_details["product_type_id"])],
                "floor":int(exceptor(product_details["floor"], "სართულის", prod['product_id'])),
                "floors":int(exceptor(product_details["floors"], "სართულების რაოდენობის", prod['product_id'])),
                "car_spaces": int(exceptor(product_details["parking_id"], "გარაჟი", prod['product_id'])),
                "location": [{
                    "country": {
                        "id":"GE"
                    },
                    "city": {
                        "id":  geo_names(prod["city"], prod['product_id']),
                        "name": prod["city"],
                        "subdivision": ""
                    },
                    "street": exceptor(product_details["name"], "ქუჩის", prod['product_id']),
                    "address": exceptor(product_details["street_address"], "მისამართის", prod['product_id']),
                    "geo_cord" : {
                        "lang": exceptor(product_details["map_lon"], "long კოორდიანის", prod['product_id']),
                        "lat":  exceptor(product_details["map_lat"], "lat კოორდიანტის ", prod['product_id'])
                    },
                }],
                "avilable_from": f'{prod_sdate.year}-{prod_sdate.month}-{prod_sdate.day}',
                "avilable_to": f'{prod_edate.year}-{prod_edate.month}-{prod_edate.day}',
                "is_urgent":False,
                "is_agent": bool(product_details["makler_id"]),
                "detail":{
                    "title":exceptor(PrTitles[f'{product_details["adtype_id"]}_{product_details["product_type_id"]}_{product_details["estate_type_id"]}'], "სათაურის", prod['product_id']),
                    "houses_rules": "",
                    "description": product_details["comment"]
                    # "description": Translate(product_details["comment"])
                },
                "created_at": datetime.datetime.utcnow(),
                "price":{
                    "price_type": "total_price",
                    "fin_price":0,
                    "fax_price":0,
                    "fix_price": int(float(product_details["price_value"])),
                    "currency": "GEL"
                },
                "metric": "feet_square", 
                "total_area": int(exceptor(product_details["area_size_value"], "ფართობის", prod['product_id'])),
                "bedrooms":int(exceptor(product_details["bedrooms"], "საძინებლების რაოდენობის", prod['product_id'])),
                "bathrooms":int(exceptor(product_details["bathrooms"], "აბაზანის რაოდენობის", prod['product_id'])),          
                "outdoor_features":get_features("outdoor",product_details),
                "indoor_features":get_features("indoor", product_details),
                "climat_control":get_features("climat_control", product_details),
                "phone": {
                    "country_code":int(product_details['client_phone'][0:3]),
                    "number": product_details['client_phone'][3:len(product_details['client_phone'])]
                },
                "property_type":get_property_type(product_details["product_type_id"]),
                "files": get_images( 
                    product_details["photos_count"], 
                    product_details["photo"], 
                    product_details["product_id"]
                ),
                "status": is_new_building(product_details["estate_type_id"]),
            })
            _print.ok(f'{product_details["product_id"]} პროდუქტი დაემატა')
            ids_db.update({"product_id":prod['product_id']},{"$set":{"parsed": True}})
            remove_error(prod['product_id'])
        except:
            _print.fail('line 987: პროდუქტი არ დაემატა')
            log_error(prod['product_id'], "პროდუქტი არ დაემატა")

get_products()            