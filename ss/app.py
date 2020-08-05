import pymongo,  requests,  datetime, _print
from scrapy.selector import Selector 
from scripts import string_to_int, geo_names
from db_scripts import log_error, is_duplicate
from pprint import pprint
from bson import ObjectId

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
database = myclient["scrapping"]
links_db = database["home_links"]
real_estate_db = database["real_estate"]
users_db = database["myhome_users"]


_selectors = {
    "item_links": ".DesktopArticleLayout .latest_desc > div > a::attr(href)",
    "page_len":".last.pagenation_numbers a::text"
}


def get_property_type(_type_id, _url):
    try:
        switcher={
            "Land": "land",
            "Private House": "houses",
            "Summer cottage": "houses",
            "Flat": "appartments",
            "Commercial Real Estate": "commercial_properties",
        }

        return switcher.get(_type_id, "any")
    except:
        log_error(_url, f'Property Type', True)
        return "any"


def get_deal_type(_type_id,_url):
    try:
        switcher={
            "For Rent": "rent",
            "For Sale": "sell",
            "Daily rent": "rent",
            "Lease": "lease",
        }

        return switcher.get(_type_id, "any")
    except:
        log_error(_url, f'Deal Type', True)
        return "any"

def get_status(_type_id,_url):
    try:
        switcher={
            "New building": "new_building",
            "Under construction": "under_construction",
            "Old building": "old_build",
        }

        return switcher.get(_type_id, "")
    except:
        log_error(_url, f'სტატუსის', True)
        return ""

def select_many(req, select):
    try:
        _sel = Selector(response=req).css(select).getall()
        return _sel
    except:
        log_error(req.url, f'{select} სელექტორების', True)
        return []

def select_one(req, select, comments=False):
    try:
        _sel = Selector(response=req).css(select).get()
        if _sel != None and comments:
            _sel = _sel.split('-->')[1]
            _sel = _sel.split('<!--')[0]
        if _sel == None:
            return ""
            log_error(req.url, f'{select} სელექტორის ტექსტის', True)
        return _sel
    except:
        log_error(req.url, f'{select} სელექტორის', True)
        return ""

def get_outdoor_features(req):
    outdoor_features = []
    try:
        try:    
            if bool(select_one(req, ".parameteres_item_each:not(.lacking) .fa-pool")): outdoor_features.append("swimming_pool")
        except:
            log_error(req.url, "აუზის", True)
        try:
            if bool(select_one(req, ".parameteres_item_each:not(.lacking) .fa-garage")): outdoor_features.append("garage")
        except:
            log_error(req.url, "გარაჟის", True)
        try:
            if bool(select_one(req, ".parameteres_item_each:not(.lacking) .fa-balcony2")): outdoor_features.append("balcony")
        except:
            log_error(req.url, "აივნის", True)
    except:
            log_error(req.url, "Outdoor Features", True)
    return outdoor_features

def get_climate_control(req):
    climate_control = []
    try:
        try:
            if select_one(req, "#df_field_heating .value",True) == "Central heating system": climate_control.append("zonal_heating")
        except:
            log_error(req.url, "ცენტრალური გათბობის", True)

        try:    
            if select_one(req, "#df_field_heating .value",True) == "Solar heater": climate_control.append("solar_hot_water")
        except:
            log_error(req.url, "ცხელი წყლის", True)

        try:
            if select_one(req, "#df_field_heating .value",True) == "Tank": climate_control.append("water_tank")
        except:
            log_error(req.url, "წყლის რეზერუარი", True)

        try:    
            if bool(select_one(req, "df_field_technic .checkboxes > .active[title='Air Conditioning']",True)) : climate_control.append("air_conditioning")
        except:
            log_error(req.url, "კონდიციონერი", True)

    except:
        log_error(req.url, "კლიმატ კონტროლის", True)
        _print.fail(f'line 124: get_features() - ს აქვს რაღაც ხარვეზი')
    
    return climate_control

def get_indoor_features(req):
    indoor_features = []
    try:
        try:
            if select_one(req, "#df_field_elevator .value",True) != "No": indoor_features.append("lift")
        except:
            log_error(req.url, "ლიფტის", True)

        try:    
            if bool(select_one(req, "#df_field_essentials .checkboxes > .active[title='Dishwasher']",True)) : indoor_features.append("dishwasher")
        except:
            log_error(req.url, "აბაზანის", True)

    except:
        log_error(req.url, "Indoor Features", True)
    
    return indoor_features

def converted_price(price, pr_link):
    price_value = ""
    try:
        for price_index in string_to_int(price):
            price_value+=str(price_index)
    except:
        log_error(pr_link, "ფასის ან ნომრის", True)

    return price_value
 
def get_images(req):
    images = select_many(req, ".swiper-wrapper .swiper-slide img::attr(src)")
    img_arr = []
    for img in images:
        split_link = img.split("/")
        img_arr.append({
            "id":ObjectId(),
            "name":	split_link[len(split_link)-1],
            "mime":	"image/jpg",
            "url": img	
        })

    return img_arr

# GET IDS 
def get_links():

        for_len = requests.get(f'https://ss.ge/en/real-estate/l?Page=2&PriceType=false&CurrencyId=1')
        page_len = string_to_int(Selector(response=for_len).css(_selectors["page_len"]).get())[0]
        print(page_len)
        for page_index in range(page_len):
            products_req = requests.get(f'https://ss.ge/en/real-estate/l?Page={str(page_index)}&PriceType=false&CurrencyId=1')
            products_link = Selector(response=products_req).css(_selectors["item_links"]).getall()
            for _link in products_link:
                _print.value("https://ss.ge"+_link)
                if not is_duplicate(links_db, "link", "https://ss.ge"+_link):
                    links_db.insert_one({
                        'link': "https://ss.ge"+_link,
                        'parsed': False,
                        "source": "ss.ge"
                    })
                else:
                    _print.fail("დუპლიკატი")

def get_products():
    from translator import Translate
    for prod in links_db.find({"parsed":False, "source": "ss.ge"0}):
        req = requests.get(prod["link"])
        _print.value(prod["link"])
        
        bread_crumbs = select_many(req, ".detailed_page_navlist ul li a::text")


        city = bread_crumbs[3]

        geonames_id = geo_names(city)

        deal_type = get_deal_type(bread_crumbs[2].strip(), prod['link'])

        property_type = get_property_type(bread_crumbs[1].strip(), prod['link'])

        status = get_status(select_one(req, "fieldValueStatusId2::text"), prod["link"])
        street = select_one(req, ".StreeTaddressList.realestatestr:text").strip()
        address = street
        bedrooms = int(select_many(req, ".ParamsHdBlk text::text")[2])
        bathrooms = ""
        total_area = string_to_int(select_many(req, ".ParamsHdBlk text::text")[0])[0]
        floor = string_to_int(select_many(req, ".ParamsHdBlk text::text")[3])
        floors = string_to_int(select_one(req, ".ParamsHdBlk text text span::text"))
        try:
            _view = int(select_one(req, "#.article_views span::text"))
        except:
            _view = 0

        outdoor_features =  get_outdoor_features(req)
        indoor_features = get_indoor_features(req)
        climate_control = get_climate_control(req)
        details = [{
            "title": select_one(req, "#area_listing > h1"),
            "house_rules": "",
            "description": Translate(select_one(req, "#df_field_additional_information .value", True))
        }]
        price ={
            "price_type":"total_price",
            "min_price":0,
            "max_price":0,
            "fix_price": converted_price(select_one(req, "#lm_loan_amount::attr(value)"),prod["link"]),
            "currency": "USD"
        }
        phones =  [{
            "country_code":995,
            "number":  converted_price(select_one(req, "#df_field_phone .value a::text"),prod["link"])
        }]
        files = get_images(req)
        pprint({
            "location": {
                "country":{
                    "id":"GE"
                },
                "city": {
                    "id":geonames_id,
                    "name": city,
                    "subdivision": ""
                },
                "street":street,
                "address": address,
            },
            "created_at": datetime.datetime.utcnow(),
            "deal_type": deal_type,
            "type_of_property": [property_type],
            "status": status,
            "bedrooms": bedrooms,
            "bathrooms": bathrooms,
            "total_area":total_area,
            "metric":"feet_square",
            "floor":floor,
            "floors":floors,
            "car_spaces":0,
            "is_agent":	True,
            "outdoor_features":outdoor_features,
            "indoor_features":indoor_features,
            "climate_control":climate_control,
            "detail":details,
            "price":price,
            "phones":phones,
            "files": files,
            "source": "Home.ge",
            "view":_view
        })
        # links_db.update_one({"link":prod["link"]}, {"$set": {"parsed":True}})
        

get_links()