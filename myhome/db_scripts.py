from pymongo import  MongoClient


db = MongoClient("mongodb://localhost:27017/")
collection = db["scrapping"]
error_db = collection["myhome_error_id"]
myhome_product_id = collection["myhome_product_id"]


def is_duplicate(_collection, _property, _value):
    if _collection.find_one({_property: _value}) == None:
        return False
    return True


def log_error(_id, text, saved=False):
    error_db.insert_one({
        "product_id":_id,
        "text": text,
        "saved":saved
    })

def remove_error(_id):
    error_db.delete_many({"product_id":_id, "saved": False})





def get_data(col, query={}):
    try:
        return collection[col].find_one(query)
    except:
        return ""

def insert_db(col, data):
    try:
        collection[col].insert(data)
    except:
        print("some error") 
