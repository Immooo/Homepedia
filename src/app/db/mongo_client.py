import os
from pymongo import MongoClient

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB = os.getenv("MONGO_DB", "homepedia")

_client = None

def get_mongo_collection(name: str):
    global _client
    if _client is None:
        _client = MongoClient(MONGO_URI)
    db = _client[MONGO_DB]
    return db[name]
