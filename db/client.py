from pymongo import MongoClient
from config import MONGO_URI, MONGO_DB
import threading

_client = None
_lock   = threading.Lock()

def get_db():
    global _client
    if _client is None:
        with _lock:
            if _client is None:
                _client = MongoClient(MONGO_URI)
    return _client[MONGO_DB]