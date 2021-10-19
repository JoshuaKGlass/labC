from pymongo import MongoClient
import random

client = MongoClient("mongodb://127.0.0.1:27017")
db = client.bizDB
businesses = db.biz

for business in businesses.find():
    businesses.update_one(
        {"_id": business["_id"]},
        {
            "$set": {
                "num_employees": random.randint(1, 100),
                "profit": [
                    {"year": "2019",
                     "gross": random.randint(-500000, 500000)
                     },
                    {"year": "2020",
                     "gross": random.randint(-500000, 500000)
                     },
                    {"year": "2021",
                     "gross": random.randint(-500000, 500000)

                     },
                ],
                "dummy": "Test"
            }
        }
    )
