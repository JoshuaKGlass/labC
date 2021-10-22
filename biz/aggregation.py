from pymongo import MongoClient

client = MongoClient("mongodb://127.0.0.1:27017")
db = client.bizDB
businesses = db.biz

# pipeline = [
#     {"$match": {"town": "Banbridge"}},
#     {"$project": {"town": 1, "profit": 1}}
# ]

pipeline = [
    {"$group": {
        "_id": "$town",
        "count": {"$sum": 1}
    }}
]

for business in businesses.aggregate(pipeline):
    print(business["_id"], business["count"])
