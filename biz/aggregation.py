from pymongo import MongoClient

client = MongoClient("mongodb://127.0.0.1:27017")
db = client.bizDB
businesses = db.biz

# pipeline = [
#     {"$match": {"town": "Banbridge"}},
#     {"$project": {"town": 1, "profit": 1}}
# ]

pipeline = [
    {"$project": {
        "town": 1, "profit": 1, "num_employees": 1}},
    {"$unwind": "$profit"},
    {"$match": {
        "num_employees": {"$gte": 99},
        "profit.gross": {"$gte": 0}}}
]

# pipeline = [
#     {"$match": {
#         "num_employees": {"$gte": 99},
#         "profit.gross": {"$gte": 0}}}
# ]

for business in businesses.aggregate(pipeline):
    print(business["town"], str(business["profit"]["gross"]))
