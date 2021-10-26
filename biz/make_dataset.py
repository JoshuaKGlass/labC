from pymongo import MongoClient
import json
from pathlib import Path
from bson import ObjectId


def create_database():
    client = MongoClient("mongodb://127.0.0.1:27017")
    db = client.yelpDB

    businesses = db.businesses
    with open('businesses.json') as f:
        for line in f:
            businesses.insert_one(json.loads(line))
    print("Businesses loaded")

    users = db.users
    with open('users.json') as f:
        for line in f:
            users.insert_one(json.loads(line))
    print("Users loaded")

    # checkins = db.checkins
    # with open('yelp_academic_dataset_checkin.json') as f:
    #     for line in f:
    #         checkins.insert_one(json.loads(line))
    # print("Checkins loaded")
    #
    # reviews = db.reviews
    # with open('yelp_academic_dataset_review.json') as f:
    #     for line in f:
    #         reviews.insert_one(json.loads(line))
    # print("Reviews loaded")
    #
    # tips = db.tips
    # with open('yelp_academic_dataset_tip.json') as f:
    #     for line in f:
    #         tips.insert_one(json.loads(line))
    # print("Tips loaded")
    #
    # photos = db.photos
    # with open('../yelp_photos/photos.json') as f:
    #     for line in f:
    #         photos.insert_one(json.loads(line))
    # print("Photos loaded")


def add_reviews_to_businesses():
    client = MongoClient("mongodb://127.0.0.1:27017")
    db = client.yelpDB
    businesses = db.businesses
    reviews = db.reviews

    num_processed = 0
    for business in businesses.find({}, {"business_id": 1}, no_cursor_timeout=True):
        reviews_of_business = []
        for review in reviews.find({"business_id": business["business_id"]}):
            reviews_of_business.append(review)

        businesses.update_one({"business_id": business["business_id"]},
                              {"$set": {"reviews": reviews_of_business, "review_count": len(reviews_of_business)}})
        num_processed = num_processed + 1
        if num_processed % 1000 == 0:
            print(str(num_processed) + " businesses processed")


def add_photographs_to_businesses():
    client = MongoClient("mongodb://127.0.0.1:27017")
    db = client.yelpDB
    businesses = db.businesses
    photos = db.photos

    num_processed = 0
    for business in businesses.find({}, {"business_id": 1}, no_cursor_timeout=True):
        photos_of_business = []
        for photo in photos.find({"business_id": business["business_id"]}):
            photo_obj = {"filename": photo["photo_id"] + ".jpg",
                         "caption": photo["caption"],
                         "label": photo["label"]
                         }
            photos_of_business.append(photo_obj)

        businesses.update_one({"business_id": business["business_id"]},
                              {"$set": {"photos": photos_of_business, "num_photos": len(photos_of_business)}})
        num_processed = num_processed + 1
        if num_processed % 1000 == 0:
            print(str(num_processed) + " businesses processed")


def add_checkins_to_businesses():
    client = MongoClient("mongodb://127.0.0.1:27017")
    db = client.yelpDB
    businesses = db.businesses
    checkins = db.checkins

    num_processed = 0
    for business in businesses.find({}, {"business_id": 1}, no_cursor_timeout=True):
        checkins_of_business = []
        for checkin in checkins.find({"business_id": business["business_id"]}):
            checkins_of_business = checkin["date"].split(',')
        checkins_of_business = [c_i.strip() for c_i in checkins_of_business]
        businesses.update_one({"business_id": business["business_id"]},
                              {"$set": {"checkins": checkins_of_business, "checkin_count": len(checkins_of_business)}})
        num_processed = num_processed + 1
        if num_processed % 1000 == 0:
            print(str(num_processed) + " businesses processed")


def add_tips_to_businesses():
    client = MongoClient("mongodb://127.0.0.1:27017")
    db = client.yelpDB
    businesses = db.businesses
    tips = db.tips

    num_processed = 0
    for business in businesses.find({}, {"business_id": 1}, no_cursor_timeout=True):
        tips_of_business = []
        for tip in tips.find({"business_id": business["business_id"]}):
            tips_of_business.append(tip)

        businesses.update_one({"business_id": business["business_id"]},
                              {"$set": {"tips": tips_of_business, "tip_count": len(tips_of_business)}})
        num_processed = num_processed + 1
        if num_processed % 1000 == 0:
            print(str(num_processed) + " businesses processed")


def fetch_required_photos():
    client = MongoClient("mongodb://127.0.0.1:27017")
    db = client.yelpDB
    businesses = db.businesses
    users = db.users

    num_processed = 0
    for business in businesses.find({}, {"photos": 1}, no_cursor_timeout=True):
        for photo in business["photos"]:
            Path("../yelp_photos/photos/" + photo["filename"]).rename("./photos/" + photo["filename"])

        num_processed = num_processed + 1
        if num_processed % 1000 == 0:
            print(str(num_processed) + " businesses processed")


def drop_unwanted_collections():
    client = MongoClient("mongodb://127.0.0.1:27017")
    db = client.yelpDB

    db.checkins.drop()
    db.reviews.drop()
    db.tips.drop()
    db.photos.drop()


def purge_inactive_users():
    client = MongoClient("mongodb://127.0.0.1:27017")
    db = client.yelpDB
    businesses = db.businesses
    users = db.users

    # get the set of active users - those that have left a review or a tip
    active_users = set()
    for business in businesses.find({}, {"reviews": 1, "tips": 1}, no_cursor_timeout=True):
        for review in business["reviews"]:
            active_users.add(review["user_id"])
        for tip in business["tips"]:
            active_users.add(tip["user_id"])
    print(str(len(active_users)) + " active users found")

    # remove inactive users from the collection
    # drop the collection and re-build it
    users.drop()
    with open('yelp_academic_dataset_user.json') as f:
        for line in f:
            this_user = json.loads(line)
            if this_user["user_id"] in active_users:
                users.insert_one(this_user)
    print("Inactive users removed")

    # remove any inactive users from the friends list of active users
    users_processed = 0
    users_list = list(users.find({}, {"user_id": 1, "friends": 1}))
    for user in users_list:
        active_friends = []
        friends_of_user = user["friends"].split(',')
        friends_of_user = [f.strip() for f in friends_of_user]
        for friend in friends_of_user:
            if friend in active_users:
                active_friends.append(friend)
        db.users.update_one({"user_id": user["user_id"]},
                            {"$set": {"friends": active_friends}})
        users_processed = users_processed + 1
        if users_processed % 10000 == 0:
            print(str(users_processed) + " users processed")
    print("Inactive users purged from users' friends lists")


def make_locations_GeoJSON():
    client = MongoClient("mongodb://127.0.0.1:27017")
    db = client.yelpDB
    businesses = db.businesses

    num_processed = 0
    for business in businesses.find({}, {"business_id": 1, "latitude": 1, "longitude": 1}, no_cursor_timeout=True):
        geo_point = {"type": "Point", "coordinates": [business["longitude"], business["latitude"]]}
        businesses.update_one({"business_id": business["business_id"]},
                              [{"$set": {"location": geo_point}},
                               {"$unset": ["longitude", "latitude"]}
                               ])
        num_processed = num_processed + 1
        if num_processed % 1000 == 0:
            print(str(num_processed) + " businesses processed")


def fix_user_ids_in_businesses():
    client = MongoClient("mongodb://127.0.0.1:27017")
    db = client.yelpDB
    businesses = db.businesses
    users = db.users

    num_processed = 0
    businesses_list = list(businesses.find({}, {"reviews": 1, "tips": 1}))
    for business in businesses_list:

        ''' approach
            read all reviews and tips into lists
            modify the list contents
            replace current reviews and tips entries with the modified values
            == only a single query for each business
        '''
        business_id = str(business["_id"])

        for review in business["reviews"]:
            this_user = review["user_id"]
            user = users.find_one({"user_id": this_user}, {"_id": 1})
            review["user_id"] = str(user["_id"])
            del review["review_id"]
            del review["business_id"]

        for tip in business["tips"]:
            this_user = tip["user_id"]
            user = users.find_one({"user_id": this_user}, {"_id": 1})
            tip["user_id"] = str(user["_id"])
            del tip["business_id"]

        businesses.update_one({"_id": ObjectId(business_id)},
                              {"$set": {"reviews": business["reviews"],
                                        "tips": business["tips"]
                                        }
                               }
                              )

        num_processed = num_processed + 1
        print(str(num_processed) + ". Business " + business_id + " processed")


def fix_user_ids_in_users():
    client = MongoClient("mongodb://127.0.0.1:27017")
    db = client.yelpDB
    users = db.users

    users_dict = {}
    users_list = list(users.find({}, {"_id": 1, "user_id": 1}))
    for user in users_list:
        users_dict[user["user_id"]] = str(user["_id"])
    print("Dictionary built")

    num_processed = 0
    users_list = list(users.find({}, {"_id": 1, "friends": 1}))
    for user in users_list:

        this_user = str(user["_id"])
        new_friends = []
        for friend in user["friends"]:
            new_friends.append(users_dict[friend])

        users.update_one({"_id": ObjectId(this_user)}, {"$set": {"friends": new_friends}})

        num_processed = num_processed + 1
        if num_processed % 1000 == 0:
            print(str(num_processed) + ". User " + this_user + " processed")


# create_database()
# add_reviews_to_businesses()
# add_photographs_to_businesses()
# add_checkins_to_businesses()
# add_tips_to_businesses()
# fetch_required_photos()
# purge_inactive_users()
# make_locations_GeoJSON()
# fix_user_ids_in_businesses()
# fix_user_ids_in_users()
# drop_unwanted_collections()