from flask import Flask, request, jsonify, make_response
from pymongo import MongoClient
from bson import ObjectId

app = Flask(__name__)

client = MongoClient("mongodb://127.0.0.1:27017")
db = client.bizDB
businesses = db.biz


@app.route("/api/v1.0/businesses", methods=["GET"])
def show_all_businesses():
    page_num, page_size = 1, 10
    if request.args.get('pn'):
        page_num = int(request.args.get('pn'))
    if request.args.get('ps'):
        page_size = int(request.args.get('ps'))
    page_start = (page_size * (page_num - 1))

    data_to_return = []
    for business in businesses.find().skip(page_start).limit(page_size):
        business['_id'] = str(business['_id'])
        for review in business['reviews']:
            review['_id'] = str(review['_id'])
        data_to_return.append(business)

    return make_response(jsonify(data_to_return), 200)


# TODO random string is 24 characters and return proper error
@app.route("/api/v1.0/businesses/<string:id>", methods=["GET"])
def show_one_business(id):
    business = businesses.find_one({'_id': ObjectId(id)})
    if business is not None:
        business['_id'] = str(business['_id'])
        for review in business['reviews']:
            review['_id'] = str(review['_id'])
        return make_response(jsonify(business), 200)
    else:
        return make_response(jsonify({"error": "Invalid business ID"}), 404)


@app.route("/api/v1.0/businesses", methods=["POST"])
def add_business():
    if "name" in request.form and "town" in request.form and "rating" in request.form:
        new_business = {
            "name": request.form["name"],
            "town": request.form["town"],
            "rating": request.form["rating"],
            "reviews": {},
        }
        new_business_id = businesses.insert_one(new_business)
        new_business_link = "http://localhost:500/api/v1.0/businesses/" + str(new_business_id.inserted_id)
        return make_response(jsonify({"url": new_business_link}), 201)
    else:
        return make_response(jsonify({"error": "Missing form data"}), 404)


@app.route("/api/v1.0/businesses/<string:id>", methods=["PUT"])
def edit_business(id):
    if "name" in request.form and "town" in request.form and "rating" in request.form:
        result = businesses.update_one({
            {"_id": ObjectId(id)},
            {"$set": {"name": request.form["name"],
                      "town": request.form["town"],
                      "rating": request.form["rating"]}
             }
        })
        if result.matched_count == 1:
            edit_business_link = "http://localhost:500/api/v1.0/businesses/" + id
            return make_response(jsonify({"url": edit_business_link}), 200)
        else:
            return make_response(jsonify({"error": "Invalid business ID"}), 404)
    else:
        return make_response(jsonify({"error": "Missing form data"}), 404)


@app.route("/api/v1.0/businesses/<string:id>", methods=["DELETE"])
def delete_business(id):
    result = businesses.delete_one({"_id": ObjectId(id)})
    if result.deleted_count == 1:
        return make_response(jsonify({}), 204)
    else:
        return make_response(jsonify({"error": "Invalid bussines ID"}), 404)


@app.route("/api/v1.0/businesses/<string:id>/reviews", methods=["POST"])
def add_new_review(id):
    new_review = {
        "_id": ObjectId(),
        "username": request.form["username"],
        "comment": request.form["comment"],
        "stars": request.form["stars"]
    }

    new_review_link = "http://localhost:500/api/v1.0/businesses/" + id + "/reviews/" + str(new_review["_id"])

    return make_response(jsonify({"url": new_review_link}), 201)


@app.route("/api/v1.0/businesses/<string:id>/reviews", methods=["GET"])
def fetch_all_reviews(id):
    data_to_return = []
    business = businesses.find_one(
        {"_id": ObjectId(id),
         "reviews": 1, "_id": 0})
    for review in business["reviews"]:
        review["_id"] = str(review["_id"])
        data_to_return.append(review)
    return make_response(jsonify(data_to_return), 200)


@app.route("/api/v1.0/businesses/<bid>/reviews/<rid>", methods=["GET"])
def fetch_one_review(bid, rid):
    business = businesses.find_one(
        {"reviews._id": ObjectId(rid)},
        {"_id": 0, "reviews.$": 1})
    if business is None:
        return make_response(jsonify({"error": "Invalid ID or review ID"}), 404)
    business["reviews"][0]["_id"] = str(business["reviews"][0]["_id"])
    return make_response(jsonify(business["reviews"][0]), 200)


@app.route("/api/v1.0/businesses/<bid>/reviews/<rid>", methods=["PUT"])
def edit_review(bid, rid):
    edited_review = {
        "reviews.$.username": request.form["username"],
        "reviews.$.comment": request.form["comment"],
        "reviews.$.stars": request.form['stars']
    }

    businesses.update_one({"reviews._id": ObjectId(rid)}, {"$set": edited_review})
    edit_review_url = "http://localhost:5000/api/v1.0/businesses/" + bid + "/reviews/" + rid
    return make_response(jsonify({"url": edit_review_url}), 200)


if __name__ == "__main__":
    app.run(debug=True)
