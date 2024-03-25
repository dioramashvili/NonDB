import json
import pymongo
from pymongo import MongoClient


def regions():
    with open("data.json", "r") as f:
        data = json.load(f)
    return list(data.keys())


class DatabaseManager:
    def __init__(self):
        self.client = MongoClient("localhost", 27017)
        self.db = self.client["Database"]

    def create_table(self, collection_name):
        if collection_name not in self.db.list_collection_names():
            self.db.create_collection(collection_name)

    def add_data(self, collection_name, data):
        self.db[collection_name].insert_one(data)

    def get_existing_relations(self, collection_name):
        return list(self.db[collection_name].find())

    def delete_row(self, collection_name, query):
        self.db[collection_name].delete_one(query)

    def load_data(self, collection_name):
        return list(self.db[collection_name].find())

    def search(self, collection_name, query):
        return list(self.db[collection_name].find(query))

    def update(self, collection_name, query, new_data):
        self.db[collection_name].update_one(query, {"$set": new_data})

    def check_db(self):
        return bool(self.db.list_collection_names())

    def list_advisors_with_students_count(self, order_by):
        advisors_collection = self.db["advisors"]

        aggregation_pipeline = [
            {
                "$lookup": {
                    "from": "student_advisor",
                    "localField": "advisor_id",
                    "foreignField": "advisor_id",
                    "as": "student_docs"
                }
            },
            {
                "$unwind": {
                    "path": "$student_docs",
                    "preserveNullAndEmptyArrays": True
                }
            },
            {
                "$group": {
                    "_id": "$_id",
                    "name": {"$first": "$name"},
                    "surname": {"$first": "$surname"},
                    "student_count": {"$sum": 1}
                }
            },
            {
                "$sort": {order_by: pymongo.ASCENDING}
            }
        ]

        return list(advisors_collection.aggregate(aggregation_pipeline))

    def list_students_with_advisors_count(self, order_by):
        students_collection = self.db["students"]

        aggregation_pipeline = [
            {
                "$lookup": {
                    "from": "student_advisor",
                    "localField": "student_id",
                    "foreignField": "student_id",
                    "as": "advisor_docs"
                }
            },
            {
                "$unwind": {
                    "path": "$advisor_docs",
                    "preserveNullAndEmptyArrays": True
                }
            },
            {
                "$group": {
                    "_id": "$_id",
                    "name": {"$first": "$name"},
                    "surname": {"$first": "$surname"},
                    "advisor_count": {"$sum": 1}
                }
            },
            {
                "$sort": {order_by: pymongo.ASCENDING}
            }
        ]

        return list(students_collection.aggregate(aggregation_pipeline))
