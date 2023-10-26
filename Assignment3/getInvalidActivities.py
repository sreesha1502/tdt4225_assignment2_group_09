from DbConnector import DbConnector
from bson.objectid import ObjectId
from tabulate import tabulate


class ExampleProgram:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def fetch_trackpoints(self):
        collection = self.db['activities']
        objInstance1 = ObjectId("652eb251f15b380b11af44be")
        objInstance2 = ObjectId("652eb25af15b380b11b06a22")
        documents = collection.aggregate([
            {"$match": {'transportation_mode': {'$exists': True}}},
            {"$lookup": {
                "from": "trackpoints",
                "localField": "_id",
                "foreignField": "activity_id",
                "as": "tpoints"
            }},
            {"$project": {
                "_id": 1,
                "user_id": 1,
                'datedays': "$tpoints.date_days",
                'dateTime': "$tpoints.date_time"
            }}
        ])
        # for doc in documents:
        #   pprint(doc)
        return documents


def main():
    program = None
    try:
        program = ExampleProgram()
        activities = program.fetch_trackpoints()
        users = dict()
        for activity in activities:
            user_id = activity['user_id']
            activity_id = activity['_id']
            tps_dates = activity['dateTime']
            current_tp_time = tps_dates[0]
            invalid_activity = False
            for i in range(len(tps_dates)):
                if i != 0: current_tp_time = tps_dates[i-1]
                next_tp_time = tps_dates[i]
                delta = next_tp_time - current_tp_time
                minute = delta.total_seconds() / 60
                if minute > 5:
                    invalid_activity = True
                    break
            if invalid_activity:
                if user_id in users.keys():
                    users[user_id] += 1
                else:
                    users[user_id] = 1
                print(user_id, activity_id, current_tp_time, invalid_activity)
        print(users)
        my_users = [(u, users[u]) for u in users]
        columns = ["user", "Number of Invalid activities"]
        print(tabulate(my_users, headers=columns))
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
