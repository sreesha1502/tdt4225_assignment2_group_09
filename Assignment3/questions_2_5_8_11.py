from pprint import pprint
from DbConnector import DbConnector


class MongoDBProgram:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def question_2(self):
        activities = self.db["activities"].count_documents({})
        users = self.db["users"].count_documents({})
        print("The average number of activities per user is", activities / users)

    def question_5(self):
        transportation_modes = self.db["activities"].aggregate([
            {
                "$match": {"transportation_mode": {"$ne": None}}  # only count activities with transportation mode
            },
            {
                "$group": {
                    "_id": "$transportation_mode",
                    "number_of_activities": {"$sum": 1}  # sum number of activities per transportation mode
                }
            }
        ])
        for tm in transportation_modes:
            pprint(tm)

    def question_8(self):
        # get all activities where transportation mode is not null
        # not specified in the subject, but if we don't do it the query run for multiple hours
        # replace by activities = self.db["activities"].find({})
        # to get all activities
        activities = self.db["activities"].find({"transportation_mode": {"$ne": None}})
        user_altitudes_dict = dict()
        # for each activity, calculating the sum of the altitudes of the trackpoints
        for a in activities:
            tr_of_a = self.db["trackpoints"].aggregate([
                {
                    "$match": {"altitude": {"$ne": -777}, "activity_id": a["_id"]}
                },
                {
                    "$group": {
                        "_id": None,
                        "sum_alt": {"$sum": "$altitude"}
                    }
                },
                {
                    "$project": {
                        "sum_alt_in_meters": {"$divide": ["$sum_alt", 3.281]},
                        "sum_alt": "$sum_alt"
                    }
                }
            ])
            # updating the sum of altitude gained by user
            for tr in tr_of_a:
                if a["user_id"] in user_altitudes_dict:
                    user_altitudes_dict[a["user_id"]] = user_altitudes_dict[a["user_id"]] + tr["sum_alt_in_meters"]
                elif not a["user_id"] in user_altitudes_dict:
                    user_altitudes_dict[a["user_id"]] = tr["sum_alt_in_meters"]
        # sort by most altitude in meters
        sorted_by_altitude = dict(sorted(user_altitudes_dict.items(), key=lambda item: item[1], reverse=True))
        # putting in tuple for respecting the format of the problem text
        dictionary_to_tuple = [(u, sorted_by_altitude[u]) for u in sorted_by_altitude]
        # print only top 20 users
        pprint(dictionary_to_tuple[:20])

    def question_11(self):
        users_with_tm = self.db["activities"].aggregate([
            {
                "$match": {"transportation_mode": {"$ne": None}}
            },
            {
                "$group": {
                    "_id": {
                        "a_user_id": "$user_id",
                        "b_tm": "$transportation_mode"
                    },
                    "c_most_used_tm_num": {"$sum": 1},
                }
            },
            {
                # sort asc so that further processing takes only the last into account
                "$sort": {"c_most_used_tm_num": 1}
            }
        ])
        # putting all the results in a dictionary (allow registering only the most used
        # transportation mode + suppressing doubles)
        dictionary = dict()
        for u in users_with_tm:
            dictionary[u["_id"]["a_user_id"]] = u["_id"]["b_tm"]
        # putting in tuple for respecting the format of the problem text
        dictionary_to_tuple = [(u, dictionary[u]) for u in dictionary]
        # sorting on user_id
        dictionary_to_tuple.sort()
        pprint(dictionary_to_tuple)


def main():
    program = None
    try:
        program = MongoDBProgram()
        program.question_2()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
