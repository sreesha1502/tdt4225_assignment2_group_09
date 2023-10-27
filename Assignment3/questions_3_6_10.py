import datetime
import os
from pprint import pprint
from DbConnector import DbConnector
from tabulate import tabulate

class ExampleProgram:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db
        
    def q3(self):
        activities = self.db["activities"]
        user_activities = {}
        for x in activities.find():
            user_id = x['user_id']
            if user_id in user_activities:
                user_activities[user_id] += 1
            else:
                user_activities[user_id] = 1
        
        sorted_user_activities = sorted(user_activities.items(), key=lambda item: item[1], reverse=True)
        top20_users = sorted_user_activities[:20]
        
        print('Question 3: \n\n')
        print('Here is the Top 20 users with the most activities') 
        print('Each tuple can we read like this -> (user_id, number_of_activities_in_total):')
        for x in top20_users:
            print(x)
        return top20_users
    
    def q6(self):
        activities = self.db["activities"]
        year_activities = {}
        for x in activities.find():
            start_date_time = x['start_date_time']
            year = start_date_time.year
            if year in year_activities:
                year_activities[year] += 1
            else:
                year_activities[year] = 1
        
        most_active_year = max(year_activities, key=year_activities.get)
        print('Question 6: \n\n')
        print('Here is the year with the most activities:') 
        print(most_active_year,"with the outstanding number of :",year_activities[most_active_year], "activities !")
        return most_active_year
    
    def q10(self):
        activities = self.db["activities"]
        forbidden_city_lat = 39.916
        forbidden_city_lon = 116.397
        users_in_forbidden_city = set()
        
        for activity in self.db["activities"].find():
           if self.db["trackpoints"].find_one({"activity_id": activity["_id"], "lat": forbidden_city_lat, "lon": forbidden_city_lon}):
               users_in_forbidden_city.add(activity["user_id"])

        print("Users who have tracked an activity in the Forbidden City of Beijing:")
        for user_id in users_in_forbidden_city:
           print(user_id)
        
        return users_in_forbidden_city
    
    def find_activities_in_forbidden_city(self):
        forbidden_city_lat = 39.916
        forbidden_city_lon = 116.397

        pipeline = [
            {
                "$lookup": {
                    "from": "trackpoints",
                    "localField": "_id",
                    "foreignField": "activity_id",
                    "as": "trackpoints"
                }
            },
            {
                "$match": {
                    "trackpoints": {
                        "$elemMatch": {
                            "lat": forbidden_city_lat,
                            "lon": forbidden_city_lon
                        }
                    }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "user_id": 1
                }
            }
        ]

        results = list(self.db["activities"].aggregate(pipeline))

        return results

            
def main():
    program = ExampleProgram()
    #program.q3()
    #program.q6()
    #program.q9()
    program.q10()

    
if __name__ == '__main__':
    main()
