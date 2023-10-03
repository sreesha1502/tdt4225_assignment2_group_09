import datetime

from DbConnector import DbConnector
from tabulate import tabulate
from math import radians, cos, sin, asin, sqrt
from datetime import date
import numpy
class ExampleGetData:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def fetch_TrackPoints(self, activityId):

        query = "SELECT id, activity_id, date_time FROM TrackPoint WHERE activity_id = %i"
        self.cursor.execute(query % (activityId))
        rows = self.cursor.fetchall()
        # print("Data from table %s, raw format:" % table_name)
        # print(rows)
        # # Using tabulate to show the table in a nice way
        # print("Data from table %s, tabulated:" % table_name)
        # print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def fetch_Activities(self):

        query = "SELECT id, user_id  FROM Activity"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        # print("Data from table %s, raw format:" % table_name)
        # print(rows)
        # # Using tabulate to show the table in a nice way
        # print("Data from table %s, tabulated:" % table_name)
        # print(tabulate(rows, headers=self.cursor.column_names))
        return rows

def mergeUsers(users):
    merged_objects = {}
    for obj in users:
        key = obj['user']
        if key not in merged_objects:
            merged_objects[key] = obj.copy()
        else:
            merged_objects[key]['activity'] = obj['activity']

    return list(merged_objects.values())


def main():
    program = None
    try:
        program = ExampleGetData()
        activities = program.fetch_Activities()
        users = dict()
        columns = ["user", "Number of Invalid activities"]
        numberActivitiesInvalid = 0
        for activity in activities:
            trackpoints = program.fetch_TrackPoints(activityId= activity[0])
            currentTrackPoint = trackpoints[0]
            invalid = 0
            for i in range(len(trackpoints)):
                if (i != 0): currentTrackPoint = trackpoints[i-1]
                nextTrackPoint = trackpoints[i]

                # difference between datetime in timedelta
                delta = nextTrackPoint[2] - currentTrackPoint[2]
                minutes = delta.total_seconds()/60
                if (minutes > 5):
                    print("user",activity[1],activity[0], minutes)
                    if activity[1] in users.keys():
                        users[activity[1]] = users[activity[1]] + 1
                    else:
                        users[activity[1]] = 1
        # result = mergeUsers(users)
        print(users)
        my_users = [(u, users[u]) for u in users]
        print(tabulate(my_users, headers=columns))

        #print(tabulate(users, headers=columns, tablefmt="grid"))

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()

if __name__ == '__main__':
    main()