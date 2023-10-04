from DbConnector import DbConnector
from tabulate import tabulate

class ExampleGetData:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def fetch_TrackPoints(self, activityId):

        query = "SELECT id, activity_id, date_time FROM TrackPoint WHERE activity_id = %i"
        self.cursor.execute(query % (activityId))
        rows = self.cursor.fetchall()
        return rows

    def fetch_Activities(self):

        query = "SELECT id, user_id  FROM Activity"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        return rows

def main():
    program = None
    try:
        program = ExampleGetData()
        activities = program.fetch_Activities()
        users = dict()
        columns = ["user", "Number of Invalid activities"]

        # loop through the activities table get each activity
        for activity in activities:
            trackpoints = program.fetch_TrackPoints(activityId= activity[0])
            currentTrackPoint = trackpoints[0]

            # loop through each trackpoint in an activity to get consequetive trackpoints
            for i in range(len(trackpoints)):
                if (i != 0): currentTrackPoint = trackpoints[i-1]
                nextTrackPoint = trackpoints[i]

                # difference between datetime in hours
                delta = nextTrackPoint[2] - currentTrackPoint[2]
                minutes = delta.total_seconds()/60
                if (minutes > 5):
                    print("user",activity[1],activity[0], minutes) # To print out the activities with user id and minute difference for each consequetive tractpoints with difference more than 5 minutes
                    if activity[1] in users.keys():
                        users[activity[1]] = users[activity[1]] + 1
                    else:
                        users[activity[1]] = 1
        # print(users)
        my_users = [(u, users[u]) for u in users]
        print(tabulate(my_users, headers=columns))

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()

if __name__ == '__main__':
    main()