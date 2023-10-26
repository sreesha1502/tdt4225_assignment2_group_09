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



    def insert_user(self, user_id, has_labels):
        user = {
                "id": user_id,
                "has_labels": has_labels
        }
        collection = self.db["users"]
        collection.insert_one(user)

    def insert_activity(self, start_date_time, end_date_time, user_id, transportation_mode=None):
        if transportation_mode is not None:
            activity = {
                    "transportation_mode": transportation_mode,
                    "start_date_time": datetime.datetime.strptime(start_date_time, "%Y-%m-%d %H:%M:%S"),
                    "end_date_time": datetime.datetime.strptime(end_date_time, "%Y-%m-%d %H:%M:%S"),
                    "user_id": user_id
            }
        else:
            activity = {
                "start_date_time": datetime.datetime.strptime(start_date_time, "%Y-%m-%d %H:%M:%S"),
                "end_date_time": datetime.datetime.strptime(end_date_time, "%Y-%m-%d %H:%M:%S"),
                "user_id": user_id
            }
        collection = self.db["activities"]
        x = collection.insert_one(activity)
        return x.inserted_id

    def insert_trackpoints(self, trackpoints):
        collection = self.db["trackpoints"]
        collection.insert_many(trackpoints)

def main():
    program = ExampleProgram()
    i = 0
    f = open("./dataset/labeled_ids.txt", "r")
    user_ids_has_labels = f.read().split("\n")  # IDs of the users which we have id in (in string)
    count_activities = 0
    count_track=0
    for root, dirs, files in os.walk("./dataset/Data", topdown=False):
        for user_id in dirs:
            if user_id.isdigit():
                has_labels = False
                for id in user_ids_has_labels:
                    if id == user_id:
                        has_labels = True
                # ---- insert user
                program.insert_user(user_id, has_labels)
                if has_labels:
                    print(user_id, "has id")
                    f = open("./dataset/Data/" + user_id + "/labels.txt", "r")
                    activities_labels_lines = f.read().split("\n")  # Activities as lines (string)
                    activities_labels_lines = [ac.split("\t") for ac in activities_labels_lines if
                                               ac.split("\t")[0] != "Start Time" and ac.split("\t")[0] != ""]
                    # --------------------------------- Browse trajectory directory
                    for root_traj, dirs_traj, files_traj in os.walk("./dataset/Data/" + user_id + "/Trajectory",
                                                                    topdown=False):
                        for name_traj in files_traj:
                            f = open("./dataset/Data/" + user_id + "/Trajectory/" + name_traj)  # Trajectory file currently opened
                            if (len(f.readlines()) - 6) < 2500:  # If file has more than 2500 points, we are not interested
                                f.seek(0)
                                file_lines = [line for line in f.readlines() if line != ""]
                                first_line = file_lines[6]
                                last_line = file_lines[len(file_lines)-1]
                                for ac in activities_labels_lines:  # Looking for start-end of user's activities
                                    start_time = ac[0].replace("/", "-")
                                    end_time = ac[1].replace("/", "-")
                                    transportation_mode = ac[2]

                                    start_found = False
                                    end_found = False

                                    lt, lg, unimportant, alt, datenum, datestr, timestr = first_line.split(",")
                                    date_time_first_line = (datestr + " " + timestr).replace("\n", "")
                                    lt, lg, unimportant, alt, datenum, datestr, timestr = last_line.split(",")
                                    date_time_last_line = (datestr + " " + timestr).replace("\n", "")

                                    if date_time_first_line == start_time:
                                        start_found = True

                                    if date_time_last_line == end_time:
                                        end_found = True

                                    if start_found and end_found:
                                        # insert the activity
                                        activity_id = program.insert_activity(start_time, end_time, user_id, transportation_mode)
                                        # todo: insert the trackpoints
                                        trackpoints = file_lines[6:]
                                        array_trackpoints = []
                                        for t in trackpoints:
                                            lt, lg, unimportant, alt, datenum, datestr, timestr = t.split(",")
                                            date_time = datestr + " " + timestr
                                            date_time = date_time.replace("\n", "")
                                            array_trackpoints.append({
                                                "lat": float(lt),
                                                "lon": float(lg),
                                                "altitude": float(alt),
                                                "date_days": float(datenum),
                                                "date_time": datetime.datetime.strptime(date_time, "%Y-%m-%d %H:%M:%S"),
                                                "activity_id": activity_id
                                            })
                                        program.insert_trackpoints(array_trackpoints)
                                        count_activities = count_activities+1
                                        count_track = count_track + len(file_lines)-7
                                        #print(user_id, " count ", ac)


                else:
                    print(user_id, "does not have id")
                    # --------------------------------- Browse trajectory directory
                    for root_traj, dirs_traj, files_traj in os.walk("./dataset/Data/" + user_id + "/Trajectory", topdown=False):
                        for name_traj in files_traj:
                            f = open("./dataset/Data/" + user_id + "/Trajectory/" + name_traj)  # Trajectory file currently opened
                            if (len(f.readlines()) - 6) < 2500:  # If file has more than 2500 points, we are not interested
                                f.seek(0)
                                file_lines = [line for line in f.readlines() if line != ""]
                                # todo: insert the activity
                                first_line = file_lines[6]
                                last_line = file_lines[len(file_lines) - 1]
                                lt, lg, unimportant, alt, datenum, datestr, timestr = first_line.split(",")
                                date_time_first_line = (datestr + " " + timestr).replace("\n", "")
                                lt, lg, unimportant, alt, datenum, datestr, timestr = last_line.split(",")
                                date_time_last_line = (datestr + " " + timestr).replace("\n", "")
                                activity_id = program.insert_activity(date_time_first_line, date_time_last_line, user_id)
                                # insert the trackpoints
                                trackpoints = file_lines[6:]
                                array_trackpoints = []
                                for t in trackpoints:
                                    lt, lg, unimportant, alt, datenum, datestr, timestr = t.split(",")
                                    date_time = datestr + " " + timestr
                                    date_time = date_time.replace("\n", "")
                                    array_trackpoints.append({
                                        "lat": float(lt),
                                        "lon": float(lg),
                                        "altitude": float(alt),
                                        "date_days": float(datenum),
                                        "date_time": datetime.datetime.strptime(date_time, "%Y-%m-%d %H:%M:%S"),
                                        "activity_id": activity_id
                                    })
                                program.insert_trackpoints(array_trackpoints)
                                count_activities = count_activities + 1
                                count_track = count_track + len(file_lines) - 7

    print(count_activities)
    print(count_track)



if __name__ == '__main__':
    main()
