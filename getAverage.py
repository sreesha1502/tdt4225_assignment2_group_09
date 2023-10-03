from DbConnector import DbConnector
from tabulate import tabulate

class ExampleGetData:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def fetch_Userdata_WithContition(self, table_name):
        query = "SELECT * FROM %s  WHERE has_labels = 1 ORDER BY id DESC"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        # print("Data from table %s, raw format:" % table_name)
        # print(rows)
        # # Using tabulate to show the table in a nice way
        # print("Data from table %s, tabulated:" % table_name)
        # print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def fetch_activityData_ForUser(self, table_name, userId):
        query = "SELECT * FROM %s WHERE user_id = %s"
        self.cursor.execute(query % (table_name,userId))
        rows = self.cursor.fetchall()
        # print("Data from table %s, raw format:" % table_name)
        # print(rows)
        # # Using tabulate to show the table in a nice way
        # print("Data from table %s, tabulated:" % table_name)
        # print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def fetch_tpData_ForUser(self, table_name, activityId):
        query = "SELECT * FROM %s WHERE activity_id = %s"
        self.cursor.execute(query % (table_name,activityId))
        rows = self.cursor.fetchall()
        # print("Data from table %s, raw format:" % table_name)
        # print(rows)
        # # Using tabulate to show the table in a nice way
        # print("Data from table %s, tabulated:" % table_name)
        # print(tabulate(rows, headers=self.cursor.column_names))
        return rows
def main():
        program = None
        try:
            program = ExampleGetData()
            users = program.fetch_Userdata_WithContition(table_name="User")
            row = []
            columns = ["user_Id", "Average_TrackPoint", "Minimum_TrackPoint", "Maximum_TrackPoint"]
            for user in users:
                totalTrackPoint = 0
                numberOfTrackPoints = 0
                activities = program.fetch_activityData_ForUser(table_name="Activity", userId=user[0])
                for activity in activities:
                    trackpoints = program.fetch_tpData_ForUser(table_name="TrackPoint", activityId=activity[0])
                    totalTrackPoint = totalTrackPoint + len(trackpoints)
                    if numberOfTrackPoints == 0:
                        minimumTP = len(trackpoints)
                        maximumTP = len(trackpoints)
                    else:
                        if len(trackpoints) > maximumTP:
                            maximumTP = len(trackpoints)
                        if len(trackpoints) < minimumTP:
                            minimumTP = len(trackpoints)
                    numberOfTrackPoints = numberOfTrackPoints + 1

                    averageTP = totalTrackPoint / numberOfTrackPoints
                print("userId", user[0],'averageTP',averageTP, 'minimumTP',minimumTP,'maximumTP', maximumTP)
                row.append([user[0],averageTP,minimumTP,maximumTP])
            print(tabulate(row,headers=columns, tablefmt="grid"))
        except Exception as e:
            print("ERROR: Failed to use database:", e)
        finally:
            if program:
                program.connection.close_connection()

if __name__ == '__main__':
    main()