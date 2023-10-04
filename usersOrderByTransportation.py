from DbConnector import DbConnector
from tabulate import tabulate

class ExampleGetData:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def fetch_activityData(self, table_name, mode):
        query = "SELECT  User_id, transportation_mode, count(*) as amount FROM %s WHERE transportation_mode = '%s' GROUP BY user_id, transportation_mode  ORDER BY amount desc"
        self.cursor.execute(query % (table_name,mode))
        rows = self.cursor.fetchall()
        return rows

 # This function is not referenced in the python code for reference purpose only
    def fetch_activityData_onTransportMode(self, table_name1,table_name2,transportMode):
        query = "SELECT user.id as userId, count(transportation_mode) as mode FROM %s user,%s aty WHERE user.id = aty.user_id AND aty.transportation_mode = %s GROUP BY user.id ORDER BY mode desc;"
        self.cursor.execute(query % (table_name1, table_name2,transportMode))
        rows = self.cursor.fetchall()
        return rows

def main():
    program = None

    try:
        program = ExampleGetData()
        transport_modes = ['walk', 'bike', 'bus', 'taxi', 'car','subway', 'train', 'airplane', 'boat', 'run', 'motorcycle']

        # loop to get the users and transportation modes and the count of each transportation mode in decending order
        for tmode in transport_modes:
            print("Top ten users who use the", tmode)
            activities = program.fetch_activityData(table_name="Activity", mode=tmode)
            row = []
            columns = ["user_Id", "Amount"]

            # Loop through the fetched data to get the first 10 results
            for activity in activities[:10]:
                row.append([activity[0],activity[2]])
            print(tabulate(row, headers=columns, tablefmt="grid"))
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()

if __name__ == '__main__':
    main()