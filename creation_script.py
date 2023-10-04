from tabulate import tabulate

from DbConnector import DbConnector


class ExampleProgram:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_table_user(self):
        query = """CREATE TABLE IF NOT EXISTS %s (
                id VARCHAR(30) NOT NULL PRIMARY KEY,
                has_labels BOOLEAN)
                """
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query % "User")
        self.db_connection.commit()

    def create_table_activity(self):
        query = """CREATE TABLE IF NOT EXISTS %s (
                   id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                   user_id VARCHAR(30),
                   transportation_mode VARCHAR(30),
                   start_date_time DATETIME,
                   end_date_time DATETIME,
                   FOREIGN KEY (user_id) REFERENCES User(id)
                )
                """
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query % "Activity")
        self.db_connection.commit()
    def create_table_trackpoint(self):
        query = """CREATE TABLE IF NOT EXISTS %s (
                id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                activity_id INT NOT NULL,
                lat DOUBLE,
                lon DOUBLE,
                altitude INT,
                date_days DOUBLE,
                date_time DATETIME,
                FOREIGN KEY (activity_ID) REFERENCES Activity(id)
          )
          """
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query % "TrackPoint")
        self.db_connection.commit()


def main():
    program = None
    try:
        program = ExampleProgram()

        # creating tables
        program.create_table_user()
        program.create_table_activity()
        program.create_table_trackpoint()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()

