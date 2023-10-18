from pprint import pprint
from DbConnector import DbConnector

class ExampleProgram:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def fetch_users_transportationMode(self, transportationMode):
        collection = self.db['activities']
        documents = collection.aggregate([
            {"$match": {'transportation_mode': transportationMode}},
            {"$group": {
                "_id": "$user_id",
                "count": {"$sum": 1}
            }},
            {"$project": {
                "users_id": "$_id",
                "_id": 0
            }}
        ])
        for doc in documents:
            pprint(doc)

def main():
    program = None
    try:
        program = ExampleProgram()
        program.fetch_users_transportationMode("taxi")
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()

if __name__ == '__main__':
    main()