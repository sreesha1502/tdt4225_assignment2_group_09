from pprint import pprint
from DbConnector import DbConnector

class ExampleProgram:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def fetch_users(self):
        collection = self.db['users']
        documents = collection.find()
        for doc in documents:
            pprint(doc)

def main():
    program = None
    try:
        program = ExampleProgram()
        program.fetch_users()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()

if __name__ == '__main__':
    main()