from pprint import pprint
from DbConnector import DbConnector
from haversine import haversine
class ExampleProgram:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def fetch_user(self, user_id,transport_mode):
        collection = self.db['activities']
        documents = collection.aggregate([
            {"$match":{ "user_id": user_id, "transportation_mode": transport_mode}},
            {"$project": {"user_id":1}},
            {"$lookup": {
                "from": "trackpoints",
                "localField": "_id",
                "foreignField": "activity_id",
                "as": "tpoints"
            }},
            {"$project": {
              'tpoints': {
                'lat': 1,
                'lon': 1
              }
            }}
        ])
        # for doc in documents:
        #     pprint(doc)
        return documents

def main():
    program = None
    try:
        program = ExampleProgram()
        activities = program.fetch_user("112","walk")
        totalDistance = 0
        for activity in activities:
            act = list(activity.values())
            length_tp = len(act[1])-1
            # act[1] has the track points for the activity
            # get latitude and longitude values for the first track point
            startCoord = act[1][0]
            # get latitude and longitude values for the first track point
            endCoord = act[1][length_tp]
            startsAt = (startCoord['lat'],startCoord['lon'])
            endsAt = (endCoord['lat'], endCoord['lon'])
            # calculating the distance for each activity
            distance = haversine(startsAt,endsAt)
            totalDistance += distance
        print("Total distance walked by the user 112: ", totalDistance)
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()

if __name__ == '__main__':
    main()