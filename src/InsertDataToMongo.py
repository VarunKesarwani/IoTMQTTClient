'''
This file is created to insert device data to mongo database from where aggregate function would be called,
since Kinesis service did not work from me.
'''
import datetime
import json
import random
import time
import sched
from pymongo import MongoClient


class Database:
    HOST = '127.0.0.1'
    PORT = '27017'
    DB_NAME = 'Aws_Device_db'

    def __init__(self):
        self._db_conn = MongoClient(
            f'mongodb://{Database.HOST}:{Database.PORT}')
        self._db = self._db_conn[Database.DB_NAME]

    def get_single_data(self, collection, key, limit):
        filter = key
        sort = list({
            'timestamp': 1
        }.items())
        limit = limit
        db_collection = self._db[collection]
        document = db_collection.find(filter=filter,
                                      sort=sort,
                                      limit=limit)
        return document

    def get_multi_data(self, collection):
        db_collection = self._db[collection]
        document = db_collection.find()
        return document

    # This method inserts the data in a new document. It assumes that any uniqueness check is done by the caller
    def insert_single_data(self, collection, data):
        db_collection = self._db[collection]
        document = db_collection.insert_one(data)
        # return document.inserted_id

    def insert_multi_data(self, collection, data):
        records = json.loads(data).values()
        db_collection = self._db[collection]
        db_collection.insert_many(records)


# Publish to the same topic in a loop forever
loopCount = 0

PublishFreqHeartRate = 1
PublishFreqTemperature = 15
PublishFreqOxygen = 10
scheduler = sched.scheduler(time.time, time.sleep)

now = time.time()

A = Database()


def publishBedSideMonitorData_1(loopCount):
    message = {}
    message['deviceid'] = 'BSM_G101'
    try:
        if loopCount % PublishFreqTemperature == 0:
            value = float(random.normalvariate(99, 1.5))
            value = round(value, 1)
            timestamp = str(datetime.datetime.now())
            message['timestamp'] = timestamp
            message['datatype'] = 'Temperature'
            message['value'] = value
            A.insert_single_data("BSM", message)

        if loopCount % PublishFreqOxygen == 0:
            value = int(random.normalvariate(90, 3.0))
            timestamp = str(datetime.datetime.now())
            message['timestamp'] = timestamp
            message['datatype'] = 'SPO2'
            message['value'] = value
            A.insert_single_data("BSM", message)

        if loopCount % PublishFreqHeartRate == 0:
            value = int(random.normalvariate(85, 12))
            timestamp = str(datetime.datetime.now())
            message['timestamp'] = timestamp
            message['datatype'] = 'HeartRate'
            message['value'] = value
            A.insert_single_data("BSM", message)

    except BaseException as err:
        print(err)
        print("---Error---")


def publishBedSideMonitorData_2(loopCount):
    message = {}
    message['deviceid'] = 'BSM_G102'
    try:
        if loopCount % PublishFreqTemperature == 0:
            value = float(random.normalvariate(99, 1.5))
            value = round(value, 1)
            timestamp = str(datetime.datetime.now())
            message['timestamp'] = timestamp
            message['datatype'] = 'Temperature'
            message['value'] = value
            A.insert_single_data("BSM", message)

        if loopCount % PublishFreqOxygen == 0:
            value = int(random.normalvariate(90, 3.0))
            timestamp = str(datetime.datetime.now())
            message['timestamp'] = timestamp
            message['datatype'] = 'SPO2'
            message['value'] = value
            A.insert_single_data("BSM", message)

        if loopCount % PublishFreqHeartRate == 0:
            value = int(random.normalvariate(85, 12))
            timestamp = str(datetime.datetime.now())
            message['timestamp'] = timestamp
            message['datatype'] = 'HeartRate'
            message['value'] = value
            A.insert_single_data("BSM", message)

    except BaseException as err:
        print(err)
        print("---Error---")


if __name__ == "__main__":
    while True:
        try:
            scheduler.enterabs(
                now+loopCount, 1, publishBedSideMonitorData_1, (loopCount,))
            scheduler.enterabs(
                now+loopCount, 1, publishBedSideMonitorData_2, (loopCount,))
            loopCount += 1
            scheduler.run()
        except KeyboardInterrupt:
            break
