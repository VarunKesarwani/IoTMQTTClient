import boto3
import time
import json
import datetime
import pandas as pd
from decimal import Decimal
from botocore.exceptions import ClientError
from InsertDataToMongo import Database
import schedule
from boto3.dynamodb.conditions import Key, Attr


# Reading the configuration file
f = open("config.json")
config = json.loads(f.read())
f.close()

upload_toCloud = False


class AlertDataModel:
    def __init__(self, start_time):
        self.dynamodb = boto3.resource('dynamodb')
        self.table_alert_data = self.dynamodb.Table('bsm_alerts')
        self.start_time = start_time

    def get_local_data(self, collection, key, lmit):
        mongoDB = Database()
        cursor = mongoDB.get_single_data(collection, key, lmit)
        return cursor

    def get_cloud_data(self, device_id, startdate):
        devices_dynamodb = boto3.resource('dynamodb')
        devices_table = devices_dynamodb.Table('bsm_agg_data')
        try:
            response = devices_table.scan(
                # Select= 'ALL_ATTRIBUTES',
                FilterExpression=Attr('deviceid').eq(device_id) & Attr(
                    'timeStamp').between(startdate, startdate+datetime.timedelta(minutes=10))
            )
        except ClientError as e:
            print(e.response['Error']['Message'])
        else:
            print(response)
            return response['Item']

    def Upload_Agg_Data(self):
        print('Started schedule for time{}'.format(self.start_time))
        current = self.start_time
        endtime = current + datetime.timedelta(minutes=5)
        while current < endtime:
            Message_temp = []
            Message_spo2 = []
            Message_heart = []
            df = pd.DataFrame()
            if upload_toCloud:
                df =self.get_cloud_data('BSM_G101',self.start_time)
            else:
                filter = {
                    'timestamp': {
                        '$gte': current,
                        '$lt': current+datetime.timedelta(seconds=60)
                    }
                }
                print(filter)
                cursor = self.get_local_data('BSM_Agg', filter, 0)
                df = pd.DataFrame(list(cursor))
            for index, readings in df.iterrows():
                try:
                    # print("Received Message for device and type: {},{}".format(
                    #     readings["deviceid"], readings["datatype"]))

                    config_value = next(
                        x for x in config['devices'] if x["type"] == readings['datatype'])
                    if ((config_value['avg_min'] > int(readings['average'])) or (int(readings['average']) > config_value['avg_max'])):
                        if (readings['datatype'] == 'HeartRate'):
                            Message_heart.append({"deviceid": readings["deviceid"], "timestamp": readings["timestamp"],
                                                  "datatype": readings["datatype"], "average": readings["average"],
                                                  "minimum": readings["minimum"], "maximum": readings["maximum"]})
                            if(len(Message_heart) >= config_value['trigger_count']):
                                print("Abnormality detected in HeartRate starting from time {}".format(
                                    Message_heart[0]['timestamp']))
                                if upload_toCloud:
                                    self.table_alert_data.put_item(
                                        Item=Message_heart[0])
                                else:
                                    mongoDB = Database()
                                    mongoDB.insert_single_data(
                                        'BSM_Alert', Message_heart[0])

                        elif (readings['datatype'] == 'SPO2'):
                            Message_spo2.append({"deviceid": readings["deviceid"], "timestamp": readings["timestamp"],
                                                "datatype": readings["datatype"], "average": readings["average"],
                                                 "minimum": readings["minimum"], "maximum": readings["maximum"]})
                            if(len(Message_spo2) >= config_value['trigger_count']):
                                print("Abnormality detected in SPO2 starting from time {}".format(
                                    Message_spo2[0]['timestamp']))
                                if upload_toCloud:
                                    self.table_alert_data.put_item(
                                        Item=Message_spo2[0])
                                else:
                                    mongoDB = Database()
                                    mongoDB.insert_single_data(
                                        'BSM_Alert', Message_spo2[0])
                        elif (readings['datatype'] == 'Temperature'):
                            Message_temp.append({"deviceid": readings["deviceid"], "timestamp": readings["timestamp"],
                                                "datatype": readings["datatype"], "average": readings["average"],
                                                 "minimum": readings["minimum"], "maximum": readings["maximum"]})
                            if(len(Message_temp) >= config_value['trigger_count']):
                                print("Abnormality detected in Temperature starting from time {}".format(
                                    Message_heart[0]['timestamp']))
                                if upload_toCloud:
                                    self.table_alert_data.put_item(
                                        Item=Message_temp[0])
                                else:
                                    mongoDB = Database()
                                    mongoDB.insert_single_data(
                                        'BSM_Alert', Message_temp[0])
                except BaseException as err:
                    print(err)
            current += datetime.timedelta(seconds=60)
        self.start_time = endtime
        print('Complete for time between {} and {}'.format(self.start_time,endtime))


if __name__ == "__main__":
    mongoDB = Database()
    first = list(mongoDB.get_single_data("BSM_Agg", {}, 1))
    start_date = first[0]['timestamp']
    alert = AlertDataModel(start_date)
    schedule.every(5).minutes.do(alert.Upload_Agg_Data)

    while True:
        schedule.run_pending()
        time.sleep(1)
