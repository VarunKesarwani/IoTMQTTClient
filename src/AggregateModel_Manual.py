'''
This file is created to manually insert aggregate data from mongo database to dynamo database,
since Kinesis service did not work from me.
'''
from decimal import Decimal
import json
import pandas as pd
from InsertDataToMongo import Database
import datetime
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr

mongoDB = Database()
Message = []
upload_toCloud = False

agg_dynamodb = boto3.resource('dynamodb')
agg_table = agg_dynamodb.Table('bsm_agg_data')


def get_device(device_id, dynamodb=None):
    raw_dynamodb = boto3.resource('dynamodb')
    raw_dynamodb = raw_dynamodb.Table('bsm_raw_data')

    try:
        response = raw_dynamodb.scan(
            #Select= 'ALL_ATTRIBUTES',
            FilterExpression=Attr('deviceid').eq(device_id)
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print(response)
        return response['Item']


cursor = mongoDB.get_multi_data("BSM")

#cursor = get_device('BSM_G101',1000)

df = pd.DataFrame(list(cursor))

# Stimulating Streaming Data
for index, readings in df.iterrows():
    try:
        print("Received Message for device and type: {},{}".format(
            readings["deviceid"], readings["datatype"]))

        date = readings['timestamp'].split(" ")
        date[-1] = date[-1][:5]
        date = date[0]+" "+date[1]
        d = datetime.datetime.strptime(date, '%Y-%m-%d %H:%M')

        if len(Message) == 0 or d.minute == Message[0]['timestamp'].minute:
            Message.append({"deviceid": readings["deviceid"], "timestamp": d,
                           "datatype": readings["datatype"], "value": readings["value"]})
        else:
            df_data = pd.DataFrame(Message)

            average_column = pd.DataFrame({'average': df_data.groupby(
                ['deviceid', 'datatype', 'timestamp'])['value'].mean()}).reset_index()
            minimum_column = pd.DataFrame({'minimum': df_data.groupby(
                ['deviceid', 'datatype', 'timestamp'])['value'].min()}).reset_index()
            maximum_column = pd.DataFrame({'maximum': df_data.groupby(
                ['deviceid', 'datatype', 'timestamp'])['value'].max()}).reset_index()

            union = pd.concat(
                [average_column, minimum_column, maximum_column], axis=1)
            columns_filter = [True, True, True, True, False,
                              False, False, True, False, False, False, True]
            bsm_dt = union.loc[:, columns_filter].copy()
            print("Uploading data for time: {}".format(bsm_dt['timestamp'][0]))

            for index, row in bsm_dt.iterrows():
                item = (row.to_dict())
                if upload_toCloud:
                    item['timestamp'] = str(item['timestamp'])
                    s1 = json.dumps(item)
                    data = json.loads(s1, parse_float=Decimal)
                    agg_table.put_item(Item=data)
                else:
                    mongoDB.insert_single_data('BSM_Agg', item)

            print("Completed uploading aggregate data to cloud")
            Message.clear()
            Message.append({"deviceid": readings["deviceid"], "timestamp": d,
                           "datatype": readings["datatype"], "value": readings["value"]})
    except BaseException as err:
        print(err)
