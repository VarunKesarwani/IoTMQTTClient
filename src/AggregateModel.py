import boto3
import time
import json
import datetime
import pandas as pd
from decimal import Decimal


class AggregateModel:
    def __init__(self):
        self.dynamodb = boto3.resource('dynamodb')
        self.table_agg_data = self.dynamodb.Table('bsm_agg_data')
        self.client = boto3.client('kinesis')
        self.shardIterator = self.client.get_shard_iterator(
            StreamName='BSMStream1',
            ShardId='shardId-000000000000',
            ShardIteratorType='LATEST',
        )['ShardIterator']

    def Upload_Agg_Data(self):
        Message = []
        while True:
            response = self.client.get_records(
                ShardIterator=self.shardIterator
            )
            shardIterator = response['NextShardIterator']
            if len(response['Records']) > 0:
                for item in response['Records']:
                    try:
                        readings = json.loads(item["Data"])

                        print("Received Message for device and type: {},{}".format(
                            readings["deviceid"], readings["datatype"]))

                        date = readings['timestamp'].split(" ")
                        date[-1] = date[-1][:5]
                        date = date[0]+" "+date[1]
                        d = datetime.datetime.strptime(date, '%Y-%m-%d %H:%M')

                        if len(Message) == 0 or d.minute == Message[0]['timestamp'].minute:
                            Message.append(
                                {"deviceid": readings["deviceid"], "timestamp": d, "datatype": readings["datatype"], "value": readings["value"]})
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
                            columns_filter = [
                                True, True, True, True, False, False, False, True, False, False, False, True]
                            bsm_dt = union.loc[:, columns_filter].copy()
                            print("Uploading data for time: {}".format(
                                bsm_dt['timestamp'][0]))

                            for index, row in bsm_dt.iterrows():
                                item = (row.to_dict())
                                item['timestamp'] = str(item['timestamp'])
                                s1 = json.dumps(item)
                                data = json.loads(s1, parse_float=Decimal)
                                self.table_agg_data .put_item(Item=data)

                            print("Completed loading aggregate data to cloud")
                            Message.clear()
                            Message.append(
                                {"deviceid": readings["deviceid"], "timestamp": d, "datatype": readings["datatype"], "value": readings["value"]})
                    except BaseException as err:
                        print(err)
            else:
                print('No Record Found')
            time.sleep(0.5)


if __name__ == "__main__":
    agg_data = AggregateModel()
    agg_data.Upload_Agg_Data()
