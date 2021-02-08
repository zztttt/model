from json.decoder import JSONDecodeError

from quart import Quart, request, make_response, render_template
import queue
import time
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import kafka_errors
import traceback
import importlib
import json
import asyncio
import logging

from resp import Resp

logging.basicConfig(level = logging.INFO,format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
app = Quart(__name__)

consumer = KafkaConsumer('zztin', bootstrap_servers=['101.133.161.108:9092'])
producer = KafkaProducer(bootstrap_servers='101.133.161.108:9092',
                         key_serializer=lambda k: json.dumps(k).encode(),
                         value_serializer=lambda v: json.dumps(v).encode())
topic = "zztout"
resp = Resp()

async def pull_msg_from_kafka(consumer):
    logger.info("pull data")
    while True:
        msgs = consumer.poll(1000, 1)
        if msgs:
            logger.info("get msg")
            data = None
            for topicPartition in msgs:
                data = msgs[topicPartition]
                msg = data[0]
                data = msg.value
                #msg_queue.put(data[0].value)
            logger.info("get data: %", data)
        else:
            logger.info("no msg")
        await asyncio.sleep(1)

@app.before_serving
async def startup():
    logger.info("startup")
    #asyncio.gather(pull_msg_from_kafka(consumer))

@app.after_serving
async def shutdown():
    logger.info("shutdown")

@app.route('/')
async def hello():
    return 'hello, zzt1'


async def check_model(model_name):
    return True
# {
#     "moduleInstanceId": 1,
#     "moduleInstanceName": "instanceName",
#     "type": "online",
#     "model": {
#         "modelId": 2,
#         "name": "moduleName",
#         "filePath": "filePath",
#         "input": [
#             {"paramName": "arg1", "paramType": "string"},
#             {"paramName": "arg2", "paramType": "int"}
#         ]
#     },
#     "inputMapping": {
#         "dataSourceId": {
#             "id": "dataSourceId",
#             "name": "dataSourceName",
#             "topic": "dataSourceTopic",
#             "server": "server",
#             "config": [
#                 {"columnName": "col1", "columnType": "columnType", "columnDesc": "columnDesc", "columnDefinition": "columnDefinition"},
#                 {"columnName": "col2", "columnType": "columnType", "columnDesc": "columnDesc", "columnDefinition": "columnDefinition"}
#             ],
#             "mapping": [
#                 {"modelInput": "param1", "dataSourceInput": "dataSourceInput"},
#                 {"modelInput": "param2", "dataSourceInput": "dataSourceInput"}
#             ]
#         }
#     }
# }
@app.route('/execute_model', methods=['POST'])
async def create_model():
    data = await request.get_data()
    data = str(data, "utf-8")
    try:
        json_data = json.loads(data)
        json_model = json_data['model']
        model_id = json_model['modelId']
        model_name = json_model['name']
        file_path = json_model['filePath']
        input = json_model['input']

        json_input_mapping = json_data['inputMapping']
        config = json_input_mapping['dataSourceId']['config']
        mapping = json_input_mapping['dataSourceId']['mapping']
    except JSONDecodeError:
        return resp.fail("json decode error")
    except BaseException:
        return resp.fail("json value error")

    args = [arg['modelInput'] for arg in mapping]
    if check_model(model_name):
        logger.info("model is existing")
        try:
            metaclass = importlib.import_module(model_name)
            result = metaclass.execute(*args)
        except BaseException:
            return resp.fail("model execute fail")

        future = producer.send(
            'zztout',
            key='model',  # 同一个key值，会被送至同一个分区
            value=str(result))  # 向分区1发送消息
        logger.info("RESULT: {}".format(result))
        try:
            future.get(timeout=10)  # 监控是否发送成功
            logger.info("send mesage to kafka")
        except kafka_errors:  # 发送失败抛出kafka_errors
            traceback.format_exc()
            return resp.fail(str(kafka_errors))
    else:
        logger.info("model is not existing")

    response = await make_response(resp.success())
    response.timeout = None  # No timeout for this route
    return response

def execute(*args):
    for arg in args:
        print("arg in *args:", arg)

#logger.info(data)

if __name__ == '__main__':
    app.run()
