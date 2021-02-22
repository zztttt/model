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
import requests
import os

from resp import Resp

logging.basicConfig(level = logging.INFO,format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
app = Quart(__name__)

servers = ['101.133.161.108:9092']
consumer = KafkaConsumer('zztin', bootstrap_servers=servers)
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

def pull_msg(topic):
    consumer = KafkaConsumer(topic, bootstrap_servers=servers)
    msgs = consumer.poll(1000, 1)
    if msgs:
        logger.info("get msg")
        data = None
        for topicPartition in msgs:
            data = msgs[topicPartition]
            msg = data[0]
            data = msg.value
        logger.info("get data: %", data)
        return data
    else:
        logger.info("no msg")
        return ""

def check_model(model_file_name):
    return os.path.exists("./lib/" + model_file_name)

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

@app.route('/execute_model', methods=['POST'])
async def create_model():
    data = await request.get_data()
    data = str(data, "utf-8")
    try:
        json_data = json.loads(data)

        in_datasource_array = json_data['inDataSource']
        in_datasource = in_datasource_array[0]
        kafka_topic = in_datasource['kafkaTopic']

        in_config_array = json_data['inConfig']

        model_array = json_data['model']
        #for model_json in model_array:
        model = model_array[0]
        model_path = model['modelPath']
        in_parameter = model['inParameter']
        model_file_name = model['modelFileName']
        model_name = model['modelName']
        #logger.info("model_file_name:" + model_file_name)

    except JSONDecodeError as e:
        return resp.fail(e.msg)
    except Exception as e:
        logger.exception("")
        return resp.fail("json value error")

    data_str = pull_msg(kafka_topic)

    # if data == "":
    #     return resp.fail("there is no data in kafka topic:" + kafka_topic)
    #data_json = json.loads(data_str)

    test_json = {"fileId": "data1"}
    args = []
    try:
        for in_config in in_config_array:
            key = in_config['input']['columnDefinition']
            #value = data_json[key]
            value = test_json[key]
            args.append(value)
    except Exception as e:
        logger.exception("parse argument error")
        return resp.fail(e.msg)

    if check_model(model_file_name) == False:
        try:
            down_file = requests.get(url=model_path)
            with open("lib/" + model_file_name, "wb") as code:
                code.write(down_file.content)
            logger.info("downloading finish")
        except Exception as e:
            logger.exception("downloading model error")
            return resp.fail("downloading model error. url:" + model_path)

    try:
        metaclass = importlib.import_module(model_file_name)
        result = metaclass.execute(*args)
    except Exception as e:
        logger.exception("execute model error")
        return resp.fail("model execute error")

    future = producer.send(
        'zztout',
        key='model',  # 同一个key值，会被送至同一个分区
        value=str(result))  # 向分区1发送消息
    logger.info("RESULT: {}".format(result))
    try:
        future.get(timeout=10)  # 监控是否发送成功
        logger.info("send mesage to kafka")
    except kafka_errors:  # 发送失败抛出kafka_errors
        logger.exception("")
        return resp.fail(str(kafka_errors))

    response = await make_response(resp.success())
    response.timeout = None  # No timeout for this route
    return response

def execute(*args):
    for arg in args:
        print("arg in *args:", arg)

#logger.info(data)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port='8000')
