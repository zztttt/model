from json.decoder import JSONDecodeError
from quart import Quart, request, make_response
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import kafka_errors
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
producer = KafkaProducer(bootstrap_servers='101.133.161.108:9092',
                         key_serializer=lambda k: json.dumps(k).encode(),
                         value_serializer=lambda v: json.dumps(v).encode())
resp = Resp()
def pull_msg(topic):
    consumer = KafkaConsumer(topic, bootstrap_servers=servers, auto_offset_reset='earliest', group_id="zzt_group3")
    while True:
        msgs = consumer.poll(timeout_ms=50, max_records=1)
        if msgs:
            logger.info("get msg")
            # data = None
            for topicPartition in msgs:
                list = msgs[topicPartition]
                msg = list[0]
                data = msg.value
            logger.info(data)
            consumer.close()
            return data
        print("next loop")


def check_model(model_file_name):
    return os.path.exists("./lib/" + model_file_name)

@app.before_serving
async def startup():
    logger.info("startup")

@app.after_serving
async def shutdown():
    logger.info("shutdown")

@app.route('/')
async def hello():
    return 'hello, zzt1'

@app.route('/execute_model', methods=['POST'])
async def create_model():
    request_data = await request.get_data()
    request_data = str(request_data, "utf-8")
    try:
        json_data = json.loads(request_data)

        in_datasource_array = json_data['inDataSource']
        out_datasource_array = json_data['outDataSource']
        in_datasource = in_datasource_array[0]
        out_datasource = out_datasource_array[0]
        kafka_in_topic = in_datasource['kafkaTopic']
        kafka_out_topic = out_datasource['kafkaTopic']

        in_config_array = json_data['inConfig']
        out_config_array = json_data['outConfig']

        model_array = json_data['model']
        model = model_array[0]
        model_path = model['modelPath']
        in_parameter = model['inParameter']
        model_file_name = model['modelFileName']
    except JSONDecodeError as e:
        return resp.fail(e.msg)
    except Exception as e:
        logger.exception("")
        return resp.fail("json value error")

    # TODO test topic only
    #data_str = pull_msg(kafka_in_topic)
    data_str = pull_msg("upload_topic")

    if data_str == "":
        return resp.fail("there is no request_data in kafka topic:" + kafka_in_topic)
    data_json = json.loads(data_str)

    args = []
    try:
        for in_config in in_config_array:
            key = in_config['input']['columnDefinition']
            if key not in data_json:
                return resp.fail("datasource error. key:" + key + " is not existing")
            value = data_json[key]
            #value = test_json[key]
            args.append(value)
    except Exception as e:
        logger.exception("parse argument error")
        return resp.fail("")

    if not check_model(model_file_name):
        try:
            down_file = requests.get(url=model_path)
            with open("lib/" + model_file_name, "wb") as code:
                code.write(down_file.content)
            logger.info("downloading finish")
        except Exception as e:
            logger.exception("downloading model error")
            return resp.fail("downloading model error. url:" + model_path)

    try:
        name = model_file_name[:-3]
        metaclass = importlib.import_module("lib." + name)
        result = metaclass.execute(*args)
    except Exception as e:
        logger.exception("execute model error")
        return resp.fail("model execute error")

    out_config = out_config_array[0]
    data = {}
    try:
        column = out_config['output']['columnDefinition']
        data[column] = result
        data_str = json.dumps(data)
    except Exception as e:
        logger.exception("")
        return resp.fail("build out request_data error.")

    future = producer.send(
        kafka_out_topic,
        key='model',  # 同一个key值，会被送至同一个分区
        value=data_str)  # 向分区1发送消息
    try:
        future.get(timeout=10)  # 监控是否发送成功
        logger.info(f'send message to kafka. data:{data_str}')
    except kafka_errors:  # 发送失败抛出kafka_errors
        logger.exception("")
        return resp.fail(str(kafka_errors))

    response = await make_response(resp.success(data_str))
    response.timeout = None  # No timeout for this route
    return response

if __name__ == '__main__':
    app.run(host='0.0.0.0', port='8000')
