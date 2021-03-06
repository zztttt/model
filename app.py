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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('app')

app = Quart(__name__)

servers = ['101.133.161.108:9092']
producer = KafkaProducer(bootstrap_servers='101.133.161.108:9092',
                         key_serializer=lambda k: json.dumps(k).encode(),
                         value_serializer=lambda v: json.dumps(v).encode())
resp = Resp()
tasks = {}
result = None


def pull_msg(topic, instance_logger, model_id):
    instance_logger.info(f"start pull msg from topic: {topic}.")
    consumer = KafkaConsumer(topic, bootstrap_servers=servers, auto_offset_reset='earliest', group_id=str(model_id))
    instance_logger.info(f"connect to kafka success. start pull")
    count = 0
    while True:
        msgs = consumer.poll(timeout_ms=50, max_records=1)
        if msgs:
            instance_logger.info(f"get msg from topic:{topic}")
            # data = None
            for topicPartition in msgs:
                list = msgs[topicPartition]
                msg = list[0]
                data = msg.value
            instance_logger.info(data)
            consumer.close()
            instance_logger.info("end pull msg.")
            return data
        else:
            count = count + 1
            if count == 10:
                instance_logger.info(f"pull data from topic:{topic} time out")
                consumer.close()
                instance_logger.info("end pull msg.")
                return None


def check_model(model_file_name):
    return os.path.exists("./lib/" + model_file_name)


async def execute_model(model_id, kafka_in_topic, kafka_out_topic, in_config_array, out_config_array, model_file_name):
    instance_logger = logging.getLogger(f'instance_logger{model_id}')
    log_handler = logging.FileHandler('./logs/' + str(model_id))
    instance_logger.addHandler(log_handler)
    while True:
        instance_logger.info(f"model_id:{model_id} is scheduling")
        data_str = pull_msg(kafka_in_topic, instance_logger, model_id)
        if data_str is not None:
            instance_logger.info("data_str is not none. start processing.")
            # parse argument
            try:
                instance_logger.info("start parsing argument.")
                data_json = json.loads(data_str)
                args = []
                in_config = in_config_array[0]
                key = in_config['input']['columnDefinition']
                if key not in data_json:
                    instance_logger.exception("datasource error. key:" + key + " is not existing")
                    await asyncio.sleep(0)
                    continue
                value = data_json[key]
                args.append(value)
                instance_logger.info("end parsing.")
            except Exception as e:
                instance_logger.exception("parse argument error")
                await asyncio.sleep(0)
                continue
            # import lib
            try:
                instance_logger.info("start import lib.")
                name = "lib." + model_file_name[:-3]
                instance_logger.info(f"import model_file:{name}")
                metaclass = importlib.import_module(name)
                global result
                result = metaclass.execute(*args)
                instance_logger.info("end execute lib.")
            except Exception as e:
                instance_logger.exception("execute model error. execute next message.")
                await asyncio.sleep(0)
                continue
            # send data
            try:
                instance_logger.info("start building data.")
                out_config = out_config_array[0]
                instance_logger.info("start send result to kafka.")
                column = out_config['output']['columnDefinition']
                data_json[column] = result
                data_str = json.dumps(data_json)
                instance_logger.info("end building.")
            except Exception as e:
                instance_logger.exception("")
                await asyncio.sleep(0)
                continue

            future = producer.send(kafka_out_topic,
                                    key='model',  # 同一个key值，会被送至同一个分区
                                    value=data_str)  # 向分区1发送消息
            try:
                instance_logger.info("start sending data.")
                future.get(timeout=10)  # 监控是否发送成功
                instance_logger.info(f'send message to kafka success. data:{data_str}')
            except kafka_errors as e:  # 发送失败抛出kafka_errors
                instance_logger.exception("send to kafka error")
                await asyncio.sleep(0)
                continue
        else:
            instance_logger.info("data_str is NONE. wait for next event_loop.")
        await asyncio.sleep(5) #switch controller


@app.before_serving
async def startup():
    logger.info("delete logs")
    if not os.path.exists('./logs'):
        os.mkdir('./logs')
    else:
        del_list = os.listdir('./logs')
        for f in del_list:
            file_path = os.path.join('./logs', f)
            if os.path.isfile(file_path):
                os.remove(file_path)
            # elif os.path.isdir(file_path):
            #     shutil.rmtree(file_path)


@app.after_serving
async def shutdown():
    logger.info("shutdown")


@app.route('/')
async def hello():
    return 'hello, zzt1'

# async def task(id):
#     while True:
#         print(f"task:{id}")
#         await asyncio.sleep(2)
#
# @app.route('/task',methods=['POST'])
# async def add_task():
#     request_data = await request.get_data()
#     request_data = str(request_data, "utf-8")
#     json_data = json.loads(request_data)
#     id = json_data['id']
#     t = asyncio.ensure_future(task(id))
#     tasks[id] = t
#     return 'success'
#
# @app.route('/cancel', methods=['POST'])
# async def cancel_task():
#     request_data = await request.get_data()
#     request_data = str(request_data, "utf-8")
#     json_data = json.loads(request_data)
#     id = json_data['id']
#     t = tasks[id]
#     if t == None:
#         return f'no task:{id}'
#     else:
#         t.cancel()
#         return f'stop task:{id}'

@app.route('/startModelInstance', methods=['POST'])
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
        file_name = json_data['model'][0]['modelPath'].split('/')[-1]
        model_id = json_data['id']
    except JSONDecodeError as e:
        return resp.fail(e.msg)
    except Exception as e:
        logger.exception("")
        return resp.fail("json value error")

    # TODO test topic only
    # data_str = pull_msg(kafka_in_topic)
    # kafka_in_topic = "upload_topic"

    if not check_model(file_name):
        try:
            down_file = requests.get(url=model_path)
            with open("lib/" + file_name, "wb") as code:
                code.write(down_file.content)
            logger.info("downloading finish")
        except Exception as e:
            logger.exception("downloading model error")
            return resp.fail("downloading model error. url:" + model_path)

    if model_id in tasks:
        logger.error(f"model_it:{model_id} is already existing")
        return resp.fail(f"model_it:{model_id} is already existing")
    try:
        t = asyncio.ensure_future(execute_model(model_id, kafka_in_topic, kafka_out_topic, in_config_array, out_config_array, file_name))
        tasks[model_id] = t
    except Exception as e:
        logger.exception("")
        return resp.fail("execute model fail. aynicio.ensure_future(...) error.")

    response = await make_response(resp.success(f"model_id:{model_id} file_name:{file_name} is running"))
    response.timeout = None  # No timeout for this route
    return response

@app.route('/stopModelInstance', methods=['Post'])
async def stop():
    request_data = await request.get_data()
    request_data = str(request_data, "utf-8")
    ret = None
    try:
        json_data = json.loads(request_data)
        id = json_data['id']
        if id not in tasks:
            ret = resp.fail(f"stop error. {id} is not running.")
        else:
            task = tasks[id]
            task.cancel()
            tasks.pop(id)
            ret =  resp.success(f"stop model_id:{id} success.")
    except Exception as e:
        logger.exception("")
        ret = resp.fail("stop raise exception.")
    finally:
        response = await make_response(ret)
        response.timeout = None  # No timeout for this route
        return response

@app.route('/execute', methods=['POST'])
async def executeModel():
    request_data = await request.get_data()
    request_data = str(request_data, "utf-8")
    try:
        json_data = json.loads(request_data)

        instanceId = str(json_data['id']) + '_' + json_data['instanceName']
        #modelName = json_data['model'][0]['modelName']
        #modelFileName = json_data['model'][0]['modelFileName']
        modelPath = json_data['model'][0]['modelPath']
        fileName = json_data['model'][0]['modelPath'].split('/')[-1]

    except JSONDecodeError as e:
        print(e)
        return resp.fail(e.msg)
    except Exception as e:
        print(e)
        logger.exception("")
        return resp.fail("json value error")

    if not check_model(fileName):
        try:
            down_file = requests.get(url=modelPath)
            with open("lib/" + fileName, "wb") as code:
                code.write(down_file.content)
            logger.info("downloading finish")
        except Exception as e:
            print(e)
            logger.exception("downloading model error")
            return resp.fail("downloading model error. url:" + modelPath)

    try:
        name = fileName[:-3]
        metaclass = importlib.import_module("lib." + name)
        result = metaclass.execute()
    except TypeError as e:
        print(e)
        logger.exception("model execute() arguments error")
        return resp.fail("model execute() arguments error")
    except Exception as e:
        print(e)
        logger.exception("execute model error")
        return resp.fail("execute model error")

    response = await make_response(resp.success(f"execute model: {fileName}" + result))
    response.timeout = None  # No timeout for this route
    return response

@app.route('/showActiveInstance', methods=['GET'])
async def show_active_instance():
    ret = []
    for k,v in tasks.items():
        ret.append(k)
    response = await make_response(resp.success(json.dumps(ret)))
    response.timeout = None  # No timeout for this route
    return response

if __name__ == '__main__':
    app.run(host='0.0.0.0', port='8000')
