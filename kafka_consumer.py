from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import kafka_errors
import traceback
import importlib
import json

topic_in = "ds_msg_gdtpdfresult"
#topic_in = "upload_topic"
topic = "zztout"
consumer = KafkaConsumer(topic_in, bootstrap_servers=['101.133.161.108:9092'], auto_offset_reset='earliest', group_id="zzt_test5")
producer = KafkaProducer(bootstrap_servers='101.133.161.108:9092',
                         key_serializer=lambda k: json.dumps(k).encode(),
                         value_serializer=lambda v: json.dumps(v).encode())  # 连接kafka


if __name__ == '__main__':
    while True:
        msgs = consumer.poll(timeout_ms=50, max_records=1)
        if msgs:
            print("get msg")
            # data = None
            for topicPartition in msgs:
                list = msgs[topicPartition]
                msg = list[0]
                data = msg.value
            print(data)
            consumer.close()
            break
        print("next loop")
    # for msg in consumer:
    #     recv = "%s:%d:%d: key=%s value=%s" % (msg.topic, msg.partition, msg.offset, msg.key, msg.value)
    #     print(recv)
    #     #if msg.key == b'"model"':
    #     model_name = "lib.test"
    #     model_arg = str(msg.value, "utf-8")
    #     metaclass = importlib.import_module(model_name)
    #     result = metaclass.execute(model_arg)
    #
    #     future = producer.send(
    #         'zztout',
    #         key='model',  # 同一个key值，会被送至同一个分区
    #         value=str(result))  # 向分区1发送消息
    #     print("RESULT: {}".format(result))
    #     try:
    #         future.get(timeout=10)  # 监控是否发送成功
    #     except kafka_errors:  # 发送失败抛出kafka_errors
    #         traceback.format_exc()