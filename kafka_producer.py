from kafka import KafkaProducer
from kafka.errors import kafka_errors
import json
import traceback

producer = KafkaProducer(bootstrap_servers='101.133.161.108:9092',
                         key_serializer=lambda k: json.dumps(k).encode(),
                         value_serializer=lambda v: json.dumps(v).encode())  # 连接kafka

msg = "Hello World".encode('utf-8')  # 发送内容,必须是bytes类型
for i in range(3):
    model = "model" + str(i)
    future = producer.send(
        'zztin',
        key='model',  # 同一个key值，会被送至同一个分区
        value=model)  # 向分区1发送消息
    print("send {}".format(model))
    try:
        future.get(timeout=10)  # 监控是否发送成功
    except kafka_errors:  # 发送失败抛出kafka_errors
        traceback.format_exc()

#producer.send('zztin', key="model", value=msg, partition=1)  # 发送的topic为test
producer.close()