#! /usr/bin/env python3
# -*- coding:utf-8 -*-

import argparse
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.client import KafkaClient

# qa kafka brokers
bootstrap_servers = ['115.159.123.178:9092']
bootstrap_servers = ['10.10.51.14:19091','10.10.51.14:19092','10.10.51.14:19093']

class Producer(object):
    '''参考： https://github.com/dpkp/kafka-python'''

    def __init__(self):
        self._producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda v: v.decode('utf-8'))

    def send(self, topic, value, key=None):

        # 暂时写死
        # value = {'openid':'sjh5850b1caf97eec523534058b','groupid':1,'taskid':1,'type':0,'time':1509001503}

        future = self._producer.send(topic, value=value, key=key)
        result = future.get(timeout=60)
        # 写入日志
        print(result)


class Consumer(object):

    @staticmethod
    def checkversion():
        print('brokers: {}'.format(','.join(bootstrap_servers)))
        client = KafkaClient(bootstrap_servers=bootstrap_servers)
        version = client.check_version()
        print('version: {}'.format(version))

    @staticmethod
    def topics():
        print('brokers: {}'.format(','.join(bootstrap_servers)))
        consumerclient = KafkaConsumer(bootstrap_servers=bootstrap_servers)
        topics = consumerclient.topics()
        print(topics)

    @staticmethod
    def info(topic):
        print('brokers: {}'.format(','.join(bootstrap_servers)))
        consumerclient = KafkaConsumer(bootstrap_servers=bootstrap_servers)
        partitions = consumerclient.partitions_for_topic(topic)

        print('topic: {}'.format(topic))
        print('partitions: {}'.format(','.join(str(partition) for partition in partitions)))

        partitioninstances = []
        for partition in partitions:
            partitioninstance = TopicPartition(topic, int(partition))
            partitioninstances.append(partitioninstance)

        beginningoffsets = consumerclient.beginning_offsets(partitioninstances)
        endoffsets = consumerclient.end_offsets(partitioninstances)
        for pi in partitioninstances:
            msg = 'partition: {}, beginning_offset: {}, end_offset: {}'
            msg = msg.format(pi.partition, beginningoffsets[pi], endoffsets[pi])
            print(msg)

    @staticmethod
    def latest(topic):
        print('brokers: {}'.format(','.join(bootstrap_servers)))
        # consumer_timeout_ms: 设置堵塞时间，默认永久堵塞(即consumer一直消费消息，永不停止)；如果设置堵塞时间，则堵塞时间过后consumer就退出不再消费消息
        # bootstrap_servers不支持连接zookeeper
        # auto_offset_reset: 'earliest' will move to the oldest available message, 'latest' will move to the most recent. Default: 'latest'.
        # enable_auto_commit (bool): If True , the consumer's offset will be periodically committed in the background. Default: True.
        consumerclient = KafkaConsumer(client_id='kafka_test_latest',
                                       group_id='kafka_test_latest_group',
                                       bootstrap_servers=bootstrap_servers,
                                       value_deserializer=lambda v: v.decode('utf-8'),
                                       consumer_timeout_ms=30000, # StopIteration if no message after 30sec
                                       auto_offset_reset='latest',
                                       enable_auto_commit=True)
        # 订阅话题
        consumerclient.subscribe(topic)

        for message in consumerclient:
            print(message)

    @staticmethod
    def earliest(topic):
        print('brokers: {}'.format(','.join(bootstrap_servers)))
        # consumer_timeout_ms: 设置堵塞时间，默认永久堵塞(即consumer一直消费消息，永不停止)；如果设置堵塞时间，则堵塞时间过后consumer就退出不再消费消息
        # bootstrap_servers不支持连接zookeeper
        # auto_offset_reset: 'earliest' will move to the oldest available message, 'latest' will move to the most recent. Default: 'latest'.
        # enable_auto_commit (bool): If True , the consumer's offset will be periodically committed in the background. Default: True.
        consumerclient = KafkaConsumer(client_id='kafka_test_earliest',
                                       group_id='kafka_test_earliest_group',
                                       bootstrap_servers=bootstrap_servers,
                                       value_deserializer=lambda v: v.decode('utf-8'),
                                       consumer_timeout_ms=30000, # StopIteration if no message after 30sec
                                       auto_offset_reset='earliest',
                                       enable_auto_commit=False)
        # 订阅话题
        consumerclient.subscribe(topic)

        for message in consumerclient:
            print(message)

    @staticmethod
    def seek(topic, start, end=None, partitions=None):
        print('brokers: {}'.format(','.join(bootstrap_servers)))
        consumer = KafkaConsumer(client_id='kafka_test_seek',
                                 group_id='kafka_test_seek_group',
                                 bootstrap_servers=bootstrap_servers,
                                 consumer_timeout_ms=30000, # StopIteration if no message after 30sec
                                 enable_auto_commit=False)

        partitioninstances = []
        for partition in partitions:
            # TODO TopicPartition的partition一定得是整数，否则会报如下错误：
            #
            # Traceback (most recent call last):
            #   File "/usr/local/lib/python3.5/dist-packages/kafka/protocol/types.py", line 10, in _pack
            #     return pack(f, value)
            # struct.error: required argument is not an integer
            partitioninstance = TopicPartition(topic, int(partition))
            partitioninstances.append(partitioninstance)

        consumer.assign(partitioninstances)

        for partitioninstance in partitioninstances:
            #consumer.seek_to_beginning(partitioninstance)
            consumer.seek(partitioninstance, start)
            # TODO print current offset
            #print(consumer.position(partitioninstance))
            for msg in consumer:
                if end is not None and msg.offset > end:
                    break
                else:
                    print(msg)

def main():
    # 解析参数
    parser = argparse.ArgumentParser()
    parser.add_argument('cls', help='producer or consumer',
                        choices=['producer', 'consumer'])
    parser.add_argument('action', help='Command to be executed',
                        choices=['checkversion', 'topics', 'info', 'latest', 'earliest', 'seek'])
    parser.add_argument('-t', '--topic', help='kafka topic')
    parser.add_argument('-m', '--message', help='kafka producer send message')
    parser.add_argument('-s', '--start', help='the start position in kafka partition')
    parser.add_argument('-e', '--end', help='the end position in kafka partition')
    parser.add_argument('-p', '--partitions', help='the partitions of kafka topic, separated by comma')
    parser.add_argument('-v', '--verbose', action='store_true', help='Output verbosity')
    args = parser.parse_args()
    cls = args.cls
    action = args.action
    topic = args.topic
    message = args.message
    start = args.start
    end = args.end
    partitions = args.partitions
    verbose = args.verbose

    argsfmt = 'cls: {}, action: {}, topic: {}, message: {}, start: {}, end: {}, partitions: {}, verbose: {}'
    print(args, argsfmt.format(cls, action, topic, message, start, end, partitions, verbose))

    # action=topics不需要topic参数
    if action not in ['topics', 'checkversion']:
        if topic is None: return

    if cls == 'producer':
        Producer().send(topic, message)
    elif cls == 'consumer':
        if action == 'checkversion':
            Consumer.checkversion()
        elif action == 'topics':
            Consumer.topics()
        elif action == 'info':
            Consumer.info(topic)
        elif action == 'latest':
            Consumer.latest(topic)
        elif action == 'earliest':
            Consumer.earliest(topic)
        elif action == 'seek':
            start = int(start)
            if end in [None, '', 0, '0']:
                end = None
            else:
                end = int(end)
            if partitions in [None, '']:
                partitions = [0]
            else:
                partitions = partitions.split(',')
            Consumer.seek(topic, start, end=end, partitions=partitions)

if __name__ == '__main__':
    main()
