# -*- coding: utf-8 -*-
"""
Created on Tue Jan 05 11:07:12 2016

@author: Jonathan Wang
"""

import time
from datetime import datetime
import socket
import logging
from pykafka import KafkaClient
from petrel import storm
from petrel.emitter import Spout


log = logging.getLogger('ExpSpout')  # set logger


class ExpSpout(Spout):
    """
    此spout目的為: 讀取檔案, 並且將每行內容傳給下個bolt
    """

    def __init__(self):
        """
        assign None to members
        """
        super(ExpSpout, self).__init__(script=__file__)
        self.conf = None
        self.client = None
        self.topic = None
        self.consumer = None
        self.counter = 0
        self.emit_thread = None
        self.message_pool = {}

        self.start_time = 0
        self.end_time = 0

    def initialize(self, conf, context):
        """
        Storm calls this function when a task for this component starts up.
        :param conf: topology.yaml內的設定
        :param context:
        開啟kafka連線
        """
        log.debug("ExpSpout initialize start")
        self.conf = conf
        self.client = KafkaClient(hosts=conf["ExpSpout.initialize.hosts"])
        self.topic = self.client.topics[str(conf["ExpSpout.initialize.topics"])]
        timestamp_now = time.time()
        datetime_now = datetime.fromtimestamp(timestamp_now).strftime("%Y%m%d%H%M")
        self.consumer = self.topic.get_balanced_consumer(
            consumer_group=str(conf["ExpSpout.initialize.consumer_group"]) + datetime_now,
            zookeeper_connect=str(conf["ExpSpout.initialize.zookeeper"]),
            consumer_timeout_ms=int(conf["ExpSpout.initialize.consumer_timeout_ms"]),
            auto_commit_enable=True)

        log.debug("ExpSpout initialize done")

    @classmethod
    def declareOutputFields(cls):
        """
        定義emit欄位(設定tuple group條件用)
        """
        return ['line']

    def ack(self, msg_id):
        """
        delete the message cache in spout, log if the tuple is a re-emit one
        :param msg_id: id of ack tuple
        """
        # log.warning("ack of message #{0}".format(msg_id))
        del self.message_pool[msg_id]
        if msg_id[:5] == "fail_":
            log.warning("ack of message #{0}".format(msg_id))

    def fail(self, msg_id):
        """
        emit message again with id which is composed with a prefix and the failed tuple.id
        :param msg_id: id of failed tuple
        """
        # log.warning("fail of message #{0}".format(msg_id))
        fail_id = "fail_{0}".format(msg_id)
        fail_message = self.message_pool[msg_id]
        self.message_pool[fail_id] = fail_message
        storm.emit([fail_message], id=fail_id)  # emit message again
        del self.message_pool[msg_id]

    def nextTuple(self):
        """
        consume message from kafka
        messages (m) are named tuples with attributes:
        m.offset: message offset on topic-partition log (int)
        m.value: message (output of deserializer_class - default is raw bytes)
        """
        if self.consumer is None:
            log.debug("self.consumer is not ready yet.")
            return

        if self.counter >= 1000000:
            return

        # log.debug("ExpSpout.nextTuple()")
        # time.sleep(3)  # prototype減速觀察
        try:
            # message = self.consumer.consume()
            for message in self.consumer:
                if message is not None:
                    # log.warning("offset: %s \t value: %s \t at %s", message.offset, message.value, time.time())
                    if self.counter == 0:
                        self.start_time = time.time()
                        log.warning("start process 1000000 records at {0} (timestamp@{1})".format(time.time(),
                                                                                                  socket.gethostname()))
                    msg_id = str(self.counter)
                    self.message_pool[msg_id] = message.value  # message cache for fail over
                    storm.emit([message.value], id=msg_id)
                    self.counter += 1
                    if self.counter % 10000 == 0:
                        log.warning("mark @ #{0}".format(self.counter))
                if self.counter == 1000000:  # mark time
                    self.end_time = time.time()
                    log.warning("finish process 1000000 records at {0} (timestamp@{1})".format(time.time(),
                                                                                               socket.gethostname()))
                    log.warning("spend {0} seconds processing 1000000 records".format(self.end_time - self.start_time))
                if self.counter % 100 == 0:
                    break
        except Exception as inst:
            log.debug("Exception Type: %s ; Args: %s", type(inst), inst.args)


def run():
    """
    給petrel呼叫用, 為module function, 非class function
    """
    ExpSpout().run()
