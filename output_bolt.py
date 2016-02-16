# -*- coding: utf-8 -*-
"""
Created on Wed Jan 06 10:48:48 2016

@author: Jonathan Wang
"""
import time
import socket
import logging
from pymongo import MongoClient
from petrel import storm
from petrel.emitter import BasicBolt

log = logging.getLogger('OutputBolt')  # set logger


class OutputBolt(BasicBolt):
    """
    此bolt的目的為: 將接收到的msisdn與uplink & downlink累計結果寫入mongodb
    """

    split_freq_secs = 60

    def __init__(self):
        """
        assign None to member
        """
        super(OutputBolt, self).__init__(script=__file__)
        self.conf = None
        self.client = None
        self.db = None
        self.collection = None
        self.counter = 0

    def initialize(self, conf, context):
        """
        Storm calls this function when a task for this component starts up.
        :param conf: topology.yaml內的設定
        :param context:
        開啟mongodb連線
        """
        log.debug("OutputBolt initialize start")
        self.conf = conf
        self.client = MongoClient(str(conf["OutputBolt.initialize.host"]), int(conf["OutputBolt.initialize.port"]))
        self.db = self.client[str(conf["OutputBolt.initialize.db"])]
        self.collection = self.db[str(conf["OutputBolt.initialize.collection"])]
        log.debug("OutputBolt initialize done")

    @classmethod
    def declareOutputFields(self):
        """
        定義emit欄位(設定tuple group條件用)
        """
        return ['msisdn']

    def process(self, tup):
        """
        將接收到的tuple寫入mongodb
        若為tick tuple, 則log紀錄
        """
        if self.collection is None:
            log.debug("self.collection is not ready yet.")
            return
        if tup.is_tick_tuple():
            log.debug("tuple is tick")
        else:
            doc = {"msisdn": tup.values[0],
                   "rec_opening_time": tup.values[1],
                   "uplink": tup.values[2],
                   "downlink": tup.values[3]
                   }
            object_id = self.collection.insert_one(doc).inserted_id

    def getComponentConfiguration(self):
        """
        設定此bolt參數, 可控制tick tuple頻率
        """
        return {"topology.tick.tuple.freq.secs": self.split_freq_secs}


def run():
    """
    給petrel呼叫用, 為module function, 非class function
    """
    OutputBolt().run()
