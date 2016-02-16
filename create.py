# -*- coding: utf-8 -*-
"""
Created on Tue Jan 05 15:36:59 2016

@author: Jonathan Wang
"""

import exp_spout
import split_bolt
import output_bolt


def create(builder):
    """
    設定topology的spout & bolt 關係
    """
    
    spout_id = "spout"
    split_bolt_id = "split_row"
    output_bolt_id = "output"
    
    builder.setSpout(spout_id, exp_spout.ExpSpout(), 1)  # 建立spout
    builder.setBolt(split_bolt_id, split_bolt.SplitBolt(), 3) \
        .shuffleGrouping(spout_id)  # 建立bolt, 設定tuple(亂數)
    builder.setBolt(output_bolt_id, output_bolt.OutputBolt(), 12) \
        .shuffleGrouping(split_bolt_id)  # 建立bolt, 設定tuple(亂數)
