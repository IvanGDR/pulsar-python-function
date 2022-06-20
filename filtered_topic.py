#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May 18 16:52:13 2022

@author: ivan.garciadelrisco
"""
import pulsar
from pulsar import Function
from abc import abstractmethod
# My own FilterFunction taking messages from a topic filtering them and sending messages
# to another topic
class FilterFunction(Function):
    def __init__(self):
        self.filtered_topic = "persistent://public/default/filtered_topic"

    @staticmethod
    def is_off(item):
        if 'Off' in item:
            return item
          
    @abstractmethod
    def process(self, input, context):
        if self.is_off(input):
            context.publish(self.filtered_topic, input)
