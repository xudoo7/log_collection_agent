#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pyinotify
import os
import time
import re
from kafka import KafkaProducer

#PATH = './2.logs'

class EventHandler(pyinotify.ProcessEvent):
    def __init__(self, **kwargs):
        super(EventHandler, self).__init__(**kwargs)
        #self.client_obj = kwargs.get('call')
        self.filepath = kwargs.get('filepath')
        self.file_fd = self.process_IN_FD()
        self.position = 0
        self.mark_time = self.process_IN_MARK()
        self.print_lines()

    def process_IN_MARK(self):
        return time.time().__str__().split('.')[1]

    def process_IN_FD(self):
        return open(self.filepath, 'r')

    def process_IN_MODIFY(self, event):
        self.print_lines()

    def process_IN_RE(self, num):
        #return re.compile(r'\[.*-(\d+)\].*-\s(\w+\s\w+:)')
        re_dict = {
            0 : "re.compile(r'\[.*-(\d+)\].*-\s(Service Begin:)')",
            1 : "re.compile(r'^\s+')",
        }
        return eval(re_dict.get(num))

    def print_lines(self):
        last_n = os.stat(self.filepath)[6]
        if last_n > self.position:
            pass
        else:
            self.position = 0
        while last_n>self.position:
            new_lines = self.file_fd.readline()
            self.position += 1
            if new_lines:
                if self.process_IN_RE(0).search(new_lines):
                    self.mark_time = self.process_IN_MARK()
                    print('{0} ssc:{1}'.format(new_lines.strip(), self.mark_time.strip()))
                    #self.client_obj.send(
                    #                     '{0} ssc:{1}'.format(new_lines.strip(), self.mark_time.strip())
                    #                     )
                elif self.process_IN_RE(1).search(new_lines):
                    print('  {0}'.format(new_lines.strip()))
                    #self.client_obj.send('  {0}'.format(new_lines.strip()))
                else:
                    print('{0} ssc:{1}'.format(new_lines.strip(), self.mark_time.strip()))
                    # self.client_obj.send(
                    #                     '{0} ssc:{1}'.format(new_lines.strip(), self.mark_time.strip())
                    #                     )
        self.file_fd.seek(self.position)


class KafkaClient(object):
    def __init__(self, host, port, topic):
        self.host = host
        self.port = port
        self.topic = topic
        self.client_obj = self.connect()

    def connect(self):
        return KafkaProducer(bootstrap_servers=['{kafka_host}:{kafka_port}'.format(
            kafka_host = self.host,
            kafka_port = self.port
        )])

    def send(self, message):
        #respone = self.client_obj.send(topic, message,encode('utf-8'))
        return self.client_obj.send(self.topic, message.encode('utf-8'))


class FileInotify(object):
    def __init__(self):
        self.wm = None
        self.mask = None
        self.notifier = None
        self.__init_option()

    def __init_option(self):
        self.wm = pyinotify.WatchManager()
        self.mask = pyinotify.IN_MODIFY

    def start(self, filename):
        handler = EventHandler(filepath=filename)
        self.notifier = pyinotify.Notifier(self.wm, handler)
        self.wm.add_watch(filename, self.mask, rec=True)
        self.notifier.loop()


if __name__ == "__main__":
    file = './2.logs'
    b = FileInotify()
    b.start(file)


#host = None
#port = None
#topic = None
#client = KafkaClient(host, port, topic)
#wm = pyinotify.WatchManager()
#handler = EventHandler(filepath='./2.logs', call=client)
#handler = EventHandler(filepath='./2.logs')
#notifier = pyinotify.Notifier(wm, handler)
#wm.add_watch(PATH, pyinotify.IN_MODIFY, rec=True)
#notifier.loop()
