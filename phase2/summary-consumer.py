from kafka import KafkaConsumer, TopicPartition
from json import loads
from statistics import *


class XactionConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer('bank-customer-events',
                                      bootstrap_servers=['localhost:9092'],
                                      # auto_offset_reset='earliest',
                                      value_deserializer=lambda m: loads(m.decode('ascii')))
        # These are two python dictionarys
        # Ledger is the one where all the transaction get posted
        self.ledger = {}
        # custBalances is the one where the current blance of each customer
        # account is kept.
        self.custBalances = {}
        self.dep_total = 0
        self.dep_counter = 0
        self.dep_mean = 0
        self.dep_list = []
        self.wth_total = 0
        self.wth_counter = 0
        self.wth_mean = 0
        self.wth_list = []
        # THE PROBLEM is every time we re-run the Consumer, ALL our customer
        # data gets lost!
        # add a way to connect to your database here.
        # Go back to the readme.

    def handleMessages(self):
        dep_stdev = 0
        wth_stdev = 0
        for message in self.consumer:
            message = message.value
            print('{} received'.format(message))

            self.ledger[message['custid']] = message

            if message['custid'] not in self.custBalances:
                self.custBalances[message['custid']] = 0

            if message['type'] == 'dep':
                self.dep_list.append(message['amt'])

            if message['type'] == 'wth':
                self.wth_list.append(message['amt'])

            if len(self.dep_list) > 1:
                self.mean_dep = round(mean(self.dep_list), 2)
                self.dep_stdev = round(stdev(self.dep_list), 2)

            else:
                self.custBalances[message['custid']] -= message['amt']

                self.wth_list.append(message['amt'])

            if len(self.wth_list) > 1:
                self.mean_dep = round(mean(self.dep_list), 2)
                wth_stdev = round(stdev(self.wth_list), 2)

            print(self.custBalances)
            print('dep_avg:', self.dep_mean, 'dep_stdev:', dep_stdev, 'wth_avg:', self.wth_mean, 'wth_stdev:', wth_stdev)

if __name__ == "__main__":
    c = XactionConsumer()
    c.handleMessages()
