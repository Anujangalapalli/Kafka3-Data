from kafka import KafkaConsumer, TopicPartition
from json import loads
import os

from kafka.errors import KafkaError
from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

username = os.getenv('MYSQL_user')
pswd = os.getenv('MYSQL_pw')
engine = create_engine('mysql+mysqlconnector://' + username + ":" + pswd + '@localhost/bank')
Session = sessionmaker()
Session.configure()
Base = declarative_base(bind=engine)


class Transaction(Base):
    __tablename__ = 'transaction'
    # Here we define columns for the table person
    # Notice that each column is also a normal Python instance attribute.
    id = Column(Integer, primary_key=True)
    branchid = Column(Integer)
    custid = Column(Integer)
    type = Column(String(250), nullable=False)
    date = Column(Integer)
    amt = Column(Integer)


class XactionConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer('bank-customer-events',
                                      bootstrap_servers=['localhost:9092'],
                                      # auto_offset_reset='earliest',
                                      value_deserializer=lambda m: loads(m.decode('ascii')))

        # self.consumer.assign([TopicPartition('bank-customer-events', partition=1)])
        try:
            self.consumer.assign([TopicPartition('bank-customer-events', partition=1)])
            result = True
        except KafkaError as exc:
            print("Exception during assiging partitions - {}".format(exc))
            result = False
        # return result
        # These are two python dictionarys
        # Ledger is the one where all the transaction get posted
        self.ledger = {}
        # custBalances is the one where the current blance of each customer
        # account is kept.
        self.custBalances = {}
        # THE PROBLEM is every time we re-run the Consumer, ALL our customer
        # data gets lost!
        # add a way to connect to your database here.
        # Go back to the readme.

    def handleMessages(self):

        for message in self.consumer:
            message = message.value
            print('{} received'.format(message))
            self.ledger[message['custid']] = message
            # add message to the transaction table in your SQL usinf SQLalchemy
            message_to_sqltable = Transaction(custid=message['custid'], branchid = message['branchid'], type = message['type'], date=message['date'], amt=message['amt'])
            if message['custid'] not in self.custBalances:
                self.custBalances[message['custid']] = 0
            if message['type'] == 'dep':
                self.custBalances[message['custid']] += message['amt']
            else:
                self.custBalances[message['custid']] -= message['amt']
            print(self.custBalances)
            Session = sessionmaker()
            session = Session()
            session.add(message_to_sqltable)
            session.commit()



if __name__ == "__main__":
    c = XactionConsumer()
    c.handleMessages()
