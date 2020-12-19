from kafka import KafkaConsumer, TopicPartition
from json import loads
import os
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


class Customer(Base):
    __tablename__ = 'customer'
    # Here we define columns for the table person
    # Notice that each column is also a normal Python instance attribute.
    custid = Column(Integer, primary_key=True)
    createdate = Column(Integer)
    fname = Column(String(250), nullable=False)
    lname = Column(String(250), nullable=False)

class XactionConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer('bank-customer-new',
                                      bootstrap_servers=['localhost:9092'],
                                      # auto_offset_reset='earliest',
                                      value_deserializer=lambda m: loads(m.decode('ascii')))

        self.customer = {}

    def handleMessages(self):
        for message in self.consumer:
            message = message.value
            print('{} received'.format(message))
            self.customer[message['custid']] = message
            # add message to the transaction table in your SQL usinf SQLalchemy
            message_to_sqltable = Customer(custid=message['custid'],
                                           createdate = message['createdate'],
                                           fname=message['fname'],
                                           lname=message['lname'])
            Session = sessionmaker()
            session = Session()
            session.add(message_to_sqltable)
            session.commit()



if __name__ == "__main__":
    c = XactionConsumer()
    c.handleMessages()
