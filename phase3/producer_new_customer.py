from time import sleep
from json import dumps
from kafka import KafkaProducer
import time
import random


class Producer:
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                                      value_serializer=lambda m: dumps(m).encode('ascii'))

    def emit(self, cust=55, type="dep"):
        data = {'custid': random.randint(50, 56),
                'createdate': int(time.time()),
                'fname': random.choice(['Jenny', 'Karley', 'John', 'Trip']),
                'lname': random.choice(['Smith', 'Johnson', 'Williams', 'Jones']),
                }
        return data

    def depOrWth(self):
        return 'dep' if (random.randint(0, 2) == 0) else 'wth'

    def generateRandomXactions(self, n=1000):
        for _ in range(n):
            data = self.emit()
            print('sent', data)
            self.producer.send('bank-customer-new', value=data)
            sleep(1)


if __name__ == "__main__":
    p = Producer()
    p.generateRandomXactions(n=20)
