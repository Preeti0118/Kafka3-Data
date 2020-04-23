from kafka import KafkaConsumer, TopicPartition
from json import loads
import sqlalchemy


class XactionConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer('bank-customer-events',
            bootstrap_servers=['localhost:9092'],
            # auto_offset_reset='earliest',
            value_deserializer=lambda m: loads(m.decode('ascii')))
        ## These are two python dictionarys
        # Ledger is the one where all the transaction get posted
        self.ledger = {}
        # custBalances is the one where the current blance of each customer
        # account is kept.
        self.custBalances = {}
        # THE PROBLEM is every time we re-run the Consumer, ALL our customer
        # data gets lost!
        # add a way to connect to your database here.
        ##Create connection with MySql database.
        self.mysql_engine = sqlalchemy.create_engine('mysql+pymysql://root:yourpassword@localhost:3306/zipbank')
        self.conn = self.mysql_engine.connect()
        #Go back to the readme.

    def handleMessages(self):
        for message in self.consumer:
            message = message.value
            print('{} received'.format(message))
            self.ledger[message['custid']] = message

            ### mycode insertion ###
            #convert message values into a tuple and insert into the tabke using .execute command
            myvalues = list(message.values())
            myvalues = tuple(myvalues)
            self.conn.execute("INSERT INTO transaction VALUES (%s,%s,%s,%s)", myvalues)
            ### end of my code ###

            # add message to the transaction table in your SQL usinf SQLalchemy
            if message['custid'] not in self.custBalances:
                self.custBalances[message['custid']] = 0
            if message['type'] == 'dep':
                self.custBalances[message['custid']] += message['amt']
            else:
                self.custBalances[message['custid']] -= message['amt']
            print(self.custBalances)

if __name__ == "__main__":
    c = XactionConsumer()
    c.handleMessages()