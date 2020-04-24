from kafka import KafkaConsumer, TopicPartition
from json import loads
import sqlalchemy
import statistics

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

        self.totdeposit = 0
        self.depcounter = 0
        self.totwthdrawl = 0
        self.wthcounter = 0
        self.avgdep = 0
        self.avgwth = 0
        self.deplst = []
        self.wthlst = []


    def handleMessages(self):
        stddevdep = 0
        stddevwth = 0
        for message in self.consumer:
            message = message.value
            print('{} received'.format(message))
            self.ledger[message['custid']] = message

            if message['custid'] not in self.custBalances:
                self.custBalances[message['custid']] = 0
            if message['type'] == 'dep':
                self.custBalances[message['custid']] += message['amt']

                self.totdeposit += message['amt']
                self.depcounter += 1
                self.avgdep = round(self.totdeposit / self.depcounter, 2)
                self.deplst.append(message['amt'])
                if len(self.deplst) > 1:
                    stddevdep = round(statistics.stdev(self.deplst), 2)
            else:
                self.custBalances[message['custid']] -= message['amt']
                self.totwthdrawl += message['amt']
                self.wthcounter += 1
                self.avgwth = round(self.totwthdrawl / self.wthcounter, 2)
                self.wthlst.append(message['amt'])
                if len(self.wthlst) > 1:
                    stddevwth = round(statistics.stdev(self.wthlst), 2)

            print(self.custBalances)
            print(' average deposit       =  ', self.avgdep, '\n', 'average withdrawl     =  ', self.avgwth, '\n'
                  ' std dev of deposits   =  ', stddevdep, '\n', 'std dev of withdrawls =  ',stddevwth)





if __name__ == "__main__":
    c = XactionConsumer()
    c.handleMessages()