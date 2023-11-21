import random
import time

from kafka import KafkaProducer


TOPIC_NAME = 'MyPriceData'


def main():

    producer = KafkaProducer(
        bootstrap_servers=[
            'localhost:9090',
            'localhost:9091',
            'localhost:9092'],
        key_serializer=lambda s: str(s).encode(),
        value_serializer=lambda s: str(s).encode())

    i_cnt = 0
    while True:

        i_cnt += 1

        time.sleep(.1 * random.randint(5, 15))

        market_name = 'Binance:BTCUSDT'
        close = float(100000 + i_cnt)

        print('\nSending msg: key={} value={}'.format(market_name, close))
        producer.send(TOPIC_NAME, close, market_name)

        if i_cnt > 10:
            print('Breaking out')
            break

    producer.close()

    print('\nDone\n')


if __name__ == '__main__':
    fut_list = main()
    exit(0)
