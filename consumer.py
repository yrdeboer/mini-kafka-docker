import pymysql
import pprint
import time
import os
import logging

from kafka import KafkaConsumer


TOPIC_NAME = 'MyPriceData'


logger = logging.getLogger(__name__)
logging.basicConfig()
logger.setLevel(logging.INFO)


def get_db_conn():

    """
    Attempts to setup a MySQL DB connection using
    environment variables from the docker link.

    In case there was an exception, it is assumed
    that the mysql DB is just not ready yet (but will
    be soon-ish).

    Returns the pymysql DB connection object on success.
    """

    db_conn = None

    while True:
        try:
            logger.info('Attempting to connect to DB')
            args = {
                'host': os.environ.get('MYSQL_PORT_3306_TCP_ADDR'),
                'user': os.environ.get('MYSQL_ENV_MYSQL_USER'),
                'passwd': os.environ.get('MYSQL_ENV_MYSQL_PASSWORD'),
                'db': 'items'
            }
            logger.debug('DB args:\n{}'.format(args))
            db_conn = pymysql.connect(** args)
            logger.info('Got DB connection ')
            break

        except Exception:
            logger.warning(
                'Connect DB: got exception. Retry in 5 secs')
            time.sleep(5)

    return db_conn


def split_and_execute(cur, sql):

    """
    This utility function splits a string of
    possibly lots of SQL statements and
    executes them one by one on the cursor.

    May re-raise on odd exceptions (check logs).

    Returns nothing.
    """

    for s in sql.split(';'):

        s = s.strip()

        if s == '':
            continue

        if s == ';':
            continue

        try:
            cur.execute(s)
        except pymysql.err.InternalError as ex:

            if len(ex.args) > 0:
                errno = ex.args[0]
                if errno == 1065:

                    logger.info(
                        'Got empty SQL error. '
                        'Ignoring. sql={}'.format(s))

                    continue

            logger.error(
                'Got internal error, which was not an '
                'empty SQL error (which we would have '
                'ignored). Or the exception has not args. '
                'Re-raising. type(ex)={} ex={}'.format(
                    type(ex),
                    ex))

            raise


def execute_tenaciously_sql(sql, db_conn, max_attempt_count=None):
    """
    This function attempts to execute one or more SQL
    queries. Logs exceptions but may re-raise.

    Args:
      sql:                  A string containing a sql query.
      db_conn:              A DB connection through which
                            to execute the SQL query.
      max_attempt_count:    Will not attempt to execute the
                            query more times than this number.

    Returns:
      A tuple containing a tuple per row, resulting
      from the query.
      If no rows were retrieved, the tuple will be empty.

      None on some error, check the logs in that case.
    """

    result = None
    attempt_count = 0

    while True:

        attempt_count += 1

        if max_attempt_count is not None:
            if max_attempt_count > 0:
                if attempt_count > max_attempt_count:

                    logger.info(
                        'Max attempts reached {}/{}, '
                        'fail and bail.'.format(
                            attempt_count,
                            max_attempt_count))

                    break

        try:
            with db_conn.cursor() as cur:
                split_and_execute(cur, sql)
                result = cur.fetchall()
                db_conn.commit()
                break

        except Exception as ex:
            logger.error('Got exception: {}. Reraising'.format(ex))
            raise

    return result


def init_price_data_table(db_conn):

    """
    Will create a price data table. It may not yet
    exist as no persistent storage may be used
    used (yet).
    """

    sql = 'CREATE TABLE '
    sql += '`price_data` (`id` bigint not null auto_increment, '
    sql += '`market_name` varchar(45) ,  `close` double, '
    sql += 'primary key (`id`) );'
    try:
        execute_tenaciously_sql(sql, db_conn)
    except Exception as ex:
        logger.warning(
            'Got exception creating price data table. '
            'Assuming it already exists. ex={}'.format(ex))

    logger.info('Created DB table for price data')


def deserialise_key(key):

    """
    Deserialises the key for a record, which is bytes,
    into a string. The key is the market name
    and it is further processed as a string type.

    Args:
      key:   Bytes, the market name

    Returns:
      String, the decoded (from bytes) key.
    """

    logger.debug('key: {}'.format(key))
    return key.decode()


def deserialise_value(value):

    """
    Deserialises the value for a record,
    which is bytes, into a float number.
    The value is the last
    price for the market (the key).

    Args:
      value:   Bytes, the last price for a market

    Returns:
      float, the decoded (from bytes) key.
    """

    logger.debug('value: {}'.format(value))
    return float(value)


def consume(db_conn):

    """
    This function sets up a Kafka consumer, using
    environment variables (as this is supposed to run
    in a Docker container).

    It inserts the received price updates into
    a MySQL database.

    It will exit after some fixed number of polls.

    Args:
      db_conn:   pymysql connection to a MySQL database.

    Returns:
      Nothing.
    """

    host_ip = os.environ.get('HOST_IP')
    bootstrap_servers = '{}:9090'.format(host_ip)
    logger.info('bootstrap_servers={}'.format(bootstrap_servers))

    consumer = KafkaConsumer(
        TOPIC_NAME,
        group_id='MyConsumerGroup1',
        bootstrap_servers=bootstrap_servers,
        key_deserializer=deserialise_key,
        value_deserializer=deserialise_value)

    consumer_id = 1
    logger.info('{} Waiting ... '.format(consumer_id))

    i_cnt = 0
    max_cnt = 20
    while True:

        i_cnt += 1
        logger.info('{} Polling ({}/{}) ... '.format(
            consumer_id,
            i_cnt,
            max_cnt))

        ret = consumer.poll(timeout_ms=5000)
        logger.debug('{} Done polling, got:\n{}'.format(
            consumer_id,
            pprint.pformat(ret)))

        for record_list in ret.values():
            for record in record_list:
                sql = 'INSERT INTO price_data (market_name, close) VALUES '
                sql += '("{}", {});'.format(record.key, record.value)
                logger.debug(sql)
                execute_tenaciously_sql(sql, db_conn)
                logger.info(
                    'Inserted in DB: '
                    'market_name={} price={}'.format(
                        record.key,
                        record.value))

        consumer.commit()

        if i_cnt >= max_cnt:
            logger.info('Nuff, breaking out')
            break


def main():

    db_conn = get_db_conn()
    init_price_data_table(db_conn)

    sql = 'select * from price_data;'
    result = execute_tenaciously_sql(sql, db_conn)
    logger.info('Before consuming, price count now: {}'.format(len(result)))

    consume(db_conn)

    result = execute_tenaciously_sql(sql, db_conn)
    logger.info('After consuming, price count now: {}'.format(len(result)))

    logger.info('Price data in DB now:\n{}'.format(
        pprint.pformat(result)))


if __name__ == '__main__':
    main()
    exit(0)
