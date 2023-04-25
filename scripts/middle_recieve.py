import glob
import os
import shutil
import time
from celery import Celery, bootsteps

from django.db import connections

from app_jra.consts import RABBITMQ_URL
from app_jra_receive.receive_proc import Receive_proc
from app_jra.log_commons import *

from app_jra_mddb.tasks import subscribe_message

from logging import getLogger
logger = getLogger('jra_edit_delivery')
Common_log = Common_log(DEBUGLOG_NAME_TO_TYPE[logger.name])

# from celery.utils.log import get_task_logger
# logger = get_task_logger("celery.tasks")

from kombu import Connection, Consumer, Exchange, Queue

# docker-compose exec web bashの中で
# python manage.py runscript middle_receive

EXCHANGE = Exchange('foo_exc', 'direct')
QUEUE = Queue('middle_receive_jra', exchange=EXCHANGE, routing_key='middle')

def get_msg(body, message):
    """
    業務処理呼び出し　関数
    subscribe_message(body) 業務処理大元関数へ

    Parameters
    ----------
    body : str
        ファイル名。
    message : str
        空文字。(未使用の為、削除推奨)

    """
    try:
        # Common_log.Out_Logs(7102, [body])
        Common_log.Out_Logs(9102, [body])
        logger.debug("中間受信処理 filename:%s" % body)
        subscribe_message(body)
        # Common_log.Out_Logs(7103, [body])
        Common_log.Out_Logs(9103, [body])
    except Exception as e:
        Common_log.Out_Logs(3100, [e])
        logger.error("エラー:%s" % e)

    message.ack()

def run_middle_recive():
    """
    受信側 中間ファイル名メッセージング　関数
    ファイル名をRABBITMQ_URLのキュー(middle_receive_jra)からファイル名を受け取る。subscribe。
    """
    with Connection(RABBITMQ_URL) as c:
        try:
            with Consumer(c.default_channel, queues=[QUEUE], callbacks=[get_msg]):
                c.drain_events()
        except Exception as e:
            # コネクションエラーを正式ログのエラーとするか検討
            logger.error(e)    

def run():
    """
    アプリ起動時呼び出し 関数

    受信側の中間ファイル名メッセージングのポーリングを1秒間隔で行う。
    run_middle_recive() 受信側 中間ファイル名メッセージング　関数
    connections.close_all() 処理(業務処理、配信処理)毎にコネクションクローズ
    """
    while True:
        logger.debug(RABBITMQ_URL)
        run_middle_recive()
        connections.close_all()
        time.sleep(1)
