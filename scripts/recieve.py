import glob
import os
import shutil
import time
import signal
import sys
from base64 import b64decode

from django.db import connections

from app_jra.consts import RABBITMQ_URL
from app_jra_receive.receive_proc import Receive_proc
from app_jra.log_commons import *
# from celery.utils.log import get_task_logger
# logger = get_task_logger("celery.tasks")
from logging import getLogger
logger = getLogger('jra_aws_receive')
Common_log = Common_log(DEBUGLOG_NAME_TO_TYPE[logger.name])

from kombu import Connection, Consumer, Exchange, Queue

# docker-compose exec web bashの中で
# python manage.py runscript recive

'''
参考URL
https://qiita.com/yuuichi-fujioka/items/2e19f7779aed37abc943
Messageを受け取るのがExchange
ExchangeがQueueに渡す
'''

MEDIA_EXCHANGE = Exchange('media', 'direct', durable=True)
VIDEO_QUEUE = Queue('receive_jra', exchange=MEDIA_EXCHANGE, routing_key='video')
TORECEIVEDATA = '/code/app_jra/input/Receive/'
TOTEMPORARYDATA = '/code/app_jra/input/Temporary/'

# 受信側
def process_media(body, message):
    """
    FTPサーバーからの受信ファイル取得、受信処理の大元関数

    RabbitMから受信フォルダ(Receive)へ保存。
    Receive_proc().call_receive_proc(filename)　受信処理のメイン関数へファイル名と共に処理を受け渡しする。
    ポーリングを1秒間隔で行う。

    """
    try:
        filenameData = os.path.normpath(os.path.join(TOTEMPORARYDATA, body.get('filename')))
        logger.debug("filenameData:%s" % filenameData)

        is_binary = os.path.splitext(filenameData)[1] == '.lzh' or os.path.splitext(filenameData)[1] == '.zip'
        if is_binary:
            # 受信側処理でバイナリ保存
            with open(filenameData, mode='bw') as f:
                lzh = body.get('contents') 
                f.write(b64decode(lzh))
        else :
            # 受信処理側でStringからDAT保存
            with open(filenameData, mode='w', encoding=body.get('encoding')) as f:
                f.write(body.get('contents'))

        filename = body.get('filename')
        logger.info(filename)

        # Common_log.Out_Logs(7100, [filename]) # HARMLESS 不要であれば削除
        Common_log.Out_Logs(9100, [filename]) # INFO
        Receive_proc().call_receive_proc(filename)
        # Common_log.Out_Logs(7101, [filename]) # HARMLESS 不要であれば削除
        Common_log.Out_Logs(9101, [filename]) # INFO

        time.sleep(1)

    except Exception as e:
        logger.error(e)

    message.ack()

def run_recive():
    """
    RabbitMQ呼び出し 関数

    ファイル情報をRABBITMQ_URLのキュー(receive_jra)から取得する。
    process_media内でファイル受信と受信処理を行う。
    """
    logger.debug(RABBITMQ_URL)

    try:
        # connections
        with Connection(RABBITMQ_URL) as conn:

            # consume
            with conn.Consumer(VIDEO_QUEUE, callbacks=[process_media]):
                # Process messages and handle events on all channels
                conn.drain_events()
    except:
        logger.error("RabbitMQ コネクションエラー")

    # 複数
    # video_queue = Queue('video', exchange=MEDIA_EXCHANGE, key='video')
    # image_queue = Queue('image', exchange=MEDIA_EXCHANGE, key='image')

    # with Connection.Consumer([video_queue, image_queue], callbacks=[process_media]) as consumer:
    #     while True:
    #         consumer.drain_events()

# 何かしらのプロセスが死ぬ時にシグナルを出力する
def handler(signum, frame):
    """
    シグナルでプロセスダウンキャッチ関数
    何かしらのプロセスが死ぬ時にシグナルをprintする
    Criticalエラーログ出力
    
    Parameters
    ----------
    signum : int
        シグナル番号。
    frame : str
        ファイル名。
    """
    dying_msg = "signal=%s frame=%s" % (signum, frame)
    # print('Error: configuration failed', file=sys.stderr)
    print(dying_msg, file=sys.stderr)
    logger.critical("dying_message %s" % dying_msg)

def run():
    """
    アプリ起動時呼び出し 関数(BCP、開発環境)

    signal.signal(signal.SIGINT, handler) シグナル設定
    受信側のRabbitMQのポーリングを1秒間隔で行う。
    run_recive() RabbitMQ呼び出し
    """
    logger.info("signal handler setting")
    signal.signal(signal.SIGINT, handler)
    
    while True:
        try:
            logger.debug(RABBITMQ_URL)
            run_recive()
            time.sleep(1)
        except OSError as o:
            Common_log.Out_Logs(3100,[o])
            logger.error("ファイル操作エラー：%s" % o)
        except Exception as e:
            Common_log.Out_Logs(3100,[e])
            logger.error("エラー：%s" % e)
        except:
            Common_log.Out_Logs(3100,["キャッチ不可エラー"])
            logger.error("キャッチ不可エラー")
        finally:
            # logger.debug("STG_MODE:%s" % STG_MODE)
            connections.close_all()
            logger.debug("RabbitMQ 受信処理終了")
