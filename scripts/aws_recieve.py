import os
import boto3
import time
import signal
import sys
from logging import getLogger

from django.db import connections

from app_jra.consts import *
from app_jra_receive.receive_proc import Receive_proc
from app_jra.log_commons import *

logger = getLogger('jra_aws_receive')
Common_log = Common_log(DEBUGLOG_NAME_TO_TYPE[logger.name])

TORECEIVEDATA = '/code/app_jra/input/Receive/'
TOTEMPORARYDATA = '/code/app_jra/input/Temporary/'

class AWS_func():
    """
    受信側 SQS＋S3メッセージング　関数
    ファイル名をSQSから取得し、取得したファイル名のファイルをS3から受信フォルダ(Receive)へダウンロード。
    ダウンロードしたファイルを受信処理していく。
    
    Attributes
    ----------
    __NAME : str
        SQSキュー名、S3バケット名。共通。
    __KEY_ID : str
        AWSアクセスキー。
    __SECRET_ACCESS_KEY : str
        AWSシークレットキー。
    __REGION : str
        AWSリージョン。ap-northeast-1
    __DIR : str
        S3内のフォルダディレクトリPath。
    s3 : S3.ServiceResource
        S3操作用リソース。バケット取得とダウンロード操作で使用。
    s3_cl : S3.Client
        S3操作用クライアント。S3削除処理で使用。
    sqs : SQS.Client
        SQS操作用クライアント。SQS取得、削除処理で使用。
    bucket : S3.Bucket
        S3バケット。
    queue_url : str
        SQSのメッセージ格納URL。
    running : bool
        無限ポーリング許可。default True
    path : str
        S3内のファイルPath。
    ReceiptHandle : str
        SQSのキューから受信したメッセージを一意とする暗号。ユニークでSQSメッセージ削除等に使用。
    filename : str
        受信ファイル名。

    """

    def __init__(self):
        """
        初期化関数
        S3、SQSの操作に必要な情報を格納しておく。
        """
        self.__NAME = SQS_ENDPOINT_RECEIVE # キューの名前とバケット名で共通予定
        self.__KEY_ID = getattr(settings, "AWS_ACCESS_KEY_ID", None)
        self.__SECRET_ACCESS_KEY = getattr(settings, "AWS_SECRET_ACCESS_KEY", None)
        self.__REGION = 'ap-northeast-1'
        self.__DIR = 'ftp/'
        self.s3 = self.Init_resource('s3')
        self.s3_cl = self.Init_client('s3')
        self.sqs = self.Init_client_fifo('sqs')
        self.bucket = self.s3.Bucket(self.__NAME) # S3のバケットを取得
        self.queue_url = SQS_QUE_URL + self.__NAME + ".fifo"
        self.running = True
        self.path = ""
        self.ReceiptHandle = ""
        self.filename = ""

    def Get_File(self):
        """
        S3、SQS処理の中間関数
        TODO 本来はここが中間関数になっていましたが、現在は1関数になっているので直接self.Get_filename_by_SQS()を呼ぶ方が良いかもしれません。
        """
        # logger.info("AWS SQS+S3 開始")
        # SQSのメッセージでS3の保存したfile名を取得
        self.Get_filename_by_SQS()
        # logger.info("AWS SQS+S3 終了")
        # 配信用NewsMLをS3へ保存
        # self.Download_S3(filename)

    def Get_filename_by_SQS(self):
        """
        S3、SQS処理関数

        Notes
        -------
        self.sqs.receive_message()で毎回の最終のキューを取得し直す。可視性タイムアウト5分、Fifo(FirstInFirstOut)
        SQS取得情報にMessagesが無ければSQSにキューが無い状態なので、返却。1秒後に再度ポーリングする。
        SQS取得情報にMessagesがあれば次へ

        Messagesの中に格納されているBodyにファイル名、ReceiptHandleにユニークな暗号が格納されているので、
        それぞれself.filename、self.ReceiptHandleに格納、両方存在すれば次へ
        どちらかでも存在しなければself.Delete_AWS()で該当ファイルのS3とSQSを削除し、返却。1秒後に再度ポーリングする。

        self.Download_S3(filename) filenameのS3を受信フォルダ(Receive)へダウンロードする。
        Receive_proc().call_receive_proc(filename)　受信処理のメイン関数へファイル名と共に処理を受け渡しする。
        エラーが起きても起きなくても、self.Delete_AWS()で該当ファイルのS3とSQSを削除し、返却。1秒後に再度ポーリングする。

        """
        # logger.info("Get_filename_by_SQS 開始")
        # logger.info(self.queue_url)
        res = self.sqs.receive_message(
            QueueUrl=self.queue_url,
            # 可視性タイムアウト 最大5分に設定(キューを受け取ってから非表示になるまでに時間)
            VisibilityTimeout=300
        )
        # logger.info("Get_filename_by_SQS 終了")

        if 'Messages' not in res:
            # logger.info("データ出力:%s" % res)
            self.running = True
            return
        logger.info("SQS 取得成功")

        for contents in res['Messages']:
            self.ReceiptHandle = ""
            try:
                filename = contents['Body']
                self.filename = filename ## 可視性タイムアウトエラー用にfilename保存
                self.ReceiptHandle = contents['ReceiptHandle']
                if filename and self.ReceiptHandle:
                    logger.debug("S3取得 ダウンロード")
                    # 配信用NewsMLをS3からダウンロード
                    self.Download_S3(filename)
                    # 同期処理で受信処理を呼び出す
                    logger.debug("受信処理開始 関数呼び出し")
                    try:
                        # Common_log.Out_Logs(7100, [filename]) # HARMLESS 不要であれば削除
                        Common_log.Out_Logs(9100, [filename]) # INFO
                        Receive_proc().call_receive_proc(filename)
                        # Common_log.Out_Logs(7101, [filename]) # HARMLESS 不要であれば削除
                        Common_log.Out_Logs(9101, [filename]) # INFO
                    except:
                        msg = "編集失敗 ファイル名:%s" % filename
                        Common_log.Out_Logs(3100,[msg])
                        logger.debug(msg)
                        self.running = True
                        return
                    # メッセージの削除
                    # logger.info("S3削除")
                    # self.sqs.delete_message(QueueUrl=self.queue_url, ReceiptHandle=ReceiptHandle)
                # else:
                #     self.sqs.delete_message(QueueUrl=self.queue_url, ReceiptHandle=ReceiptHandle)
            finally:
                logger.debug("SQS 削除")
                self.Delete_AWS()
                # self.sqs.delete_message(QueueUrl=self.queue_url, ReceiptHandle=ReceiptHandle)
        logger.info("SQS ループ処理終了")

        self.running = True

    def Download_S3(self, filename):
        """
        S3からファイルダウンロードする関数
        S3から受信フォルダ(Receive)へ保存。

        Parameters
        ----------
        filename : str
            ファイル名。
        """
        # logger.info("Download_S3 開始")
        self.path = "%s%s" % (self.__DIR, filename)
        res = self.bucket.download_file("%s%s" % (self.__DIR, filename), "%s%s" % (TOTEMPORARYDATA, filename)) # S3から直接指定フォルダへ保存
        logger.info("S3:%s" % res)
    
    def Delete_AWS(self):
        """
        該当ファイルのS3とSQSを削除する関数
        
        SQSを削除し、可視性タイムアウトなどで失敗した場合は、再度ReceiptHandleを取得した後(ReceiptHandleが使用不可状態の為)に削除を行う。
        S3の該当ファイル削除を行う。
        """
        # logger.info("sqs:%s" % self.ReceiptHandle)
        try:
            self.sqs.delete_message(QueueUrl=self.queue_url, ReceiptHandle=self.ReceiptHandle)
        except Exception as e:
            # 可視性タイムアウトが発生した時に、ReceiptHandleが変更される対策
            ## 前回取得ファイル名と同じファイル名を取得されたらSQSから再削除する
            Common_log.Out_Logs(3100,[e])
            logger.error("エラー：%s" % e)

            res = self.sqs.receive_message(
                QueueUrl=self.queue_url,
                VisibilityTimeout=300
            )

            for contents in res['Messages']:
                filename = contents['Body']
                if self.filename == filename:
                    ReceiptHandle = contents['ReceiptHandle']
                    self.sqs.delete_message(QueueUrl=self.queue_url, ReceiptHandle=ReceiptHandle)

        # S3 削除
        self.s3_cl.delete_object(Bucket=self.__NAME, Key=self.path)

    def Init_client_fifo(self, aws_tool):
        """
        AWSクライアントツール(SQSfifo専用)を初期設定する関数
        
        Parameters
        ----------
        aws_tool : str
            対象操作ツール名。(sqs)
        """
        return boto3.client(
            aws_tool, 
            aws_access_key_id=self.__KEY_ID,
            aws_secret_access_key=self.__SECRET_ACCESS_KEY,
            region_name=self.__REGION,
            endpoint_url = ENDPOINT_URL            
        )

    def Init_client(self, aws_tool):
        """
        AWSクライアントツールを初期設定する関数
        
        Parameters
        ----------
        aws_tool : str
            対象操作ツール名。(sqs, s3)
        """
        return boto3.client(
            aws_tool,
            aws_access_key_id=self.__KEY_ID,
            aws_secret_access_key=self.__SECRET_ACCESS_KEY,
            region_name=self.__REGION
        )

    def Init_resource(self, aws_tool):
        """
        AWSリソースツールを初期設定する関数
        
        Parameters
        ----------
        aws_tool : str
            対象操作ツール名。(sqs, s3)
        """
        return boto3.resource(
            aws_tool, 
            aws_access_key_id=self.__KEY_ID,
            aws_secret_access_key=self.__SECRET_ACCESS_KEY,
            region_name=self.__REGION
        )

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
    print(dying_msg, file=sys.stderr)
    logger.critical("dying_message %s" % dying_msg)

def run():
    """
    アプリ起動時呼び出し 関数(AWS 本番、ステージング)

    signal.signal(signal.SIGINT, handler) シグナル設定
    受信側のSQS＋S3のポーリングを1秒間隔で行う。
    Af.Get_File() 受信処理大元関数
    """
    Af = AWS_func()
    logger.info(ENDPOINT_URL)
    logger.info("AWS開始")
    # 1秒毎にSQSにアクセスする

    # シグナルを設定  
    logger.info("signal handler setting")  
    signal.signal(signal.SIGINT, handler)
    while Af.running:
        Af.running = False
        try:
            Af.Get_File()
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
            connections.close_all()
            logger.debug("SQS 受信処理終了")
            Af.running = True

        time.sleep(1)
