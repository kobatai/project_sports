import re
import sys
from logging import getLogger
from app_jra.consts import *
from app_jra.models import *
from app_jra.log_commons import *
from datetime import datetime as dt
from datetime import timedelta

logger = getLogger('jra_edit_delivery')
Common_log = Common_log(DEBUGLOG_NAME_TO_TYPE[logger.name])
log_err_msg_id = 3300
log_info_msg_id = 9399

try:
    from app_jra_receive.receive_commons import Common
except Exception as e:
    Common_log.Out_Logs(log_err_msg_id, [e])
    logger.error(f'commonsファイル読み込み失敗 : {e}')

def failure(e):
    exc_type, exc_obj, tb=sys.exc_info()
    lineno=tb.tb_lineno
    return str(lineno) + ":" + str(type(e)) + str(e)

class Trn_MdDBStatus():

    def update_Trn_MdDBStatus(self, datfilename, datDataFileFlg, status, error_mddb_code_list=[]):
        # 中間DB登録状況テーブルを、受信ファイル名から更新する処理

        # 戻り値初期値
        mddb_updated = False

        try:
            # 受信ファイル種別から、更新対象のmddbオブジェクトのリストを取得
            # 中間DBステータス更新対象の中間DBコードマスタ
            update_mddb_code_obj_list = Mst_Receive_File.objects.get(Receive_file_code=datDataFileFlg).mddb_code_key.all()
            if update_mddb_code_obj_list:
                for mddb_code_obj in update_mddb_code_obj_list:
                    
                    if mddb_code_obj.MdDB_code in error_mddb_code_list:
                        update_status = MDDBSTATUSUNFINISHED # エラー発生時は問答無用で未完とする
                    else:
                        update_status = status
                    # datfilenameが登録されているTran_ReceiveStatusから、場・開催日・レース番号を取得する
                    receive_data_list = Tran_ReceiveStatus.objects.filter(Receive_filename=datfilename)
                    for receive_data in receive_data_list:
                        kaisaibi = receive_data.Kaisai_date if receive_data.Kaisai_date else None
                        jou_obj = receive_data.Jou_code if receive_data.Jou_code else None
                        rebangou = receive_data.Race_bangou if receive_data.Race_bangou else None

                        # 完了に変更する場合
                        if update_status == MDDBSTATUSCREATED:
                            # 必要な受信ファイルが全て受信済みとなっていることを確認する。
                            # receive_obj_list : 該当の中間DB(mddb_code_obj)を完了にするのに必要な受信ファイルのオブジェクトのリスト
                            receive_obj_list = mddb_code_obj.Key_Receive_File.all()

                            # 該当の中間DB(mddb_code_obj)を完了にするのに必要な受信ファイルが全て受信済みとなっていることを確認。なければ未完にする。
                            # M10/M11の場合、Key_Receive_Fileは「I204翌日出馬表」「I904当日出馬表」だが、いずれかを受信していればいい
                            if mddb_code_obj.MdDB_code in ["M10","M11"]:
                                receivestatus_list = Tran_ReceiveStatus.objects.filter(
                                    Kaisai_date=kaisaibi,
                                    Jou_code=jou_obj,
                                    Race_bangou=rebangou,
                                    Receive_code__in=receive_obj_list,
                                    Receive_status = 1, # 1:受信済み
                                    Mddb_registered_flg = True # 中間DB登録済み
                                    )
                                if not receivestatus_list.exists():
                                    update_status = MDDBSTATUSUNFINISHED
                                    break
                            # M130の場合、Key_Receive_Fileは「K191重勝発売要項」があれば良い
                            elif mddb_code_obj.MdDB_code  == "M130":
                                # K191重勝発売要項 
                                receivestatus_list = Tran_ReceiveStatus.objects.filter(
                                    Kaisai_date=kaisaibi,
                                    Receive_code__Receive_file_code=JUSYOU_HATSUBAI_YOUKOU,
                                    Receive_status = 1, # 1:受信済み
                                    Mddb_registered_flg = True # 中間DB登録済み
                                    )
                                if not receivestatus_list.exists():
                                    update_status = MDDBSTATUSUNFINISHED
                                    break
                            else:
                                for receive_obj in receive_obj_list:
                                    receivestatus_list = Tran_ReceiveStatus.objects.filter(
                                        Kaisai_date=kaisaibi,
                                        Jou_code=jou_obj,
                                        Race_bangou=rebangou,
                                        Receive_code=receive_obj,
                                        Receive_status = 1, # 1:受信済み
                                        Mddb_registered_flg = True # 中間DB登録済み
                                        )
                                    if not receivestatus_list.exists():
                                        update_status = MDDBSTATUSUNFINISHED
                                        break

                        # 該当するmddbstatusをupdateする
                        tran_mddbstatus_list = Tran_MdDBStatus.objects.filter(
                            Kaisai_date = kaisaibi,
                            Md_db_code = mddb_code_obj,
                            Jou_code = jou_obj if not mddb_code_obj.Unit_type == 3 else None,
                            Race_bangou = rebangou if mddb_code_obj.Unit_type == 1 else None,
                        )
                        if tran_mddbstatus_list.exists():
                            for tran_mddbstatus in tran_mddbstatus_list:
                                # DB登録ステータスをupdateする。
                                tran_mddbstatus.Md_db_status = update_status
                                tran_mddbstatus.save()
                        else:
                            # 新規登録
                            Tran_MdDBStatus.objects.create(
                                Kaisai_date = kaisaibi,
                                Md_db_code = mddb_code_obj,
                                Jou_code = jou_obj if not mddb_code_obj.Unit_type == 3 else None,
                                Race_bangou = rebangou if mddb_code_obj.Unit_type == 1 else None,
                                Md_db_status = update_status,
                            )
                    mddb_updated = True
            else:
                mddb_updated = 0
            
            return mddb_updated

        except Exception as e:
            Common_log.Out_Logs(log_err_msg_id, [failure(e)])
            logger.error(failure(e))
            return ABNORMAL
