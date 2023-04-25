from logging import getLogger
import re
import mojimoji
from app_jra.consts import *
import sys
import linecache
from app_jra.log_commons import *
from datetime import datetime as dt
from datetime import timedelta


logger = getLogger('jra_edit_delivery')
Common_log = Common_log(DEBUGLOG_NAME_TO_TYPE[logger.name])
log_err_msg_id = 3400

try:
    from app_jra_mddb.mddb_commons import Common
except Exception as e:
    Common_log.Out_Logs(log_err_msg_id, [e])
    logger.error(f'commonsファイル読み込み失敗 : {e}')


def failure(e):
    exc_type, exc_obj, tb=sys.exc_info()
    lineno=tb.tb_lineno
    return str(lineno) + ":" + str(type(e))


class Trn_M160_shuryo():

    def insert_or_update_M160_shuryo(self, datfilename, datDataFileFlg):
        try:
            from app_jra.models import (
                Trn_N090_Syuuryou_tsuuchi,
                M160_shuuryo
            )
            Cmn = Common()

            # 受信ファイルごとに、M160レコードを新規作成・更新していく。
            # 正しく登録完了した場合、後続の待ち合わせ・配信処理で使うために、更新した中間DBのレコードを保持しておき、リストにして返す。
            # 登録失敗時には、False(ABNOMAL or 空のリスト)を返す。
            edit_mddb_list = [] # 初期値

            if SYURYOUTSUCHI == datDataFileFlg:
                # N090 終了通知
                N090_list = Trn_N090_Syuuryou_tsuuchi.objects.filter(Receive_filename=datfilename)
                for data in N090_list:

                    kaisaibi = dt.strptime(str(dt.now().year) + data.Syori_date, '%Y%m%d').date()
                    m160_obj = None
                    if M160_shuuryo.objects.filter(kaisaibi=kaisaibi).exists():
                        m160_obj = M160_shuuryo.objects.filter(kaisaibi=kaisaibi).last()
                    else:
                        m160_obj = M160_shuuryo.objects.create(kaisaibi=kaisaibi)

                edit_mddb_list.append(m160_obj)
            return edit_mddb_list

        except Exception as e:
            Common_log.Out_Logs(log_err_msg_id, [failure(e)])
            logger.error(failure(e))
            return False