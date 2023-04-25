from logging import getLogger
import re
from app_jra.consts import *
import sys
import linecache
from app_jra.log_commons import *
from datetime import datetime as dt


logger = getLogger('jra_edit_delivery')
Common_log = Common_log(DEBUGLOG_NAME_TO_TYPE[logger.name])
log_err_msg_id = 3300
log_info_msg_id = 9399
try:
    from app_jra_mddb.mddb_commons import Common
except Exception as e:
    Common_log.Out_Logs(log_err_msg_id, [e])
    logger.error(f'commonsファイル読み込み失敗 : {e}')


def failure(e):
    exc_type, exc_obj, tb=sys.exc_info()
    lineno=tb.tb_lineno
    return str(lineno) + ":" + str(type(e)) + str(e)

class Trn_Mst_Umanushi():

    def chk_blank_zero(self, record):
        # 受信データにはブランクのパターンがいくつかあるが、ゼロの場合と空データの場合は全てブランクとみなす処理
        blank_list = ['','　',None,[],{},0,'0','00','０','００']
        if record in blank_list or re.fullmatch('\s+', record):
            return False
        else:
            return True

    def make_name(self, rowname):
        # 余計な空白を削除して、「苗字　名前」形式にして返す
        return re.sub('[ 　]+', ' ', rowname.replace('　',' ').rstrip()).replace(' ','　')

    def insert_or_update_Mst_Umanushi(self, datfilename, datDataFileFlg):
        try:
            from app_jra.models import (
                Mst_JOWN_Banushi,
                Mst_Umanushi,
            )
            Cmn = Common()
            update_num = 0

            if M_BANUSHI == datDataFileFlg:
                # JOWN 馬主マスタ
                JOWN_list = Mst_JOWN_Banushi.objects.filter(Receive_filename=datfilename)
                for data in JOWN_list:
                    if self.chk_blank_zero(data.Banushi_number):
                        umanushi_objs = Mst_Umanushi.objects.filter(Umanushi_code=data.Banushi_number)
                        if umanushi_objs.exists():
                            # 更新
                            umanushi_obj = umanushi_objs.last() # Umanushi_codeはunique
                            umanushi_obj.Umanushi_name = self.make_name(data.Banushi_name) if self.chk_blank_zero(data.Banushi_name) else data.Banushi_name_EN
                            umanushi_obj.save()
                        else:
                            # 新規作成
                            umanushi_obj = Mst_Umanushi.objects.create(
                                Umanushi_code = data.Banushi_number,
                                Umanushi_name = self.make_name(data.Banushi_name) if self.chk_blank_zero(data.Banushi_name) else data.Banushi_name_EN
                            )
                        update_num += 1
                logger.info(f'【馬主マスタ】{str(update_num)}件 登録・更新')

            return NORMAL

        except Exception as e:
            Common_log.Out_Logs(log_err_msg_id, [e])
            logger.error(failure(e))
            return ABNORMAL
