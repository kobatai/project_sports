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


class Trn_M40_jiko():


    def get_kaisaibi(self, jou_obj, kai, nichime):
        from app_jra.models import Mst_Schedule
        kaisaibi = None
        schedule_objs = Mst_Schedule.objects.filter(Jou=jou_obj, Kai=kai, Nichime=nichime)
        if schedule_objs.exists():
            kaisaibi = schedule_objs.last().Date
        return kaisaibi


    def chk_blank(self, record):
        # 受信データにはブランクのパターンがいくつかあるが、空データの場合は全てブランクとみなす処理
        blank_list = ['','　',None,[],{}]
        if record in blank_list or re.fullmatch('\s+', record):
            return False
        else:
            return True

    def chk_blank_zero(self, record):
        # 受信データにはブランクのパターンがいくつかあるが、ゼロの場合と空データの場合は全てブランクとみなす処理
        blank_list = ['','　',None,[],{},0,'0','00','000','０','００']
        if record in blank_list or re.fullmatch('\s+', record):
            return False
        else:
            return True

    def insert_or_update_M40_jiko(self, datfilename, datDataFileFlg):
        try:
            from app_jra.models import (
                Trn_A200_1_JARIS_seiseki,
                Trn_A200_2_Seiseki_info,
                M40_jiko
            )
            Cmn = Common()

            # 受信ファイルごとに、M40レコードを新規作成・更新していく。
            # 正しく登録完了した場合、後続の待ち合わせ・配信処理で使うために、更新した中間DBのレコードを保持しておき、リストにして返す。
            # 登録失敗時には、False(ABNOMAL or 空のリスト)を返す。
            edit_mddb_list = [] # 初期値
            m40_obj_list = []
            
            if JARIS_SEISEKI == datDataFileFlg:
                # A200 JARIS成績
                A200_list = Trn_A200_1_JARIS_seiseki.objects.filter(Receive_filename=datfilename)
                for data in A200_list:

                    jou_obj = Cmn.chk_master_Mst_Jou(data.Bangumi_Jou_code)
                    kaisuu = int(data.Bangumi_kai)
                    kainichime = int(data.Bangumi_day)
                    rebangou = int(data.Bangumi_race_number)

                    kaisai_date = self.get_kaisaibi(jou_obj,kaisuu,kainichime)
                    if not kaisai_date:
                        kaisai_date = dt.strptime(data.Soushin_date, '%y%m%d').date()

                    M40_jiko.objects.filter(
                        joumei=jou_obj,
                        kaisuu=kaisuu,
                        kainichime=kainichime,
                        rebangou=rebangou,
                        ).delete()
                        
                    # A200 JARIS成績を元に成績詳細を取得
                    A200_2_list = Trn_A200_2_Seiseki_info.objects.filter(A200_1=data)
                    if A200_2_list.exists():
                        M40_obj = None
                        for seiseki_data in A200_2_list:
                            # M40 事故情報を新規作成
                            if self.chk_blank_zero(seiseki_data.Ijo_kubun):
                                M40_obj = M40_jiko.objects.create(
                                    kaisaibi = kaisai_date,
                                    joumei = jou_obj,
                                    kaisuu = kaisuu, 
                                    kainichime = kainichime,
                                    rebangou = rebangou,
                                    umajouhou = Cmn.chk_master_Mst_Horse(seiseki_data.Ketto_touroku_number),
                                    ikubun = Cmn.chk_master_Mst_Ijou(seiseki_data.Ijo_kubun)
                                )
                        if not M40_obj:
                            # 事故無しでも、一つつくる(後続処理・手入力用)
                            M40_obj = M40_jiko.objects.create(
                                kaisaibi = kaisai_date,
                                joumei = jou_obj,
                                kaisuu = kaisuu, 
                                kainichime = kainichime,
                                rebangou = rebangou,
                            )

                        m40_obj_list.append(M40_obj)
            if m40_obj_list:
                # 後続処理に渡すm40は、レース番号まで分かればいいので一つだけでいい
                edit_mddb_list.append(m40_obj_list[0])
            return edit_mddb_list

        except Exception as e:
            Common_log.Out_Logs(log_err_msg_id, [failure(e)])
            logger.error(failure(e))
            return False