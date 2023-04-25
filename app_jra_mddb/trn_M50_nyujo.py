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


class Trn_M50_nyujo():


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

    def insert_or_update_M50_nyujo(self, datfilename, datDataFileFlg):
        try:
            from app_jra.models import (
                Trn_I164_Baitokukingaku_toujitsu,
                M50_nyujo
            )
            Cmn = Common()

            # 受信ファイルごとに、M50レコードを新規作成・更新していく。
            # 正しく登録完了した場合、後続の待ち合わせ・配信処理で使うために、更新した中間DBのレコードを保持しておき、リストにして返す。
            # 登録失敗時には、False(ABNOMAL or 空のリスト)を返す。
            edit_mddb_list = [] # 初期値
            m50_obj_list = []

            if BAITOKUKINGAKU_TOUJITSU == datDataFileFlg:
                # I164 売得金額当日
                I164_list = Trn_I164_Baitokukingaku_toujitsu.objects.filter(Receive_filename=datfilename)
                for data in I164_list:

                    jou_obj = Cmn.chk_master_Mst_Jou(data.Bangumi_Jou_code)
                    kaisuu = int(data.Bangumi_kai)
                    kainichime = int(data.Bangumi_day)

                    kaisai_date = self.get_kaisaibi(jou_obj, kaisuu, kainichime)
                    if not kaisai_date:
                        kaisai_date = dt.strptime(data.Soushin_date, '%y%m%d').date()

                    # M50から場名、回数、日目を元にレコードを取得　既に存在する場合は更新
                    m50_list = M50_nyujo.objects.filter(
                        joumei=jou_obj,
                        kaisuu=kaisuu,
                        kainichime=kainichime
                        )

                    if not m50_list.exists():
                        # M50 入場・人員を新規作成
                        m50_obj = M50_nyujo.objects.create(
                            kaisaibi=kaisai_date,
                            joumei=jou_obj,
                            kaisuu=kaisuu,
                            kainichime=kainichime,
                            tounyuujinin=int(data.Honjitsu),
                            touuriagekin=int(data.Goukei_baitoku_kingaku),
                            ruinyuujinin=int(data.Ruikei),
                            ruiuriagekin=int(data.Ruikei_baitoku_kingaku)
                            )
                        m50_obj_list.append(m50_obj)
                    
                    else:
                        m50_obj = m50_list.last()

                        # m50_objの情報を更新する
                        m50_obj.tounyuujinin=int(data.Honjitsu)
                        m50_obj.touuriagekin=int(data.Goukei_baitoku_kingaku)
                        m50_obj.ruinyuujinin=int(data.Ruikei)
                        m50_obj.ruiuriagekin=int(data.Ruikei_baitoku_kingaku)
                        m50_obj.save()
                        m50_obj_list.append(m50_obj)


            if m50_obj_list:
                # 後続処理に渡すm50は、レース番号まで分かればいいので一つだけでいい
                edit_mddb_list.append(m50_obj_list[0])
            return edit_mddb_list

        except Exception as e:
            Common_log.Out_Logs(log_err_msg_id, [failure(e)])
            logger.error(failure(e))
            return False