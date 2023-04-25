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



class Trn_M70_tsuusin():



    def chk_blank(self, record):
        # 受信データにはブランクのパターンがいくつかあるが、空データの場合は全てブランクとみなす処理
        blank_list = ['','　',None,[],{}]
        if record in blank_list or re.fullmatch('\s+', record):
            return False
        else:
            return True


    def get_kaisaibi(self, jou_obj, kai, nichime):
        from app_jra.models import Mst_Schedule
        kaisaibi = None
        schedule_objs = Mst_Schedule.objects.filter(Jou=jou_obj, Kai=kai, Nichime=nichime)
        if schedule_objs.exists():
            kaisaibi = schedule_objs.last().Date
        return kaisaibi


    def make_kijinai(self, kiji):
        kijinai = None
        chk_kiji = self.chk_blank(kiji)
        if not chk_kiji:
            kijinai = "なし"
        else:
            kijinai = kiji.strip()
        return kijinai

    def insert_or_update_M70_tsuusin(self, datfilename, datDataFileFlg):
        try:
            from app_jra.models import (
                Trn_A223_Kiji,
                M70_tsuusinbun
            )
            Cmn = Common()

            # 受信ファイルごとに、M70レコードを新規作成・更新していく。
            # 正しく登録完了した場合、後続の待ち合わせ・配信処理で使うために、更新した中間DBのレコードを保持しておき、リストにして返す。
            # 登録失敗時には、False(ABNOMAL or 空のリスト)を返す。
            edit_mddb_list = [] # 初期値

            if KIJI == datDataFileFlg:
                # A223記事から情報を取得
                A223_list = Trn_A223_Kiji.objects.filter(Receive_filename=datfilename)
                for data in A223_list:

                    jou_obj = Cmn.chk_master_Mst_Jou(data.Bangumi_Jou_code)
                    kaisuu = int(data.Bangumi_kai)
                    kainichime = int(data.Bangumi_day)
                    rebangou = int(data.Bangumi_race_number)

                    kaisai_date = self.get_kaisaibi(jou_obj,kaisuu,kainichime)
                    if not kaisai_date:
                        kaisai_date = dt.strptime(data.Soushin_date, '%y%m%d').date()

                    kijinai = self.make_kijinai(data.Kiji)
                        
                    # M70を場コード、回、日、レース番号、記事通番を元に確認
                    m70_list = M70_tsuusinbun.objects.filter(
                        kaisaibi=kaisai_date,
                        joumei=jou_obj,
                        kaisuu=kaisuu,
                        kainichime=kainichime,
                        rebangou=rebangou,
                        kijiban=data.Kiji_tsuuban
                        )

                    if not m70_list.exists():
                        # M70を新規作成
                        m70_obj = M70_tsuusinbun.objects.create(
                            kaisaibi = kaisai_date,
                            joumei = jou_obj,
                            kaisuu = kaisuu,
                            kainichime = kainichime,
                            rebangou = rebangou,
                            kijiban = data.Kiji_tsuuban,
                            kijinai = kijinai
                        )
                        edit_mddb_list.append(m70_obj) 

                    else:
                        # 送信回数が1回の場合
                        if data.Tanmatsu_soushin_kaisuu == "1":
                            # M70を新規作成
                            m70_obj = M70_tsuusinbun.objects.create(
                                kaisaibi = kaisai_date,
                                joumei = jou_obj,
                                kaisuu = kaisuu,
                                kainichime = kainichime,
                                rebangou = rebangou,
                                kijiban = data.Kiji_tsuuban,
                                kijinai = kijinai
                            )
                            edit_mddb_list.append(m70_obj) 


                        # 送信回数が1回ではない場合
                        else:
                            m70_obj = m70_list.last()
                            
                            # m70_objの情報を更新する
                            m70_obj.kijinai = kijinai
                            m70_obj.save()
                            edit_mddb_list.append(m70_obj)

            return edit_mddb_list

           

        except Exception as e:
            Common_log.Out_Logs(log_err_msg_id, [failure(e)])
            logger.error(failure(e))
