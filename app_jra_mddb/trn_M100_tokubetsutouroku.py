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


class Trn_M100_tokubetsutouroku():


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


    def get_horse_gaikokuseki(self, name):
        from app_jra.models import Mst_Horse
        name = name.strip()
        if self.chk_blank(name):
            if Mst_Horse.objects.filter(Bamei=name).exists():
                return Mst_Horse.objects.filter(Bamei=name).last().Gaikokuseki
            else:
                return None
        else:
            return None


    def make_fujuu(self, data):
        fujuu = None
        if self.chk_blank(data):
            fujuu = data
            if len(data) == 3:
                if data == '０００' or data == '000':
                    fujuu = None
                elif data[2:3] == '０' or data[2:3] == '0':
                    fujuu = data[0:2]
                else:
                    fujuu = data[0:2] + '．' + data[2:3]
        return fujuu


    def insert_or_update_M100_tokubetsutouroku(self, datfilename, datDataFileFlg):
        try:
            from app_jra.models import (
                Mst_JTRE_Tokubetsu_touroku_info,
                M100_tokubetsutouroku,
                M101_tokubetsutouroku_uma
            )
            Cmn = Common()

            # 受信ファイルごとに、M100レコードを新規作成・更新していく。
            # 正しく登録完了した場合、後続の待ち合わせ・配信処理で使うために、更新した中間DBのレコードを保持しておき、リストにして返す。
            # 登録失敗時には、False(ABNOMAL or 空のリスト)を返す。
            edit_mddb_list = [] # 初期値

            if M_TOKUBETSU_TOUROKU_INFO == datDataFileFlg:
                # JTRE 特別登録情報
                JTRE_list = Mst_JTRE_Tokubetsu_touroku_info.objects.filter(Receive_filename=datfilename)
                for data in JTRE_list:
                    # まず該当レースのM100 特別登録馬を全削除する
                    kaisaibi = dt.strptime(str(data.Tokubetsu_touroku_info_date), '%Y%m%d').date()
                    jou_obj = Cmn.chk_master_Mst_Jou(data.Tokubetsu_touroku_info_jou)
                    rebangou = int(data.Tokubetsu_touroku_info_R)
                    m100_list = M100_tokubetsutouroku.objects.filter(
                        kaisaibi=kaisaibi,
                        joumei=jou_obj,
                        rebangou=rebangou)
                    if m100_list.exists():
                        m100_list.delete()

                for data in JTRE_list:
                    # M100 特別登録馬を新規に登録していく。
                    kaisaibi = dt.strptime(str(data.Tokubetsu_touroku_info_date), '%Y%m%d').date()
                    jou_obj = Cmn.chk_master_Mst_Jou(data.Tokubetsu_touroku_info_jou)
                    kaisuu = int(data.Tokubetsu_touroku_info_kai)
                    kainichime = int(data.Tokubetsu_touroku_info_nichi)
                    rebangou = int(data.Tokubetsu_touroku_info_R)

                    tokusouhonsuu = None
                    tokusoumeihon = None
                    tokusoufukusuu = None
                    tokusoumeifuku = None
                    
                    tokusoumeihon = data.Tokubetsu_touroku_info_kyoso_name_main.rstrip()
                    tokusoumeifuku = data.Tokubetsu_touroku_info_kyoso_name_sub.rstrip()
                    fukuhon_kubun = int(data.Tokubetsu_touroku_info_huku_hon_kubun)
                    if int(data.Tokubetsu_touroku_info_jusyo_kai):
                        if fukuhon_kubun == 0 or fukuhon_kubun == 1:
                            tokusouhonsuu = int(data.Tokubetsu_touroku_info_jusyo_kai)
                        elif fukuhon_kubun == 2:
                            tokusoufukusuu = int(data.Tokubetsu_touroku_info_jusyo_kai)

                    if data.Tokubetsu_touroku_info_touroku_tousuu == '000':
                        # 登録頭数が0の場合特別登録馬を更新・新規作成しない
                        continue
                    else:
                        # M100 特別登録馬を更新・新規作成
                        m100_obj, m100_created = M100_tokubetsutouroku.objects.update_or_create(
                            kaisaibi=kaisaibi,
                            joumei=jou_obj,
                            rebangou=rebangou,
                            defaults={
                                "kaisuu" : kaisuu,
                                "kainichime" : kainichime,
                                "tokusouhonsuu" : tokusouhonsuu,
                                "tokusoumeihon" : tokusoumeihon,
                                "tokusoufukusuu" : tokusoufukusuu,
                                "tokusoumeifuku" : tokusoumeifuku,
                                "guredo" : Cmn.chk_master_Mst_Grade(data.Tokubetsu_touroku_info_grade_code_old),
                                "kyori" : int(data.Tokubetsu_touroku_info_kyori),
                                "kazu" : int(data.Tokubetsu_touroku_info_touroku_tousuu),
                            }
                        )
                        # M101 特別登録馬_出走馬を更新・新規作成
                        m101_obj, m101_created = M101_tokubetsutouroku_uma.objects.update_or_create(
                            m100=m100_obj,
                            umajouhou=Cmn.chk_master_Mst_Horse(data.Tokubetsu_touroku_info_ketto_number),
                            defaults={
                                'fujuu' : self.make_fujuu(data.Tokubetsu_touroku_info_hutan_juryo)
                            }
                        )
                        if not m100_obj in edit_mddb_list:
                            edit_mddb_list.append(m100_obj)

            return edit_mddb_list

        except Exception as e:
            Common_log.Out_Logs(log_err_msg_id, [failure(e)])
            logger.error(failure(e))
            return False