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
    return str(lineno) + ":" + str(type(e)) + str(e)


class Trn_M120_agari():


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


    # 【共通処理】
    # １．半角int（またはdecimal）→ 全角str用の辞書と関数
    # （参考） https://qiita.com/YuukiMiyoshi/items/6ce77bf402a29a99f1bf
    def intToZen(self, i):
        HAN2ZEN = str.maketrans({"0": "０", "1": "１", "2": "２", "3": "３", "4": "４", "5": "５", "6": "６", "7": "７", "8": "８", "9": "９", ".": "．", "G": "Ｇ"})
        if i or i == 0:
            return str(i).translate(HAN2ZEN)
        else:
            return


    def get_umaban_fields(self, renum):
        umaban = "Time" + str(renum) + "_horse_ban"
        return str(umaban)


    def make_a4hakei(self, data, renum):
        a4hakei = None
        sec1 = getattr(data, "Time" + str(renum) + "_4_seconds")
        sec2 = getattr(data, "Time" + str(renum) + "_4_seconds2")
        if self.chk_blank(sec1) and self.chk_blank(sec2):
            if not sec1 + sec2 == "999":
                a4hakei = self.intToZen(int(sec1)) + "." + self.intToZen(sec2)
        return a4hakei


    def make_a3hakei(self, data, renum):
        a3hakei = None
        sec1 = getattr(data, "Time" + str(renum) + "_3_seconds")
        sec2 = getattr(data, "Time" + str(renum) + "_3_seconds2")
        if self.chk_blank(sec1) and self.chk_blank(sec2):
            if not sec1 + sec2 == "999":
                a3hakei = self.intToZen(int(sec1)) + "." + self.intToZen(sec2)
        return a3hakei


    def insert_or_update_M120_agari(self, datfilename, datDataFileFlg):
        try:
            from app_jra.models import (
                Trn_A200_1_JARIS_seiseki,
                Trn_A200_2_Seiseki_info,
                Trn_A326_Umabetsu_agari_time,
                M120_agari
            )
            Cmn = Common()

            # 受信ファイルごとに、M120レコードを新規作成・更新していく。
            # 正しく登録完了した場合、後続の待ち合わせ・配信処理で使うために、更新した中間DBのレコードを保持しておき、リストにして返す。
            # 登録失敗時には、False(ABNOMAL or 空のリスト)を返す。
            edit_mddb_list = []  # 初期値
            m120_obj = None

            if UMABETSU_AGARI_TIME == datDataFileFlg:
                # A326 馬別上がりタイム
                A326_list = Trn_A326_Umabetsu_agari_time.objects.filter(Receive_filename=datfilename)
                for data in A326_list:


                    jou_obj = Cmn.chk_master_Mst_Jou(data.Bangumi_Jou_code)
                    kaisuu = int(data.Bangumi_kai)
                    kainichime = int(data.Bangumi_day)
                    rebangou = int(data.Bangumi_race_number)

                    kaisai_date = self.get_kaisaibi(jou_obj,kaisuu,kainichime)
                    if not kaisai_date:
                        kaisai_date = dt.strptime(data.Soushin_date, '%y%m%d').date()

                    # A326は最大18頭分なので18回ループ
                    for i in range(18):
                        renum = i + 1

                        # ループごとの馬番で空チェックして無ければスキップ
                        if self.chk_blank(getattr(data, self.get_umaban_fields(renum))):
                            # M120から番組情報、馬番を元に情報を取得する
                            m120_list = M120_agari.objects.filter(
                                kaisaibi = kaisai_date,
                                joumei=jou_obj,
                                kaisuu=kaisuu,
                                kainichime=kainichime,
                                rebangou=rebangou,
                                uma=int(getattr(data, self.get_umaban_fields(renum)))
                                )

                            # 各馬番に該当するM120 馬別上がりタイムレコードを検索する
                            if not m120_list.exists():
                                # M120 馬別上がりタイムを新規作成
                                m120_obj = M120_agari.objects.create(
                                    kaisaibi = kaisai_date,
                                    joumei = jou_obj,
                                    kaisuu = kaisuu, 
                                    kainichime = kainichime,
                                    rebangou = rebangou,
                                    uma = int(getattr(data, self.get_umaban_fields(renum))),
                                    a4hakei = self.make_a4hakei(data, renum),
                                    a3hakei = self.make_a3hakei(data, renum),
                                    heikin1ha = self.make_a3hakei(data, renum)
                                    )

                            else:
                                m120_obj = m120_list.last()

                                # m120_objに情報を追加していく
                                m120_obj.a4hakei = self.make_a4hakei(data, renum)
                                m120_obj.a3hakei = self.make_a3hakei(data, renum)
                                m120_obj.heikin1ha = self.make_a3hakei(data, renum)
                                m120_obj.save()

            
            if JARIS_SEISEKI == datDataFileFlg:
                # A200 JARIS成績
                A200_list = Trn_A200_1_JARIS_seiseki.objects.filter(Receive_filename=datfilename)
                for data in A200_list:


                    jou_obj = Cmn.chk_master_Mst_Jou(data.Bangumi_Jou_code)
                    kaisuu = int(data.Bangumi_kai)
                    kainichime = int(data.Bangumi_day)
                    rebangou = int(data.Bangumi_race_number)

                    # A200 JARIS成績を元に成績詳細を取得
                    A200_2_list = Trn_A200_2_Seiseki_info.objects.filter(A200_1=data)
                    if A200_2_list.exists():
                        for seiseki_data in A200_2_list:
                            if self.chk_blank(seiseki_data.Ketto_touroku_number):
                                
                                kaisai_date = self.get_kaisaibi(jou_obj,kaisuu,kainichime)
                                if not kaisai_date:
                                    kaisai_date = dt.strptime(data.Soushin_date, '%y%m%d').date()

                                m120_list = M120_agari.objects.filter(
                                    kaisaibi = kaisai_date,
                                    joumei=jou_obj,
                                    kaisuu=kaisuu,
                                    kainichime=kainichime,
                                    rebangou=rebangou,
                                    uma=int(seiseki_data.Ban)
                                    )

                                if not m120_list.exists():
                                    # M120 馬別上がりタイムを新規作成
                                    m120_obj = M120_agari.objects.create(
                                        kaisaibi = kaisai_date,
                                        joumei = jou_obj,
                                        kaisuu = kaisuu, 
                                        kainichime = kainichime,
                                        rebangou = rebangou,
                                        shougaikubun = Cmn.chk_master_Mst_Kyousou_shubetsu(data.Seiseki_data_kyoso_syubetsu_code),
                                        uma = int(seiseki_data.Ban),
                                        umajouhou = Cmn.chk_master_Mst_Horse(seiseki_data.Ketto_touroku_number),
                                    )
                                else:
                                    m120_obj = m120_list.last()
                                    
                                    # m120_objに情報を追加していく
                                    m120_obj.kaisaibi = kaisai_date
                                    m120_obj.shougaikubun = Cmn.chk_master_Mst_Kyousou_shubetsu(data.Seiseki_data_kyoso_syubetsu_code)
                                    m120_obj.umajouhou = Cmn.chk_master_Mst_Horse(seiseki_data.Ketto_touroku_number)
                                    m120_obj.save()

            edit_mddb_list.append(m120_obj)
            return edit_mddb_list

        except Exception as e:
            Common_log.Out_Logs(log_err_msg_id, [failure(e)])
            logger.error(failure(e))
            return False