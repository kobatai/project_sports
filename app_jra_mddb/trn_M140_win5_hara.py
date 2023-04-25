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


class Trn_M140_win5_hara():

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

    def make_comma_str(self, data, rowfield):
        edit_str = None
        for i in range(18):
            strnum = str(i + 1).zfill(2)
            if hasattr(data, rowfield + strnum):
                if self.chk_blank(getattr(data, rowfield + strnum)):
                    if not edit_str:
                        edit_str = getattr(data, rowfield + strnum)
                    else:
                        edit_str = edit_str + ',' + getattr(data, rowfield + strnum)
        return edit_str

    def get_umajouhou(self, FSOD_obj, jusho_num, umaban):
        # レース情報と馬番から、馬名と単勝人気を取得する。
        # JARISとFSOSが元データだが、JARISとFSOSを元に作っているM21から取得可能のため、M21を検索する
        from app_jra.models import M20_seiseki, M21_seiseki_chakujun

        Cmn = Common()

        bamei = None
        tannin = None

        jou_obj = Cmn.chk_master_Mst_Jou(getattr(FSOD_obj, 'Kaisai_Jou_code' + str(jusho_num)))
        kaisuu = int(getattr(FSOD_obj, 'Kaisai_kai' + str(jusho_num)))
        kainichime = int(getattr(FSOD_obj, 'Kaisai_day' + str(jusho_num)))
        rebangou = int(getattr(FSOD_obj, 'Kaisai_race_number' + str(jusho_num)))

        if M20_seiseki.objects.filter(joumei=jou_obj, kaisuu=kaisuu, kainichime=kainichime, rebangou=rebangou).exists():
            m20_obj = M20_seiseki.objects.filter(joumei=jou_obj, kaisuu=kaisuu, kainichime=kainichime, rebangou=rebangou).last()
            if M21_seiseki_chakujun.objects.filter(m20=m20_obj, uma=umaban).exists():
                m21_obj = M21_seiseki_chakujun.objects.filter(m20=m20_obj, uma=umaban).last()
                bamei = m21_obj.umajouhou.Bamei
                tannin = m21_obj.tannin
        else:
            Common_log.Out_Logs(log_err_msg_id, [f'{str(kaisuu)}回{jou_obj.Jou_name} {str(kainichime)}日目の成績データがありません。WIN5払戻情報の１着馬情報は空のままで登録されます（送信時にエラーとなります）'])
            logger.error(f'{str(kaisuu)}回{jou_obj.Jou_name} {str(kainichime)}日目の成績データがありません。WIN5払戻情報の１着馬情報は空のままで登録されます（送信時にエラーとなります）')

        return bamei,tannin


    # 払戻情報を取得する
    def make_haraijoukyou(self, data):
        haraijoukyou = None
        if data.Jusyo_huseiritsu == "*":
            haraijoukyou = STR_HUSEIRITSU
        elif data.Jusyo_tekichu_nashi == "*":
            haraijoukyou = STR_TEKICHUUNASHI
        return haraijoukyou



    def insert_or_update_M140_win5_hara(self, datfilename, datDataFileFlg):
        try:
            from app_jra.models import (
                Trn_FSOD_1_Jusyo_hayamihyo,
                Trn_FSOD_3_Haraimodoshi_info,
                Trn_FSLI_Jusyou_yuukou_hyousuu,
                M140_win5_hara,
                M141_win5_hara_haraimodoshi
            )
            Cmn = Common()

            # 受信ファイルごとに、M140レコードを新規作成・更新していく。
            # 正しく登録完了した場合、後続の待ち合わせ・配信処理で使うために、更新した中間DBのレコードを保持しておき、リストにして返す。
            # 登録失敗時には、False(ABNOMAL or 空のリスト)を返す。
            edit_mddb_list = [] # 初期値

            if JUSYOU_HAYAMI_HYOU == datDataFileFlg:
                # FSOD 重勝早見表
                FSOD_1_list = Trn_FSOD_1_Jusyo_hayamihyo.objects.filter(Receive_filename=datfilename)
                for data in FSOD_1_list:

                    jou_obj = Cmn.chk_master_Mst_Jou(data.Kaisai_Jou_code1)
                    kaisuu = int(data.Kaisai_kai1)
                    kainichime = int(data.Kaisai_day1)

                    kaisai_date = self.get_kaisaibi(jou_obj, kaisuu, kainichime)

                    if not kaisai_date:
                        Common_log.Out_Logs(log_err_msg_id, [f'{str(kaisuu)}回{jou_obj.Jou_name} {str(kainichime)}日目の、スケジュールマスタがありません'])
                        logger.error(f'{str(kaisuu)}回{jou_obj.Jou_name} {str(kainichime)}日目の、スケジュールマスタがありません')
                        return False

                    # M140 WIN5払戻金を新規作成または更新
                    m140_obj, created = M140_win5_hara.objects.update_or_create(
                        kaisaibi=kaisai_date,

                        joumei=Cmn.chk_master_Mst_Jou(data.Kaisai_Jou_code1),
                        kaisuu=int(data.Kaisai_kai1),
                        kainichime=int(data.Kaisai_day1),
                        rebangou=int(data.Kaisai_race_number1),

                        joumei_2=Cmn.chk_master_Mst_Jou(data.Kaisai_Jou_code2),
                        kaisuu_2=int(data.Kaisai_kai2),
                        kainichime_2=int(data.Kaisai_day2),
                        rebangou_2=int(data.Kaisai_race_number2),

                        joumei_3=Cmn.chk_master_Mst_Jou(data.Kaisai_Jou_code3),
                        kaisuu_3=int(data.Kaisai_kai3),
                        kainichime_3=int(data.Kaisai_day3),
                        rebangou_3=int(data.Kaisai_race_number3),

                        joumei_4=Cmn.chk_master_Mst_Jou(data.Kaisai_Jou_code4),
                        kaisuu_4=int(data.Kaisai_kai4),
                        kainichime_4=int(data.Kaisai_day4),
                        rebangou_4=int(data.Kaisai_race_number4),

                        joumei_5=Cmn.chk_master_Mst_Jou(data.Kaisai_Jou_code5),
                        kaisuu_5=int(data.Kaisai_kai5),
                        kainichime_5=int(data.Kaisai_day5),
                        rebangou_5=int(data.Kaisai_race_number5),

                        defaults={
                            'henkanumaban_1' : self.make_comma_str(data, 'Henkan1_umaban'),
                            'henkanumaban_2' : self.make_comma_str(data, 'Henkan2_umaban'),
                            'henkanumaban_3' : self.make_comma_str(data, 'Henkan3_umaban'),
                            'henkanumaban_4' : self.make_comma_str(data, 'Henkan4_umaban'),
                            'henkanumaban_5': self.make_comma_str(data, 'Henkan5_umaban'),
                            'henkanjoukyou': STR_HENKANARI if data.Henkan_flag == 1 else None,
                            'haraijoukyou': self.make_haraijoukyou(data),
                            'zenkaikurikoshi': int(data.CO_kingaku),
                            'jikaikurikoshi': int(data.CO_kingaku_zan),
                        }
                        )
                    edit_mddb_list.append(m140_obj)

                    m140 = M140_win5_hara.objects.get(kaisaibi=kaisai_date)
                    FSOD_3_list = Trn_FSOD_3_Haraimodoshi_info.objects.filter(FSOD_1=data)
                    for FSOD_3_obj in FSOD_3_list:
                        # M141 WIN5払戻金 重勝式払戻金情報を新規作成または更新
                        if self.chk_blank(FSOD_3_obj.Jusyo_umaban):
                            ichakuumaban_1=int(FSOD_3_obj.Jusyo_umaban[0:2])
                            ichakuumaban_2=int(FSOD_3_obj.Jusyo_umaban[2:4])
                            ichakuumaban_3=int(FSOD_3_obj.Jusyo_umaban[4:6])
                            ichakuumaban_4=int(FSOD_3_obj.Jusyo_umaban[6:8])
                            ichakuumaban_5 = int(FSOD_3_obj.Jusyo_umaban[8:10])
                            
                            umainfo_1 = self.get_umajouhou(data, 1, ichakuumaban_1)
                            umainfo_2 = self.get_umajouhou(data, 2, ichakuumaban_2)
                            umainfo_3 = self.get_umajouhou(data, 3, ichakuumaban_3)
                            umainfo_4 = self.get_umajouhou(data, 4, ichakuumaban_4)
                            umainfo_5 = self.get_umajouhou(data, 5, ichakuumaban_5)
                            
                            m141_obj, created = M141_win5_hara_haraimodoshi.objects.update_or_create(
                                M140=m140,
                                ichakuumaban_1=ichakuumaban_1,
                                ichakuumaban_2=ichakuumaban_2,
                                ichakuumaban_3=ichakuumaban_3,
                                ichakuumaban_4=ichakuumaban_4,
                                ichakuumaban_5=ichakuumaban_5,
                                defaults={
                                    'ichakubamei_1': umainfo_1[0],
                                    'ichakuninki_1': umainfo_1[1],
                                    'ichakubamei_2': umainfo_2[0],
                                    'ichakuninki_2': umainfo_2[1],
                                    'ichakubamei_3': umainfo_3[0],
                                    'ichakuninki_3': umainfo_3[1],
                                    'ichakubamei_4': umainfo_4[0],
                                    'ichakuninki_4': umainfo_4[1],
                                    'ichakubamei_5': umainfo_5[0],
                                    'ichakuninki_5': umainfo_5[1],
                                    'haraikin': int(FSOD_3_obj.Jusyo_haraimodoshi),
                                    'tekichuuhyousuu': int(FSOD_3_obj.Tekichu_hyo),
                                }
                            )

            if JUSYOU_YUUKOU_HYOUSUU == datDataFileFlg:
                # FSLI 重勝有効票数
                FSLI_list = Trn_FSLI_Jusyou_yuukou_hyousuu.objects.filter(Receive_filename=datfilename)
                for data in FSLI_list:

                    jou_obj = Cmn.chk_master_Mst_Jou(data.Jou_code)
                    kaisuu = int(data.Kai)
                    kainichime = int(data.Day)
                    kaisai_date = self.get_kaisaibi(jou_obj, kaisuu, kainichime)

                    win5_race_jun = int(data.Jusyou_yuukou_race_jun)
                    yuukou_hyosuu = int(data.Yuukou_hyousuu)

                    if not kaisai_date:
                        Common_log.Out_Logs(log_err_msg_id, [f'{str(kaisuu)}回{jou_obj.Jou_name} {str(kainichime)}日目の、スケジュールマスタがありません'])
                        logger.error(f'{str(kaisuu)}回{jou_obj.Jou_name} {str(kainichime)}日目の、スケジュールマスタがありません')
                        return False

                    # M140 WIN5払戻金を新規作成または更新
                    m140_yuukouhyousuu_field = 'yuukouhyousuu_' + str(win5_race_jun)

                    m140_obj, created = M140_win5_hara.objects.update_or_create(
                        kaisaibi=kaisai_date,
                        defaults={
                            m140_yuukouhyousuu_field : yuukou_hyosuu
                        }
                    )
                    
                    edit_mddb_list.append(m140_obj)

            return edit_mddb_list

        except Exception as e:
            Common_log.Out_Logs(log_err_msg_id, [failure(e)])
            logger.error(failure(e))
            logger.error(e)
            return False