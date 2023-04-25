from logging import getLogger
import re
from app_jra.consts import *
import sys
import linecache
from datetime import timedelta
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

class Trn_Mst_Schedule():
            
    def insert_or_update_Mst_Schedule(self, datfilename, datDataFileFlg):
        try:
            from app_jra.models import (
                Trn_I221_Honjitsu_okuridashi_yotei,
                Trn_FSIN_1_Syussouba_meihyo,
                Mst_Schedule,
            )
            Cmn = Common()
            
            if SYUSSOUBA_MEIHYO == datDataFileFlg:
                # FSIN 出走馬名表データレコードの場合
                FSIN1_list = Trn_FSIN_1_Syussouba_meihyo.objects.filter(Receive_filename=datfilename)
                schedule_obj_list = []
                for data in FSIN1_list:
                    jou_obj = Cmn.chk_master_Mst_Jou(data.Bangumi_Jou_code)
                    kaisaibi = dt.strptime(data.Syutsuba_info_kaisai_date, '%Y%m%d').date()
                    kaisuu = int(data.Syutsuba_info_kaisai_kai)
                    kainichime = int(data.Syutsuba_info_kaisai_nichi)

                    # G1の場合のみ、レース名取得（画面表示用）
                    g1_racemei = None
                    if "A" == data.Tokubetsu_kyoso_grade: # "A":Grade_code(G1)
                        g1_racemei = data.Tokubetsu_kyoso_hondai.rstrip()

                    # スケジュールマスタ
                    if jou_obj:
                        if Mst_Schedule.objects.filter(Jou=jou_obj, Kai=kaisuu, Nichime=kainichime).exists():
                            schedule_obj = Mst_Schedule.objects.filter(Jou=jou_obj, Kai=kaisuu, Nichime=kainichime).last()
                            schedule_obj.Date = kaisaibi
                            if g1_racemei:
                                schedule_obj.G1_racename = g1_racemei
                            schedule_obj.save()
                        else:
                            schedule_obj = Mst_Schedule.objects.create(
                                Date=kaisaibi,
                                Jou=jou_obj,
                                Kai=kaisuu,
                                Nichime=kainichime,
                                G1_racename=g1_racemei
                                )
                        if not schedule_obj in schedule_obj_list:
                            schedule_obj_list.append(schedule_obj)
                    
                    # FSINの中でも最終レコードの場合、該当受信ファイルのデータ数からレース数を取得してスケジュールマスタに登録する
                    if data.Final_recode == '1':
                        for schedule_obj in schedule_obj_list:
                            racesuu = FSIN1_list.filter(Syutsuba_info_kaisai_date=schedule_obj.Date.strftime('%Y%m%d') ,Bangumi_Jou_code=schedule_obj.Jou.Jou_code).count()
                            schedule_obj.Racesuu = racesuu
                            schedule_obj.save()
                            logger.info(f'スケジュールマスタ登録・更新 {schedule_obj}')



            if HONJITSU_OKURIDASHI_YOTEI == datDataFileFlg:
                # I221 本日送出予定データレコードの場合
                I221_list = Trn_I221_Honjitsu_okuridashi_yotei.objects.filter(Receive_filename=datfilename)
                for data in I221_list:
                    jou_obj_list = []
                    soutei_schedule_obj_dict = {} # {jou_obj:schedule_obj}
                    yokujitsu_schedule_obj_dict = {} # {jou_obj:schedule_obj}
                    maeuri_schedule_obj_dict = {} # {jou_obj:schedule_obj}

                    for i in range(15):
                        num = i + 1
                        # 想定出馬投票情報
                        jou_obj = Cmn.chk_master_Mst_Jou(getattr(data, 'Souteishutsuba_touhyou_Jou_code' + str(num)))
                        kaisuu = int(getattr(data, 'Souteishutsuba_touhyou_Kai' + str(num)))
                        kainichime = int(getattr(data, 'Souteishutsuba_touhyou_Day' + str(num) ))
                        racesuu = int(getattr(data, 'Souteishutsuba_touhyou_Race_number' + str(num)))

                        if jou_obj:
                            if Mst_Schedule.objects.filter(Jou=jou_obj, Kai=kaisuu, Nichime=kainichime).exists():
                                schedule_obj = Mst_Schedule.objects.filter(Jou=jou_obj, Kai=kaisuu, Nichime=kainichime).last()
                                schedule_obj.Racesuu = racesuu
                                schedule_obj.save()
                            else:
                                schedule_obj = Mst_Schedule.objects.create(
                                    Jou=jou_obj,
                                    Kai=kaisuu,
                                    Nichime=kainichime,
                                    Racesuu=racesuu
                                    )

                            if jou_obj in soutei_schedule_obj_dict.keys():
                                soutei_schedule_obj_dict[jou_obj].append(schedule_obj)
                            else:
                                soutei_schedule_obj_dict[jou_obj] = [schedule_obj]

                            if not jou_obj in jou_obj_list:
                                jou_obj_list.append(jou_obj)
                            
                        # 出馬投票詳細情報
                        jou_obj = Cmn.chk_master_Mst_Jou(getattr(data, 'Shutsuba_touhyou_Jou_code' + str(num)))
                        kaisuu = int(getattr(data, 'Shutsuba_touhyou_Kai' + str(num)))
                        kainichime = int(getattr(data, 'Shutsuba_touhyou_Day' + str(num) ))
                        racesuu = int(getattr(data, 'Shutsuba_touhyou_Race_number' + str(num)))

                        if jou_obj:
                            if Mst_Schedule.objects.filter(Jou=jou_obj, Kai=kaisuu, Nichime=kainichime).exists():
                                schedule_obj = Mst_Schedule.objects.filter(Jou=jou_obj, Kai=kaisuu, Nichime=kainichime).last()
                                schedule_obj.Racesuu = racesuu
                                schedule_obj.save()
                            else:
                                schedule_obj = Mst_Schedule.objects.create(
                                    Jou=jou_obj,
                                    Kai=kaisuu,
                                    Nichime=kainichime,
                                    Racesuu=racesuu
                                    )
                            
                            if jou_obj in yokujitsu_schedule_obj_dict.keys():
                                yokujitsu_schedule_obj_dict[jou_obj].append(schedule_obj)
                            else:
                                yokujitsu_schedule_obj_dict[jou_obj] = [schedule_obj]

                            if not jou_obj in jou_obj_list:
                                jou_obj_list.append(jou_obj)
                    
                    for i in range(3):
                        num = i + 1
                        # 前日売詳細情報
                        jou_obj = Cmn.chk_master_Mst_Jou(getattr(data, 'Zenjitsu_uri_Jou_code' + str(num)))
                        kaisuu = int(getattr(data, 'Zenjitsu_uri_Kai' + str(num)))
                        kainichime = int(getattr(data, 'Zenjitsu_uri_Day' + str(num) ))
                        racesuu = int(getattr(data, 'Zenjitsu_uri_Race_number' + str(num)))

                        if jou_obj:
                            if Mst_Schedule.objects.filter(Jou=jou_obj, Kai=kaisuu, Nichime=kainichime).exists():
                                schedule_obj = Mst_Schedule.objects.filter(Jou=jou_obj, Kai=kaisuu, Nichime=kainichime).last()
                                schedule_obj.Racesuu = racesuu
                                schedule_obj.save()
                            else:
                                schedule_obj = Mst_Schedule.objects.create(
                                    Jou=jou_obj,
                                    Kai=kaisuu,
                                    Nichime=kainichime,
                                    Racesuu= racesuu
                                    )
                            
                            if jou_obj in maeuri_schedule_obj_dict.keys():
                                maeuri_schedule_obj_dict[jou_obj].append(schedule_obj)
                            else:
                                maeuri_schedule_obj_dict[jou_obj] = [schedule_obj]

                            if not jou_obj in jou_obj_list:
                                jou_obj_list.append(jou_obj)
                    
                        # 成績詳細情報
                        jou_obj = Cmn.chk_master_Mst_Jou(getattr(data, 'Seiseki_Jou_code' + str(num)))
                        kaisuu = int(getattr(data, 'Seiseki_Kai' + str(num)))
                        kainichime = int(getattr(data, 'Seiseki_Day' + str(num) ))
                        racesuu = int(getattr(data, 'Seiseki_Race_number' + str(num)))
                        # 成績データの場合は、送信日を開催日と見なす
                        kaisaibi = dt.strptime(str(data.create_date.year) + data.Shori_date, '%Y%m%d').date()
                            
                        if jou_obj:
                            if Mst_Schedule.objects.filter(Jou=jou_obj, Kai=kaisuu, Nichime=kainichime).exists():
                                schedule_obj = Mst_Schedule.objects.filter(Jou=jou_obj, Kai=kaisuu, Nichime=kainichime).last()
                                schedule_obj.Date = kaisaibi
                                schedule_obj.Racesuu = racesuu
                                schedule_obj.save()
                            else:
                                schedule_obj = Mst_Schedule.objects.create(
                                    Date=kaisaibi,
                                    Jou=jou_obj,
                                    Kai=kaisuu,
                                    Nichime=kainichime,
                                    Racesuu= racesuu
                                    )
                            if not jou_obj in jou_obj_list:
                                jou_obj_list.append(jou_obj)


                    
                    # 作成・更新した各場/日のスケジュールマスタオブジェクトを、当日の各場のスケジュールマスタオブジェクトに紐付けていく
                    toujitsu_date = dt.strptime(str(data.create_date.year) + data.Shori_date, '%Y%m%d').date()
                    for jou_obj in jou_obj_list:
                        # 当日の場ごとのスケジュールマスタオブジェクトを用意する
                        if Mst_Schedule.objects.filter(Jou=jou_obj, Date=toujitsu_date).exists():
                            toujitsu_obj = Mst_Schedule.objects.filter(Jou=jou_obj, Date=toujitsu_date).last()
                        else:
                            toujitsu_obj = Mst_Schedule.objects.create(Jou=jou_obj, Date=toujitsu_date)

                        # 想定出馬表の送信予定がある場の場合
                        if jou_obj in soutei_schedule_obj_dict.keys():
                            for soutei in soutei_schedule_obj_dict[jou_obj]:
                                toujitsu_obj.Soutei_kaisai.add(soutei)

                        # 翌日出馬表の送信予定がある場の場合
                        if jou_obj in yokujitsu_schedule_obj_dict.keys():
                            for yoku in yokujitsu_schedule_obj_dict[jou_obj]:
                                toujitsu_obj.Yokujitsu_kaisai.add(yoku)
                
                        # 前売出馬表の送信予定がある場の場合
                        if jou_obj in maeuri_schedule_obj_dict.keys():
                            for maeuri in maeuri_schedule_obj_dict[jou_obj]:
                                toujitsu_obj.Maeuri_kaisai.add(maeuri)
                

                        # 最後に、場にかかわらない当日情報を、当日のスケジュール全てに登録
                        # 重勝式払戻金＿該当競走フラグが0ならFalse
                        toujitsu_obj.Win5_youkou_sousin_flg = int(data.Haraimodoshi_Gaitou_kyousou_flag)
                        # 重勝式発売要項＿該当競走フラグが0ならFalse
                        toujitsu_obj.Win5_kekka_sousin_flg = int(data.Hatsubaiyoukou_Gaitou_kyousou_flag)
                        # ベスト30送信フラグ
                        toujitsu_obj.Best30_flg = int(data.Best30_yesorno)
                        toujitsu_obj.save()

                        logger.info(f'スケジュールマスタ登録・更新 {toujitsu_obj}')
            return NORMAL

        except Exception as e:
            Common_log.Out_Logs(log_err_msg_id, [e])
            logger.error(failure(e))
            return ABNORMAL
