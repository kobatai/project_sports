
import os
import calendar
import sys
from app_jra.log_commons import *
from datetime import timedelta
from datetime import datetime as dt
from logging import getLogger

from celery import shared_task

from app_jra.consts import *
from app_jra.models import *
from app_jra_mddb.trn_Mst_Schedule import Trn_Mst_Schedule
from app_jra_mddb.trn_Mst_Kyousou_seiseki import Trn_Mst_Kyousou_seiseki
from app_jra_mddb.trn_Mst_Kishu import Trn_Mst_Kishu
from app_jra_mddb.trn_Mst_Choukyoushi import Trn_Mst_Choukyoushi
from app_jra_mddb.trn_Mst_Horse import Trn_Mst_Horse
from app_jra_mddb.trn_Mst_Umanushi import Trn_Mst_Umanushi
from app_jra_mddb.trn_Mst_Seisansha import Trn_Mst_Seisansha
from app_jra_mddb.trn_Mst_Chaku_seiseki import trn_Mst_Chaku_seiseki

from app_jra_mddb.trn_M00_tousuuhyou import Trn_M00_tousuuhyou
from app_jra_mddb.trn_M10_shutsubahyou import Trn_M10_shutsubahyou
from app_jra_mddb.trn_M20_seiseki import Trn_M20_seiseki
from app_jra_mddb.trn_M40_jiko import Trn_M40_jiko
from app_jra_mddb.trn_M50_nyujo import Trn_M50_nyujo
from app_jra_mddb.trn_M60_rap import Trn_M60_rap
from app_jra_mddb.trn_M70_tsuusinbun import Trn_M70_tsuusin
from app_jra_mddb.trn_M80_choukyou_best30 import Trn_M80_choukyou_best30
from app_jra_mddb.trn_M90_kishu_best30 import Trn_M90_kishu_best30
from app_jra_mddb.trn_M100_tokubetsutouroku import Trn_M100_tokubetsutouroku
from app_jra_mddb.trn_M111_odds_tan_fuku import Trn_M111_odds_tan_fuku
from app_jra_mddb.trn_M112_odds_wakuren import Trn_M112_odds_wakuren
from app_jra_mddb.trn_M113_odds_umaren_wide import Trn_M113_odds_umaren_wide
from app_jra_mddb.trn_M114_odds_umatan import Trn_M114_odds_umatan
from app_jra_mddb.trn_M115_odds_sanpuku import Trn_M115_odds_sanpuku
from app_jra_mddb.trn_M116_odds_santan import Trn_M116_odds_santan
from app_jra_mddb.trn_M120_agari import Trn_M120_agari
from app_jra_mddb.trn_M130_win5_youkou import Trn_M130_win5_youkou
from app_jra_mddb.trn_M140_win5_hara import Trn_M140_win5_hara
from app_jra_mddb.trn_M150_joui_odds import Trn_M150_joui_odds
from app_jra_mddb.trn_M160_shuryo import Trn_M160_shuryo

from app_jra_mddb.trn_MdDBStatus import Trn_MdDBStatus
from app_jra_edit_submit.output_NewsML import Output_NewsML
from app_jra_receive.receive_commons import Common
from prj_sports_jra.celery import app

from app_jra_edit_submit.tran_SubmitStatus import SubmitStatus

logger = getLogger('jra_edit_delivery')
base = os.path.dirname(os.path.abspath(__file__))  # app_jra_mddb
Common_log = Common_log(DEBUGLOG_NAME_TO_TYPE[logger.name])
log_err_msg_id = 3300
log_info_msg_id = 9399

def failure(e):
    exc_type, exc_obj, tb=sys.exc_info()
    lineno=tb.tb_lineno
    return str(lineno) + ":" + str(type(e)) + " " + str(e)

def get_souce_list(mddb_code):
    # 指定の中間DBに紐づく受信ファイルの受信ファイルコードを、リスト形式で取得する
    mddb_source_receivecode_list = list(Mst_MdDB_Code.objects.get(MdDB_code=mddb_code).Souce_Receive_File.all().values_list('Receive_file_code', flat=True))
    return mddb_source_receivecode_list

def run_mddb_proc(datfilename,datDataFileFlg):

    logger.info(f'中間DB登録処理 {datDataFileFlg} {datfilename}')
    mddb_list = [] # 後続処理用
    error_mddb_code_list = [] # エラー発生時の中間DBステータス更新用

    # 本日送出予定または出走馬名表の場合、スケジュールマスタを登録
    if HONJITSU_OKURIDASHI_YOTEI == datDataFileFlg or SYUSSOUBA_MEIHYO == datDataFileFlg:
        # スケジュールマスタ
        if not Trn_Mst_Schedule().insert_or_update_Mst_Schedule(datfilename,datDataFileFlg):
            Common_log.Out_Logs(log_err_msg_id, [f'【スケジュールマスタ】 登録処理失敗  {datfilename}'])
            logger.error(f'【スケジュールマスタ】 登録処理失敗  {datfilename}')
        else:
            # マスタ登録がOKだった場合
            logger.info(f'【スケジュールマスタ】データ登録・更新完了(受信ファイル:{datfilename})')

    # JRES 競走馬成績マスタまたはCRES 競走馬地方成績マスタの場合、競走馬成績マスタを登録
    if M_RACEHORSE_SEISEKI == datDataFileFlg or M_TIHOU_SEISEKI == datDataFileFlg:
        # 競走馬成績マスタ
        if not Trn_Mst_Kyousou_seiseki().insert_or_update_Mst_Kyousou_seiseki(datfilename,datDataFileFlg):
            Common_log.Out_Logs(log_err_msg_id, [f'【競走馬成績マスタ】 登録処理失敗  {datfilename}'])
            logger.error(f'【競走馬成績マスタ】 登録処理失敗  {datfilename}')
        else:
            # マスタ登録がOKだった場合
            Tran_Chikuseki_Mst.objects.create(Receive_filename=datfilename)
            logger.info(f'【競走馬成績マスタ】データ登録・更新完了(受信ファイル:{datfilename})')

    # JJOC 騎手マスタの場合、騎手マスタを登録
    if M_KISHU == datDataFileFlg:
        # 騎手マスタ
        if not Trn_Mst_Kishu().insert_or_update_Mst_Kishu(datfilename,datDataFileFlg):
            Common_log.Out_Logs(log_err_msg_id, [f'【騎手マスタ】 登録処理失敗  {datfilename}'])
            logger.error(f'【騎手マスタ】 登録処理失敗  {datfilename}')
        else:
            # マスタ登録がOKだった場合
            Tran_Chikuseki_Mst.objects.create(Receive_filename=datfilename)
            logger.info(f'【騎手マスタ】データ登録・更新完了(受信ファイル:{datfilename})')

    # JTRA 調教師マスタの場合、調教師マスタを登録
    if M_CHOUKYOUSHI == datDataFileFlg:
        # 調教師マスタ
        if not Trn_Mst_Choukyoushi().insert_or_update_Mst_Choukyoushi(datfilename,datDataFileFlg):
            Common_log.Out_Logs(log_err_msg_id, [f'【調教師マスタ】 登録処理失敗  {datfilename}'])
            logger.error(f'【調教師マスタ】 登録処理失敗  {datfilename}')
        else:
            # マスタ登録がOKだった場合
            Tran_Chikuseki_Mst.objects.create(Receive_filename=datfilename)
            logger.info(f'【調教師マスタ】データ登録・更新完了(受信ファイル:{datfilename})')

    # JHOS 競走馬マスタの場合、競走馬マスタを登録
    if M_RACEHORSE == datDataFileFlg:
        # 競走馬マスタ
        if not Trn_Mst_Horse().insert_or_update_Mst_Horse(datfilename,datDataFileFlg):
            Common_log.Out_Logs(log_err_msg_id, [f'【競走馬マスタ】 登録処理失敗  {datfilename}'])
            logger.error(f'【競走馬マスタ】 登録処理失敗  {datfilename}')
        else:
            # マスタ登録がOKだった場合
            Tran_Chikuseki_Mst.objects.create(Receive_filename=datfilename)
            logger.info(f'【競走馬マスタ】データ登録・更新完了(受信ファイル:{datfilename})')

    # JOWN 馬主マスタの場合、馬主マスタを登録
    if M_BANUSHI == datDataFileFlg:
        # 馬主マスタ
        if not Trn_Mst_Umanushi().insert_or_update_Mst_Umanushi(datfilename,datDataFileFlg):
            Common_log.Out_Logs(log_err_msg_id, [f'【馬主マスタ】 登録処理失敗  {datfilename}'])
            logger.error(f'【馬主マスタ】 登録処理失敗  {datfilename}')
        else:
            # マスタ登録がOKだった場合
            Tran_Chikuseki_Mst.objects.create(Receive_filename=datfilename)
            logger.info(f'【馬主マスタ】データ登録・更新完了(受信ファイル:{datfilename})')

    # JBRD 生産者マスタの場合、生産者マスタを登録
    if M_BREEDER == datDataFileFlg:
        # 生産者マスタ
        if not Trn_Mst_Seisansha().insert_or_update_Mst_Seisansha(datfilename,datDataFileFlg):
            Common_log.Out_Logs(log_err_msg_id, [f'【生産者マスタ】 登録処理失敗  {datfilename}'])
            logger.error(f'【生産者マスタ】 登録処理失敗  {datfilename}')
        else:
            # マスタ登録がOKだった場合
            Tran_Chikuseki_Mst.objects.create(Receive_filename=datfilename)
            logger.info(f'【生産者マスタ】データ登録・更新完了(受信ファイル:{datfilename})')

    # UMAR 競走馬成績（成績確定）の場合、競走馬着順成績マスタを登録
    if KYOUSOUBA_SEISEKI_KAKUTEI == datDataFileFlg:
        # 生産者マスタ
        if not trn_Mst_Chaku_seiseki().insert_or_update_Mst_Chaku_seiseki(datfilename,datDataFileFlg):
            Common_log.Out_Logs(log_err_msg_id, [f'【競走馬着順成績マスタ】 登録処理失敗  {datfilename}'])
            logger.error(f'【競走馬着順成績マスタ】 登録処理失敗  {datfilename}')
        else:
            # マスタ登録がOKだった場合
            Tran_Chikuseki_Mst.objects.create(Receive_filename=datfilename)
            logger.info(f'【競走馬着順成績マスタ】データ登録・更新完了(受信ファイル:{datfilename})')


    # M00 頭数表
    if datDataFileFlg in get_souce_list('M00'):
        # 頭数表のデータの場合

        # 【中間DB】頭数表
        # 登録DB: M00
        edit_mddb_list = Trn_M00_tousuuhyou().insert_or_update_M00_tousuuhyou(datfilename,datDataFileFlg)
        if not edit_mddb_list:
            if edit_mddb_list == []:
                Common_log.Out_Logs(log_info_msg_id, [f'【中間DB】頭数表 登録なし  {datfilename}'])
                logger.info(f'【中間DB】頭数表 登録なし  {datfilename}')
            else:
                Common_log.Out_Logs(log_info_msg_id, [f'【中間DB】頭数表 登録処理失敗  {datfilename}'])
                logger.error(f'【中間DB】頭数表 登録処理失敗  {datfilename}')
                error_mddb_code_list.append('M00')
        else:
            # 中間DB登録がOKだった場合
            mddb_list.extend(edit_mddb_list) # 編集済みmddbオブジェクトを格納する
            logger.info(f'【中間DB】頭数表 データ登録・更新完了(受信ファイル:{datfilename})')


    # M10系 出馬表
    if datDataFileFlg in get_souce_list('M10'):
        # 出走表系のデータの場合

        # 【中間DB】出走表
        # 登録DB: M10/M11(馬ごと)
        edit_mddb_list = Trn_M10_shutsubahyou().insert_or_update_M10_shutsubahyou(datfilename,datDataFileFlg)
        if not edit_mddb_list:
            if edit_mddb_list == []:
                Common_log.Out_Logs(log_info_msg_id, [f'【中間DB】出走表 登録なし  {datfilename}'])
                logger.info(f'【中間DB】出走表 登録なし  {datfilename}')
            else:
                Common_log.Out_Logs(log_info_msg_id, [f'【中間DB】出走表 登録処理失敗  {datfilename}'])
                logger.error(f'【中間DB】出走表 登録処理失敗  {datfilename}')
                error_mddb_code_list.append('M10')
        else:
            # 中間DB登録がOKだった場合
            mddb_list.extend(edit_mddb_list) # 編集済みmddbオブジェクトを格納する
            logger.info(f'【中間DB】出走表 データ登録・更新完了(受信ファイル:{datfilename})')


    # M10s系 出馬表(想定)
    if datDataFileFlg in get_souce_list('M10s'):
        # 出馬表(想定)のデータの場合

        # 【中間DB】出馬表(想定)
        # 登録DB: M10/M11(馬ごと)
        edit_mddb_list = Trn_M10_shutsubahyou().insert_or_update_M10_shutsubahyou(datfilename,datDataFileFlg)
        if not edit_mddb_list:
            if edit_mddb_list == []:
                Common_log.Out_Logs(log_info_msg_id, [f'【中間DB】出馬表(想定) 登録なし  {datfilename}'])
                logger.info(f'【中間DB】出馬表(想定) 登録なし  {datfilename}')
            else:
                Common_log.Out_Logs(log_info_msg_id, [f'【中間DB】出馬表(想定) 登録処理失敗  {datfilename}'])
                logger.error(f'【中間DB】出馬表(想定) 登録処理失敗  {datfilename}')
                error_mddb_code_list.append('M10s')
        else:
            # 中間DB登録がOKだった場合
            mddb_list.extend(edit_mddb_list) # 編集済みmddbオブジェクトを格納する
            logger.info(f'【中間DB】出馬表(想定) データ登録・更新完了(受信ファイル:{datfilename})')

    # M20系 成績
    if datDataFileFlg in get_souce_list('M20'):
        # 成績表系のデータの場合

        # 【中間DB】成績表
        # 登録DB: M20/M21(馬ごと)/M22_1～M22_8
        edit_mddb_list = Trn_M20_seiseki().insert_or_update_M20_seiseki(datfilename,datDataFileFlg)
        if not edit_mddb_list:
            if edit_mddb_list == []:
                Common_log.Out_Logs(log_info_msg_id, [f'【中間DB】成績 登録なし  {datfilename}'])
                logger.info(f'【中間DB】成績 登録なし  {datfilename}')
            else:
                Common_log.Out_Logs(log_info_msg_id, [f'【中間DB】成績 登録処理失敗  {datfilename}'])
                logger.error(f'【中間DB】成績 登録処理失敗  {datfilename}')
            error_mddb_code_list.append('M20')
        else:
            # 中間DB登録がOKだった場合
            mddb_list.extend(edit_mddb_list) # 編集済みmddbオブジェクトを格納する
            logger.info(f'【中間DB】成績 データ登録・更新完了(受信ファイル:{datfilename})')


    # M40系 事故情報
    if datDataFileFlg in get_souce_list('M40'):
        # 事故情報のデータの場合

        # 【中間DB】事故情報
        # 登録DB: M40
        edit_mddb_list = Trn_M40_jiko().insert_or_update_M40_jiko(datfilename,datDataFileFlg)
        if not edit_mddb_list:
            if edit_mddb_list == []:
                Common_log.Out_Logs(log_info_msg_id, [f'【中間DB】事故情報 登録なし  {datfilename}'])
                logger.info(f'【中間DB】事故情報 登録なし  {datfilename}')
            else:
                Common_log.Out_Logs(log_info_msg_id, [f'【中間DB】事故情報 登録処理失敗  {datfilename}'])
                logger.error(f'【中間DB】事故情報 登録処理失敗  {datfilename}')
            error_mddb_code_list.append('M40')
        else:
            # 中間DB登録がOKだった場合
            mddb_list.extend(edit_mddb_list) # 編集済みmddbオブジェクトを格納する
            logger.info(f'【中間DB】事故情報 データ登録・更新完了(受信ファイル:{datfilename})')


    # M50 入場・人員
    if datDataFileFlg in get_souce_list('M50'):
        # 入場・人員のデータの場合

        # 【中間DB】入場・人員
        # 登録DB: M50
        edit_mddb_list = Trn_M50_nyujo().insert_or_update_M50_nyujo(datfilename,datDataFileFlg)
        if not edit_mddb_list:
            Common_log.Out_Logs(log_err_msg_id, [f'【中間DB】入場・人員 登録処理失敗  {datfilename}'])
            logger.error(f'【中間DB】入場・人員 登録処理失敗  {datfilename}')
            error_mddb_code_list.append('M50')
        else:
            # 中間DB登録がOKだった場合
            mddb_list.extend(edit_mddb_list) # 編集済みmddbオブジェクトを格納する
            logger.info(f'【中間DB】入場・人員 データ登録・更新完了(受信ファイル:{datfilename})')


    # M60 ラップ
    if datDataFileFlg in get_souce_list('M60'):
        # ラップのデータの場合

        # 【中間DB】ラップ
        # 登録DB: M60
        edit_mddb_list = Trn_M60_rap().insert_or_update_M60_rap(datfilename,datDataFileFlg)
        if not edit_mddb_list:
            Common_log.Out_Logs(log_err_msg_id, [f'【中間DB】ラップ 登録処理失敗  {datfilename}'])
            logger.error(f'【中間DB】ラップ 登録処理失敗  {datfilename}')
            error_mddb_code_list.append('M60')
        else:
            # 中間DB登録がOKだった場合
            mddb_list.extend(edit_mddb_list) # 編集済みmddbオブジェクトを格納する
            logger.info(f'【中間DB】ラップ データ登録・更新完了(受信ファイル:{datfilename})')


    # M70 通信文
    if datDataFileFlg in get_souce_list('M70'):
        # 通信文のデータの場合

        # 【中間DB】通信文
        # 登録DB: M70
        edit_mddb_list = Trn_M70_tsuusin().insert_or_update_M70_tsuusin(datfilename,datDataFileFlg)
        if not edit_mddb_list:
            if edit_mddb_list == []:
                Common_log.Out_Logs(log_info_msg_id, [f'【中間DB】通信文 登録なし  {datfilename}'])
                logger.info(f'【中間DB】通信文 登録なし  {datfilename}')
            else:
                Common_log.Out_Logs(log_info_msg_id, [f'【中間DB】通信文 登録処理失敗  {datfilename}'])
                logger.error(f'【中間DB】通信文 登録処理失敗  {datfilename}')
                error_mddb_code_list.append('M70')
        else:
            # 中間DB登録がOKだった場合
            mddb_list.extend(edit_mddb_list) # 編集済みmddbオブジェクトを格納する
            logger.info(f'【中間DB】通信文 データ登録・更新完了(受信ファイル:{datfilename})')


    # M80 調教師ベスト３０
    if datDataFileFlg in get_souce_list('M80'):
        # 調教師ベスト３０のデータの場合

        # 【中間DB】調教師ベスト３０
        # 登録DB: M80
        edit_mddb_list = Trn_M80_choukyou_best30().insert_or_update_M80_choukyou_best30(datfilename,datDataFileFlg)
        if not edit_mddb_list:
            if edit_mddb_list == []:
                Common_log.Out_Logs(log_info_msg_id, [f'【中間DB】調教師ベスト３０ 登録なし  {datfilename}'])
                logger.info(f'【中間DB】調教師ベスト３０ 登録なし  {datfilename}')
            else:
                Common_log.Out_Logs(log_info_msg_id, [f'【中間DB】調教師ベスト３０ 登録処理失敗  {datfilename}'])
                logger.error(f'【中間DB】調教師ベスト３０ 登録処理失敗  {datfilename}')
                error_mddb_code_list.append('M80')
        else:
            # 中間DB登録がOKだった場合
            logger.info(f'【中間DB】調教師ベスト３０ データ登録・更新完了(受信ファイル:{datfilename})')
            if not edit_mddb_list.count() < 90:
                # M80が全部（関東30以上/関西30以上/全国30以上）揃っていたら、mddb_listに追加して中間DBを完了にする
                mddb_list.extend([edit_mddb_list.last()]) # 編集済みmddbオブジェクトを格納する


    # M90 騎手ベスト３０
    if datDataFileFlg in get_souce_list('M90'):
        # 騎手ベスト３０のデータの場合

        # 【中間DB】騎手ベスト３０
        # 登録DB: M90
        edit_mddb_list = Trn_M90_kishu_best30().insert_or_update_M90_kishu_best30(datfilename,datDataFileFlg)
        if not edit_mddb_list:
            if edit_mddb_list == []:
                Common_log.Out_Logs(log_info_msg_id, [f'【中間DB】騎手ベスト３０ 登録なし  {datfilename}'])
                logger.info(f'【中間DB】騎手ベスト３０ 登録なし  {datfilename}')
            else:
                Common_log.Out_Logs(log_info_msg_id, [f'【中間DB】騎手ベスト３０ 登録処理失敗  {datfilename}'])
                logger.error(f'【中間DB】騎手ベスト３０ 登録処理失敗  {datfilename}')
                error_mddb_code_list.append('M90')
        else:
            # 中間DB登録がOKだった場合
            logger.info(f'【中間DB】騎手ベスト３０ データ登録・更新完了(受信ファイル:{datfilename})')
            if not edit_mddb_list.count() < 90:
                # M90が全部（関東30以上/関西30以上/全国30以上）揃っていたら、mddb_listに追加して中間DBを完了にする
                mddb_list.extend([edit_mddb_list.last()]) # 編集済みmddbオブジェクトを格納する


    # M100 特別登録馬
    if datDataFileFlg in get_souce_list('M100'):
        # 特別登録馬のデータの場合

        # 【中間DB】特別登録馬
        # 登録DB: M100
        edit_mddb_list = Trn_M100_tokubetsutouroku().insert_or_update_M100_tokubetsutouroku(datfilename,datDataFileFlg)
        if not edit_mddb_list:
            if edit_mddb_list == []:
                Common_log.Out_Logs(log_info_msg_id, [f'【中間DB】特別登録馬 登録なし  {datfilename}'])
                logger.info(f'【中間DB】特別登録馬 登録なし  {datfilename}')
            else:
                Common_log.Out_Logs(log_info_msg_id, [f'【中間DB】特別登録馬 登録処理失敗  {datfilename}'])
                logger.error(f'【中間DB】特別登録馬 登録処理失敗  {datfilename}')
                error_mddb_code_list.append('M100')
        else:
            # 中間DB登録がOKだった場合
            mddb_list.extend(edit_mddb_list) # 編集済みmddbオブジェクトを格納する
            logger.info(f'【中間DB】特別登録馬 データ登録・更新完了(受信ファイル:{datfilename})')


    # M111系 単勝前売オッズ
    if datDataFileFlg in get_souce_list('M111'):
        # 単勝前売オッズ系のデータの場合

        # 【中間DB】単勝前売オッズ
        # 登録DB: M111
        edit_mddb_list = Trn_M111_odds_tan_fuku().insert_or_update_M111_odds_tan_fuku(datfilename,datDataFileFlg)
        if not edit_mddb_list:
            if edit_mddb_list == []:
                Common_log.Out_Logs(log_info_msg_id, [f'【中間DB】単勝前売オッズ 登録なし  {datfilename}'])
                logger.info(f'【中間DB】単勝前売オッズ 登録なし  {datfilename}')
            else:
                Common_log.Out_Logs(log_info_msg_id, [f'【中間DB】単勝前売オッズ 登録処理失敗  {datfilename}'])
                logger.error(f'【中間DB】単勝前売オッズ 登録処理失敗  {datfilename}')
                error_mddb_code_list.append('M111')
        else:
            # 中間DB登録がOKだった場合
            mddb_list.extend(edit_mddb_list) # 編集済みmddbオブジェクトを格納する
            logger.info(f'【中間DB】単勝前売オッズ データ登録・更新完了(受信ファイル:{datfilename})')

    # M112 枠連オッズ
    if datDataFileFlg in get_souce_list('M112'):
        # 枠連オッズ系のデータの場合

        # 【中間DB】枠連オッズ
        # 登録DB: M112
        edit_mddb_list = Trn_M112_odds_wakuren().insert_or_update_M112_odds_wakuren(datfilename,datDataFileFlg)
        if not edit_mddb_list:
            if edit_mddb_list == []:
                Common_log.Out_Logs(log_info_msg_id, [f'【中間DB】枠連オッズ 登録なし  {datfilename}'])
                logger.info(f'【中間DB】枠連オッズ 登録なし  {datfilename}')
            else:
                Common_log.Out_Logs(log_info_msg_id, [f'【中間DB】枠連オッズ 登録処理失敗  {datfilename}'])
                logger.error(f'【中間DB】枠連オッズ 登録処理失敗  {datfilename}')
                error_mddb_code_list.append('M112')
        else:
            # 中間DB登録がOKだった場合
            mddb_list.extend(edit_mddb_list) # 編集済みmddbオブジェクトを格納する
            logger.info(f'【中間DB】枠連オッズ データ登録・更新完了(受信ファイル:{datfilename})')

    
    # M113 馬連＋ワイドオッズ
    if datDataFileFlg in get_souce_list('M113'):
        # 馬連＋ワイドオッズ系のデータの場合

        # 【中間DB】馬連＋ワイドオッズ
        # 登録DB: M113
        edit_mddb_list = Trn_M113_odds_umaren_wide().insert_or_update_M113_odds_umaren_wide(datfilename,datDataFileFlg)
        if not edit_mddb_list:
            if edit_mddb_list == []:
                Common_log.Out_Logs(log_info_msg_id, [f'【中間DB】馬連＋ワイドオッズ 登録なし  {datfilename}'])
                logger.info(f'【中間DB】馬連＋ワイドオッズ 登録なし  {datfilename}')
            else:
                Common_log.Out_Logs(log_info_msg_id, [f'【中間DB】馬連＋ワイドオッズ 登録処理失敗  {datfilename}'])
                logger.error(f'【中間DB】馬連＋ワイドオッズ 登録処理失敗  {datfilename}')
                error_mddb_code_list.append('M113')
        else:
            # 中間DB登録がOKだった場合
            mddb_list.extend(edit_mddb_list) # 編集済みmddbオブジェクトを格納する
            logger.info(f'【中間DB】馬連＋ワイドオッズ データ登録・更新完了(受信ファイル:{datfilename})')

    
    # M114 馬単オッズ
    if datDataFileFlg in get_souce_list('M114'):
        # 馬単オッズ系のデータの場合

        # 【中間DB】馬単オッズ
        # 登録DB: M114
        edit_mddb_list = Trn_M114_odds_umatan().insert_or_update_M114_odds_umatan(datfilename,datDataFileFlg)
        if not edit_mddb_list:
            if edit_mddb_list == []:
                Common_log.Out_Logs(log_info_msg_id, [f'【中間DB】馬単オッズ 登録なし  {datfilename}'])
                logger.info(f'【中間DB】馬単オッズ 登録なし  {datfilename}')
            else:
                Common_log.Out_Logs(log_info_msg_id, [f'【中間DB】馬単オッズ 登録処理失敗  {datfilename}'])
                logger.error(f'【中間DB】馬単オッズ 登録処理失敗  {datfilename}')
                error_mddb_code_list.append('M114')
        else:
            # 中間DB登録がOKだった場合
            mddb_list.extend(edit_mddb_list) # 編集済みmddbオブジェクトを格納する
            logger.info(f'【中間DB】馬単オッズ データ登録・更新完了(受信ファイル:{datfilename})')

    # M115 ３連複オッズ
    if datDataFileFlg in get_souce_list('M115'):
        # ３連複オッズ系のデータの場合

        # 【中間DB】３連複オッズ
        # 登録DB: M115
        edit_mddb_list = Trn_M115_odds_sanpuku().insert_or_update_M115_odds_sanpuku(datfilename,datDataFileFlg)
        if not edit_mddb_list:
            if edit_mddb_list == []:
                Common_log.Out_Logs(log_info_msg_id, [f'【中間DB】３連複オッズ 登録なし  {datfilename}'])
                logger.info(f'【中間DB】３連複オッズ 登録なし  {datfilename}')
            else:
                Common_log.Out_Logs(log_info_msg_id, [f'【中間DB】３連複オッズ 登録処理失敗  {datfilename}'])
                logger.error(f'【中間DB】３連複オッズ 登録処理失敗  {datfilename}')
                error_mddb_code_list.append('M115')
        else:
            # 中間DB登録がOKだった場合
            mddb_list.extend(edit_mddb_list) # 編集済みmddbオブジェクトを格納する
            logger.info(f'【中間DB】３連複オッズ データ登録・更新完了(受信ファイル:{datfilename})')

    # M116 ３連単オッズ
    if datDataFileFlg in get_souce_list('M116'):
        # ３連単オッズ系のデータの場合

        # 【中間DB】３連単オッズ
        # 登録DB: M116
        edit_mddb_list = Trn_M116_odds_santan().insert_or_update_M116_odds_santan(datfilename,datDataFileFlg)
        if not edit_mddb_list:
            if edit_mddb_list == []:
                Common_log.Out_Logs(log_info_msg_id, [f'【中間DB】３連単オッズ 登録なし  {datfilename}'])
                logger.info(f'【中間DB】３連単オッズ 登録なし  {datfilename}')
            else:
                Common_log.Out_Logs(log_info_msg_id, [f'【中間DB】３連単オッズ 登録処理失敗  {datfilename}'])
                logger.error(f'【中間DB】３連単オッズ 登録処理失敗  {datfilename}')
                error_mddb_code_list.append('M116')
        else:
            # 中間DB登録がOKだった場合
            mddb_list.extend(edit_mddb_list) # 編集済みmddbオブジェクトを格納する
            logger.info(f'【中間DB】３連単オッズ データ登録・更新完了(受信ファイル:{datfilename})')


    # M120 馬別上がりタイム
    if datDataFileFlg in get_souce_list('M120'):
        # 馬別上がりタイムのデータの場合

        # 【中間DB】馬別上がりタイム
        # 登録DB: M120
        edit_mddb_list = Trn_M120_agari().insert_or_update_M120_agari(datfilename,datDataFileFlg)
        if not edit_mddb_list:
            if edit_mddb_list == []:
                Common_log.Out_Logs(log_info_msg_id, [f'【中間DB】馬別上がりタイム 登録なし  {datfilename}'])
                logger.info(f'【中間DB】馬別上がりタイム 登録なし  {datfilename}')
            else:
                Common_log.Out_Logs(log_info_msg_id, [f'【中間DB】馬別上がりタイム 登録処理失敗  {datfilename}'])
                logger.error(f'【中間DB】馬別上がりタイム 登録処理失敗  {datfilename}')
                error_mddb_code_list.append('M120')
        else:
            # 中間DB登録がOKだった場合
            mddb_list.extend(edit_mddb_list) # 編集済みmddbオブジェクトを格納する
            logger.info(f'【中間DB】馬別上がりタイム データ登録・更新完了(受信ファイル:{datfilename})')


    # M130 ＷＩＮ５発売要項
    if datDataFileFlg in get_souce_list('M130'):
        # ＷＩＮ５発売要項のデータの場合

        # 【中間DB】ＷＩＮ５発売要項
        # 登録DB: M130
        edit_mddb_list = Trn_M130_win5_youkou().insert_or_update_M130_win5_youkou(datfilename,datDataFileFlg)
        if not edit_mddb_list:
            if edit_mddb_list == []:
                Common_log.Out_Logs(log_info_msg_id, [f'【中間DB】ＷＩＮ５発売要項 登録なし  {datfilename}'])
                logger.info(f'【中間DB】ＷＩＮ５発売要項 登録なし  {datfilename}')
            else:
                Common_log.Out_Logs(log_info_msg_id, [f'【中間DB】ＷＩＮ５発売要項 登録処理失敗  {datfilename}'])
                logger.error(f'【中間DB】ＷＩＮ５発売要項 登録処理失敗  {datfilename}')
                error_mddb_code_list.append('M130')
        else:
            # 中間DB登録がOKだった場合
            mddb_list.extend(edit_mddb_list) # 編集済みmddbオブジェクトを格納する
            logger.info(f'【中間DB】ＷＩＮ５発売要項 データ登録・更新完了(受信ファイル:{datfilename})')


    # M140 ＷＩＮ５払戻金
    if datDataFileFlg in get_souce_list('M140'):
        # ＷＩＮ５払戻金のデータの場合

        # 【中間DB】ＷＩＮ５払戻金
        # 登録DB: M140
        edit_mddb_list = Trn_M140_win5_hara().insert_or_update_M140_win5_hara(datfilename,datDataFileFlg)
        if not edit_mddb_list:
            if edit_mddb_list == []:
                Common_log.Out_Logs(log_info_msg_id, [f'【中間DB】ＷＩＮ５払戻金 登録なし  {datfilename}'])
                logger.info(f'【中間DB】ＷＩＮ５払戻金 登録なし  {datfilename}')
            else:
                Common_log.Out_Logs(log_info_msg_id, [f'【中間DB】ＷＩＮ５払戻金 登録処理失敗  {datfilename}'])
                logger.error(f'【中間DB】ＷＩＮ５払戻金 登録処理失敗  {datfilename}')
                error_mddb_code_list.append('M140')
        else:
            # 中間DB登録がOKだった場合
            mddb_list.extend(edit_mddb_list) # 編集済みmddbオブジェクトを格納する
            logger.info(f'【中間DB】ＷＩＮ５発売要項 データ登録・更新完了(受信ファイル:{datfilename})')

    
    # M150 上位人気オッズ
    if datDataFileFlg in get_souce_list('M150'):
        # 上位人気オッズ系のデータの場合

        # 【中間DB】上位人気オッズ
        # 登録DB: M150
        edit_mddb_list = Trn_M150_joui_odds().insert_or_update_M150_joui_odds(datfilename,datDataFileFlg)
        if not edit_mddb_list:
            if edit_mddb_list == []:
                Common_log.Out_Logs(log_info_msg_id, [f'【中間DB】上位人気オッズ 登録なし  {datfilename}'])
                logger.info(f'【中間DB】上位人気オッズ 登録なし  {datfilename}')
            else:
                Common_log.Out_Logs(log_info_msg_id, [f'【中間DB】上位人気オッズ 登録処理失敗  {datfilename}'])
                logger.error(f'【中間DB】上位人気オッズ 登録処理失敗  {datfilename}')
                error_mddb_code_list.append('M150')
        else:
            # 中間DB登録がOKだった場合
            mddb_list.extend(edit_mddb_list) # 編集済みmddbオブジェクトを格納する
            logger.info(f'【中間DB】上位人気オッズ データ登録・更新完了(受信ファイル:{datfilename})')

    # M160 終了通知
    if datDataFileFlg in get_souce_list('M160'):
        # 終了通知のデータの場合

        # 【中間DB】終了通知
        # 登録DB: M160
        edit_mddb_list = Trn_M160_shuryo().insert_or_update_M160_shuryo(datfilename,datDataFileFlg)
        if not edit_mddb_list:
            if edit_mddb_list == []:
                Common_log.Out_Logs(log_info_msg_id, [f'【中間DB】終了通知 登録なし  {datfilename}'])
                logger.info(f'【中間DB】終了通知 登録なし  {datfilename}')
            else:
                Common_log.Out_Logs(log_info_msg_id, [f'【中間DB】終了通知 登録処理失敗  {datfilename}'])
                logger.error(f'【中間DB】終了通知 登録処理失敗  {datfilename}')
                error_mddb_code_list.append('M160')
        else:
            # 中間DB登録がOKだった場合
            mddb_list.extend(edit_mddb_list) # 編集済みmddbオブジェクトを格納する
            logger.info(f'【中間DB】終了通知 データ登録・更新完了(受信ファイル:{datfilename})')

    if mddb_list:
        # ここで受信ファイルの受信ステータスに中間ＤＢ登録済みフラグを立てる
        if Tran_ReceiveStatus.objects.filter(Receive_filename=datfilename).exists():
            for receivestatus in Tran_ReceiveStatus.objects.filter(Receive_filename=datfilename):
                receivestatus.Mddb_registered_flg = True
                receivestatus.save()

        # 中間DBの更新が完了したら、受信ファイルに紐づく中間DBステータスを更新する
        if Trn_MdDBStatus().update_Trn_MdDBStatus(datfilename, datDataFileFlg, MDDBSTATUSCREATED, error_mddb_code_list):
            logger.info('【中間DB状態管理テーブル】レコード更新完了(登録済)')

    # 最後に、後続処理に編集済み中間DB情報を渡す
    return mddb_list



def check_mddbstatus(tran_mddbstatus_list):
    matiawase_flg = True
    # コンテンツ出力に必要な中間DBのステータステーブルがなかったり、一つでも未完や未登録がある場合は、コンテンツ出力を実施しない
    if tran_mddbstatus_list is None:
        matiawase_flg = False
    elif not tran_mddbstatus_list.exists():
        matiawase_flg = False
    else:
        for tran_mddbstatus in tran_mddbstatus_list:
            if tran_mddbstatus.Md_db_status == MDDBSTATUSUNFINISHED or tran_mddbstatus.Md_db_status == MDDBSTATUSUNREGISTERED:
                matiawase_flg = False

    return matiawase_flg

# @shared_task
def subscribe_message(message):
    try:
        logger.info(f'=====================================================================================================================================================')
        # datファイルの名前
        datfilename = message
        datDataFileFlg = Common().checkDatData(datfilename)
        
        # 【１．中間DB登録、中間DB登録状態更新】
        # edit_mddb_obj_list：編集した中間DBオブジェクトのリスト
        edit_mddb_obj_list = run_mddb_proc(datfilename, datDataFileFlg)
        if not edit_mddb_obj_list:
            # 登録対象がなければ処理終了
            return

        # 【２．待ち合わせ判定】
        # 待ち合わせの条件が全てクリアされたら次の配信処理へ、それ以外は処理終了
        logger.info(f'作成/修正した中間DBレコード {edit_mddb_obj_list}')
        for mddb_obj in edit_mddb_obj_list:
            output_newsml_objs = []

            # 更新された中間DBの種類から、発行するNewsMLを取得する
            mddb_name = str(type(mddb_obj).__name__)
            if '_' in mddb_name:
                mddb_code = mddb_name.split('_')[0]
                mddb_code_obj = Mst_MdDB_Code.objects.get(MdDB_code=mddb_code)
                output_newsml_objs = Mst_NewsML_code.objects.filter(Souce_MdDB=mddb_code_obj)

                # mddb_obj : M10で、受信ファイルが「FSIN出走馬名表」の場合、output_newsml_objs : F02想定出馬表 とする
                if datDataFileFlg == SYUSSOUBA_MEIHYO and mddb_code == 'M10':
                    mddb_code_obj = Mst_MdDB_Code.objects.get(MdDB_code=mddb_code + 's') # 想定出馬表中間DBコードに変換
                    output_newsml_objs = Mst_NewsML_code.objects.filter(Souce_MdDB=mddb_code_obj)
            
            # オッズ系のコンテンツ出力処理分岐
            if hasattr(mddb_obj, 'm110'):
                mddb_obj = mddb_obj.m110
                if mddb_obj.maeuri_flg and not mddb_code == 'M111' and not datDataFileFlg == HAYAMI_HYOU:
                    # 早見表を除くファイルでオッズ系中間DBが「前売」で更新された場合、単勝以外の出力オッズコンテンツを、前売とする
                    mddb_code_obj = Mst_MdDB_Code.objects.get(MdDB_code=mddb_code + 'm') # 前売オッズ系中間DBコードに変換
                    output_newsml_objs = Mst_NewsML_code.objects.filter(Souce_MdDB=mddb_code_obj)
                if not mddb_obj.maeuri_flg and mddb_code == 'M111':
                    # 更新対象のM111に紐づくM110の前売フラグが確定に更新されたときは、F15単勝前売オッズは出力しない
                    output_newsml_objs = output_newsml_objs.exclude(NewsML_code=NewsML_ODDS_TAN)

            # 待ち合わせ 出力対象のコンテンツごとに、出力に必要な中間DBのステータスが完了になっているかを確認する。
            for newsml_obj in output_newsml_objs:

                # 待ち合わせフラグ初期値
                matiawase_flg = True
                # 出力コンテンツのNewsMLオブジェクト取得
                newsml_code = newsml_obj.NewsML_code
                # 1コンテンツあたりの出力単位 1:レースごと、2:場ごと、3:日ごと、4:速報
                output_type = newsml_obj.Output_type
                # 出力コンテンツの元データとなる中間DBオブジェクト
                souce_mddb_obj_list = newsml_obj.Souce_MdDB.all()

                # 基本情報
                kaisaibi = mddb_obj.kaisaibi
                jou_obj = mddb_obj.joumei if hasattr(mddb_obj, 'joumei') else None
                rebangou = mddb_obj.rebangou if hasattr(mddb_obj, 'rebangou') else None

                # 成績表Aなど場ごとのコンテンツの場合、レース数を取得してracesuuに格納する
                racesuu = None
                if output_type == OUTPUT_TYPE_JOU:
                    # まずスケジュールマスタを参照して、当日場のレース数を取得。全レース分データを登録していることを確認する
                    if Mst_Schedule.objects.filter(Date=kaisaibi,Jou=mddb_obj.joumei).exists():
                        racesuu = Mst_Schedule.objects.filter(Date=kaisaibi,Jou=mddb_obj.joumei).last().Racesuu
                    if not racesuu:
                        # レース数が取れなければ待ち合わせ失敗
                        logger.info(f'待ち合わせNG {newsml_obj.NewsML_name} レース数を取得できません。スケジュールマスタを登録してください')
                        matiawase_flg = False
            
                # コンテンツ出力に必要な中間DBのステータステーブルを確認
                logger.info(f'{newsml_code} {newsml_obj.NewsML_name} に出力に必要な中間DB {souce_mddb_obj_list}')
                for souce_mddb_obj in souce_mddb_obj_list:
                    tran_mddbstatus_list = None
                    if souce_mddb_obj.Unit_type == 1: # レースごとの中間DBの場合
                        if racesuu:
                            # 全レース分の中間DBが必要なコンテンツの場合、全レース分チェックする
                            for i in range(racesuu):
                                if not Tran_MdDBStatus.objects.filter(
                                    Kaisai_date = kaisaibi,
                                    Jou_code = jou_obj,
                                    Md_db_code = souce_mddb_obj,
                                    Race_bangou = i + 1
                                    ).exists():
                                    # ステータス登録が無い場合、その時点で待ち合わせ失敗
                                    matiawase_flg = False
                            tran_mddbstatus_list = Tran_MdDBStatus.objects.filter(
                                Kaisai_date = kaisaibi,
                                Jou_code = jou_obj,
                                Md_db_code = souce_mddb_obj
                                # rebangouは指定しない（登録されている全レース分取得する）
                            )
                        else:
                            tran_mddbstatus_list = Tran_MdDBStatus.objects.filter(
                                Kaisai_date = kaisaibi,
                                Jou_code = jou_obj,
                                Md_db_code = souce_mddb_obj,
                                Race_bangou = rebangou
                            )
                    elif souce_mddb_obj.Unit_type == 2: # 場ごとの中間DBの場合
                        tran_mddbstatus_list = Tran_MdDBStatus.objects.filter(
                            Kaisai_date = kaisaibi,
                            Jou_code = jou_obj,
                            Md_db_code = souce_mddb_obj
                        )
                    elif souce_mddb_obj.Unit_type == 3: # 日ごとの中間DBの場合
                        tran_mddbstatus_list = Tran_MdDBStatus.objects.filter(
                            Kaisai_date = kaisaibi,
                            Md_db_code = souce_mddb_obj
                        )
                    if not check_mddbstatus(tran_mddbstatus_list):
                        matiawase_flg = False

                # F27 WIN5出馬表は、「重勝式発走順」の入力が必須
                if newsml_code == NewsML_WIN5_SHUTSUBAHYO:
                    if hasattr(mddb_obj, 'juushouhassoujun'):
                        if not mddb_obj.juushouhassoujun:
                            matiawase_flg = False

                # 成績速報、事故情報速報、前売りオッズなど速報系のコンテンツの場合
                # if output_type == OUTPUT_TYPE_SOKUHOU:
                #     pass
                
                # 後続処理用にfile_datalistを用意
                joucode = jou_obj.Jou_code if jou_obj else None

                if newsml_obj.Output_type == 1:
                    file_datalist = [str(kaisaibi), joucode, rebangou]
                elif newsml_obj.Output_type == 2:
                    file_datalist = [str(kaisaibi), joucode, None]
                elif newsml_obj.Output_type == 3:
                    file_datalist = [str(kaisaibi), None, None]

                # ログ
                jou_n = ' ' + mddb_obj.joumei.Jou_name if file_datalist[1] else ''
                re_n = ' ' + str(mddb_obj.rebangou) + 'R' if file_datalist[2] else ''
                logger.info(f'■■■ 待ち合わせ結果 {newsml_code} {newsml_obj.NewsML_name}({file_datalist[0]}{jou_n}{re_n}) 【{matiawase_flg}】 ■■■')

                # 待ち合わせ失敗時は、対象のコンテンツの送信管理ステータスを未完で更新して終了
                if matiawase_flg == False:
                    # 受信ファイルが「I204翌日出馬表」で、「F15単勝前売オッズ」の待ち合わせ失敗時は、送信管理ステータスを作成しない(基本的に出力しないので)
                    if datDataFileFlg == YOKUJITSU_SYUTSUBA_HYOU and newsml_code == 'F15':
                        pass
                    else:
                        SubmitStatus().Update(None, status=SUBMIT_STATUS_MIKAN, newsno=newsml_code, file_datalist=file_datalist)

                # 待ち合わせOKなら、レース情報を後続の配信処理に渡す
                else:
                    # 後続処理用にTran_SubmitStatusを用意
                    sub_obj = None
                    if Tran_SubmitStatus.objects.filter(Kaisai_date=kaisaibi,Jou_code=jou_obj,Race_bangou=rebangou,NewsML_code=newsml_obj).exists():
                        sub_obj = Tran_SubmitStatus.objects.filter(Kaisai_date=kaisaibi, Jou_code=jou_obj, Race_bangou=rebangou, NewsML_code=newsml_obj).last()
                    else:
                        sub_obj = SubmitStatus().Update(None, newsno=newsml_code, file_datalist=file_datalist)

                    # 【３．配信処理へ連携】
                    # システム状態 運用モード管理の確認
                    # オフライン時は作成止めとする。
                    tran_system = Tran_Systemstatus.objects.all().first()
                    unyou_date = tran_system.Unyou_date
                    autosubmit_mode = True
                    if tran_system:
                        if OPERATIONMODEON == tran_system.Operationmode.Operationmode_code:
                            delivery_or_edit_status = DELIVERY_STATUS
                        elif OPERATIONMODEOFF == tran_system.Operationmode.Operationmode_code:
                            delivery_or_edit_status = EDIT_STATUS
                    # 成績速報と事故速報は自動送信しない（作成のみ）
                    if newsml_code in [NewsML_SEISEKIHYO_SOKUHOU, NewsML_JIKO_SOKUHOU]:
                        delivery_or_edit_status = EDIT_STATUS
                        autosubmit_mode = False # 自動送信：停止

                    if sub_obj:
                        # TODO ★テスト出力時はコメント
                        # kaisaibi = sub_obj.Kaisai_date
                        # if not str(kaisaibi) == str(unyou_date + timedelta(days=1)):
                        #     # 出走日が運用日の翌日以外のときは、作成止めとする。
                        #     delivery_or_edit_status = EDIT_STATUS
    
                        logger.info(f'~~~~~~~~~~~~~~~~~~ ★NewsML編集・配信処理を実行 {sub_obj} ~~~~~~~~~~~~~~~~~~')
                        
                        # ★NewsML編集・配信処理を実行
                        if newsml_code in [NewsML_ODDS_SANTAN_MAEURI, NewsML_ODDS_SANTAN_KAKUTEI]:
                            # 3連単オッズ系の場合、出走頭数の数だけコンテンツ出力が発生する
                            if hasattr(mddb_obj, 'shusuu'):
                                for num in range(mddb_obj.shusuu):
                                    sanrentan_umaban = num + 1

                                    Output_NewsML(
                                        logger,
                                        delivery_or_edit_status,
                                        AUTODELI,
                                        sub_obj=sub_obj,
                                        receive_filename=datfilename,
                                        autosubmit_mode=autosubmit_mode,
                                        sanrentan_umaban=sanrentan_umaban
                                        ).Main()
                            else:
                                logger.info(f'{newsml_obj.NewsML_name} 出力NG 出走頭数を取得できません')
                        else:
                            # それ以外は、一つ分のコンテンツを出力する
                            Output_NewsML(
                                logger,
                                delivery_or_edit_status,
                                AUTODELI,
                                sub_obj=sub_obj,
                                receive_filename=datfilename,
                                autosubmit_mode=autosubmit_mode
                                ).Main()

                        logger.info(f'~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')

    except Exception as e:
        Common_log.Out_Logs(log_err_msg_id, [e])
        logger.error(failure(e))
