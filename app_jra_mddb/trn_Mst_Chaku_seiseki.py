from logging import getLogger
import re
from app_jra.consts import *
import sys
import linecache
from app_jra.log_commons import *
from datetime import datetime as dt
from django.db import transaction


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

class trn_Mst_Chaku_seiseki():

    def chk_blank_zero(self, record):
        # 受信データにはブランクのパターンがいくつかあるが、ゼロの場合と空データの場合は全てブランクとみなす処理
        blank_list = ['','　',None,[],{},0,'0','00','０','００']
        if record in blank_list or re.fullmatch('\s+', record):
            return False
        else:
            return True


    def insert_or_update_Mst_Chaku_seiseki(self, datfilename, datDataFileFlg):
        try:
            from app_jra.models import (
                Trn_UMAR_1_Kyousouba_seiseki,
                Trn_UMAR_2_Kyousouba_seiseki_kakutei,
                Mst_Chaku_seiseki
            )
            Cmn = Common()
            update_num = 0

            if KYOUSOUBA_SEISEKI_KAKUTEI == datDataFileFlg:
                # UMAR 競走馬成績（成績確定）
                UMAR_list = Trn_UMAR_1_Kyousouba_seiseki.objects.filter(Receive_filename=datfilename)
                for UMAR_1_data in UMAR_list:
                    UMAR_2_list = Trn_UMAR_2_Kyousouba_seiseki_kakutei.objects.filter(UMAR_1=UMAR_1_data)
                    for data in UMAR_2_list:
                        if self.chk_blank_zero(data.Ketto_touroku_number):
                            update_dict = {
                                'all_1' : int(data.Course_terf_right_kaisuu1) + int(data.Course_terf_left_kaisuu1) + int(data.Course_dert_right_kaisuu1) + int(data.Course_dert_left_kaisuu1) + int(data.Course_terf_choku_kaisuu1) + int(data.Course_dert_choku_kaisuu1),
                                'all_2' : int(data.Course_terf_right_kaisuu2) + int(data.Course_terf_left_kaisuu2) + int(data.Course_dert_right_kaisuu2) + int(data.Course_dert_left_kaisuu2) + int(data.Course_terf_choku_kaisuu2) + int(data.Course_dert_choku_kaisuu2),
                                'all_3' : int(data.Course_terf_right_kaisuu3) + int(data.Course_terf_left_kaisuu3) + int(data.Course_dert_right_kaisuu3) + int(data.Course_dert_left_kaisuu3) + int(data.Course_terf_choku_kaisuu3) + int(data.Course_dert_choku_kaisuu3),
                                'all_g' : int(data.Course_terf_right_kaisuu4) + int(data.Course_terf_left_kaisuu4) + int(data.Course_dert_right_kaisuu4) + int(data.Course_dert_left_kaisuu4) + int(data.Course_terf_choku_kaisuu4) + int(data.Course_dert_choku_kaisuu4),
                                'sg_1' : int(data.Course_syougai_choku_kaisuu1),
                                'sg_2' : int(data.Course_syougai_choku_kaisuu2),
                                'sg_3' : int(data.Course_syougai_choku_kaisuu3),
                                'sg_g' : int(data.Course_syougai_choku_kaisuu4),
                                # コース別の条件
                                's_R_1' : int(data.Course_terf_right_kaisuu1),
                                's_R_2' : int(data.Course_terf_right_kaisuu2),
                                's_R_3' : int(data.Course_terf_right_kaisuu3),
                                's_R_g' : int(data.Course_terf_right_kaisuu4),
                                's_L_1' : int(data.Course_terf_left_kaisuu1),
                                's_L_2' : int(data.Course_terf_left_kaisuu2),
                                's_L_3' : int(data.Course_terf_left_kaisuu3),
                                's_L_g' : int(data.Course_terf_left_kaisuu4),
                                'd_R_1' : int(data.Course_dert_right_kaisuu1),
                                'd_R_2' : int(data.Course_dert_right_kaisuu2),
                                'd_R_3' : int(data.Course_dert_right_kaisuu3),
                                'd_R_g' : int(data.Course_dert_right_kaisuu4),
                                'd_L_1' : int(data.Course_dert_left_kaisuu1),
                                'd_L_2' : int(data.Course_dert_left_kaisuu2),
                                'd_L_3' : int(data.Course_dert_left_kaisuu3),
                                'd_L_g' : int(data.Course_dert_left_kaisuu4),
                                's_S_1' : int(data.Course_terf_choku_kaisuu1),
                                's_S_2' : int(data.Course_terf_choku_kaisuu2),
                                's_S_3' : int(data.Course_terf_choku_kaisuu3),
                                's_S_g' : int(data.Course_terf_choku_kaisuu4),
                                'd_S_1' : int(data.Course_dert_choku_kaisuu1),
                                'd_S_2' : int(data.Course_dert_choku_kaisuu2),
                                'd_S_3' : int(data.Course_dert_choku_kaisuu3),
                                'd_S_g' : int(data.Course_dert_choku_kaisuu4),
                                # 馬場状態の条件
                                'A_1' : int(data.Baba_terf_ryou_kaisuu1) + int(data.Baba_dert_ryou_kaisuu1) + int(data.Baba_shougai_ryou_kaisuu1),
                                'A_2' : int(data.Baba_terf_ryou_kaisuu2) + int(data.Baba_dert_ryou_kaisuu2) + int(data.Baba_shougai_ryou_kaisuu2),
                                'A_3' : int(data.Baba_terf_ryou_kaisuu3) + int(data.Baba_dert_ryou_kaisuu3) + int(data.Baba_shougai_ryou_kaisuu3),
                                'A_g' : int(data.Baba_terf_ryou_kaisuu4) + int(data.Baba_dert_ryou_kaisuu4) + int(data.Baba_shougai_ryou_kaisuu4),
                                'B_1' : int(data.Baba_terf_yayaomo_kaisuu1) + int(data.Baba_dert_yayaomo_kaisuu1) + int(data.Baba_shougai_yayaomo_kaisuu1),
                                'B_2' : int(data.Baba_terf_yayaomo_kaisuu2) + int(data.Baba_dert_yayaomo_kaisuu2) + int(data.Baba_shougai_yayaomo_kaisuu2),
                                'B_3' : int(data.Baba_terf_yayaomo_kaisuu3) + int(data.Baba_dert_yayaomo_kaisuu3) + int(data.Baba_shougai_yayaomo_kaisuu3),
                                'B_g' : int(data.Baba_terf_yayaomo_kaisuu4) + int(data.Baba_dert_yayaomo_kaisuu4) + int(data.Baba_shougai_yayaomo_kaisuu4),
                                'C_1' : int(data.Baba_terf_omo_kaisuu1) + int(data.Baba_dert_omo_kaisuu1) + int(data.Baba_shougai_omo_kaisuu1),
                                'C_2' : int(data.Baba_terf_omo_kaisuu2) + int(data.Baba_dert_omo_kaisuu2) + int(data.Baba_shougai_omo_kaisuu2),
                                'C_3' : int(data.Baba_terf_omo_kaisuu3) + int(data.Baba_dert_omo_kaisuu3) + int(data.Baba_shougai_omo_kaisuu3),
                                'C_g' : int(data.Baba_terf_omo_kaisuu4) + int(data.Baba_dert_omo_kaisuu4) + int(data.Baba_shougai_omo_kaisuu4),
                                'D_1' : int(data.Baba_terf_furyou_kaisuu1) + int(data.Baba_dert_furyou_kaisuu1) + int(data.Baba_shougai_furyou_kaisuu1),
                                'D_2' : int(data.Baba_terf_furyou_kaisuu2) + int(data.Baba_dert_furyou_kaisuu2) + int(data.Baba_shougai_furyou_kaisuu2),
                                'D_3' : int(data.Baba_terf_furyou_kaisuu3) + int(data.Baba_dert_furyou_kaisuu3) + int(data.Baba_shougai_furyou_kaisuu3),
                                'D_g' : int(data.Baba_terf_furyou_kaisuu4) + int(data.Baba_dert_furyou_kaisuu4) + int(data.Baba_shougai_furyou_kaisuu4),
                                # 距離別の条件
                                'S_1' : int(data.Kyori_terf_1000to1300_kaisuu1) + int(data.Kyori_dert_1000to1300_kaisuu1),
                                'S_2' : int(data.Kyori_terf_1000to1300_kaisuu2) + int(data.Kyori_dert_1000to1300_kaisuu2),
                                'S_3' : int(data.Kyori_terf_1000to1300_kaisuu3) + int(data.Kyori_dert_1000to1300_kaisuu3),
                                'S_g' : int(data.Kyori_terf_1000to1300_kaisuu4) + int(data.Kyori_dert_1000to1300_kaisuu4),
                                'M_1' : int(data.Kyori_terf_1301to1899_kaisuu1) + int(data.Kyori_dert_1301to1899_kaisuu1),
                                'M_2' : int(data.Kyori_terf_1301to1899_kaisuu2) + int(data.Kyori_dert_1301to1899_kaisuu2),
                                'M_3' : int(data.Kyori_terf_1301to1899_kaisuu3) + int(data.Kyori_dert_1301to1899_kaisuu3),
                                'M_g' : int(data.Kyori_terf_1301to1899_kaisuu4) + int(data.Kyori_dert_1301to1899_kaisuu4),
                                'I_1' : int(data.Kyori_terf_1900to2100_kaisuu1) + int(data.Kyori_dert_1900to2100_kaisuu1),
                                'I_2' : int(data.Kyori_terf_1900to2100_kaisuu2) + int(data.Kyori_dert_1900to2100_kaisuu2),
                                'I_3' : int(data.Kyori_terf_1900to2100_kaisuu3) + int(data.Kyori_dert_1900to2100_kaisuu3),
                                'I_g' : int(data.Kyori_terf_1900to2100_kaisuu4) + int(data.Kyori_dert_1900to2100_kaisuu4),
                                'L_1' : int(data.Kyori_terf_2101to2700_kaisuu1) + int(data.Kyori_dert_2101to2700_kaisuu1),
                                'L_2' : int(data.Kyori_terf_2101to2700_kaisuu2) + int(data.Kyori_dert_2101to2700_kaisuu2),
                                'L_3' : int(data.Kyori_terf_2101to2700_kaisuu3) + int(data.Kyori_dert_2101to2700_kaisuu3),
                                'L_g' : int(data.Kyori_terf_2101to2700_kaisuu4) + int(data.Kyori_dert_2101to2700_kaisuu4),
                                'E_1' : int(data.Kyori_terf_2701to_kaisuu1) + int(data.Kyori_dert_2701to_kaisuu1),
                                'E_2' : int(data.Kyori_terf_2701to_kaisuu2) + int(data.Kyori_dert_2701to_kaisuu2),
                                'E_3' : int(data.Kyori_terf_2701to_kaisuu3) + int(data.Kyori_dert_2701to_kaisuu3),
                                'E_g' : int(data.Kyori_terf_2701to_kaisuu4) + int(data.Kyori_dert_2701to_kaisuu4),
                                # 人気別の条件
                                'n_1_1' : int(data.Ninkijun_1ban_kaisuu1),
                                'n_1_2' : int(data.Ninkijun_1ban_kaisuu2),
                                'n_1_3' : int(data.Ninkijun_1ban_kaisuu3),
                                'n_1_g' : int(data.Ninkijun_1ban_kaisuu4),
                                'n_2_1' : int(data.Ninkijun_2ban_kaisuu1),
                                'n_2_2' : int(data.Ninkijun_2ban_kaisuu2),
                                'n_2_3' : int(data.Ninkijun_2ban_kaisuu3),
                                'n_2_g' : int(data.Ninkijun_2ban_kaisuu4),
                                'n_3_1' : int(data.Ninkijun_3ban_kaisuu1),
                                'n_3_2' : int(data.Ninkijun_3ban_kaisuu2),
                                'n_3_3' : int(data.Ninkijun_3ban_kaisuu3),
                                'n_3_g' : int(data.Ninkijun_3ban_kaisuu4),
                                'n_4_1' : int(data.Ninkijun_4ban_kaisuu1),
                                'n_4_2' : int(data.Ninkijun_4ban_kaisuu2),
                                'n_4_3' : int(data.Ninkijun_4ban_kaisuu3),
                                'n_4_g' : int(data.Ninkijun_4ban_kaisuu4),
                                'n_5_1' : int(data.Ninkijun_5ban_kaisuu1),
                                'n_5_2' : int(data.Ninkijun_5ban_kaisuu2),
                                'n_5_3' : int(data.Ninkijun_5ban_kaisuu3),
                                'n_5_g' : int(data.Ninkijun_5ban_kaisuu4),
                                'n_6_1' : int(data.Ninkijun_sonota_kaisuu1),
                                'n_6_2' : int(data.Ninkijun_sonota_kaisuu2),
                                'n_6_3' : int(data.Ninkijun_sonota_kaisuu3),
                                'n_6_g' : int(data.Ninkijun_sonota_kaisuu4)
                            }
                            # 更新または新規作成
                            obj, created = Mst_Chaku_seiseki.objects.update_or_create(
                                uma=Cmn.chk_master_Mst_Horse(data.Ketto_touroku_number),
                                defaults=update_dict
                                )
                            update_num += 1

            logger.info(f'【競走馬着順成績マスタ】{str(update_num)}件 登録・更新')
            return NORMAL

        except Exception as e:
            Common_log.Out_Logs(log_err_msg_id, [e])
            logger.error(failure(e))
            return ABNORMAL
