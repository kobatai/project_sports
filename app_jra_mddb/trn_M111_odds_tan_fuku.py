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
    return str(lineno) + ":" + str(type(e)) + " " + str(e)

class Trn_M111_odds_tan_fuku():

    def insert_or_update_M111_odds_tan_fuku(self, datfilename, datDataFileFlg):
        try:
            from app_jra.models import (
                Trn_FSOS_1_OddsShiji,
                Trn_FSOS_2_Odds_Tansyou,
                Trn_FSOS_3_Odds_Hukusyou,
                Trn_I204_1_Yokujitsu_syutsuba_hyou,
                Trn_I204_2_Yokujitsu_syutsuba_horse_info,
                M110_odds_raceinfo,
                M111_odds_tan_fuku,
                Mst_Horse,
            )
            Cmn = Common()

            # 受信ファイルごとに、M111レコードを新規作成・更新していく。
            # 正しく登録完了した場合、後続の待ち合わせ・配信処理で使うために、更新した中間DBのレコードを保持しておき、リストにして返す。
            # 登録失敗時には、False(ABNOMAL or 空のリスト)を返す。
            edit_mddb_list = [] # 初期値
            
            if ODDSSHIJI == datDataFileFlg:
                # FSOS_1 オッズ・支持率
                fsos1_list = Trn_FSOS_1_OddsShiji.objects.filter(Receive_filename=datfilename)

                # 番組情報を取得
                for data in fsos1_list:
                    jou_obj = Cmn.chk_master_Mst_Jou(data.Bangumi_Jou_code)
                    kaisaibi = dt.strptime(data.Kaisai_date, '%y%m%d').date()
                    rebangou = int(data.Bangumi_race_number)

                    m110_list = M110_odds_raceinfo.objects.filter(
                            joumei = jou_obj,
                            kaisaibi = kaisaibi,
                            rebangou = rebangou,
                    )
                    m111_obj = None
                    if m110_list.exists():
                        # 更新の場合
                        m110_obj = m110_list.last()
                        # m110を更新する
                        m110_obj.sakujikan = Cmn.make_sakusei_time(data)
                        m110_obj.hatsukubun = Cmn.make_hatsukubun(data)
                        m110_obj.maeuri_flg = Cmn.make_maeuri_flg(data)
                        m110_obj.save()
                    else:
                        # M110,M111が未登録の場合、ここで新規作成する。
                        # M110を新規作成
                        m110_obj = M110_odds_raceinfo.objects.create(
                            kaisaibi = kaisaibi,
                            shusuu = int(data.Syusso_tousuu),
                            joumei = jou_obj,
                            kaisuu = int(data.Bangumi_kai),
                            kainichime = int(data.Bangumi_day),
                            rebangou = rebangou,
                            sakujikan = Cmn.make_sakusei_time(data),
                            hatsukubun = Cmn.make_hatsukubun(data),
                            maeuri_flg = Cmn.make_maeuri_flg(data),
                            )
                    # M111を新規作成する
                    # FSOS_2 オッズ・支持率 単勝オッズ
                    FSOS_2_list = Trn_FSOS_2_Odds_Tansyou.objects.filter(FSOS_1=data)
                    for FSOS_2_obj in FSOS_2_list:
                        if Trn_FSOS_3_Odds_Hukusyou.objects.filter(FSOS_1=data, num=FSOS_2_obj.num).exists():
                            FSOS_3_obj = Trn_FSOS_3_Odds_Hukusyou.objects.filter(FSOS_1=data, num=FSOS_2_obj.num).last()
                            m111_obj, created = M111_odds_tan_fuku.objects.update_or_create(
                                m110=m110_obj,
                                uma=int(FSOS_2_obj.num) + 1,
                                defaults={
                                    'tano' : FSOS_2_obj.Tansyou_odds,
                                    'tanoreigaijouhou' : Cmn.make_reigaiinfo(FSOS_2_obj.Tansyou_odds),
                                    'saiteio' : FSOS_3_obj.Hukusyou_odds_min,
                                    'saikouo' : FSOS_3_obj.Hukusyou_odds_max,
                                    'fukuoreigaijouhou' : Cmn.make_reigaiinfo(FSOS_3_obj.Hukusyou_odds_min)
                                }
                            )

                    edit_mddb_list.append(m111_obj)

            
            elif YOKUJITSU_SYUTSUBA_HYOU == datDataFileFlg or SHUUSEI_SYUTSUBA_HYOU == datDataFileFlg or TOUJITSU_SYUTSUBA_HYOU == datDataFileFlg:
                # I204_1 翌日出馬表／Z204_1 修正出馬表／I904_1 当日出馬表データレコードの場合、M111を登録
                I2041_list = Trn_I204_1_Yokujitsu_syutsuba_hyou.objects.filter(Receive_filename=datfilename)
                # 番組情報を取得
                for data in I2041_list:
                    jou_obj = Cmn.chk_master_Mst_Jou(data.Bangumi_Jou_code)
                    kaisai_str = data.Bangumi_year + data.Syutsuba_info_tsukihi
                    kaisaibi = dt.strptime(kaisai_str, '%y%m%d').date()
                    rebangou = int(data.Bangumi_race_number)
                    kai = int(data.Bangumi_race_number)
                    nichime = int(data.Bangumi_race_number)
                    m110_obj, created = M110_odds_raceinfo.objects.update_or_create(
                        kaisuu = int(data.Bangumi_kai), 
                        kainichime = int(data.Bangumi_day), 
                        joumei = jou_obj,
                        rebangou=rebangou,
                        defaults={
                            "kaisaibi" : kaisaibi,
                            "shusuu" : int(data.Syutsuba_info_syusso_tousuu),
                            "tokusouhonsuu" : Cmn.get_honfuku_kaisuu(data, True),
                            "tokusoumeihon" : data.Tokubetsu_kyoso_hondai.rstrip(),
                            "tokusoufukusuu" : Cmn.get_honfuku_kaisuu(data, False),
                            "tokusoumeifuku" : data.Tokubetsu_kyoso_hukudai.rstrip()
                        }
                    )
                    m111_obj = None
                    I2042_list = Trn_I204_2_Yokujitsu_syutsuba_horse_info.objects.filter(I204_1=data)
                    for I2042_obj in I2042_list:
                        if int(I2042_obj.Umaban):
                            m111_obj, created = M111_odds_tan_fuku.objects.update_or_create(
                                m110 = m110_obj, 
                                uma= int(I2042_obj.Umaban),
                                defaults={
                                    "waku" : int(I2042_obj.Wakuban),
                                    "umajouhou" : Cmn.chk_master_Mst_Horse(I2042_obj.Ketto_number),
                                }
                            )
                    edit_mddb_list.append(m111_obj)

            return edit_mddb_list

        except Exception as e:
            Common_log.Out_Logs(log_err_msg_id, [failure(e)])
            logger.error(failure(e))
            return False
