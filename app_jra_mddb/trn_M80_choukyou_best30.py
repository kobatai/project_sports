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


class Trn_M80_choukyou_best30():

    def insert_or_update_M80_choukyou_best30(self, datfilename, datDataFileFlg):
        try:
            from app_jra.models import (
                Trn_K802_1_Choukyoushi_seiseki_best30,
                Trn_K802_2_Choukyoushi_seiseki_best30,
                M80_choukyou_best30
            )
            Cmn = Common()

            # 受信ファイルごとに、M80レコードを新規作成・更新していく。
            # 正しく登録完了した場合、後続の待ち合わせ・配信処理で使うために、更新した中間DBのレコードを保持しておき、リストにして返す。
            # 登録失敗時には、False(ABNOMAL or 空のリスト)を返す。
            edit_mddb_list = [] # 初期値

            if CHOUKYOUSHI_SEISEKI_BEST30 == datDataFileFlg:
                # K802 調教師成績ベスト３０
                K802_1_list = Trn_K802_1_Choukyoushi_seiseki_best30.objects.filter(Receive_filename=datfilename)
                if K802_1_list.exists():
                    k802_obj = K802_1_list.last()
                    block_kubun = BLOCK_KUBUN[k802_obj.Tanmatsu_denbun_saibun]  # 端末通番＿電文細分 7:関東,8:関西,D:全国
                    seiseki_date = dt.strptime(k802_obj.Seiseki_kakuteibi, '%y%m%d').date() # 成績確定日を、開催日および処理日とする

                    for k802_2_obj in Trn_K802_2_Choukyoushi_seiseki_best30.objects.filter(K802_1=k802_obj):
                        M80_obj, created = M80_choukyou_best30.objects.update_or_create(
                            kaisaibi=seiseki_date,
                            burokkukubun=block_kubun,
                            syorihizuke=seiseki_date,
                            kekkajuni=int(k802_2_obj.Juni),
                            defaults={
                                'choumei' : Cmn.chk_master_Mst_Choukyoushi(k802_2_obj.Choukyoushi_code),
                                'chaku1suu' : int(k802_2_obj.Firstrank_kaisuu),
                                'chaku2suu' : int(k802_2_obj.Secondrank_kaisuu),
                                'chaku3suu' : int(k802_2_obj.Thirdrank_kaisuu),
                                'chaku4suu' : int(k802_2_obj.Fourthrank_kaisuu),
                                'chaku5suu' : int(k802_2_obj.Fifthrank_kaisuu),
                                'chakugaisuu' : int(k802_2_obj.Chakugai_kaisuu),
                                'tokusoushorisuu' : int(k802_2_obj.Tokubetsu_syouhai),
                                'shusuu' : int(k802_2_obj.Kijou_kaisuu),
                                'shoritsu' : Cmn.make_shouritsu(k802_2_obj.Syouritsu),
                                'rentairitsu' : Cmn.make_shouritsu(k802_2_obj.Rentairitsu),
                                'tsuushousuu' : int(k802_2_obj.Tsuusan_syourisuu),
                                'nyuuchakushoukin' : int(k802_2_obj.Nyuuchaku_syoukin) * 100
                            }
                        )
                    # K802は複数ファイルに分かれているため、全てのM80が揃っていることを確認するために、ファイル受信ごとに後続処理に該当日の全てのM80を渡す
                    edit_mddb_list = M80_choukyou_best30.objects.filter(kaisaibi=seiseki_date)

            return edit_mddb_list

        except Exception as e:
            Common_log.Out_Logs(log_err_msg_id, [failure(e)])
            logger.error(failure(e))
            return False