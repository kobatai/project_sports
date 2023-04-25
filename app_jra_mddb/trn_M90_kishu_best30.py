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


class Trn_M90_kishu_best30():

    def insert_or_update_M90_kishu_best30(self, datfilename, datDataFileFlg):
        try:
            from app_jra.models import (
                Trn_K801_1_Kishu_seiseki_best30,
                Trn_K801_2_Kishu_seiseki_best30,
                M90_kishu_best30
            )
            Cmn = Common()

            # 受信ファイルごとに、M90レコードを新規作成・更新していく。
            # 正しく登録完了した場合、後続の待ち合わせ・配信処理で使うために、更新した中間DBのレコードを保持しておき、リストにして返す。
            # 登録失敗時には、False(ABNOMAL or 空のリスト)を返す。
            edit_mddb_list = []  # 初期値

            if KISHU_SEISEKI_BEST30 == datDataFileFlg:
                # K801 騎手成績ベスト３０
                K801_1_list = Trn_K801_1_Kishu_seiseki_best30.objects.filter(Receive_filename=datfilename)
                if K801_1_list.exists():
                    k801_obj = K801_1_list.last()
                    block_kubun = BLOCK_KUBUN[k801_obj.Tanmatsu_denbun_saibun]  # 端末通番＿電文細分 7:関東,8:関西,D:全国
                    seiseki_date = dt.strptime(k801_obj.Seiseki_kakuteibi, '%y%m%d').date() # 成績確定日を、開催日および処理日とする

                    for k801_2_obj in Trn_K801_2_Kishu_seiseki_best30.objects.filter(K801_1=k801_obj):
                        M90_obj, created = M90_kishu_best30.objects.update_or_create(
                            kaisaibi=seiseki_date,
                            burokkukubun=block_kubun,
                            syorihizuke=seiseki_date,
                            kekkajuni=int(k801_2_obj.Juni),
                            defaults={
                                'kimei' : Cmn.chk_master_Mst_Kishu(k801_2_obj.Kishu_code),
                                'chaku1suu' : int(k801_2_obj.Firstrank_kaisuu),
                                'chaku2suu' : int(k801_2_obj.Secondrank_kaisuu),
                                'chaku3suu' : int(k801_2_obj.Thirdrank_kaisuu),
                                'chaku4suu' : int(k801_2_obj.Fourthrank_kaisuu),
                                'chaku5suu' : int(k801_2_obj.Fifthrank_kaisuu),
                                'chakugaisuu' : int(k801_2_obj.Chakugai_kaisuu),
                                'tokusoushorisuu' : int(k801_2_obj.Tokubetsu_syouhai),
                                'keikijousuu' : int(k801_2_obj.Kijou_kaisuu),
                                'shoritsu' : Cmn.make_shouritsu(k801_2_obj.Syouritsu),
                                'rentairitsu' : Cmn.make_shouritsu(k801_2_obj.Rentairitsu),
                                'tsuushousuu' : int(k801_2_obj.Tsuusan_syourisuu),
                                'shuutokushoukin' : int(k801_2_obj.Nyuuchaku_syoukin) * 100
                            }
                        )
                    # K801は複数ファイルに分かれているため、全てのM90が揃っていることを確認するために、ファイル受信ごとに後続処理に該当日の全てのM90を渡す
                    edit_mddb_list = M90_kishu_best30.objects.filter(kaisaibi=seiseki_date)

            return edit_mddb_list

        except Exception as e:
            Common_log.Out_Logs(log_err_msg_id, [failure(e)])
            logger.error(failure(e))
            logger.info(e)
            return False