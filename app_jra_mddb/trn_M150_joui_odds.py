from logging import getLogger
import re
import mojimoji
from app_jra.consts import *
import sys
import linecache
import itertools
from app_jra.log_commons import *
from datetime import datetime as dt
from datetime import timedelta


logger = getLogger('jra_edit_delivery')
Common_log = Common_log(DEBUGLOG_NAME_TO_TYPE[logger.name])
log_err_msg_id = 3400
log_info_msg_id = 9399

try:
    from app_jra_mddb.mddb_commons import Common
except Exception as e:
    Common_log.Out_Logs(log_err_msg_id, [e])
    logger.error(f'commonsファイル読み込み失敗 : {e}')


def failure(e):
    exc_type, exc_obj, tb=sys.exc_info()
    lineno=tb.tb_lineno
    return str(lineno) + ":" + str(type(e)) + " " + str(e)

class Trn_M150_joui_odds():

    def chk_blank(self, record):
        # 受信データにはブランクのパターンがいくつかあるが、空データの場合は全てブランクとみなす処理
        blank_list = ['','　',None,[],{}]
        if record in blank_list or re.fullmatch('\s+', record):
            return False
        else:
            return True

    def insert_or_update_M150_joui_odds(self, datfilename, datDataFileFlg):
        try:
            from app_jra.models import (
                Trn_FSOS_1_OddsShiji,
                Trn_I204_1_Yokujitsu_syutsuba_hyou,
                Trn_I204_2_Yokujitsu_syutsuba_horse_info,
                Trn_I154_Hayami_hyou,
                M10_shutsubahyou,
                M150_joui_odds,
                Mst_Horse,
            )
            Cmn = Common()

            # 受信ファイルごとに、M150レコードを新規作成・更新していく。
            # 正しく登録完了した場合、後続の待ち合わせ・配信処理で使うために、更新した中間DBのレコードを保持しておき、リストにして返す。
            # 登録失敗時には、False(ABNOMAL or 空のリスト)を返す。
            edit_mddb_list = [] # 初期値
            
            if ODDSSHIJI == datDataFileFlg:
                # FSOS_1 オッズ・支持率
                # I204/I904は既に受信してる前提
                fsos1_list = Trn_FSOS_1_OddsShiji.objects.filter(Receive_filename=datfilename)

                # 番組情報を取得
                for data in fsos1_list:
                    if Cmn.make_maeuri_flg(data): # 前売りデータのみM115を作成する
                        jou_obj = Cmn.chk_master_Mst_Jou(data.Bangumi_Jou_code)
                        kaisaibi = dt.strptime(data.Kaisai_date, '%y%m%d').date()
                        rebangou = int(data.Bangumi_race_number)

                        # 特別競走情報を取得
                        tokusouhonsuu = None
                        tokusoumeihon = None
                        tokusoufukusuu = None
                        tokusoumeifuku = None

                        # M10から特別競走情報を取得する
                        if M10_shutsubahyou.objects.filter(joumei=jou_obj, kaisuu=int(data.Bangumi_kai), kainichime=int(data.Bangumi_day), rebangou=rebangou).exists():
                            m10_obj = M10_shutsubahyou.objects.filter(joumei=jou_obj, kaisuu=int(data.Bangumi_kai), kainichime=int(data.Bangumi_day), rebangou=rebangou).last()
                            tokusouhonsuu = m10_obj.tokusouhonsuu
                            tokusoumeihon = m10_obj.tokusoumeihon
                            tokusoufukusuu = m10_obj.tokusoufukusuu
                            tokusoumeifuku = m10_obj.tokusoumeifuku
                        
                        # M150を新規作成
                        m150_obj, created = M150_joui_odds.objects.update_or_create(
                            joumei=jou_obj,
                            kaisuu = int(data.Bangumi_kai), 
                            kainichime = int(data.Bangumi_day),
                            rebangou=rebangou,
                            defaults={
                                'kaisaibi' : kaisaibi,
                                'sakujikan' : Cmn.make_sakusei_time(data),
                                'hatsukubun' : Cmn.make_hatsukubun(data),
                                'tokusouhonsuu' : tokusouhonsuu,
                                'tokusoumeihon' : tokusoumeihon,
                                'tokusoufukusuu' : tokusoufukusuu,
                                'tokusoumeifuku' : tokusoumeifuku,
                            }
                        )
                        edit_mddb_list.append(m150_obj)
            
            elif YOKUJITSU_SYUTSUBA_HYOU == datDataFileFlg or SHUUSEI_SYUTSUBA_HYOU == datDataFileFlg or TOUJITSU_SYUTSUBA_HYOU == datDataFileFlg:
                # I204_1 翌日出馬表／Z204_1 修正出馬表／I904_1 当日出馬表データレコードの場合、M150を登録
                I2041_list = Trn_I204_1_Yokujitsu_syutsuba_hyou.objects.filter(Receive_filename=datfilename)
                # 番組情報を取得
                for data in I2041_list:
                    jou_obj = Cmn.chk_master_Mst_Jou(data.Bangumi_Jou_code)
                    kaisai_str = data.Bangumi_year + data.Syutsuba_info_tsukihi
                    kaisaibi = dt.strptime(kaisai_str, '%y%m%d').date()
                    rebangou = int(data.Bangumi_race_number)

                    # 既に同じレースのレコードが存在する場合更新する。
                    m150_list = M150_joui_odds.objects.filter(
                        joumei = jou_obj,
                        kaisaibi = kaisaibi, 
                        rebangou = rebangou
                    )
                    if m150_list.exists():
                        # 全組み合わせのM150レコードを一個ずつ更新していくと時間がかかるので、一括更新をかける
                        m150_list.update(
                            tokusouhonsuu = int(data.Tokubetsu_kyoso_kaisuu) if data.Tokubetsu_kyoso_hukuhon_kubun == '1' and int(data.Tokubetsu_kyoso_kaisuu) else None,
                            tokusoumeihon = data.Tokubetsu_kyoso_hondai.rstrip(),
                            tokusoufukusuu = int(data.Tokubetsu_kyoso_kaisuu) if data.Tokubetsu_kyoso_hukuhon_kubun == '2' and int(data.Tokubetsu_kyoso_kaisuu) else None,
                            tokusoumeifuku = data.Tokubetsu_kyoso_hukudai.rstrip(),
                        )
                        edit_mddb_list.append(m150_list.last())
                    
            return edit_mddb_list

        except Exception as e:
            Common_log.Out_Logs(log_err_msg_id, [failure(e)])
            logger.error(failure(e))
            return False
