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

class Trn_M112_odds_wakuren():

    def insert_or_update_M112_odds_wakuren(self, datfilename, datDataFileFlg):
        try:
            from app_jra.models import (
                Trn_FSOS_1_OddsShiji,
                Trn_FSOS_4_Odds_Wakuren,
                Trn_I204_1_Yokujitsu_syutsuba_hyou,
                Trn_I154_Hayami_hyou,
                M110_odds_raceinfo,
                M112_odds_wakuren,
            )
            Cmn = Common()

            # 受信ファイルごとに、M112レコードを新規作成・更新していく。
            # 正しく登録完了した場合、後続の待ち合わせ・配信処理で使うために、更新した中間DBのレコードを保持しておき、リストにして返す。
            # 登録失敗時には、False(ABNOMAL or 空のリスト)を返す。
            edit_mddb_list = [] # 初期値
            
            if ODDSSHIJI == datDataFileFlg:
                # FSOS_1 オッズ・支持率
                # I204/I904は既に受信してる前提
                fsos1_list = Trn_FSOS_1_OddsShiji.objects.filter(Receive_filename=datfilename)
                
                # 番組情報を取得
                for data in fsos1_list:
                    jou_obj = Cmn.chk_master_Mst_Jou(data.Bangumi_Jou_code)
                    kaisaibi = dt.strptime(data.Kaisai_date, '%y%m%d').date()
                    rebangou = int(data.Bangumi_race_number)
                    kaisuu = int(data.Bangumi_kai)
                    kainichime = int(data.Bangumi_day)

                    m110_list = M110_odds_raceinfo.objects.filter(
                            joumei = jou_obj,
                            kaisuu = kaisuu,
                            kainichime = kainichime,
                            rebangou = rebangou,
                    )
                    teikichuu_list = []
                    if m110_list.exists():
                        # 更新の場合
                        m110_obj = m110_list.last()
                        # M112を削除する前に、的中データがある場合は、的中組番をひかえておく
                        teikichuu_list = list(M112_odds_wakuren.objects.filter(m110=m110_obj, tekichuu_flg=True).values_list('wakusaki', 'wakuato'))
                        # M112を全削除
                        M112_odds_wakuren.objects.filter(m110=m110_obj).delete()
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
                    # M112を新規作成する
                    # FSOS_4 オッズ・支持率 枠連オッズ
                    FSOS_4_list = Trn_FSOS_4_Odds_Wakuren.objects.filter(FSOS_1=data)
                    m112_obj = None
                    if FSOS_4_list.exists():
                        # 重複を許す1～8の組み合わせをリスト化して抽出して、順番を指定して組み合わせを取り出す
                        wakupuku_l = list(itertools.combinations_with_replacement(['1','2','3','4','5','6','7','8'], 2))
                        for FSOS_4 in FSOS_4_list:
                            renpukunums = wakupuku_l[FSOS_4.num]
                            wakupukusaki = int(renpukunums[0])
                            wakupukuato = int(renpukunums[1])
                            tekichuu_flg = (wakupukusaki, wakupukuato) in teikichuu_list

                            # M112を新規作成
                            m112_obj = M112_odds_wakuren.objects.create(
                                m110=m110_obj,
                                wakusaki=wakupukusaki,
                                wakuato=wakupukuato,
                                ozzu = FSOS_4.Wakuren_odds if not Cmn.make_reigaiinfo(FSOS_4.Wakuren_odds) else None,
                                oreigaijouhou=Cmn.make_reigaiinfo(FSOS_4.Wakuren_odds),
                                tekichuu_flg=tekichuu_flg
                            )
                        wakupuku_l = None # メモリ解放  
                    
                    edit_mddb_list.append(m112_obj)
            
            elif YOKUJITSU_SYUTSUBA_HYOU == datDataFileFlg or SHUUSEI_SYUTSUBA_HYOU == datDataFileFlg or TOUJITSU_SYUTSUBA_HYOU == datDataFileFlg:
                # I204_1 翌日出馬表／Z204_1 修正出馬表／I904_1 当日出馬表データレコードの場合、M112を登録
                I2041_list = Trn_I204_1_Yokujitsu_syutsuba_hyou.objects.filter(Receive_filename=datfilename)
                # 番組情報を取得
                m110_obj = None
                m112_obj = None
                for data in I2041_list:
                    jou_obj = Cmn.chk_master_Mst_Jou(data.Bangumi_Jou_code)
                    kaisai_str = data.Bangumi_year + data.Syutsuba_info_tsukihi
                    kaisaibi = dt.strptime(kaisai_str, '%y%m%d').date()
                    rebangou = int(data.Bangumi_race_number)
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
                # 後続処理用に、m112を一つだけ取得
                if M112_odds_wakuren.objects.filter(m110=m110_obj).exists():
                    m112_obj = M112_odds_wakuren.objects.filter(m110=m110_obj).last()
                edit_mddb_list.append(m112_obj)

            elif HAYAMI_HYOU == datDataFileFlg:
                # I154 早見表データレコードの場合
                I154_list = Trn_I154_Hayami_hyou.objects.filter(Receive_filename=datfilename)
                # 番組情報を取得
                for data in I154_list:
                    jou_obj = Cmn.chk_master_Mst_Jou(data.Bangumi_Jou_code)
                    rebangou = int(data.Bangumi_race_number)
                    kaisaibi_temp = Cmn.get_kaisaibi(jou_obj, int(data.Bangumi_kai), int(data.Bangumi_day))
                    if not kaisaibi_temp:
                        kaisaibi_temp = dt.strptime(data.Soushin_date, '%y%m%d').date()
                    kaisaibi = kaisaibi_temp
                    kaisuu = int(data.Bangumi_kai)
                    kainichime = int(data.Bangumi_day)
                    m110_obj, created = M110_odds_raceinfo.objects.update_or_create(
                        joumei = jou_obj,
                        rebangou = rebangou,
                        kaisuu = kaisuu, 
                        kainichime = kainichime, 
                        defaults={
                            "kaisaibi" : kaisaibi,
                        }
                    )
                    # 一旦M112の的中フラグをリセットする
                    M112_odds_wakuren.objects.filter(m110=m110_obj).update(tekichuu_flg = False)
                    m112_obj = None # 戻り値初期値
                    for i in range(3):
                        num = i + 1
                        waku_num = getattr(data, 'Wakuren' + str(num) + '_umaban',False)
                        if int(waku_num):
                            tekichuu_saki = int(waku_num[0:1])
                            tekichuu_ato = int(waku_num[1:2])
                
                            # 的中のM112_odds_wakurenを取得する
                            m112_list = M112_odds_wakuren.objects.filter(
                                        m110 = m110_obj,
                                        wakusaki= int(tekichuu_saki),
                                        wakuato= int(tekichuu_ato),
                                        )
                            if m112_list.exists():
                                m112_obj = m112_list.last()
                            else:
                                # 的中M112オブジェクトがない場合、しょうがないので作る（オッズなし）
                                m112_obj = M112_odds_wakuren.objects.create(
                                        m110 = m110_obj,
                                        wakusaki= int(tekichuu_saki),
                                        wakuato= int(tekichuu_ato),
                                        )
                            # 的中フラグを立てる
                            m112_obj.tekichuu_flg = True
                            m112_obj.save()

                    edit_mddb_list.append(m112_obj)

            return edit_mddb_list

        except Exception as e:
            Common_log.Out_Logs(log_err_msg_id, [failure(e)])
            logger.error(failure(e))
            return False
