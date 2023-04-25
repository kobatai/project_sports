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

class Trn_M113_odds_umaren_wide():

    def insert_or_update_M113_odds_umaren_wide(self, datfilename, datDataFileFlg):
        try:
            from app_jra.models import (
                Trn_FSOS_1_OddsShiji,
                Trn_FSOS_5_Odds_Umaren,
                Trn_FSOS_6_Odds_Wide,
                Trn_I204_1_Yokujitsu_syutsuba_hyou,
                Trn_I154_Hayami_hyou,
                M110_odds_raceinfo,
                M113_odds_umaren_wide,
            )
            Cmn = Common()

            # 受信ファイルごとに、M113レコードを新規作成・更新していく。
            # 正しく登録完了した場合、後続の待ち合わせ・配信処理で使うために、更新した中間DBのレコードを保持しておき、リストにして返す。
            # 登録失敗時には、False(ABNOMAL or 空のリスト)を返す。
            edit_mddb_list = []  # 初期値
            
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
                    u_teikichuu_list = []
                    w_teikichuu_list = []
                    if m110_list.exists():
                        # 更新の場合
                        m110_obj = m110_list.last()
                        # M113を削除する前に、的中データがある場合は、的中組番をひかえておく
                        u_teikichuu_list = list(M113_odds_umaren_wide.objects.filter(m110=m110_obj, u_tekichuu_flg=True).values_list('umasaki', 'umaato'))
                        w_teikichuu_list = list(M113_odds_umaren_wide.objects.filter(m110=m110_obj, w_tekichuu_flg=True).values_list('umasaki', 'umaato'))
                        # M113を全削除する
                        M113_odds_umaren_wide.objects.filter(m110=m110_obj).delete()
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
                    # M113を新規作成する
                    m113_obj = None
                    # 重複なしの組み合わせ(順不同)をリスト化して抽出して、順番を指定して組み合わせを取り出す
                    umapuku_l = list(itertools.combinations(['01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18'], 2))
                    # 出走頭数を取得
                    shusuu = int(data.Syusso_tousuu)
                    # FSOS_5 オッズ・支持率 馬連オッズ
                    FSOS_5_list = Trn_FSOS_5_Odds_Umaren.objects.filter(FSOS_1=data)
                    if FSOS_5_list.exists():
                        for FSOS_5 in FSOS_5_list:
                            renpukunums = umapuku_l[FSOS_5.num]
                            umapukusaki = int(renpukunums[0])
                            umapukuato = int(renpukunums[1])
                            u_tekichuu_flg = (umapukusaki, umapukuato) in u_teikichuu_list

                            # 出走頭数の数だけ登録
                            if umapukusaki <= shusuu and umapukuato <= shusuu:
                                # M113を新規作成
                                m113_obj = M113_odds_umaren_wide.objects.create(
                                    m110=m110_obj,
                                    umasaki=umapukusaki,
                                    umaato=umapukuato,
                                    umareno = FSOS_5.Umaren_odds,
                                    umarenoreigaijouhou=Cmn.make_reigaiinfo(FSOS_5.Umaren_odds),
                                    u_tekichuu_flg=u_tekichuu_flg
                                )
                    # FSOS_6 オッズ・支持率 ワイドオッズ
                    FSOS_6_list = Trn_FSOS_6_Odds_Wide.objects.filter(FSOS_1=data)
                    if FSOS_6_list.exists():
                        for FSOS_6 in FSOS_6_list:
                            renpukunums = umapuku_l[FSOS_6.num]
                            wasaki = int(renpukunums[0])
                            waato = int(renpukunums[1])
                            w_tekichuu_flg = (wasaki, waato) in w_teikichuu_list

                            # 出走頭数の数だけ登録
                            if wasaki <= shusuu and waato <= shusuu:
                                # M113を更新・新規作成
                                m113_obj, created = M113_odds_umaren_wide.objects.update_or_create(
                                    m110=m110_obj,
                                    umasaki=wasaki,
                                    umaato=waato,
                                    defaults={
                                        "wasaiteio" : FSOS_6.Wide_odds_min,
                                        "wasaikouo" : FSOS_6.Wide_odds_max,
                                        "waoreigaijouhou": Cmn.make_reigaiinfo(FSOS_6.Wide_odds_min),
                                        "w_tekichuu_flg" : w_tekichuu_flg
                                    }
                                )

                    umapuku_l = None # メモリ解放  
                    edit_mddb_list.append(m113_obj)
            
            elif YOKUJITSU_SYUTSUBA_HYOU == datDataFileFlg or SHUUSEI_SYUTSUBA_HYOU == datDataFileFlg or TOUJITSU_SYUTSUBA_HYOU == datDataFileFlg:
                # I204_1 翌日出馬表／Z204_1 修正出馬表／I904_1 当日出馬表データレコードの場合、M113を登録
                I2041_list = Trn_I204_1_Yokujitsu_syutsuba_hyou.objects.filter(Receive_filename=datfilename)
                # 番組情報を取得
                m110_obj = None
                m113_obj = None
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
                # 後続処理用に、m113を一つだけ取得
                if M113_odds_umaren_wide.objects.filter(m110=m110_obj).exists():
                    m113_obj = M113_odds_umaren_wide.objects.filter(m110=m110_obj).last()
                edit_mddb_list.append(m113_obj)

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
                    
                    # 一旦M113の的中フラグをリセットする
                    M113_odds_umaren_wide.objects.filter(m110=m110_obj).update(u_tekichuu_flg = False, w_tekichuu_flg = False)
                    m113_obj = None  # 戻り値初期値
                    # M110に馬連の的中を登録
                    for i in range(3):
                        num = i + 1
                        uma_num = getattr(data, 'Umaren' + str(num) + '_umaban',False)
                        if int(uma_num):
                            tekichuu_saki = int(uma_num[0:2])
                            tekichuu_ato = int(uma_num[2:4])
                
                            # 的中のM113_odds_umaren_wideを取得する
                            m113_list = M113_odds_umaren_wide.objects.filter(
                                        m110 = m110_obj,
                                        umasaki= int(tekichuu_saki),
                                        umaato= int(tekichuu_ato),
                                        )
                            if m113_list.exists():
                                m113_obj = m113_list.last()
                            else:
                                # 的中M113オブジェクトがない場合、しょうがないので作る（オッズなし）
                                m113_obj = M113_odds_umaren_wide.objects.create(
                                        m110 = m110_obj,
                                        umasaki= int(tekichuu_saki),
                                        umaato= int(tekichuu_ato),
                                        )
                            # 的中フラグを立てる
                            m113_obj.u_tekichuu_flg = True
                            m113_obj.save()

                    # M110にワイドの的中を登録
                    for i in range(7):
                        num = i + 1
                        wide_num = getattr(data, 'Wide' + str(num) + '_umaban',False)
                        if int(wide_num):
                            tekichuu_saki = int(wide_num[0:2])
                            tekichuu_ato = int(wide_num[2:4])
                
                            # 的中のM113_odds_umaren_wideを取得する
                            m113_list = M113_odds_umaren_wide.objects.filter(
                                        m110 = m110_obj,
                                        umasaki= int(tekichuu_saki),
                                        umaato= int(tekichuu_ato),
                                        )
                            if m113_list.exists():
                                m113_obj = m113_list.last()
                            else:
                                # 的中M113オブジェクトがない場合、しょうがないので作る（オッズなし）
                                m113_obj = M113_odds_umaren_wide.objects.create(
                                        m110 = m110_obj,
                                        umasaki= int(tekichuu_saki),
                                        umaato= int(tekichuu_ato),
                                        )
                            # 的中フラグを立てる
                            m113_obj.w_tekichuu_flg = True
                            m113_obj.save()

                    edit_mddb_list.append(m113_obj)

            return edit_mddb_list

        except Exception as e:
            Common_log.Out_Logs(log_err_msg_id, [failure(e)])
            logger.error(failure(e))
            return False
