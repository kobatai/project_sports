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

class Trn_M115_odds_sanpuku():

    def insert_or_update_M115_odds_sanpuku(self, datfilename, datDataFileFlg):
        try:
            from app_jra.models import (
                Trn_FSOS_1_OddsShiji,
                Trn_FSOS_8_Odds_Sanrenhuku,
                Trn_I204_1_Yokujitsu_syutsuba_hyou,
                Trn_I154_Hayami_hyou,
                M110_odds_raceinfo,
                M115_odds_sanpuku,
            )
            Cmn = Common()

            # 受信ファイルごとに、M115レコードを新規作成・更新していく。
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
                        # M115を削除する前に、的中データがある場合は、的中組番をひかえておく
                        teikichuu_list = list(M115_odds_sanpuku.objects.filter(m110=m110_obj, tekichuu_flg=True).values_list('umasaki', 'umanaka', 'umaato'))
                        # M115を全削除する
                        M115_odds_sanpuku.objects.filter(m110=m110_obj).delete()
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
                    # M115を新規作成する
                    m115_obj = None
                    # FSOS_8 オッズ・支持率 三連複オッズ
                    FSOS_8_list = Trn_FSOS_8_Odds_Sanrenhuku.objects.filter(FSOS_1=data)
                    if FSOS_8_list.exists():
                        create_objs_list = []
                        # 出走頭数を取得
                        shusuu = int(data.Syusso_tousuu)
                        m115_id = M115_odds_sanpuku.objects.last().id + 1 if M115_odds_sanpuku.objects.all().exists() else 1
                        # 重複なしの3つの数字の組み合わせをリスト化して抽出して、順番を指定して組み合わせを取り出す
                        sanpuku_l = list(itertools.combinations(['01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18'], 3))
                        for FSOS_8 in FSOS_8_list:
                            sanpukunums = sanpuku_l[FSOS_8.num]
                            sanpukusaki = int(sanpukunums[0])
                            sanpukunaka = int(sanpukunums[1])
                            sanpukuato = int(sanpukunums[2])
                            tekichuu_flg = (sanpukusaki, sanpukunaka, sanpukuato) in teikichuu_list

                            # 出走頭数の数だけ登録
                            if sanpukusaki <= shusuu and sanpukunaka <= shusuu and sanpukuato <= shusuu:
                                # M115を新規作成
                                m115_obj = M115_odds_sanpuku(
                                    id=m115_id,
                                    m110=m110_obj,
                                    umasaki=sanpukusaki,
                                    umanaka=sanpukunaka,
                                    umaato=sanpukuato,
                                    ozzu = FSOS_8.Sanrenhuku_odds,
                                    oreigaijouhou = Cmn.make_reigaiinfo(FSOS_8.Sanrenhuku_odds),
                                    tekichuu_flg=tekichuu_flg
                                )
                                create_objs_list.append(m115_obj)
                                m115_id += 1
                        sanpuku_l = None # メモリ解放  
                        # 一括登録
                        M115_odds_sanpuku.objects.bulk_create(create_objs_list)
                    
                    edit_mddb_list.append(m115_obj)
            
            elif YOKUJITSU_SYUTSUBA_HYOU == datDataFileFlg or SHUUSEI_SYUTSUBA_HYOU == datDataFileFlg or TOUJITSU_SYUTSUBA_HYOU == datDataFileFlg:
                # I204_1 翌日出馬表／Z204_1 修正出馬表／I904_1 当日出馬表データレコードの場合、M112を登録
                I2041_list = Trn_I204_1_Yokujitsu_syutsuba_hyou.objects.filter(Receive_filename=datfilename)
                # 番組情報を取得
                m110_obj = None
                m115_obj = None
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
                # 後続処理用に、m115を一つだけ取得
                if M115_odds_sanpuku.objects.filter(m110=m110_obj).exists():
                    m115_obj = M115_odds_sanpuku.objects.filter(m110=m110_obj).last()
                edit_mddb_list.append(m115_obj)

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
                    # 一旦M115の的中フラグをリセットする
                    M115_odds_sanpuku.objects.filter(m110=m110_obj).update(tekichuu_flg = False)
                    m115_obj = None # 戻り値初期値
                    for i in range(3):
                        num = i + 1
                        sanpuku_num = getattr(data, 'Sanrenhuku' + str(num) + '_umaban', False)
                        if int(sanpuku_num):
                            tekichuu_saki = int(sanpuku_num[0:2])
                            tekichuu_naka = int(sanpuku_num[2:4])
                            tekichuu_ato = int(sanpuku_num[4:6])
                
                            # 的中のM112_odds_wakurenを取得する
                            m115_list = M115_odds_sanpuku.objects.filter(
                                        m110 = m110_obj,
                                        umasaki= int(tekichuu_saki),
                                        umanaka= int(tekichuu_naka),
                                        umaato= int(tekichuu_ato),
                                        )
                            if m115_list.exists():
                                m115_obj = m115_list.last()
                            else:
                                # 的中M112オブジェクトがない場合、しょうがないので作る（オッズなし）
                                m115_obj = M115_odds_sanpuku.objects.create(
                                        m110 = m110_obj,
                                        umasaki= int(tekichuu_saki),
                                        umanaka= int(tekichuu_naka),
                                        umaato= int(tekichuu_ato),
                                        )
                            # 的中フラグを立てる
                            m115_obj.tekichuu_flg = True
                            m115_obj.save()

                    edit_mddb_list.append(m115_obj)

            return edit_mddb_list

        except Exception as e:
            Common_log.Out_Logs(log_err_msg_id, [failure(e)])
            logger.error(failure(e))
            return False
