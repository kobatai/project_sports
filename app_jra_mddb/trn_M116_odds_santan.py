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

class Trn_M116_odds_santan():

    def insert_or_update_M116_odds_santan(self, datfilename, datDataFileFlg):
        try:
            from app_jra.models import (
                Trn_FSOS_1_OddsShiji,
                Trn_FSOS_9_Odds_Sanrentan,
                Trn_I204_1_Yokujitsu_syutsuba_hyou,
                Trn_I154_Hayami_hyou,
                M110_odds_raceinfo,
                M116_odds_santan,
            )
            Cmn = Common()

            # 受信ファイルごとに、M116レコードを新規作成・更新していく。
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
                        teikichuu_list = list(M116_odds_santan.objects.filter(m110=m110_obj, tekichuu_flg=True).values_list('umasaki', 'umanaka', 'umaato'))
                        # M116を全削除する
                        M116_odds_santan.objects.filter(m110=m110_obj).delete()
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
                    # M116を新規作成する
                    m116_obj = None
                    # FSOS_8 オッズ・支持率 三連複オッズ
                    FSOS_9_list = Trn_FSOS_9_Odds_Sanrentan.objects.filter(FSOS_1=data)
                    if FSOS_9_list.exists():
                        create_objs_list = []
                        # 出走頭数を取得
                        shusuu = int(data.Syusso_tousuu)
                        m116_id = M116_odds_santan.objects.last().id + 1 if M116_odds_santan.objects.all().exists() else 1
                         # 重複なしの3つの数字のあらゆる並びをリスト化して抽出して、順番を指定して組み合わせを取り出す
                        santan_l = list(itertools.permutations(['01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18'], 3))
                        for FSOS_9 in FSOS_9_list:
                            santannums = santan_l[FSOS_9.num]
                            santansaki = int(santannums[0])
                            santannaka = int(santannums[1])
                            santanato = int(santannums[2])
                            tekichuu_flg = (santansaki, santannaka, santanato) in teikichuu_list

                            # 出走頭数の数だけ登録
                            if santansaki <= shusuu and santannaka <= shusuu and santanato <= shusuu:
                                # M116を新規作成
                                m116_obj = M116_odds_santan(
                                    id=m116_id,
                                    m110=m110_obj,
                                    umasaki=santansaki,
                                    umanaka=santannaka,
                                    umaato=santanato,
                                    ozzu = FSOS_9.Sanrentan_odds,
                                    oreigaijouhou = Cmn.make_reigaiinfo(FSOS_9.Sanrentan_odds),
                                    tekichuu_flg=tekichuu_flg
                                )
                                create_objs_list.append(m116_obj)
                                m116_id += 1
                        santan_l = None # メモリ解放  
                        # 一括登録
                        M116_odds_santan.objects.bulk_create(create_objs_list)
                    
                    edit_mddb_list.append(m116_obj)
            
            elif YOKUJITSU_SYUTSUBA_HYOU == datDataFileFlg or SHUUSEI_SYUTSUBA_HYOU == datDataFileFlg or TOUJITSU_SYUTSUBA_HYOU == datDataFileFlg:
                # I204_1 翌日出馬表／Z204_1 修正出馬表／I904_1 当日出馬表データレコードの場合、M112を登録
                I2041_list = Trn_I204_1_Yokujitsu_syutsuba_hyou.objects.filter(Receive_filename=datfilename)
                # 番組情報を取得
                m110_obj = None
                m116_obj = None
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
                # 後続処理用に、m116を一つだけ取得
                if M116_odds_santan.objects.filter(m110=m110_obj).exists():
                    m116_obj = M116_odds_santan.objects.filter(m110=m110_obj).last()
                edit_mddb_list.append(m116_obj)

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
                    # 一旦M116の的中フラグをリセットする
                    M116_odds_santan.objects.filter(m110=m110_obj).update(tekichuu_flg = False)
                    m116_obj = None # 戻り値初期値
                    for i in range(6):
                        num = i + 1
                        santan_num = getattr(data, 'Sanrentan' + str(num) + '_umaban',False)
                        if int(santan_num):
                            tekichuu_saki = int(santan_num[0:2])
                            tekichuu_naka = int(santan_num[2:4])
                            tekichuu_ato = int(santan_num[4:6])
                
                            # 的中のM112_odds_wakurenを取得する
                            m116_list = M116_odds_santan.objects.filter(
                                        m110 = m110_obj,
                                        umasaki= int(tekichuu_saki),
                                        umanaka= int(tekichuu_naka),
                                        umaato= int(tekichuu_ato),
                                        )
                            if m116_list.exists():
                                m116_obj = m116_list.last()
                            else:
                                # 的中M112オブジェクトがない場合、しょうがないので作る（オッズなし）
                                m116_obj = M116_odds_santan.objects.create(
                                        m110 = m110_obj,
                                        umasaki= int(tekichuu_saki),
                                        umanaka= int(tekichuu_naka),
                                        umaato= int(tekichuu_ato),
                                        )
                            # 的中フラグを立てる
                            m116_obj.tekichuu_flg = True
                            m116_obj.save()

                    edit_mddb_list.append(m116_obj)

            return edit_mddb_list

        except Exception as e:
            Common_log.Out_Logs(log_err_msg_id, [failure(e)])
            logger.error(failure(e))
            return False
