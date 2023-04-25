from logging import getLogger
import re
import mojimoji
from app_jra.consts import *
import sys
import linecache
from app_jra.log_commons import *
from datetime import datetime as dt
from datetime import timedelta
import itertools

logger = getLogger('jra_edit_delivery')
Common_log = Common_log(DEBUGLOG_NAME_TO_TYPE[logger.name])
log_err_msg_id = 3300
log_info_msg_id = 9399

try:
    from .mddb_commons import Common
except Exception as e:
    Common_log.Out_Logs(log_err_msg_id, [e])
    logger.error(f'commonsファイル読み込み失敗 : {e}')


def failure(e):
    exc_type, exc_obj, tb=sys.exc_info()
    lineno=tb.tb_lineno
    return str(lineno) + ":" + str(type(e)) + " " + str(e)

class Trn_M20_seiseki():

    def chk_blank(self, record):
        # 受信データにはブランクのパターンがいくつかあるが、空データの場合は全てブランクとみなす処理
        blank_list = ['','　',None,[],{}]
        if record in blank_list or re.fullmatch('\s+', record):
            return False
        else:
            return True

    def chk_blank_zero(self, record):
        # 受信データにはブランクのパターンがいくつかあるが、ゼロの場合と空データの場合は全てブランクとみなす処理
        blank_list = ['','　',None,[],{},0,'0','00','000','０','００','*','**','***','****','*****']
        if record in blank_list or re.fullmatch('\s+', record):
            return False
        else:
            return True

    def get_bareijouken(self, data):
        if self.chk_blank_zero(data.Kyoso_joken_code_2sai):
            return data.Kyoso_joken_code_2sai
        elif self.chk_blank_zero(data.Kyoso_joken_code_3sai):
            return data.Kyoso_joken_code_3sai   
        elif self.chk_blank_zero(data.Kyoso_joken_code_4sai):
            return data.Kyoso_joken_code_4sai
        elif self.chk_blank_zero(data.Kyoso_joken_code_5sai):
            return data.Kyoso_joken_code_5sai
        else:
            return None


    def get_rekekka(self, data):
        rekekka = None
        if self.chk_blank_zero(data.Seiseki_data_kyoso_info_kubun_code):
            if int(data.Seiseki_data_kyoso_info_kubun_code) == 5:
                rekekka = 'レース中止'
            elif int(data.Seiseki_data_kyoso_info_kubun_code) == 6:
                rekekka = 'レース成立'
        return rekekka

    def get_sareigai(self, data):
        sareigai = None
        if self.chk_blank_zero(data.Douchaku_kubun):
            if int(data.Douchaku_kubun) == 1:
                sareigai = '同着'
        if self.chk_blank_zero(data.Ijo_kubun):
            if Common().chk_master_Mst_Ijou(data.Ijo_kubun):
                mst_ijou = Common().chk_master_Mst_Ijou(data.Ijo_kubun)
                if mst_ijou.Ijou_name == '降着':
                    sareigai = '降着'
        return sareigai
        
    def get_kaisaibi(self, jou_obj, kai, nichime):
        from app_jra.models import Mst_Schedule
        kaisaibi = None
        schedule_objs = Mst_Schedule.objects.filter(Jou=jou_obj, Kai=kai, Nichime=nichime)
        if schedule_objs.exists():
            kaisaibi = schedule_objs.last().Date
        return kaisaibi

    def get_harajoukyou(self, fuseiritsu, tokubarai):
        harajoukyou = None
        if self.chk_blank_zero(fuseiritsu):
            if int(fuseiritsu) == 1:
                harajoukyou = '不成立'
        if self.chk_blank_zero(tokubarai):
            if int(tokubarai) == 1:
                harajoukyou = '特払い'
        return harajoukyou

    def get_kyousou_jouken(self, data):
        kyousou_jouken = None
        if int(data.Kyoso_joken_code_2sai):
            kyousou_jouken = data.Kyoso_joken_code_2sai
        if int(data.Kyoso_joken_code_3sai):
            kyousou_jouken = data.Kyoso_joken_code_3sai
        if int(data.Kyoso_joken_code_4sai):
            kyousou_jouken = data.Kyoso_joken_code_4sai
        if int(data.Kyoso_joken_code_5sai):
            kyousou_jouken = data.Kyoso_joken_code_5sai
        return kyousou_jouken


    def make_fujuu(self, data):
        fujuu = None
        if self.chk_blank(data):
            fujuu = data
            if len(data) == 3:
                if data[2:3] == '０' or data[2:3] == '0':
                    fujuu = data[0:2]
                else:
                    fujuu = data[0:2] + '．' + data[2:3]
        return fujuu
    
    def make_bajuuzougen(self, data):
        bajuuzougen = None
        if self.chk_blank(data):
            bajuuzougen = data.strip().replace('+','＋').replace('-','－')
        return bajuuzougen

    def make_name(self, rowname):
        # 余計な空白を削除して、「苗字　名前」形式にして返す
        return re.sub('[ 　]+', ' ', rowname.replace('　',' ').rstrip()).replace(' ','　')

    def insert_or_update_M20_seiseki(self, datfilename, datDataFileFlg):
        try:
            from app_jra.models import (
                Trn_A200_1_JARIS_seiseki, 
                Trn_A200_2_Seiseki_info,
                Trn_I154_Hayami_hyou,
                Trn_FSOS_1_OddsShiji,
                Trn_FSOS_2_Odds_Tansyou,
                Trn_FSOS_10_Shiji_Tansyou,
                Trn_FSOS_12_Shiji_Wakuren,
                Trn_FSOS_13_Shiji_Umaren,
                Trn_FSOS_15_Shiji_Umatan,
                Trn_FSOS_14_Shiji_Wide,
                Trn_FSOS_16_Shiji_Sanrenhuku,
                Trn_FSOS_17_Shiji_Sanrentan,
                M20_seiseki,
                M21_seiseki_chakujun,
                M22_1_hara_tan,
                M22_2_hara_fuku,
                M22_3_hara_wakupuku,
                M22_4_hara_umapuku,
                M22_5_hara_umatan,
                M22_6_hara_sanpuku,
                M22_7_hara_santan,
                M22_8_hara_wa,
            )
            Cmn = Common()

            # 正しく登録完了した場合、後続の待ち合わせ・配信処理で使うために、更新した中間DBのレコードを保持しておき、リストにして返す。
            # 登録失敗時には、False(ABNOMAL or 空のリスト)を返す。
            edit_mddb_list = []  # 戻り値初期値
            m20_obj = None

            if JARIS_SEISEKI == datDataFileFlg:
                # A200 JARIS 成績データレコードの場合
                # M20を登録
                A200_list = Trn_A200_1_JARIS_seiseki.objects.filter(Receive_filename=datfilename)
                # 番組情報を取得
                for data in A200_list:
                    jou_obj = Cmn.chk_master_Mst_Jou(data.Bangumi_Jou_code)
                    kaisaibi = dt.strptime(data.Bangumi_year + data.Seiseki_data_date, '%y%m%d').date()
                    rebangou = int(data.Bangumi_race_number)

                    # 既に同じレースのレコードが存在する場合は、更新する。(払戻情報がある場合、それも消えてしまうので)

                    m20_obj, created = M20_seiseki.objects.update_or_create(
                        joumei=jou_obj,
                        kaisuu = int(data.Bangumi_kai), 
                        kainichime = int(data.Bangumi_day),
                        rebangou=rebangou,
                        
                        defaults={
                            "kaisaibi" : kaisaibi,
                            "tenkou" : Cmn.chk_master_Mst_Tenkou(data.Seiseki_data_kyoso_tenko_code),
                            "bajousiba" : Cmn.chk_master_Mst_Baba(data.Seiseki_data_terf_baba_jotai_code),
                            "bajyouda" : Cmn.chk_master_Mst_Baba(data.Seiseki_data_dert_baba_jotai_code),
                            "shubetsu" : Cmn.chk_master_Mst_Kyousou_shubetsu(data.Seiseki_data_kyoso_syubetsu_code),
                            
                            "tokusouhonsuu" : Cmn.get_honfuku_kaisuu(data, True),
                            "tokusoumeihon" : data.Tokubetsu_kyoso_hondai.rstrip() if self.chk_blank(data.Tokubetsu_kyoso_hondai) else None,
                            "tokusoufukusuu" : Cmn.get_honfuku_kaisuu(data, False),
                            "tokusoumeifuku" : data.Tokubetsu_kyoso_hukudai.rstrip() if self.chk_blank(data.Tokubetsu_kyoso_hukudai) else None,
                            
                            "guredo" : Cmn.chk_master_Mst_Grade(data.Seiseki_data_grade),
                            "kyori" : int(data.Seiseki_data_kyori),
                            "torakku" : Cmn.chk_master_Mst_Track(data.Seiseki_data_track_code),
                            "shusuu" : int(data.Seiseki_data_syusso_kettei_tousuu) if data.Seiseki_data_syusso_kettei_tousuu else int(data.Seiseki_data_syusso_yotei_tousuu),
                            "jouken" : Cmn.chk_master_Mst_Kyousou_jouken(self.get_kyousou_jouken(data)),

                            "kigou" : Cmn.chk_master_Mst_Kyousou_kigou(data.Seiseki_data_kyoso_kigou_code),
                            "jyuuryoushubetsu" : Cmn.chk_master_Mst_Juryo(data.Seiseki_data_juryo_syubetsu_code),
                            "rekekka" : self.get_rekekka(data)
                        }
                    )

                    # A200 JARIS 成績（成績詳細）
                    # 既に該当レースのFSOS オッズ・支持率データレコードを受信している場合、単勝オッズと単勝人気をm21_objに付与する
                    FSOS_1_obj = None
                    FSOS_1_list = Trn_FSOS_1_OddsShiji.objects.filter(
                                    Kaisai_date=kaisaibi.strftime('%y%m%d'),
                                    Bangumi_Jou_code=data.Bangumi_Jou_code,
                                    Bangumi_race_number=data.Bangumi_race_number,
                                    )
                    if FSOS_1_list.exists():
                        FSOS_1_obj = FSOS_1_list.last()

                    # M21 成績_着順情報を登録
                    A200_2_list = Trn_A200_2_Seiseki_info.objects.filter(A200_1=data)
                    for seiseki_data in A200_2_list:
                        if self.chk_blank(seiseki_data.Horse_name):
                            umaban = int(seiseki_data.Ban)
                            # 単勝オッズを取得
                            tano = None
                            if FSOS_1_obj:
                                FSOS_2_list = Trn_FSOS_2_Odds_Tansyou.objects.filter(FSOS_1=FSOS_1_obj, num=umaban - 1)
                                if FSOS_2_list.exists():
                                    FSOS_2_obj = FSOS_2_list.last()
                                    tano = FSOS_2_obj.Tansyou_odds
                            # 単勝人気を取得
                            tannin = None
                            if FSOS_1_obj:
                                FSOS_10_list = Trn_FSOS_10_Shiji_Tansyou.objects.filter(FSOS_1=FSOS_1_obj, num=umaban - 1)
                                if FSOS_10_list.exists():
                                    FSOS_10_obj = FSOS_10_list.last()
                                    tannin = int(FSOS_10_obj.Shiji_Tansyou_ninki) if self.chk_blank_zero(FSOS_10_obj.Shiji_Tansyou_ninki) else None

                            m21_obj, created = M21_seiseki_chakujun.objects.update_or_create(
                                m20 = m20_obj,
                                uma = umaban,
                                defaults={
                                    "juni" : int(seiseki_data.Kakutei_chakujun),
                                    "nyuusenjuni" : int(seiseki_data.Nyusen_juni),
                                    "waku" : int(seiseki_data.Wakuban),

                                    "umajouhou" : Cmn.chk_master_Mst_Horse(seiseki_data.Ketto_touroku_number), 
                                    "fujuu" : self.make_fujuu(seiseki_data.Horse_syusso_hutan_juryo) if self.chk_blank_zero(seiseki_data.Horse_syusso_hutan_juryo) else self.make_fujuu(seiseki_data.Horse_happyo_hutan_juryo),

                                    "kishujouhou" : Cmn.chk_master_Mst_Kishu(seiseki_data.Kishu_code),

                                    "fun" : seiseki_data.Horse_nyusen_time[0:1] if int(seiseki_data.Horse_nyusen_time) else None,
                                    "byo" : seiseki_data.Horse_nyusen_time[1:3] if int(seiseki_data.Horse_nyusen_time) else None,
                                    "miri" : seiseki_data.Horse_nyusen_time[3:4] if int(seiseki_data.Horse_nyusen_time) else None,

                                    "reko" : Cmn.chk_master_Mst_Record(seiseki_data.Horse_recode_kubun_code),
                                    "sa_1" : Cmn.chk_master_Mst_Chakusa(seiseki_data.Horse_chakusa),
                                    "sa_2" : Cmn.chk_master_Mst_Chakusa(seiseki_data.Horse_plus_chakusa),
                                    "sa_3" : Cmn.chk_master_Mst_Chakusa(seiseki_data.Horse_plusplus_chakusa),
                                    "sareigai" : self.get_sareigai(seiseki_data),

                                    "bajuu" : int(seiseki_data.Horse_taijuu),
                                    "bajuuzougen" : self.make_bajuuzougen(seiseki_data.Horse_taijuu_sa),
                                    "tano" : tano, #:単勝オッズ
                                    "tannin" : tannin, #:単勝人気
                                    "choumei" : Cmn.chk_master_Mst_Choukyoushi(seiseki_data.Chokyoshi_code),
                                    "ikubunnai" : Cmn.chk_master_Mst_Ijou(seiseki_data.Ijo_kubun),
                                    "sanchi" : seiseki_data.Breeder_sanchi.rstrip(),
                                    "seisansha" : seiseki_data.Breeder_name.rstrip()
                                }
                            )

            elif HAYAMI_HYOU == datDataFileFlg:
                # I154 早見表データレコードの場合
                # M20、M22_1～M22_8を登録
                I154_list = Trn_I154_Hayami_hyou.objects.filter(Receive_filename=datfilename)
                # 番組情報を取得
                for data in I154_list:
                    jou_obj = Cmn.chk_master_Mst_Jou(data.Bangumi_Jou_code)
                    rebangou = int(data.Bangumi_race_number)
                    kaisaibi_temp = self.get_kaisaibi(jou_obj, int(data.Bangumi_kai), int(data.Bangumi_day))
                    if not kaisaibi_temp:
                        kaisaibi_temp = dt.strptime(data.Soushin_date, '%y%m%d').date()
                    kaisaibi = kaisaibi_temp

                    # M20_seisekiを取得する
                    m20_list = M20_seiseki.objects.filter(
                                joumei = jou_obj,
                                kaisuu = int(data.Bangumi_kai), 
                                kainichime = int(data.Bangumi_day),
                                rebangou = rebangou
                                )
                    if not m20_list.exists():
                        # M20が無い場合も、仮で作成する。
                        m20_obj = M20_seiseki.objects.create(
                                joumei = jou_obj,
                                kaisaibi = kaisaibi, 
                                kaisuu = int(data.Bangumi_kai), 
                                kainichime = int(data.Bangumi_day),
                                rebangou = rebangou
                                )
                    else:
                        m20_obj = m20_list.last()

                    # m20_objに情報を追加していく
                    m20_obj.tanharajyoukyou = self.get_harajoukyou(data.Huseiritsu_tansyo_key, data.Toku_tansyo_key)
                    m20_obj.fukuharajyoukyou = self.get_harajoukyou(data.Huseiritsu_hukusyo_key, data.Toku_hukusyo_key)
                    m20_obj.wakupukuharajoukyou = self.get_harajoukyou(data.Huseiritsu_wakuren_key, data.Toku_wakuren_key)
                    m20_obj.umapukuharajoukyou = self.get_harajoukyou(data.Huseiritsu_umaren_key, data.Toku_umaren_key)
                    m20_obj.umatanharajoukyou = self.get_harajoukyou(data.Huseiritsu_umatan_key, data.Toku_umatan_key)
                    m20_obj.sanpukuharajoukyou = self.get_harajoukyou(data.Huseiritsu_3renhuku_key, data.Toku_3renhuku_key)
                    m20_obj.santanharajoukyou = self.get_harajoukyou(data.Huseiritsu_3rentan_key, data.Toku_3rentan_key)
                    m20_obj.waharajoukyou = self.get_harajoukyou(data.Huseiritsu_wide_key, data.Toku_wide_key)
                    m20_obj.save()

                    # M22_1～M22_8を登録
                    # まずきれいにする
                    M22_1_hara_tan.objects.filter(m20=m20_obj).delete() 
                    M22_2_hara_fuku.objects.filter(m20=m20_obj).delete() 
                    M22_3_hara_wakupuku.objects.filter(m20=m20_obj).delete() 
                    M22_4_hara_umapuku.objects.filter(m20=m20_obj).delete() 
                    M22_5_hara_umatan.objects.filter(m20=m20_obj).delete() 
                    M22_6_hara_sanpuku.objects.filter(m20=m20_obj).delete() 
                    M22_7_hara_santan.objects.filter(m20=m20_obj).delete() 
                    M22_8_hara_wa.objects.filter(m20=m20_obj).delete()

                    # 既に該当レースのFSOS オッズ・支持率データレコードを受信しているかを確認
                    FSOS_1_obj = None
                    FSOS_1_list = Trn_FSOS_1_OddsShiji.objects.filter(
                                    Kaisai_date=kaisaibi.strftime('%y%m%d'),
                                    Bangumi_Jou_code=data.Bangumi_Jou_code,
                                    Bangumi_race_number=data.Bangumi_race_number,
                                    )
                    if FSOS_1_list.exists():
                        FSOS_1_obj = FSOS_1_list.last()

                    # M22_1 成績・払戻_単勝
                    for i in range(3):
                        num = i + 1
                        if self.chk_blank_zero(getattr(data, 'Tansyo' + str(num) + '_umaban',False)):
                            if int(getattr(data, 'Tansyo' + str(num) + '_umaban',False)):
                                tansaki = int(getattr(data, 'Tansyo' + str(num) + '_umaban',False))
                                tantounin = None
                                # 既に該当レースのFSOS オッズ・支持率データレコードを受信している場合、人気を取得する。なければスルー
                                if FSOS_1_obj:
                                    FSOS_10_list = Trn_FSOS_10_Shiji_Tansyou.objects.filter(FSOS_1=FSOS_1_obj, num=tansaki - 1)
                                    if FSOS_10_list.exists():
                                        tantounin = FSOS_10_list.last().Shiji_Tansyou_ninki
                                M22_1_hara_tan.objects.create(
                                    m20=m20_obj,
                                    tankumijoukyou = '無投票' if int(getattr(data, 'Tansyo' + str(num) + '_kin',100)) < 100 else None,
                                    tansaki  = tansaki,
                                    tanharakin  = int(getattr(data, 'Tansyo' + str(num) + '_kin',False)) if getattr(data, 'Tansyo' + str(num) + '_kin',False) else None,
                                    tantounin = tantounin
                                )

                    # M22_2 成績・払戻_複勝
                    for i in range(5):
                        num = i + 1
                        if self.chk_blank_zero(getattr(data, 'Hukusyo' + str(num) + '_umaban',False)):
                            if int(getattr(data, 'Hukusyo' + str(num) + '_umaban',False)):
                                M22_2_hara_fuku.objects.create(
                                    m20=m20_obj,
                                    fukukumijoukyou = '無投票' if int(getattr(data, 'Hukusyo' + str(num) + '_kin',100)) < 100 else None,
                                    fukusaki  = int(getattr(data, 'Hukusyo' + str(num) + '_umaban',False)),
                                    fukuharakin  = int(getattr(data, 'Hukusyo' + str(num) + '_kin',False)) if getattr(data, 'Hukusyo' + str(num) + '_kin',False) else None,
                                )

                    # M22_3 成績・払戻_枠連複
                    # 重複を許す1～8の組み合わせをリスト化して抽出して、順番を指定して組み合わせを取り出す
                    wakupuku_l = list(itertools.combinations_with_replacement(['1','2','3','4','5','6','7','8'], 2))
                    for i in range(3):
                        num = i + 1
                        waku_num = getattr(data, 'Wakuren' + str(num) + '_umaban',False)
                        if self.chk_blank_zero(waku_num):
                            if int(waku_num):
                                wakupukutounin = None
                                # 既に該当レースのFSOS オッズ・支持率データレコードを受信している場合、人気を取得する。なければスルー
                                if FSOS_1_obj:
                                    wakupuku_num = getattr(data, 'Wakuren' + str(num) + '_umaban',False)
                                    index_num = wakupuku_l.index((wakupuku_num[0:1],wakupuku_num[1:2]))

                                    FSOS_12_list = Trn_FSOS_12_Shiji_Wakuren.objects.filter(FSOS_1=FSOS_1_obj, num=index_num)
                                    if FSOS_12_list.exists():
                                        wakupukutounin = FSOS_12_list.last().Wakuren_ninki
                                M22_3_hara_wakupuku.objects.create(
                                    m20=m20_obj,
                                    wakupukukumijoukyou = '無投票' if int(getattr(data, 'Wakuren' + str(num) + '_kin',100)) < 100 else None,
                                    wakupukusaki  = int(waku_num[0:1]),
                                    wakupukuato  = int(waku_num[1:2]),
                                    wakupukuharakin  = int(getattr(data, 'Wakuren' + str(num) + '_kin',False)) if getattr(data, 'Wakuren' + str(num) + '_kin',False) else None,
                                    wakupukutounin = wakupukutounin
                                )
                    wakupuku_l = None # メモリ解放

                    # M22_4 成績・払戻_馬連複
                    # 重複なしの組み合わせ(順不同)をリスト化して抽出して、順番を指定して組み合わせを取り出す
                    umapuku_l = list(itertools.combinations(['01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18'], 2))
                    for i in range(3):
                        num = i + 1
                        if self.chk_blank_zero(getattr(data, 'Umaren' + str(num) + '_umaban',False)):
                            if int(getattr(data, 'Umaren' + str(num) + '_umaban',False)):
                                umapukutounin = None
                                # 既に該当レースのFSOS オッズ・支持率データレコードを受信している場合、人気を取得する。なければスルー
                                if FSOS_1_obj:
                                    umapuku_num = getattr(data, 'Umaren' + str(num) + '_umaban',False)
                                    index_num = umapuku_l.index((umapuku_num[0:2],umapuku_num[2:4]))

                                    FSOS_13_list = Trn_FSOS_13_Shiji_Umaren.objects.filter(FSOS_1=FSOS_1_obj, num=index_num)
                                    if FSOS_13_list.exists():
                                        umapukutounin = FSOS_13_list.last().Umaren_ninki
                                M22_4_hara_umapuku.objects.create(
                                    m20=m20_obj,
                                    umapukukumijoukyou = '無投票' if int(getattr(data, 'Umaren' + str(num) + '_kin',100)) < 100 else None,
                                    umapukusaki  = int(getattr(data, 'Umaren' + str(num) + '_umaban',False)[0:2]),
                                    umapukuato  = int(getattr(data, 'Umaren' + str(num) + '_umaban',False)[2:4]),
                                    umapukuharakin  = int(getattr(data, 'Umaren' + str(num) + '_kin',False)) if getattr(data, 'Umaren' + str(num) + '_kin',False) else None,
                                    umapukutounin = umapukutounin
                                )
                                
                    # M22_8 成績・払戻_ワイド
                    for i in range(7):
                        num = i + 1
                        if self.chk_blank_zero(getattr(data, 'Wide' + str(num) + '_umaban',False)):
                            if int(getattr(data, 'Wide' + str(num) + '_umaban',False)):
                                watounin = None
                                # 既に該当レースのFSOS オッズ・支持率データレコードを受信している場合、人気を取得する。なければスルー
                                if FSOS_1_obj:
                                    wa_num = getattr(data, 'Wide' + str(num) + '_umaban',False)
                                    index_num = umapuku_l.index((wa_num[0:2],wa_num[2:4]))

                                    FSOS_14_list = Trn_FSOS_14_Shiji_Wide.objects.filter(FSOS_1=FSOS_1_obj, num=index_num)
                                    if FSOS_14_list.exists():
                                        watounin = FSOS_14_list.last().Wide_ninki
                                M22_8_hara_wa.objects.create(
                                    m20=m20_obj,
                                    wakumijoukyou = '無投票' if int(getattr(data, 'Wide' + str(num) + '_kin',100)) < 100 else None,
                                    wasaki  = int(getattr(data, 'Wide' + str(num) + '_umaban',False)[0:2]),
                                    waato  = int(getattr(data, 'Wide' + str(num) + '_umaban',False)[2:4]),
                                    waharakin  = int(getattr(data, 'Wide' + str(num) + '_kin',False)) if getattr(data, 'Wide' + str(num) + '_kin',False) else None,
                                    watounin = watounin
                                )
                    umapuku_l = None # メモリ解放

                    # M22_5 成績・払戻_馬連単
                    # 重複なしのあらゆる並びをリスト化して抽出して、順番を指定して組み合わせを取り出す
                    umatan_l = list(itertools.permutations(['01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18'], 2))
                    for i in range(6):
                        num = i + 1
                        if self.chk_blank_zero(getattr(data, 'Umatan' + str(num) + '_umaban',False)):
                            if int(getattr(data, 'Umatan' + str(num) + '_umaban',False)):
                                umatantounin = None
                                # 既に該当レースのFSOS オッズ・支持率データレコードを受信している場合、人気を取得する。なければスルー
                                if FSOS_1_obj:
                                    umatan_num = getattr(data, 'Umatan' + str(num) + '_umaban',False)
                                    index_num = umatan_l.index((umatan_num[0:2],umatan_num[2:4]))

                                    FSOS_15_list = Trn_FSOS_15_Shiji_Umatan.objects.filter(FSOS_1=FSOS_1_obj, num=index_num)
                                    if FSOS_15_list.exists():
                                        umatantounin = FSOS_15_list.last().Umatan_ninki
                                M22_5_hara_umatan.objects.create(
                                    m20=m20_obj,
                                    umatankumijoukyou = '無投票' if int(getattr(data, 'Umatan' + str(num) + '_kin',100)) < 100 else None,
                                    umatansaki  = int(getattr(data, 'Umatan' + str(num) + '_umaban',False)[0:2]),
                                    umatanato  = int(getattr(data, 'Umatan' + str(num) + '_umaban',False)[2:4]),
                                    umatanharakin  = int(getattr(data, 'Umatan' + str(num) + '_kin',False)) if getattr(data, 'Umatan' + str(num) + '_kin',False) else None,
                                    umatantounin = umatantounin
                                )
                    umatan_l = None # メモリ解放

                    # M22_6 成績・払戻_三連複
                    # 重複なしの3つの数字の組み合わせをリスト化して抽出して、順番を指定して組み合わせを取り出す
                    sanpuku_l = list(itertools.combinations(['01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18'], 3))
                    for i in range(3):
                        num = i + 1
                        if self.chk_blank_zero(getattr(data, 'Sanrenhuku' + str(num) + '_umaban',False)):
                            if int(getattr(data, 'Sanrenhuku' + str(num) + '_umaban',False)):
                                sanpukutounin = None
                                # 既に該当レースのFSOS オッズ・支持率データレコードを受信している場合、人気を取得する。なければスルー
                                if FSOS_1_obj:
                                    sanpuku_num = getattr(data, 'Sanrenhuku' + str(num) + '_umaban',False)
                                    index_num = sanpuku_l.index((sanpuku_num[0:2],sanpuku_num[2:4],sanpuku_num[4:6]))

                                    FSOS_16_list = Trn_FSOS_16_Shiji_Sanrenhuku.objects.filter(FSOS_1=FSOS_1_obj, num=index_num)
                                    if FSOS_16_list.exists():
                                        sanpukutounin = FSOS_16_list.last().Sanrenhuku_ninki
                                M22_6_hara_sanpuku.objects.create(
                                    m20=m20_obj,
                                    sanpukukumijoukyou = '無投票' if int(getattr(data, 'Sanrenhuku' + str(num) + '_kin',100)) < 100 else None,
                                    sanpukusaki  = int(getattr(data, 'Sanrenhuku' + str(num) + '_umaban',False)[0:2]),
                                    sanpukunaka  = int(getattr(data, 'Sanrenhuku' + str(num) + '_umaban',False)[2:4]),
                                    sanpukuato  = int(getattr(data, 'Sanrenhuku' + str(num) + '_umaban',False)[4:6]),
                                    sanpukuharakin  = int(getattr(data, 'Sanrenhuku' + str(num) + '_kin',False)) if getattr(data, 'Sanrenhuku' + str(num) + '_kin',False) else None,
                                    sanpukutounin = sanpukutounin
                                )
                    sanpuku_l = None # メモリ解放

                    # M22_7 成績・払戻_三連単
                    # 4896通りの組み合わせを格納した巨大リストをメモリに保持することになる。取り扱い要注意
                    santan_l = list(itertools.permutations(['01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18'], 3))
                    for i in range(6):
                        num = i + 1
                        if self.chk_blank_zero(getattr(data, 'Sanrentan' + str(num) + '_umaban',False)):
                            if int(getattr(data, 'Sanrentan' + str(num) + '_umaban',False)):
                                santantounin = None
                                # 既に該当レースのFSOS オッズ・支持率データレコードを受信している場合、人気を取得する。なければスルー
                                if FSOS_1_obj:
                                    santan_num = getattr(data, 'Sanrentan' + str(num) + '_umaban',False)
                                    index_num = santan_l.index((santan_num[0:2],santan_num[2:4],santan_num[4:6]))

                                    FSOS_17_list = Trn_FSOS_17_Shiji_Sanrentan.objects.filter(FSOS_1=FSOS_1_obj, num=index_num)
                                    if FSOS_17_list.exists():
                                        santantounin = FSOS_17_list.last().Sanrentan_ninki
                                M22_7_hara_santan.objects.create(
                                    m20=m20_obj,
                                    santankumijoukyou = '無投票' if int(getattr(data, 'Sanrentan' + str(num) + '_kin',100)) < 100 else None,
                                    santansaki  = int(getattr(data, 'Sanrentan' + str(num) + '_umaban',False)[0:2]),
                                    santannaka  = int(getattr(data, 'Sanrentan' + str(num) + '_umaban',False)[2:4]),
                                    santanato  = int(getattr(data, 'Sanrentan' + str(num) + '_umaban',False)[4:6]),
                                    santanharakin  = int(getattr(data, 'Sanrentan' + str(num) + '_kin',False)) if getattr(data, 'Sanrentan' + str(num) + '_kin',False) else None,
                                    santantounin = santantounin
                                )
                    santan_l = None # メモリ解放

            elif ODDSSHIJI == datDataFileFlg:
                # FSOS オッズ・支持率データレコードの場合
                # M21、M22_1～M22_8を更新する
                FSOS_1_list = Trn_FSOS_1_OddsShiji.objects.filter(Receive_filename=datfilename)
                # 番組情報を取得
                for data in FSOS_1_list:
                    jou_obj = Cmn.chk_master_Mst_Jou(data.Bangumi_Jou_code)
                    rebangou = int(data.Bangumi_race_number)
                    kaisaibi = dt.strptime(data.Kaisai_date, '%y%m%d').date()

                    # M20_seisekiを取得する
                    m20_list = M20_seiseki.objects.filter(
                                joumei = jou_obj,
                                kaisuu = int(data.Bangumi_kai), 
                                kainichime = int(data.Bangumi_day),
                                rebangou = rebangou
                                )
                    if not m20_list.exists():
                        # M20が無い場合も、仮で作成する。
                        m20_obj = M20_seiseki.objects.create(
                                joumei = jou_obj,
                                kaisaibi = kaisaibi, 
                                kaisuu = int(data.Bangumi_kai), 
                                kainichime = int(data.Bangumi_day),
                                rebangou = rebangou
                                )
                    else:
                        m20_obj = m20_list.last()

                    # M21 各馬の単勝オッズを更新する
                    FSOS_2_list = Trn_FSOS_2_Odds_Tansyou.objects.filter(FSOS_1=data)
                    for FSOS_2 in FSOS_2_list:
                        if not FSOS_2.Tansyou_odds.split('.')[0] == '0': # 0点台のオッズの場合、発売されていない組み合わせ
                            umaban = FSOS_2.num + 1
                            m21_obj_list = M21_seiseki_chakujun.objects.filter(m20=m20_obj,uma=umaban)
                            if m21_obj_list.exists():
                                for m21_obj in m21_obj_list:
                                    m21_obj.tano = FSOS_2.Tansyou_odds # 単勝オッズ
                                    m21_obj.save()
                            else:
                                # M21が無い場合も、仮で作成する。
                                m21_obj = M21_seiseki_chakujun.objects.create(
                                            m20 = m20_obj,
                                            uma = umaban, 
                                            tano = FSOS_2.Tansyou_odds # 単勝オッズ
                                        )

                    # M22_1～M22_8を更新する
                    # M22が無い場合(オッズ・支持率がJARIS成績や早見表より先に来た場合)は、
                    # JARIS成績や早見表の登録時に改めてFSOSを検索・登録するため、ここではなにもしない

                    # M22_1 成績・払戻_単勝
                    FSOS_10_list = Trn_FSOS_10_Shiji_Tansyou.objects.filter(FSOS_1=data)
                    for FSOS_10 in FSOS_10_list:
                        tansaki = FSOS_10.num + 1 
                        tantounin = int(FSOS_10.Shiji_Tansyou_ninki) if self.chk_blank_zero(FSOS_10.Shiji_Tansyou_ninki) else None
                        # M22レコードを探してくる
                        if tantounin:
                            m22_list = M22_1_hara_tan.objects.filter(m20=m20_obj,tansaki=tansaki)
                            if m22_list.exists():
                                m22_obj = m22_list.last()
                                m22_obj.tantounin = tantounin
                                m22_obj.save()

                            # M21 各馬の単勝人気も更新する
                            m21_obj_list = M21_seiseki_chakujun.objects.filter(m20=m20_obj,uma=tansaki)
                            if m21_obj_list.exists():
                                for m21_obj in m21_obj_list:
                                    m21_obj.tannin = tantounin # 単勝人気
                                    m21_obj.save()

                    # M22_3 成績・払戻_枠連複
                    FSOS_12_list = Trn_FSOS_12_Shiji_Wakuren.objects.filter(FSOS_1=data)
                    # 重複を許す1～8の組み合わせをリスト化して抽出して、順番を指定して組み合わせを取り出す
                    wakupuku_l = list(itertools.combinations_with_replacement(['1','2','3','4','5','6','7','8'], 2))
                    for FSOS_12 in FSOS_12_list:
                        renpukunums = wakupuku_l[FSOS_12.num]
                        wakupukusaki = int(renpukunums[0])
                        wakupukuato = int(renpukunums[1])
                        wakupukutounin = int(FSOS_12.Wakuren_ninki) if self.chk_blank_zero(FSOS_12.Wakuren_ninki) else None
                        # M22レコードを探してくる
                        if wakupukutounin:
                            m22_list = M22_3_hara_wakupuku.objects.filter(m20=m20_obj,wakupukusaki=wakupukusaki,wakupukuato=wakupukuato)
                            if m22_list.exists():
                                m22_obj = m22_list.last()
                                m22_obj.wakupukutounin = wakupukutounin
                                m22_obj.save()
                    wakupuku_l = None # メモリ解放

                    # M22_4 成績・払戻_馬連複
                    FSOS_13_list = Trn_FSOS_13_Shiji_Umaren.objects.filter(FSOS_1=data)
                    
                    # 重複なしの組み合わせ(順不同)をリスト化して抽出して、順番を指定して組み合わせを取り出す
                    umapuku_l = list(itertools.combinations(['01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18'], 2))
                    for FSOS_13 in FSOS_13_list:
                        renpukunums = umapuku_l[FSOS_13.num] 
                        umapukusaki = int(renpukunums[0])
                        umapukuato = int(renpukunums[1])
                        umapukutounin = int(FSOS_13.Umaren_ninki) if self.chk_blank_zero(FSOS_13.Umaren_ninki) else None
                        # M22レコードを探してくる
                        if umapukutounin:
                            m22_list = M22_4_hara_umapuku.objects.filter(m20=m20_obj,umapukusaki=umapukusaki,umapukuato=umapukuato)
                            if m22_list.exists():
                                m22_obj = m22_list.last()
                                m22_obj.umapukutounin = umapukutounin
                                m22_obj.save()

                    # M22_8 成績・払戻_ワイド
                    FSOS_14_list = Trn_FSOS_14_Shiji_Wide.objects.filter(FSOS_1=data)
                    for FSOS_14 in FSOS_14_list:
                        renpukunums = umapuku_l[FSOS_14.num]
                        wasaki = int(renpukunums[0])
                        waato = int(renpukunums[1])
                        watounin = int(FSOS_14.Wide_ninki) if self.chk_blank_zero(FSOS_14.Wide_ninki) else None
                        # M22レコードを探してくる
                        if watounin:
                            m22_list = M22_8_hara_wa.objects.filter(m20=m20_obj,wasaki=wasaki,waato=waato)
                            if m22_list.exists():
                                m22_obj = m22_list.last()
                                m22_obj.watounin = watounin
                                m22_obj.save()
                    umapuku_l = None # メモリ解放

                    # M22_5 成績・払戻_馬連単
                    # 重複なしのあらゆる並びをリスト化して抽出して、順番を指定して組み合わせを取り出す
                    umatan_l = list(itertools.permutations(['01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18'], 2))
                    FSOS_15_list = Trn_FSOS_15_Shiji_Umatan.objects.filter(FSOS_1=data)
                    for FSOS_15 in FSOS_15_list:
                        rentannums = umatan_l[FSOS_15.num]
                        umatansaki = int(rentannums[0])
                        umatanato = int(rentannums[1])
                        umatantounin = int(FSOS_15.Umatan_ninki) if self.chk_blank_zero(FSOS_15.Umatan_ninki) else None
                        # M22レコードを探してくる
                        if umatantounin:
                            m22_list = M22_5_hara_umatan.objects.filter(m20=m20_obj,umatansaki=umatansaki,umatanato=umatanato)
                            if m22_list.exists():
                                m22_obj = m22_list.last()
                                m22_obj.umatantounin = umatantounin
                                m22_obj.save()
                    umatan_l = None # メモリ解放



                    # M22_6 成績・払戻_三連複
                    FSOS_16_list = Trn_FSOS_16_Shiji_Sanrenhuku.objects.filter(FSOS_1=data)
                    # 重複なしの3つの数字の組み合わせをリスト化して抽出して、順番を指定して組み合わせを取り出す
                    sanpuku_l = list(itertools.combinations(['01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18'], 3))
                    for FSOS_16 in FSOS_16_list:
                        sanpukunums = sanpuku_l[FSOS_16.num] 
                        sanpukusaki = int(sanpukunums[0])
                        sanpukunaka = int(sanpukunums[1])
                        sanpukuato = int(sanpukunums[2])
                        sanpukutounin = int(FSOS_16.Sanrenhuku_ninki) if self.chk_blank_zero(FSOS_16.Sanrenhuku_ninki) else None
                        # M22レコードを探してくる
                        if sanpukutounin:
                            m22_list = M22_6_hara_sanpuku.objects.filter(m20=m20_obj,sanpukusaki=sanpukusaki,sanpukunaka=sanpukunaka,sanpukuato=sanpukuato)
                            if m22_list.exists():
                                m22_obj = m22_list.last()
                                m22_obj.sanpukutounin = sanpukutounin
                                m22_obj.save()
                    sanpuku_l = None # メモリ解放


                    # M22_7 成績・払戻_三連単
                    FSOS_17_list = Trn_FSOS_17_Shiji_Sanrentan.objects.filter(FSOS_1=data)
                    # 重複なしの3つの数字のあらゆる並びをリスト化して抽出して、順番を指定して組み合わせを取り出す
                    santan_l = list(itertools.permutations(['01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18'], 3))
                    for FSOS_17 in FSOS_17_list:
                        santannums = santan_l[FSOS_17.num]
                        santansaki = int(santannums[0])
                        santannaka = int(santannums[1])
                        santanato = int(santannums[2])
                        santantounin = int(FSOS_17.Sanrentan_ninki) if self.chk_blank_zero(FSOS_17.Sanrentan_ninki) else None
                        # M22レコードを探してくる
                        if santantounin:
                            m22_list = M22_7_hara_santan.objects.filter(m20=m20_obj,santansaki=santansaki,santannaka=santannaka,santanato=santanato)
                            if m22_list.exists():
                                m22_obj = m22_list.last()
                                m22_obj.santantounin = santantounin
                                m22_obj.save()
                    santan_l = None # メモリ解放

            # 最後に、M20を後続処理に渡す
            if m20_obj:
                edit_mddb_list.append(m20_obj)

            return edit_mddb_list

        except Exception as e:
            Common_log.Out_Logs(log_err_msg_id, [failure(e)])
            logger.error(failure(e))



