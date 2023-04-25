from logging import getLogger
import re
from app_jra.consts import *
import sys
import linecache
from decimal import *
from app_jra.log_commons import *
from datetime import datetime as dt
import subprocess


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

class Trn_Mst_Kyousou_seiseki():

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

    def get_kyousoujouken_JRES(self, data):
        if self.chk_blank_zero(data.RaceHorse_seiseki_kyoso_jouken_code1):
            return data.RaceHorse_seiseki_kyoso_jouken_code1
        elif self.chk_blank_zero(data.RaceHorse_seiseki_kyoso_jouken_code2):
            return data.RaceHorse_seiseki_kyoso_jouken_code2
        elif self.chk_blank_zero(data.RaceHorse_seiseki_kyoso_jouken_code3):
            return data.RaceHorse_seiseki_kyoso_jouken_code3
        elif self.chk_blank_zero(data.RaceHorse_seiseki_kyoso_jouken_code4):
            return data.RaceHorse_seiseki_kyoso_jouken_code4
        else:
            return None

    def get_kyousoujouken_CRES(self, data):
        if self.chk_blank_zero(data.Tihou_seiseki_kyoso_jouken_code1):
            return data.Tihou_seiseki_kyoso_jouken_code1
        elif self.chk_blank_zero(data.Tihou_seiseki_kyoso_jouken_code2):
            return data.Tihou_seiseki_kyoso_jouken_code2
        elif self.chk_blank_zero(data.Tihou_seiseki_kyoso_jouken_code3):
            return data.Tihou_seiseki_kyoso_jouken_code3
        elif self.chk_blank_zero(data.Tihou_seiseki_kyoso_jouken_code4):
            return data.Tihou_seiseki_kyoso_jouken_code4
        else:
            return None

    def make_name(self, rowname):
        # 余計な空白を削除して、「苗字　名前」形式にして返す
        return re.sub('[ 　]+', ' ', rowname.replace('　',' ').rstrip()).replace(' ','　')

    def intToZen(self, i):
        HAN2ZEN = str.maketrans({"0": "０", "1": "１", "2": "２", "3": "３", "4": "４", "5": "５", "6": "６", "7": "７", "8": "８", "9": "９", ".": "．", "G": "Ｇ"})
        if i or i == 0:
            return str(i).translate(HAN2ZEN)
        else:
            return

    def make_htime(self, record):
        htime = str(int(record[:3])) + "." + str(record[3:])
        return Decimal(htime)

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

    def get_grade(self, grade_new, grade_old):
        Cmn = Common()
        if self.chk_blank(grade_new):
            return Cmn.chk_master_Mst_Grade(grade_new)
        elif self.chk_blank(grade_old):
            return Cmn.chk_master_Mst_Grade(grade_old)
        else:
            return None
    
    def insert_or_update_Mst_Kyousou_seiseki(self, datfilename, datDataFileFlg):
        try:
            from app_jra.models import (
                Mst_JRES_RaceHorse_seiseki,
                Mst_CRES_Tihou_seiseki,
                Mst_Kyousou_seiseki,
                Mst_Horse
            )
            Cmn = Common()
            update_num = 0

            if M_RACEHORSE_SEISEKI == datDataFileFlg:
                
                if Mst_JRES_RaceHorse_seiseki.objects.filter(Receive_filename=datfilename).count() < 10000:
                    # 通常の成績データ受信時(1万件以下の場合)
                    JRES_list = Mst_JRES_RaceHorse_seiseki.objects.filter(Receive_filename=datfilename)
                    # 番組情報を取得
                    for data in JRES_list:
                        # 1.まず競走馬成績マスタのキーとなる馬情報(uma_obj)を取得
                        uma_obj = Cmn.chk_master_Mst_Horse(data.RaceHorse_seiseki_ketto_number)

                        # 2.競走馬成績マスタを「馬情報、出走日付、場名」で検索。レコードがあれば同一の出走履歴としていったん削除、新規作成。
                        if Mst_Kyousou_seiseki.objects.filter(
                            uma=uma_obj,
                            shubi=dt.strptime(data.RaceHorse_seiseki_kaisai_date, '%Y%m%d').date(),
                            shujoumei=Cmn.chk_master_Mst_Jou(data.RaceHorse_seiseki_kaisaijou_code)
                        ).exists():
                            Mst_Kyousou_seiseki.objects.filter(
                                uma=uma_obj,
                                shubi=dt.strptime(data.RaceHorse_seiseki_kaisai_date, '%Y%m%d').date(),
                                shujoumei=Cmn.chk_master_Mst_Jou(data.RaceHorse_seiseki_kaisaijou_code)
                            ).delete()

                        # 新規作成する
                        Mst_Kyousou_seiseki.objects.create(
                            uma = uma_obj,
                            shubi = dt.strptime(data.RaceHorse_seiseki_kaisai_date, '%Y%m%d').date(),
                            shujoumei = Cmn.chk_master_Mst_Jou(data.RaceHorse_seiseki_kaisaijou_code),
                            shukai = int(data.RaceHorse_seiseki_kaisai_kai),
                            shunichi = int(data.RaceHorse_seiseki_kaisai_nichi),
                            shubajousiba = Cmn.chk_master_Mst_Baba(data.RaceHorse_seiseki_terf_joutai_code),
                            shubajouda = Cmn.chk_master_Mst_Baba(data.RaceHorse_seiseki_dert_joutai_code),
                            shushubetsu = Cmn.chk_master_Mst_Kyousou_shubetsu(data.RaceHorse_seiseki_kyoso_syubetsu_code),
                            shutokuhonsu = int(data.RaceHorse_seiseki_kai) if data.RaceHorse_seiseki_huku_hon_kubun == '1' else None,
                            shutokuhon = data.RaceHorse_seiseki_kyoso_name_main.rstrip(),
                            shutokufukusu = int(data.RaceHorse_seiseki_kai) if data.RaceHorse_seiseki_huku_hon_kubun == '2' else None,
                            shutokufuku = data.RaceHorse_seiseki_kyoso_name_sub.rstrip() if self.chk_blank(data.RaceHorse_seiseki_kyoso_name_sub) else data.RaceHorse_seiseki_kyoso_name_kakko.rstrip(),

                            shuguredo = self.get_grade(data.RaceHorse_seiseki_grade_code_new, data.RaceHorse_seiseki_grade_code_old),
                            shukyori = int(data.RaceHorse_seiseki_kyori),
                            shutorakku = Cmn.chk_master_Mst_Track(data.RaceHorse_seiseki_track_code),
                            shujouken = Cmn.chk_master_Mst_Kyousou_jouken(self.get_kyousoujouken_JRES(data)),
                            shukigou = Cmn.chk_master_Mst_Kyousou_kigou(data.RaceHorse_seiseki_kyoso_kigou_code),
                            shushusu = int(data.RaceHorse_seiseki_syusso_yotei),

                            shujuni = int(data.RaceHorse_seiseki_kakutei_chakujun),
                            shunyuusen = int(data.RaceHorse_seiseki_nyusen_juni),
                            shuwaku = int(data.RaceHorse_seiseki_waku_ban),
                            shuuma = int(data.RaceHorse_seiseki_horse_ban),
                            shufuju = self.make_fujuu(data.RaceHorse_seiseki_syutsu_hutan_juryo),
                            shukimei = Cmn.chk_master_Mst_Kishu(data.RaceHorse_seiseki_kijo_kishu_code),
                            shumikubun = Cmn.chk_master_Mst_Mikubun(data.RaceHorse_seiseki_kijo_kishu_minarai_kubun_code),
                            shuf = int(data.RaceHorse_seiseki_nyusen_time[0:2]),
                            shub = int(data.RaceHorse_seiseki_nyusen_time[2:4]),
                            shum = int(data.RaceHorse_seiseki_nyusen_time[4:5]),
                            shuaite = data.RaceHorse_seiseki_kakutei_1or2_name.rstrip(),
                            shutsf = int(data.RaceHorse_seiseki_kakutei_1or2_sa_time[0:2]),
                            shutsb = int(data.RaceHorse_seiseki_kakutei_1or2_sa_time[2:4]),
                            shutsm = int(data.RaceHorse_seiseki_kakutei_1or2_sa_time[4:5]),
                            shuikubun = Cmn.chk_master_Mst_Ijou(data.RaceHorse_seiseki_ijou_kubun_code),
                            shukojun_1 = int(data.RaceHorse_seiseki_1corner_juni),
                            shukojun_2 = int(data.RaceHorse_seiseki_2corner_juni),
                            shukojun_3 = int(data.RaceHorse_seiseki_3corner_juni),
                            shukojun_4 = int(data.RaceHorse_seiseki_4corner_juni),

                            shua4h = self.make_htime(data.RaceHorse_seiseki_haron_time) if data.RaceHorse_seiseki_haron_time_kubun =='4' else None,
                            shua3h = self.make_htime(data.RaceHorse_seiseki_haron_time) if data.RaceHorse_seiseki_haron_time_kubun =='3' else None,
                            shubaju = int(data.RaceHorse_seiseki_horse_taijuu),
                            shunin = int(data.RaceHorse_seiseki_tansyo),
                        )
                        update_num += 1
                    logger.info(f'【競走馬成績マスタ】{str(update_num)}件 登録・更新')
                else:
                    # JRES 競走馬成績マスタ
                    
                    # 蓄積マスタCD(76万件)取り込み時など、10万件以上の場合の処理
                    # ★蓄積マスタ登録時は、馬情報がある馬の成績のみ登録する
                    ketto_num_list = list(Mst_Horse.objects.all().values_list('Number', flat=True))
                    JRES_list = Mst_JRES_RaceHorse_seiseki.objects.filter(RaceHorse_seiseki_ketto_number__in=ketto_num_list)
                    
                    create_fields_list = []
                    create_num = 0
                    interbal_num = 1

                    id_num = Mst_Kyousou_seiseki.objects.last().id + 1 if Mst_Kyousou_seiseki.objects.last() else 1

                    logger.info(f'競走馬成績マスタ {str(JRES_list.count())}件登録します')
                    logger.info(f'開始')
                    for data in JRES_list:

                        create_fields = {
                            "id" : id_num,
                            "uma" : Cmn.chk_master_Mst_Horse(data.RaceHorse_seiseki_ketto_number),
                            "shubi" : dt.strptime(data.RaceHorse_seiseki_kaisai_date, '%Y%m%d').date(),
                            "shujoumei" : Cmn.chk_master_Mst_Jou(data.RaceHorse_seiseki_kaisaijou_code),
                            "shukai" : int(data.RaceHorse_seiseki_kaisai_kai),
                            "shunichi" : int(data.RaceHorse_seiseki_kaisai_nichi),
                            "shubajousiba" : Cmn.chk_master_Mst_Baba(data.RaceHorse_seiseki_terf_joutai_code),
                            "shubajouda" : Cmn.chk_master_Mst_Baba(data.RaceHorse_seiseki_dert_joutai_code),
                            "shushubetsu" : Cmn.chk_master_Mst_Kyousou_shubetsu(data.RaceHorse_seiseki_kyoso_syubetsu_code),
                            "shutokuhonsu" :int(data.RaceHorse_seiseki_kai) if data.RaceHorse_seiseki_huku_hon_kubun == '1' else None,
                            "shutokuhon" :data.RaceHorse_seiseki_kyoso_name_main.rstrip(),
                            "shutokufukusu" :int(data.RaceHorse_seiseki_kai) if data.RaceHorse_seiseki_huku_hon_kubun == '2' else None,
                            "shutokufuku" :data.RaceHorse_seiseki_kyoso_name_sub.rstrip() if self.chk_blank(data.RaceHorse_seiseki_kyoso_name_sub) else data.RaceHorse_seiseki_kyoso_name_kakko.rstrip(),
                            
                            "shuguredo" : self.get_grade(data.RaceHorse_seiseki_grade_code_new, data.RaceHorse_seiseki_grade_code_old),
                            "shukyori" : int(data.RaceHorse_seiseki_kyori),
                            "shutorakku" : Cmn.chk_master_Mst_Track(data.RaceHorse_seiseki_track_code),
                            "shujouken" : Cmn.chk_master_Mst_Kyousou_jouken(self.get_kyousoujouken_JRES(data)),
                            "shukigou" : Cmn.chk_master_Mst_Kyousou_kigou(data.RaceHorse_seiseki_kyoso_kigou_code),
                            "shushusu" : int(data.RaceHorse_seiseki_syusso_yotei),

                            "shujuni" : int(data.RaceHorse_seiseki_kakutei_chakujun),
                            "shunyuusen" : int(data.RaceHorse_seiseki_nyusen_juni),
                            "shuwaku" : int(data.RaceHorse_seiseki_waku_ban),
                            "shuuma" : int(data.RaceHorse_seiseki_horse_ban),
                            "shufuju" : self.make_fujuu(data.RaceHorse_seiseki_syutsu_hutan_juryo),
                            "shukimei" : Cmn.chk_master_Mst_Kishu(data.RaceHorse_seiseki_kijo_kishu_code),
                            "shumikubun" : Cmn.chk_master_Mst_Mikubun(data.RaceHorse_seiseki_kijo_kishu_minarai_kubun_code),
                            "shuf" : int(data.RaceHorse_seiseki_nyusen_time[0:2]),
                            "shub" : int(data.RaceHorse_seiseki_nyusen_time[2:4]),
                            "shum" : int(data.RaceHorse_seiseki_nyusen_time[4:5]),
                            "shuaite" : data.RaceHorse_seiseki_kakutei_1or2_name.rstrip(),
                            "shutsf" : int(data.RaceHorse_seiseki_kakutei_1or2_sa_time[0:2]),
                            "shutsb" : int(data.RaceHorse_seiseki_kakutei_1or2_sa_time[2:4]),
                            "shutsm" : int(data.RaceHorse_seiseki_kakutei_1or2_sa_time[4:5]),
                            "shuikubun" : Cmn.chk_master_Mst_Ijou(data.RaceHorse_seiseki_ijou_kubun_code),
                            "shukojun_1" : int(data.RaceHorse_seiseki_1corner_juni),
                            "shukojun_2" : int(data.RaceHorse_seiseki_2corner_juni),
                            "shukojun_3" : int(data.RaceHorse_seiseki_3corner_juni),
                            "shukojun_4" : int(data.RaceHorse_seiseki_4corner_juni),

                            "shua4h" : self.make_htime(data.RaceHorse_seiseki_haron_time) if data.RaceHorse_seiseki_haron_time_kubun =='4' else None,
                            "shua3h" : self.make_htime(data.RaceHorse_seiseki_haron_time) if data.RaceHorse_seiseki_haron_time_kubun =='3' else None,
                            "shubaju" : int(data.RaceHorse_seiseki_horse_taijuu),
                            "shunin" : int(data.RaceHorse_seiseki_tansyo),
                        }
                        create_record = Mst_Kyousou_seiseki(**create_fields)
                        create_fields_list.append(create_record)
                        id_num += 1
                        if interbal_num >= 10000:
                            # 10000件ごとに一括登録
                            Mst_Kyousou_seiseki.objects.bulk_create(create_fields_list, batch_size=10000)
                            # 後始末
                            create_fields_list = []
                            logger.info(f'{str(create_num)}件まで登録 ')
                            interbal_num = 0
                        else:
                            interbal_num += 1
                        create_num += 1

                    # 最後の残りも登録
                    Mst_Kyousou_seiseki.objects.bulk_create(create_fields_list, batch_size=10000)
                    # 後始末
                    create_fields_list = []
                    update_num = create_num # ログ用
                    logger.info(f'【競走馬成績マスタ】{str(update_num)}件 登録・更新')

                # 同じ馬の成績データが5件以上ある場合は、6件目以前のデータを削除する
                logger.info(f'引退馬および、現役馬の6件目以前の成績データを削除 開始')
                delete_id_list = []
                for uma_obj in Mst_Horse.objects.all():
                    # 引退した馬の成績データを削除リストに追加する
                    if not uma_obj.JRA_Geneki_flg and not uma_obj.CK_Geneki_flg and not uma_obj.Kaigai_flg:
                        delete_id_list.extend(Mst_Kyousou_seiseki.objects.filter(uma=uma_obj).values_list('id', flat=True))
                    # 同じ馬の成績データが5件以上ある場合は、6件目以前のデータを削除リストに追加する
                    if Mst_Kyousou_seiseki.objects.filter(uma=uma_obj).count() > 5:
                        delete_id_list.extend(Mst_Kyousou_seiseki.objects.filter(uma=uma_obj).order_by('-shubi').values_list('id', flat=True)[5:])
                # 削除実行
                Mst_Kyousou_seiseki.objects.filter(id__in=delete_id_list).delete()
                logger.info(f'{str(len(delete_id_list))}件の成績データを削除')


            elif M_TIHOU_SEISEKI == datDataFileFlg:
                # CRES 競走馬地方成績マスタ
                CRES_list = Mst_CRES_Tihou_seiseki.objects.filter(Receive_filename=datfilename)
                # 番組情報を取得
                for data in CRES_list:
                    # Mst_Kyousou_seisekiに登録していく。M10との紐づけは行わず、あくまで競走馬成績マスタの更新のみ。
                    # 1.まず競走馬成績マスタのキーとなる馬情報(uma_obj)を取得
                    # 地方成績マスタの場合のみ、馬マスタにないデータが来た場合は仮データを馬名で作成する。
                    uma_obj = Cmn.chk_master_Mst_Horse(data.Tihou_seiseki_ketto_number, data)

                    # 2.競走馬成績マスタを「馬情報、出走日付、場名」で検索。レコードがあれば同一の出走履歴としていったん削除、新規作成。
                    if Mst_Kyousou_seiseki.objects.filter(
                        uma=uma_obj,
                        shubi=dt.strptime(data.Tihou_seiseki_kaisai_date, '%Y%m%d').date(),
                        shujoumei=Cmn.chk_master_Mst_Jou(data.Tihou_seiseki_kaisaijou_code)
                    ).exists():
                        Mst_Kyousou_seiseki.objects.filter(
                            uma=uma_obj,
                            shubi=dt.strptime(data.Tihou_seiseki_kaisai_date, '%Y%m%d').date(),
                            shujoumei=Cmn.chk_master_Mst_Jou(data.Tihou_seiseki_kaisaijou_code)
                        ).delete()

                    # 新規作成する
                    Mst_Kyousou_seiseki.objects.create(
                        uma=uma_obj,
                        shubi=dt.strptime(data.Tihou_seiseki_kaisai_date, '%Y%m%d').date(),
                        shujoumei=Cmn.chk_master_Mst_Jou(data.Tihou_seiseki_kaisaijou_code),
                        shukai=int(data.Tihou_seiseki_kaisai_kai),
                        shunichi=int(data.Tihou_seiseki_kaisai_nichi),
                        shubajousiba=Cmn.chk_master_Mst_Baba(data.Tihou_seiseki_terf_joutai_code),
                        shubajouda=Cmn.chk_master_Mst_Baba(data.Tihou_seiseki_dert_joutai_code),
                        shushubetsu=Cmn.chk_master_Mst_Kyousou_shubetsu(data.Tihou_seiseki_kyoso_syubetsu_code),
                        shutokuhonsu=int(data.Tihou_seiseki_kai) if data.Tihou_seiseki_huku_hon_kubun == '1' else None,
                        shutokuhon=data.Tihou_seiseki_kyoso_name_main.rstrip(),
                        shutokufukusu=int(data.Tihou_seiseki_kai) if data.Tihou_seiseki_huku_hon_kubun == '2' else None,
                        shutokufuku=data.Tihou_seiseki_kyoso_name_sub.rstrip() if self.chk_blank(data.Tihou_seiseki_kyoso_name_sub) else data.Tihou_seiseki_kyoso_name_kakko.rstrip(),
                        
                        shuguredo = self.get_grade(data.Tihou_seiseki_grade_code_new, data.Tihou_seiseki_grade_code_old),
                        shukyori = int(data.Tihou_seiseki_kyori),
                        shutorakku = Cmn.chk_master_Mst_Track(data.Tihou_seiseki_track_code),
                        shujouken = Cmn.chk_master_Mst_Kyousou_jouken(self.get_kyousoujouken_CRES(data)),
                        shukigou = Cmn.chk_master_Mst_Kyousou_kigou(data.Tihou_seiseki_kyoso_kigou_code),
                        shushusu = int(data.Tihou_seiseki_syusso_yotei),

                        shujuni = int(data.Tihou_seiseki_kakutei_chakujun),
                        shunyuusen = int(data.Tihou_seiseki_nyusen_juni),
                        shuwaku = int(data.Tihou_seiseki_waku_ban),
                        shuuma = int(data.Tihou_seiseki_horse_ban),
                        shufuju = self.make_fujuu(data.Tihou_seiseki_syutsu_hutan_juryo),
                        shukimei = Cmn.chk_master_Mst_Kishu(data.Tihou_seiseki_kijo_kishu_code),
                        shumikubun = Cmn.chk_master_Mst_Mikubun(data.Tihou_seiseki_kijo_kishu_minarai_kubun_code),
                        shuf = int(data.Tihou_seiseki_nyusen_time[0:2]),
                        shub = int(data.Tihou_seiseki_nyusen_time[2:4]),
                        shum = int(data.Tihou_seiseki_nyusen_time[4:5]),
                        shuaite = data.Tihou_seiseki_kakutei_1or2_name.rstrip(),
                        shutsf = int(data.Tihou_seiseki_kakutei_1or2_sa_time[0:2]),
                        shutsb = int(data.Tihou_seiseki_kakutei_1or2_sa_time[2:4]),
                        shutsm = int(data.Tihou_seiseki_kakutei_1or2_sa_time[4:5]),
                        shuikubun = Cmn.chk_master_Mst_Ijou(data.Tihou_seiseki_ijou_kubun_code),
                        shukojun_1 = int(data.Tihou_seiseki_1corner_juni),
                        shukojun_2 = int(data.Tihou_seiseki_2corner_juni),
                        shukojun_3 = int(data.Tihou_seiseki_3corner_juni),
                        shukojun_4 = int(data.Tihou_seiseki_4corner_juni),

                        shua4h = self.make_htime(data.RaceHorse_seiseki_haron_time) if data.Tihou_seiseki_haron_time_kubun =='4' else None,
                        shua3h = self.make_htime(data.RaceHorse_seiseki_haron_time) if data.Tihou_seiseki_haron_time_kubun =='3' else None,
                        shubaju = int(data.Tihou_seiseki_horse_taijuu),
                        shunin = int(data.Tihou_seiseki_tansyo),
                    )
                    update_num += 1
                logger.info(f'【競走馬成績マスタ】{str(update_num)}件 登録・更新')

                # 同じ馬の成績データが5件以上ある場合は、6件目以前のデータを削除する
                logger.info(f'現役馬の6件目以前の成績データを削除 開始')
                delete_id_list = []
                for uma_obj in Mst_Horse.objects.all():
                    # 同じ馬の成績データが5件以上ある場合は、6件目以前のデータを削除リストに追加する
                    if Mst_Kyousou_seiseki.objects.filter(uma=uma_obj).count() > 5:
                        delete_id_list.extend(Mst_Kyousou_seiseki.objects.filter(uma=uma_obj).order_by('-shubi').values_list('id', flat=True)[5:])
                # 削除実行
                Mst_Kyousou_seiseki.objects.filter(id__in=delete_id_list).delete()
                logger.info(f'{str(len(delete_id_list))}件の成績データを削除')

            # # jsonをdumpする場合はコメント解除
            # logger.info(f'Mst_Kyousou_seiseki_20220614.json 出力開始')
            # cmd_str = "python manage.py dumpdata app_jra.Mst_Kyousou_seiseki > Mst_Kyousou_seiseki_20220614.json"
            # subprocess.run(cmd_str, shell=True)
            # logger.info(f'json出力完了')

            return NORMAL

        except Exception as e:
            Common_log.Out_Logs(log_err_msg_id, [e])
            logger.error(failure(e))
            return ABNORMAL
