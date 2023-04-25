
from app_jra.consts import *
from app_jra.models import *

from app_jra.log_commons import *
from datetime import datetime as dt
from datetime import timedelta
from logging import getLogger
import sys
import re

logger = getLogger('jra_edit_delivery')
Common_log = Common_log(DEBUGLOG_NAME_TO_TYPE[logger.name])
log_err_msg_id = 3400

def failure(e):
    exc_type, exc_obj, tb=sys.exc_info()
    lineno=tb.tb_lineno
    return str(lineno) + ":" + str(type(e)) + " " + str(e)

class Common():
    """
    共通関数 クラス

    Attributes
    ----------
    TOPROCESSEDDATDATA : str
        処理済みフォルダ(永続フォルダ：Processed/yyyy/mmdd)作成。

    """
    try:

        from app_jra.models import (
            Mst_Jou,
            Mst_Grade,
            Mst_Track,
            Mst_Kyousou_shubetsu,
            Mst_Kyousou_jouken,
            Mst_Kyousou_kigou,
            Mst_Juryo,
            Mst_Record,
            Mst_Uma_kigou,
            Mst_Horse,
            Mst_Kishu,
            Mst_Choukyoushi,
            Mst_Mikubun,
            Mst_Ijou,
            Mst_Baba,
            Mst_Tenkou,
            Mst_Record,
            Mst_Chakusa,
            Mst_Seibetsu,
            Mst_Keiro,
            Mst_Umanushi,
            Mst_Seisansha,

            
        )

        def chk_blank(self, record):
            # 受信データにはブランクのパターンがいくつかあるが、空データの場合は全てブランクとみなす処理
            blank_list = ['','　',None,[],{}]
            if record in blank_list or re.fullmatch('\s+', record):
                return False
            else:
                return True
                
        def chk_blank_zero(self, record):
            # 受信データにはブランクのパターンがいくつかあるが、ゼロの場合と空データの場合は全てブランクとみなす処理
            blank_list = ['','　',None,[],{},0,'0','０']
            if record in blank_list or re.fullmatch('\s+', record) or re.fullmatch('0+', record) or re.fullmatch('０+', record):
                return False
            else:
                return True
        
        def get_kaisaibi(self, jou_obj, kai, nichime):
            from app_jra.models import Mst_Schedule
            kaisaibi = None
            schedule_objs = Mst_Schedule.objects.filter(Jou=jou_obj, Kai=kai, Nichime=nichime)
            if schedule_objs.exists():
                kaisaibi = schedule_objs.last().Date
            return kaisaibi

        # I204 出馬表系データから本副回数を取得
        def get_honfuku_kaisuu(self, I204, hukuhon_bool):
            # hukuhon_bool:True 本
            # hukuhon_bool:Flase 副
            kaisuu = None
            if self.chk_blank_zero(I204.Tokubetsu_kyoso_hukuhon_kubun):
                if hukuhon_bool:
                    if int(I204.Tokubetsu_kyoso_hukuhon_kubun) == 1:
                        kaisuu = int(I204.Tokubetsu_kyoso_kaisuu)
                else:
                    if int(I204.Tokubetsu_kyoso_hukuhon_kubun) == 2:
                        kaisuu = int(I204.Tokubetsu_kyoso_kaisuu)
            return kaisuu

        # オッズ系コンテンツで使用
        def make_sakusei_time(self, FSOS_1):
            if FSOS_1.Shikibetsu == "1":
                sakusei_time = ZENJITSU_SAISHU
            else:
                sakusei_time = FSOS_1.Sakusei_time[0:2] + "・" + FSOS_1.Sakusei_time[2:4]
            return sakusei_time
        
        # オッズ系コンテンツで使用
        def make_hatsukubun(self, FSOS_1):
            if FSOS_1.Shikibetsu == "1":
                hatsukubun = HATSUBAIKUBUN_STATUS[0]
            elif FSOS_1.Shikibetsu == "2":
                hatsukubun = HATSUBAIKUBUN_STATUS[1]
            elif FSOS_1.Shikibetsu == "3":
                hatsukubun = HATSUBAIKUBUN_STATUS[2]
            elif FSOS_1.Shikibetsu == "4":
                hatsukubun = HATSUBAIKUBUN_STATUS[3]
            elif FSOS_1.Shikibetsu == "5":
                hatsukubun = HATSUBAIKUBUN_STATUS[4]
            return hatsukubun
        
        # オッズ系コンテンツで使用
        def make_maeuri_flg(self, FSOS_1):
            return self.make_hatsukubun(FSOS_1) == '前売'
            
        # オッズ系コンテンツで使用
        def make_reigaiinfo(self, oddsdata):
            if oddsdata == '0.0':
                return '発売票数無し'
            elif oddsdata == '0.1':
                return '登録なし'
            elif oddsdata == '0.2':
                return '発売前取消'
            elif oddsdata == '0.3':
                return '発売後取消'
            else:
                return None
        
        # ベスト30系コンテンツで使用
        def make_shouritsu(self, data):
            # 1111 → 1.111に変換(データがある場合のみ)
            shouritsu = None
            if self.chk_blank_zero(data):
                shouritsu = data[0:1] + '.' + data[1:4]
            return shouritsu


        # マスタチェックして、レコードが無い場合は●を組み合わせた仮レコードを作る。
        def chk_master_Mst_Horse(self, record, receive_data=None):
            if self.chk_blank_zero(record):
                if len(record) == 8:
                    record = '20' + record
                if Mst_Horse.objects.filter(Number=record).exists():
                    return Mst_Horse.objects.filter(Number=record).last()
                else:
                    if receive_data:
                        # 地方成績マスタの場合のみ、馬マスタにないデータが来た場合は仮データを馬名で作成する。
                        if 'Mst_CRES_Tihou_seiseki' == str(type(receive_data).__name__):
                            mst_obj, created = Mst_Horse.objects.update_or_create(
                                        Number=record,
                                        defaults = {
                                            'Bamei':receive_data.Tihou_seiseki_horse_name.rstrip(),
                                            'Bamei_9':receive_data.Tihou_seiseki_horse_name.rstrip().ljust(9, '　'),
                                            'Seibetsu':self.chk_master_Mst_Seibetsu(receive_data.Tihou_seiseki_seibetu_code),
                                            'Keiro':self.chk_master_Mst_Keiro(receive_data.Tihou_seiseki_keiro_code),
                                            'Kigou': self.chk_master_Mst_Uma_kigou(receive_data.Tihou_seiseki_horse_kigou_code),
                                            'CK_Geneki_flg' : True
                                        }
                                    )
                            return mst_obj
                        else:
                            mst_obj = Mst_Horse.objects.create(Number=record, Bamei=MST5CHAR, Bamei_9=MST9CHAR)
                            logger.warning(f'馬マスタ に Number: {record} Bamei: {MST5CHAR} Bamei_9: {MST9CHAR} を登録しました。')
                            return mst_obj
                    else:
                        mst_obj = Mst_Horse.objects.create(Number=record, Bamei=MST5CHAR, Bamei_9=MST9CHAR)
                        logger.warning(f'馬マスタ に Number: {record} Bamei: {MST5CHAR} Bamei_9: {MST9CHAR} を登録しました。')
                        return mst_obj
            else:
                return None

        def chk_master_Mst_Jou(self, record):
            if self.chk_blank_zero(record):
                if Mst_Jou.objects.filter(Jou_code=record).exists():
                    return Mst_Jou.objects.filter(Jou_code=record).last()
                else:
                    mst_jou = Mst_Jou.objects.create(Jou_code=record, Jou_name=MST5CHAR, shortened_3=MST3CHAR, shortened_1=MST1CHAR)
                    logger.warning(f'場マスタ に Jou_code: {record} Jou_name: {MST5CHAR} shortened_3: {MST3CHAR} shortened_1: {MST1CHAR}を登録しました。')
                    return mst_jou
            else:
                return None
                
        def chk_master_Mst_Kyousou_shubetsu(self, record):
            if self.chk_blank_zero(record):
                if Mst_Kyousou_shubetsu.objects.filter(Kyousou_shubetsu_code=record).exists():
                    return Mst_Kyousou_shubetsu.objects.filter(Kyousou_shubetsu_code=record).last()
                else:
                    mst_obj = Mst_Kyousou_shubetsu.objects.create(Kyousou_shubetsu_code=record, Kyousou_shubetsu_name=MST5CHAR)
                    logger.warning(f'競走種別マスタ に Kyousou_shubetsu_code: {record} Kyousou_shubetsu_name: {MST5CHAR}を登録しました。')
                    return mst_obj
            else:
                return None

        def chk_master_Mst_Grade(self, record):
            if self.chk_blank_zero(record) and not record == '_': # 重賞の場合、_(アンダーバー)も除外
                if Mst_Grade.objects.filter(Grade_code=record).exists():
                    return Mst_Grade.objects.filter(Grade_code=record).last()
                else:
                    mst_obj = Mst_Grade.objects.create(Grade_code=record, Grade_name=MST5CHAR)
                    logger.warning(f'グレードマスタ に Grade_code: {record} Grade_name: {MST5CHAR}を登録しました。')
                    return mst_obj
            else:
                return None
                    
        def chk_master_Mst_Track(self, record):
            if self.chk_blank_zero(record):
                if Mst_Track.objects.filter(Track_code=record).exists():
                    return Mst_Track.objects.filter(Track_code=record).last()
                else:
                    mst_obj = Mst_Track.objects.create(Track_code=record, Track_name=MST5CHAR)
                    logger.warning(f'トラックマスタ に Track_code: {record} Track_name: {MST5CHAR}を登録しました。')
                    return mst_obj
            else:
                return None


        def chk_master_Mst_Kyousou_kigou(self, record):
            if self.chk_blank_zero(record):
                if Mst_Kyousou_kigou.objects.filter(Kyosou_kigou_code=record).exists():
                    return Mst_Kyousou_kigou.objects.filter(Kyosou_kigou_code=record).last()
                elif Mst_Kyousou_kigou.objects.filter(Kyosou_kigou_code_2B=record).exists():
                    return Mst_Kyousou_kigou.objects.filter(Kyosou_kigou_code_2B=record).last()
                else:
                    mst_obj = Mst_Kyousou_kigou.objects.create(Kyosou_kigou_code=record, Kyosou_kigou_name=MST5CHAR)
                    logger.warning(f'競走記号マスタ に Kyosou_kigou_code: {record} Kyosou_kigou_name: {MST5CHAR}を登録しました。')
                    return mst_obj
            else:
                return None


        def chk_master_Mst_Juryo(self, record):
            if self.chk_blank_zero(record):
                if Mst_Juryo.objects.filter(Juryo_code=record).exists():
                    return Mst_Juryo.objects.filter(Juryo_code=record).last()
                else:
                    mst_obj = Mst_Juryo.objects.create(Juryo_code=record, Juryo_name=MST5CHAR)
                    logger.warning(f'重量種別マスタ に Juryo_code: {record} Juryo_name: {MST5CHAR}を登録しました。')
                    return mst_obj
            else:
                return None

        def chk_master_Mst_Record(self, record):
            if self.chk_blank_zero(record):
                if Mst_Record.objects.filter(Record_code=record).exists():
                    return Mst_Record.objects.filter(Record_code=record).last()
                else:
                    mst_obj = Mst_Record.objects.create(Record_code=record, Record_name=MST5CHAR)
                    logger.warning(f'レコード区分マスタ に Record_code: {record} Record_name: {MST5CHAR}を登録しました。')
                    return mst_obj
            else:
                return None

        def chk_master_Mst_Uma_kigou(self, record):
            if self.chk_blank_zero(record):
                if Mst_Uma_kigou.objects.filter(Uma_kigou_code=record).exists():
                    return Mst_Uma_kigou.objects.filter(Uma_kigou_code=record).last()
                else:
                    mst_obj = Mst_Uma_kigou.objects.create(Uma_kigou_code=record, Uma_kigou_name=MST1CHAR)
                    logger.warning(f'馬記号マスタ に Uma_kigou_code: {record} Uma_kigou_name: {MST1CHAR}を登録しました。')
                    return mst_obj
            else:
                return None


        def chk_master_Mst_Kyousou_jouken(self, record):
            if self.chk_blank_zero(record):
                if Mst_Kyousou_jouken.objects.filter(Kyousou_jouken_code=record).exists():
                    return Mst_Kyousou_jouken.objects.filter(Kyousou_jouken_code=record).last()
                else:
                    mst_obj = Mst_Kyousou_jouken.objects.create(Kyousou_jouken_code=record, Kyousou_jouken_name=MST5CHAR)
                    logger.warning(f'競走条件マスタ に Kyousou_jouken_code: {record} Kyousou_jouken_name: {MST5CHAR}を登録しました。')
                    return mst_obj
            else:
                return None


        def chk_master_Mst_Mikubun(self, record):
            if self.chk_blank_zero(record):
                if Mst_Mikubun.objects.filter(Mikubun_code=record).exists():
                    return Mst_Mikubun.objects.filter(Mikubun_code=record).last()
                else:
                    mst_obj = Mst_Mikubun.objects.create(Mikubun_code=record, Mikubun_name=MST5CHAR)
                    logger.warning(f'見習区分マスタ に Mikubun_code: {record} Mikubun_name: {MST5CHAR}を登録しました。')
                    return mst_obj
            else:
                return None

        def chk_master_Mst_Ijou(self, record):
            if self.chk_blank_zero(record):
                if Mst_Ijou.objects.filter(Ijou_code=record).exists():
                    return Mst_Ijou.objects.filter(Ijou_code=record).last()
                else:
                    mst_obj = Mst_Ijou.objects.create(Ijou_code=record, Ijou_name=MST5CHAR)
                    logger.warning(f'異常区分マスタ に Ijou_code: {record} Ijou_name: {MST5CHAR}を登録しました。')
                    return mst_obj
            else:
                return None

        def chk_master_Mst_Kishu(self, record):
            if self.chk_blank_zero(record):
                record = record.zfill(5)
                if Mst_Kishu.objects.filter(Kishu_code=record).exists():
                    return Mst_Kishu.objects.filter(Kishu_code=record).last()
                else:
                    mst_obj = Mst_Kishu.objects.create(Kishu_code=record, Kishu_name=MST3CHAR, Kishu_name_sei=MST5CHAR, Kishu_name_mei=MST5CHAR)
                    logger.warning(f'騎手マスタ に Kishu_code: {record} Kishu_name: {MST3CHAR} Kishu_name_sei: {MST5CHAR} Kishu_name_mei: {MST5CHAR}を登録しました。')
                    return mst_obj
            else:
                return None

        def chk_master_Mst_Choukyoushi(self, record):
            if self.chk_blank_zero(record):
                record = record.zfill(5)
                if Mst_Choukyoushi.objects.filter(Choukyoushi_code=record).exists():
                    return Mst_Choukyoushi.objects.filter(Choukyoushi_code=record).last()
                else:
                    mst_obj = Mst_Choukyoushi.objects.create(Choukyoushi_code=record, Choukyoushi_name=MST3CHAR ,Choukyoushi_name_sei=MST5CHAR ,Choukyoushi_name_mei=MST5CHAR)
                    logger.warning(f'調教師マスタ に Choukyoushi_code: {record} Choukyoushi_name: {MST3CHAR} Choukyoushi_name_sei :{MST5CHAR} Choukyoushi_name_mei :{MST5CHAR}を登録しました。')
                    return mst_obj
            else:
                return None

        def chk_master_Mst_Baba(self, record):
            if self.chk_blank_zero(record):
                if Mst_Baba.objects.filter(Baba_code=record).exists():
                    return Mst_Baba.objects.filter(Baba_code=record).last()
                else:
                    mst_obj = Mst_Baba.objects.create(Baba_code=record, Baba_name=MST5CHAR)
                    logger.warning(f'馬場状態マスタ に Baba_code: {record} Baba_name: {MST5CHAR}を登録しました。')
                    return mst_obj
            else:
                return None


        def chk_master_Mst_Tenkou(self, record):
            if self.chk_blank_zero(record):
                if Mst_Tenkou.objects.filter(Tenkou_code=record).exists():
                    return Mst_Tenkou.objects.filter(Tenkou_code=record).last()
                else:
                    mst_obj = Mst_Tenkou.objects.create(Tenkou_code=record, Tenkou_name=MST5CHAR)
                    logger.warning(f'天候マスタ に Tenkou_code: {record} Tenkou_name: {MST5CHAR}を登録しました。')
                    return mst_obj
            else:
                return None

        def chk_master_Mst_Chakusa(self, record):
            if self.chk_blank_zero(record) and not record == ' sp':
                if Mst_Chakusa.objects.filter(Chakusa_code=record).exists():
                    return Mst_Chakusa.objects.filter(Chakusa_code=record).last()
                else:
                    mst_obj = Mst_Chakusa.objects.create(Chakusa_code=record, Chakusa_name=MST5CHAR)
                    logger.warning(f'着差マスタ に Chakusa_code: {record} Chakusa_name: {MST5CHAR}を登録しました。')
                    return mst_obj
            else:
                return None

        def chk_master_Mst_Seibetsu(self, record):
            if self.chk_blank_zero(record):
                if Mst_Seibetsu.objects.filter(Seibetsu_code=record).exists():
                    return Mst_Seibetsu.objects.filter(Seibetsu_code=record).last()
                else:
                    mst_obj = Mst_Seibetsu.objects.create(Seibetsu_code=record, Seibetsu_name=MST1CHAR)
                    logger.warning(f'性別マスタ に Seibetsu_code: {record} Seibetsu_name: {MST1CHAR}を登録しました。')
                    return mst_obj
            else:
                return None

        def chk_master_Mst_Keiro(self, record):
            if self.chk_blank_zero(record):
                if Mst_Keiro.objects.filter(Keiro_code=record).exists():
                    return Mst_Keiro.objects.filter(Keiro_code=record).last()
                else:
                    mst_obj = Mst_Keiro.objects.create(Keiro_code=record, Keiro_name=MST3CHAR)
                    logger.warning(f'毛色マスタ に Keiro_code: {record} Keiro_name: {MST3CHAR}を登録しました。')
                    return mst_obj
            else:
                return None

        def chk_master_Mst_Umanushi(self, record):
            if self.chk_blank_zero(record):
                if Mst_Umanushi.objects.filter(Umanushi_code=record).exists():
                    return Mst_Umanushi.objects.filter(Umanushi_code=record).last()
                else:
                    mst_obj = Mst_Umanushi.objects.create(Umanushi_code=record, Umanushi_name=MST5CHAR)
                    logger.warning(f'馬主マスタ に Umanushi_code: {record} Umanushi_name: {MST5CHAR}を登録しました。')
                    return mst_obj
            else:
                return None


        def chk_master_Mst_Seisansha(self, record):
            if self.chk_blank_zero(record):
                if Mst_Seisansha.objects.filter(Seisansha_code=record).exists():
                    return Mst_Seisansha.objects.filter(Seisansha_code=record).last()
                else:
                    mst_obj = Mst_Seisansha.objects.create(Seisansha_code=record, Seisansha_name=MST5CHAR)
                    logger.warning(f'生産者マスタ に Seisansha_code: {record} Seisansha_name: {MST5CHAR}を登録しました。')
                    return mst_obj
            else:
                return None



    except Exception as e:
        Common_log.Out_Logs(log_err_msg_id, [failure(e)])
        logger.error(failure(e))