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


class Trn_M00_tousuuhyou():


    def get_kaisaibi(self, jou_obj, kai, nichime):
        from app_jra.models import Mst_Schedule
        kaisaibi = None
        schedule_objs = Mst_Schedule.objects.filter(Jou=jou_obj, Kai=kai, Nichime=nichime)
        if schedule_objs.exists():
            kaisaibi = schedule_objs.last().Date
        return kaisaibi


    def chk_blank(self, record):
        # 受信データにはブランクのパターンがいくつかあるが、空データの場合は全てブランクとみなす処理
        blank_list = ['','　',None,[],{}]
        if record in blank_list or re.fullmatch('\s+', record):
            return False
        else:
            return True

    def chk_blank_zero(self, record):
        # 受信データにはブランクのパターンがいくつかあるが、ゼロの場合と空データの場合は全てブランクとみなす処理
        blank_list = ['','　',None,[],{},0,'0','00','000','０','００']
        if record in blank_list or re.fullmatch('\s+', record) or re.fullmatch('0+', record) or re.fullmatch('０+', record):
            return False
        else:
            return True

    def get_shusuu_fields(self, renum):
        shusuu = "Kyoso_info" + str(renum) + "_Syusso_tousuu"
        return str(shusuu)


    def get_rebangou_fields(self, renum):
        rebangou = "Kyoso_info" + str(renum) + "_Kyoso_number"
        return str(rebangou)


    def insert_or_update_M00_tousuuhyou(self, datfilename, datDataFileFlg):
        try:
            from app_jra.models import (
                Trn_A222_Tousuuhyou,
                M00_tousuuhyou
            )
            Cmn = Common()

            # 受信ファイルごとに、M00レコードを新規作成・更新していく。
            # 正しく登録完了した場合、後続の待ち合わせ・配信処理で使うために、更新した中間DBのレコードを保持しておき、リストにして返す。
            # 登録失敗時には、False(ABNOMAL or 空のリスト)を返す。
            edit_mddb_list = [] # 初期値
            m00_obj_list = []
         
            if TOUSUUHYOU == datDataFileFlg:
                # A222 頭数表
                A222_list = Trn_A222_Tousuuhyou.objects.filter(Receive_filename=datfilename)
                for data in A222_list:

                    jou_obj = Cmn.chk_master_Mst_Jou(data.Tousuu_Jou_code)
                    kaisuu = int(data.Tousuu_kai)
                    kainichime = int(data.Tousuu_day)

                    kaisai_date = self.get_kaisaibi(jou_obj,kaisuu,kainichime)
                    if not kaisai_date:
                        Common_log.Out_Logs(log_err_msg_id, [f'{str(kaisuu)}回{jou_obj.Jou_name}{str(kainichime)}日目の、スケジュールマスタがありません'])
                        logger.error(f'{str(kaisuu)}回{jou_obj.Jou_name}{str(kainichime)}日目の、スケジュールマスタがありません')
                        return False
                    # A222は16競走分なので16回ループ
                    for i in range(16):
                        renum = i + 1

                        rebangou = int(getattr(data, self.get_rebangou_fields(renum)))
                        # ループごとの競走番号で00かチェックして00ならスキップ
                        if rebangou:

                            # 初期値
                            henriyuu=None
                            kyuurebangou=None
                            kyuutorakku=None
                            kyuukyori=None
                            bunkatsu=None
                            sintorakku=None
                            sinkyori=None
                            chuushinai=None

                            # 旧競走番号がレース番号と異なる場合
                            old_rebangou = getattr(data, 'Kyoso_info'+ str(renum) +'_Old_kyoso_number')
                            if self.chk_blank_zero(old_rebangou):
                                if not int(old_rebangou) == rebangou:
                                    kyuurebangou = int(old_rebangou)
                                    henriyuu = '競走番号変更'
                            
                            # 順序変更区分がある場合
                            junjo_henkou = getattr(data, 'Kyoso_info'+ str(renum) +'_Junjo_henkou_kubun')
                            if self.chk_blank_zero(junjo_henkou):
                                if junjo_henkou == '1':
                                    henriyuu = henriyuu + '、' + '順序変更' if henriyuu else '順序変更'

                            # 距離トラック変更区分がある場合
                            henkou_kubun = getattr(data, 'Kyoso_info'+ str(renum) +'_Kyoritrack_henkou_kubun')
                            if self.chk_blank_zero(henkou_kubun):
                                if int(henkou_kubun) == 2:
                                    henriyuu = henriyuu + '、' + '馬場変更' if henriyuu else '馬場変更'
                                    kyuutorakku = Cmn.chk_master_Mst_Track(getattr(data, 'Kyoso_info'+ str(renum) +'_Track_code1'))
                                    if self.chk_blank_zero(getattr(data, 'Kyoso_info'+ str(renum) +'_Kyori1')):
                                        kyuukyori = int(getattr(data, 'Kyoso_info'+ str(renum) +'_Kyori1'))
                                    sintorakku = Cmn.chk_master_Mst_Track(getattr(data, 'Kyoso_info'+ str(renum) +'_Track_code2'))
                                    if self.chk_blank_zero(getattr(data, 'Kyoso_info'+ str(renum) +'_Kyori2')):
                                        sinkyori = int(getattr(data, 'Kyoso_info'+ str(renum) +'_Kyori2'))
                                        henriyuu = henriyuu + '、' + '距離変更' if henriyuu else '距離変更'
                                    
                            # 分割区分がある場合
                            bunkatsu_kubun = getattr(data, 'Kyoso_info'+ str(renum) +'_Bunkatsu_kubun')
                            if self.chk_blank_zero(bunkatsu_kubun):
                                if bunkatsu_kubun == 'A':
                                    bunkatsu = '分割A'
                                elif bunkatsu_kubun == 'B':
                                    bunkatsu = '分割B'
                                elif bunkatsu_kubun == 'C':
                                    bunkatsu = '分割C'
                                elif bunkatsu_kubun == 'D':
                                    bunkatsu = '分割D'

                            # 中止・取止区分がある場合
                            chuushi_kubun = getattr(data, 'Kyoso_info'+ str(renum) +'_Chuushi_Toriyame_kubun')
                            if self.chk_blank_zero(chuushi_kubun):
                                if chuushi_kubun == '1':
                                    chuushinai = '特別登録時に取止め'
                                elif chuushi_kubun == '2':
                                    chuushinai = '出馬投票中止'

                            # M00を更新・作成
                            m00_obj, created = M00_tousuuhyou.objects.update_or_create(
                                kaisaibi=kaisai_date,
                                joumei=jou_obj,
                                kaisuu=kaisuu,
                                kainichime=kainichime,
                                rebangou=rebangou,
                                defaults={
                                    'shusuu' : int(getattr(data, self.get_shusuu_fields(renum))),
                                    'henriyuu':henriyuu,
                                    'kyuurebangou':kyuurebangou,
                                    'kyuutorakku':kyuutorakku,
                                    'kyuukyori':kyuukyori,
                                    'bunkatsu':bunkatsu,
                                    'sintorakku':sintorakku,
                                    'sinkyori':sinkyori,
                                    'chuushinai':chuushinai,
                                    'keishusuu' : int(data.Goukei_syussou_number),
                                    'ji' : int(data.First_race_hasso_time[0:2]),
                                    'fun' : int(data.First_race_hasso_time[2:4]),
                                }
                            )
                            if not m00_obj_list:
                                m00_obj_list.append(m00_obj)

            if m00_obj_list:
                # 後続処理に渡すm00は、レース番号まで分かればいいので一つだけでいい
                edit_mddb_list.append(m00_obj_list[0])
            return edit_mddb_list

        except Exception as e:
            Common_log.Out_Logs(log_err_msg_id, [failure(e)])
            logger.error(failure(e))
            return False