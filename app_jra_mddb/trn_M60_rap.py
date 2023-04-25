from logging import getLogger
import re
import mojimoji
from app_jra.consts import *
from app_jra.models import *
import sys
import linecache
from app_jra.log_commons import *
from datetime import datetime as dt
from datetime import timedelta


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
    return str(lineno) + ":" + str(type(e)) + " " + str(e)

class Trn_M60_rap():

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
        if record in blank_list or re.fullmatch('\s+', record):
            return False
        else:
            return True

    def make_a4htime(self, data):
        htime = data.Seiseki_data_ato_4_time[:2] + "." + data.Seiseki_data_ato_4_time[2:]
        kyousou_shubetsu = Mst_Kyousou_shubetsu.objects.filter(Kyousou_shubetsu_code=data.Seiseki_data_kyoso_syubetsu_code)
        if kyousou_shubetsu.exists():
            if kyousou_shubetsu.last().Shougai_kubun == '障害':
                htime = None
        return htime
    
    def make_a3htime(self, data):
        htime = data.Seiseki_data_ato_3_time[:2] + "." + data.Seiseki_data_ato_3_time[2:]
        kyousou_shubetsu = Mst_Kyousou_shubetsu.objects.filter(Kyousou_shubetsu_code=data.Seiseki_data_kyoso_syubetsu_code)
        if kyousou_shubetsu.exists():
            if kyousou_shubetsu.last().Shougai_kubun == '障害':
                htime = None
        return htime

    def make_ta(self, data):
        ta = ''
        for i in range(1, 26):
            row_ta = getattr(data, "Seiseki_data_heichi_rap_time" + str(i))
            if self.chk_blank_zero(row_ta):
                if int(row_ta):
                    edit_ta = str(int(row_ta[:2])) + "." + row_ta[2:]
                    ta = edit_ta if i == 1 else ta + ',' + edit_ta
        return ta
        
    def insert_or_update_M60_rap(self, datfilename, datDataFileFlg):
        try:
            from app_jra.models import (
                Trn_A200_1_JARIS_seiseki,
                Trn_A200_2_Seiseki_info,
                Trn_A322_Furlong_lap_corner_tsuukajun,
                M60_rap,
                
            )
            Cmn = Common()

            # 受信ファイルごとに、M60レコードを新規作成・更新していく。
            # 正しく登録完了した場合、後続の待ち合わせ・配信処理で使うために、更新した中間DBのレコードを保持しておき、リストにして返す。
            # 登録失敗時には、False(ABNOMAL or 空のリスト)を返す。
            edit_mddb_list = []  # 初期値
            m60_obj = None
            
            if JARIS_SEISEKI == datDataFileFlg:
                # A200 JARIS 成績
                rap_list = Trn_A200_1_JARIS_seiseki.objects.filter(Receive_filename=datfilename)

                # 番組情報を取得
                for data in rap_list:
                    jou_obj = Cmn.chk_master_Mst_Jou(data.Bangumi_Jou_code)
                    kaisaibi = dt.strptime("%s%s" % (data.Bangumi_year, data.Seiseki_data_date), '%y%m%d').date()
                    rebangou = int(data.Bangumi_race_number)

                    # 既に同じレースのレコードが存在する場合は、更新ではなく一旦全削除→新規作成とする。
                    M60_rap.objects.filter(
                        joumei = jou_obj,
                        kaisaibi = kaisaibi, 
                        rebangou = rebangou
                    ).delete()

                    m60_obj = M60_rap.objects.create(
                        joumei=jou_obj,
                        kaisuu = int(data.Bangumi_kai), 
                        kainichime = int(data.Bangumi_day),
                        kaisaibi = kaisaibi,
                        rebangou= rebangou,
                        shougaikubun = Cmn.chk_master_Mst_Kyousou_shubetsu(data.Seiseki_data_kyoso_syubetsu_code),
                        a4ha = self.make_a4htime(data),
                        a3ha = self.make_a3htime(data),
                        ta = self.make_ta(data),
                        kojunijouhou_1 = data.Seiseki_data_1_corner_jun.rstrip(),
                        kojunijouhou_2 = data.Seiseki_data_2_corner_jun.rstrip(),
                        kojunijouhou_3 = data.Seiseki_data_3_corner_jun.rstrip(),
                        kojunijouhou_4 = data.Seiseki_data_4_corner_jun.rstrip(),
                        # chuukanjouhou  TODO 1000m直線レースの中間情報の入力
                    )

                    # A200 JARIS 成績
                    A200_2_list = Trn_A200_2_Seiseki_info.objects.filter(A200_1=data, Kakutei_chakujun="01")
                    if A200_2_list.exists():
                        # 番組情報を取得
                        for seiseki_data in A200_2_list:
                            if not m60_obj.chaku1uma_1:
                                m60_obj.chaku1uma_1 = int(seiseki_data.Ban)
                                m60_obj.save()
                            elif not m60_obj.chaku1uma_2:
                                m60_obj.chaku1uma_2 = int(seiseki_data.Ban)
                                m60_obj.save()
                            elif not m60_obj.chaku1uma_3:
                                m60_obj.chaku1uma_3 = int(seiseki_data.Ban)
                                m60_obj.save()

                    edit_mddb_list.append(m60_obj)

            return edit_mddb_list

        except Exception as e:
            Common_log.Out_Logs(log_err_msg_id, [failure(e)])
            logger.error(failure(e))
            return False
