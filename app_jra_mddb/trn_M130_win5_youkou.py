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
    return str(lineno) + ":" + str(type(e)) + str(e)


class Trn_M130_win5_youkou():


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
        if record in blank_list or re.fullmatch('\s+', record):
            return False
        else:
            return True


    def get_ji(self, time):
        ji = time[0:2]
        return ji

    def get_fun(self, time):
        fun = time[2:4]
        return fun


    def make_m130_obj(self, m130_obj, data, num):
        from app_jra.models import M130_win5_youkou
        if int(data.Tokubetsu_kyoso_hukuhon_kubun) == 1:
            if self.chk_blank_zero(data.Tokubetsu_kyoso_kaisuu):
                setattr(m130_obj, "tokusouhonsuu_"+str(num), int(data.Tokubetsu_kyoso_kaisuu))
        else:
            if self.chk_blank_zero(data.Tokubetsu_kyoso_kaisuu):
                setattr(m130_obj, "tokusoufukusuu_"+str(num), int(data.Tokubetsu_kyoso_kaisuu))
        
        if self.chk_blank(data.Tokubetsu_kyoso_hondai):
            setattr(m130_obj, "tokusoumeihon_"+str(num), data.Tokubetsu_kyoso_hondai.rstrip())
        if self.chk_blank(data.Tokubetsu_kyoso_hukudai):
            setattr(m130_obj, "tokusoumeifuku_"+str(num), data.Tokubetsu_kyoso_hukudai.rstrip())

        m130_obj.save()
        return m130_obj


    def insert_or_update_M130_win5_youkou(self, datfilename, datDataFileFlg):
        try:
            from app_jra.models import (
                Trn_K191_Jusyou_hatsubai_youkou,
                Trn_FSIN_1_Syussouba_meihyo,
                Trn_I204_1_Yokujitsu_syutsuba_hyou,
                M10_shutsubahyou,
                M130_win5_youkou,
                M131_win5_youkou_race
            )
            Cmn = Common()

            # 受信ファイルごとに、M130レコードを新規作成・更新していく。
            # 正しく登録完了した場合、後続の待ち合わせ・配信処理で使うために、更新した中間DBのレコードを保持しておき、リストにして返す。
            # 登録失敗時には、False(ABNOMAL or 空のリスト)を返す。
            edit_mddb_list = [] # 初期値
            m130_obj = None

            if JUSYOU_HATSUBAI_YOUKOU == datDataFileFlg:
                # K191 重勝発売要項
                K191_list = Trn_K191_Jusyou_hatsubai_youkou.objects.filter(Receive_filename=datfilename)
                for data in K191_list:
                    # たまに空データが来る
                    if self.chk_blank(data.Toujitsu1_jou_code):

                        jou_obj = Cmn.chk_master_Mst_Jou(data.Toujitsu1_jou_code)
                        kaisuu = int(data.Toujitsu1_kai)
                        kainichime = int(data.Toujitsu1_day)

                        kaisai_date = self.get_kaisaibi(jou_obj, kaisuu, kainichime)
                        if not kaisai_date:
                            logger.error(f'{str(kaisuu)}回{str(kainichime)}日目 {jou_obj.Jou_name} の開催日を取得できません。 / 該当日のスケジュールマスタを登録のうえ、再度 K191 重勝発売要項({datfilename}) を手動投入して下さい。')
                            return ABNOMAL

                        zhji = None
                        zhfun = None
                        thji = None
                        thfun = None

                        # 前日発売ありの場合
                        if data.Zenjitsu_5jusyou_simekiri_time:
                            zhji = int(self.get_ji(data.Zenjitsu_5jusyou_simekiri_time))
                            zhfun = int(self.get_fun(data.Zenjitsu_5jusyou_simekiri_time))
                        # 前日発売無しの場合
                        else:
                            thji = int(self.get_ji(data.Toujitsu_5jusyou_simekiri_time))
                            thfun = int(self.get_fun(data.Toujitsu_5jusyou_simekiri_time))

                        # M130 ＷＩＮ５発売要項を新規作成/更新
                        m130_obj, created = M130_win5_youkou.objects.update_or_create(
                            kaisaibi=kaisai_date,
                            defaults={
                                "zhji" : zhji,
                                "zhfun" : zhfun,
                                "thji" : thji,
                                "thfun" : thfun,
                                }
                            )

                        # M131 WIN5発売要項_レース の登録
                        for i in range(5):
                            win5_num = i + 1

                            joucode = getattr(data, 'Toujitsu' + str(win5_num) + '_jou_code') if getattr(data, 'Toujitsu' + str(win5_num) + '_jou_code') else getattr(data, 'Zenjitsu' + str(win5_num) + '_jou_code')
                            kaisuu = getattr(data, 'Toujitsu' + str(win5_num) + '_kai') if getattr(data, 'Toujitsu' + str(win5_num) + '_kai') else getattr(data, 'Zenjitsu' + str(win5_num) + '_kai')
                            kainichime = getattr(data, 'Toujitsu' + str(win5_num) + '_day') if getattr(data, 'Toujitsu' + str(win5_num) + '_day') else getattr(data, 'Zenjitsu' + str(win5_num) + '_day')
                            rebangou = getattr(data, 'Toujitsu' + str(win5_num) + '_race_no') if getattr(data, 'Toujitsu' + str(win5_num) + '_race_no') else getattr(data, 'Zenjitsu' + str(win5_num) + '_race_no')

                            m131_obj, created = M131_win5_youkou_race.objects.update_or_create(
                                m130=m130_obj,
                                win5_num = win5_num,
                                defaults={
                                    "joumei" : Cmn.chk_master_Mst_Jou(joucode),
                                    "kaisuu" : int(kaisuu),
                                    "kainichime" : int(kainichime),
                                    "rebangou" : int(rebangou),
                                    }
                                )

                            # 特別競走情報を取得
                            # M10から取得
                            if M10_shutsubahyou.objects.filter(kaisaibi=kaisai_date, joumei__Jou_code=joucode, kaisuu=int(kaisuu), kainichime=int(kainichime), rebangou=int(rebangou)).exists():
                                m10_obj = M10_shutsubahyou.objects.filter(kaisaibi=kaisai_date, joumei__Jou_code=joucode, kaisuu=int(kaisuu), kainichime=int(kainichime), rebangou=int(rebangou)).last()
                                m131_obj.tokusouhonsuu = m10_obj.tokusouhonsuu
                                m131_obj.tokusoumeihon = m10_obj.tokusoumeihon
                                m131_obj.tokusoufukusuu = m10_obj.tokusoufukusuu
                                m131_obj.tokusoumeifuku = m10_obj.tokusoumeifuku
                                
                                m131_obj.save()

            # 出馬表系のデータの場合、M10から特別競走情報を取得する
            # FSIN 出走馬名表の場合
            elif SYUSSOUBA_MEIHYO == datDataFileFlg:
                FSIN1_list = Trn_FSIN_1_Syussouba_meihyo.objects.filter(Receive_filename=datfilename)
                # 番組情報を取得
                for data in FSIN1_list:
                    jou_obj = Cmn.chk_master_Mst_Jou(data.Bangumi_Jou_code)
                    kaisai_date = dt.strptime(data.Syutsuba_info_kaisai_date, '%Y%m%d').date()
                    rebangou = int(data.Bangumi_race_number)

                    m131_objs = M131_win5_youkou_race.objects.filter(joumei=jou_obj, m130__kaisaibi=kaisai_date, rebangou=rebangou)
                    if m131_objs.exists():
                        m131_obj = m131_objs.last()

                        # 特別競走情報を取得
                        # M10から取得
                        if M10_shutsubahyou.objects.filter(kaisaibi=kaisai_date, joumei=jou_obj, rebangou=rebangou).exists():
                            m10_obj = M10_shutsubahyou.objects.filter(kaisaibi=kaisai_date, joumei=jou_obj, rebangou=rebangou).last()
                            m131_obj.tokusouhonsuu = m10_obj.tokusouhonsuu
                            m131_obj.tokusoumeihon = m10_obj.tokusoumeihon
                            m131_obj.tokusoufukusuu = m10_obj.tokusoufukusuu
                            m131_obj.tokusoumeifuku = m10_obj.tokusoumeifuku
                            
                            m131_obj.save()
                            m130_obj = m131_obj.m130 # 後続処理用

            # I204 翌日出馬表／Z204 修正出馬表／I904 当日出馬表データレコードの場合
            elif YOKUJITSU_SYUTSUBA_HYOU == datDataFileFlg or SHUUSEI_SYUTSUBA_HYOU == datDataFileFlg or TOUJITSU_SYUTSUBA_HYOU == datDataFileFlg:
                I2041_list = Trn_I204_1_Yokujitsu_syutsuba_hyou.objects.filter(Receive_filename=datfilename)
                # 番組情報を取得
                for data in I2041_list:
                    jou_obj = Cmn.chk_master_Mst_Jou(data.Bangumi_Jou_code)
                    kaisai_str = data.Bangumi_year + data.Syutsuba_info_tsukihi
                    kaisai_date = dt.strptime(kaisai_str, '%y%m%d').date()
                    rebangou = int(data.Bangumi_race_number)

                    m131_objs = M131_win5_youkou_race.objects.filter(joumei=jou_obj, m130__kaisaibi=kaisai_date, rebangou=rebangou)
                    if m131_objs.exists():
                        m131_obj = m131_objs.last()

                        # 特別競走情報を取得
                        # M10から取得
                        if M10_shutsubahyou.objects.filter(kaisaibi=kaisai_date, joumei=jou_obj, rebangou=rebangou).exists():
                            m10_obj = M10_shutsubahyou.objects.filter(kaisaibi=kaisai_date, joumei=jou_obj, rebangou=rebangou).last()
                            m131_obj.tokusouhonsuu = m10_obj.tokusouhonsuu
                            m131_obj.tokusoumeihon = m10_obj.tokusoumeihon
                            m131_obj.tokusoufukusuu = m10_obj.tokusoufukusuu
                            m131_obj.tokusoumeifuku = m10_obj.tokusoumeifuku
                            
                            m131_obj.save()
                            m130_obj = m131_obj.m130 # 後続処理用

            edit_mddb_list.append(m130_obj)
            return edit_mddb_list

        except Exception as e:
            Common_log.Out_Logs(log_err_msg_id, [failure(e)])
            logger.error(failure(e))
            return False