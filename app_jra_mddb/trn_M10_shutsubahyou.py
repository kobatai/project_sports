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

class Trn_M10_shutsubahyou():

    def chk_blank(self, record):
        # 受信データにはブランクのパターンがいくつかあるが、空データの場合は全てブランクとみなす処理
        blank_list = ['','　',None,[],{}]
        if record in blank_list or re.fullmatch('\s+', record):
            return False
        else:
            return True

    def make_rekobamei_FSIN(self, data):
        rekobamei = data.Course_recode1_name.rstrip()
        if self.chk_blank(data.Course_recode2_name):
            rekobamei = rekobamei + ',' + data.Course_recode2_name.rstrip()
        if self.chk_blank(data.Course_recode3_name):
            rekobamei = rekobamei + ',' + data.Course_recode3_name.rstrip()
        return rekobamei

    def make_rekobamei_I204(self, data):
        rekobamei = data.Cource_name1_horse_name.rstrip()
        if self.chk_blank(data.Cource_name2_horse_name):
            rekobamei = rekobamei + ',' + data.Cource_name2_horse_name.rstrip()
        if self.chk_blank(data.Cource_name3_horse_name):
            rekobamei = rekobamei + ',' + data.Cource_name3_horse_name.rstrip()
        return rekobamei

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


    def make_name(self, rowname):
        # 余計な空白を削除して、「苗字　名前」形式にして返す
        return re.sub('[ 　]+', ' ', rowname.replace('　', ' ').rstrip()).replace(' ', '　')
        
    def get_jouken_FSIN(self, data):
        Cmn = Common()
        kyousou_jouken = None
        for code in [data.Syutsuba_info_kyoso_joken_code1, data.Syutsuba_info_3sai, data.Syutsuba_info_4sai, data.Syutsuba_info_5sai, data.Syutsuba_info_6sai]:
            if int(code):
                kyousou_jouken = code
                break
        return Cmn.chk_master_Mst_Kyousou_jouken(kyousou_jouken)

    def get_jouken_I204(self, data):
        Cmn = Common()
        kyousou_jouken = None
        for code in [data.Syutsuba_info_kyoso_joken1_code, data.Kyoso_jouken2_code_2sai, data.Kyoso_jouken2_code_3sai, data.Kyoso_jouken2_code_4sai, data.Kyoso_jouken2_code_5sai, data.Kyoso_jouken2_code_6sai]:
            if int(code):
                kyousou_jouken = code
                break
        return Cmn.chk_master_Mst_Kyousou_jouken(kyousou_jouken)

    def insert_or_update_M10_shutsubahyou(self, datfilename, datDataFileFlg):
        try:
            from app_jra.models import (
                Trn_FSIN_1_Syussouba_meihyo, # M10（前売、想定出馬表）
                Trn_FSIN_2_Syussouba_RaceHorse,  # M11（前売、想定出馬表）
                Trn_I204_1_Yokujitsu_syutsuba_hyou, # M10（翌日出馬表）
                Trn_I204_2_Yokujitsu_syutsuba_horse_info,  # M11（翌日出馬表）
                Trn_FSKS_Syutsuba_hyou_kaigai,  # M10/M11（海外）
                Trn_K191_Jusyou_hatsubai_youkou,
                Trn_N192_Jusyou_hatsubai_youkou_henko,
                Trn_I221_Honjitsu_okuridashi_yotei,
                M10_shutsubahyou,
                M11_shutsubahyou_shujouhou,
                Mst_Horse,
                Mst_Schedule
            )
            Cmn = Common()

            # 受信ファイルごとに、M10～M13レコードを新規作成・更新していく。
            # 正しく登録完了した場合、後続の待ち合わせ・配信処理で使うために、更新した中間DBのレコードを保持しておき、リストにして返す。
            # 登録失敗時には、False(ABNOMAL or 空のリスト)を返す。
            edit_mddb_list = [] # 初期値

            if SYUSSOUBA_MEIHYO == datDataFileFlg:
                # FSIN 出走馬名表データレコードの場合
                # M10/M11を登録
                FSIN1_list = Trn_FSIN_1_Syussouba_meihyo.objects.filter(Receive_filename=datfilename)
                # 番組情報を取得
                for data in FSIN1_list:
                    jou_obj = Cmn.chk_master_Mst_Jou(data.Bangumi_Jou_code)
                    kaisaibi = dt.strptime(data.Syutsuba_info_kaisai_date, '%Y%m%d').date()
                    rebangou = int(data.Bangumi_race_number)
                    kyousou_jouken = None
                    for code in [data.Syutsuba_info_kyoso_joken_code1, data.Syutsuba_info_3sai, data.Syutsuba_info_4sai, data.Syutsuba_info_5sai, data.Syutsuba_info_6sai]:
                        if int(code):
                            kyousou_jouken = code
                            break

                    # 要確認 既に同じレースのレコードが存在する場合は、更新ではなく一旦全削除→新規作成とする。
                    # （出走表の修正が選手数が減る形でされた場合に修正前の選手が残る可能性があるので）
                    M10_shutsubahyou.objects.filter(
                        joumei = jou_obj,
                        kaisaibi = kaisaibi, 
                        rebangou = rebangou
                    ).delete()

                    m10_obj = M10_shutsubahyou.objects.create(
                        joumei=jou_obj,
                        kaisuu = int(data.Bangumi_kai), 
                        kainichime = int(data.Bangumi_day),
                        shutsubakubun = '想定',  # 本日送出予定：出走馬名表データレコードの場合は「想定」
                        kaisaibi = kaisaibi,
                        rebangou= rebangou,
                        shubetsu=Cmn.chk_master_Mst_Kyousou_shubetsu(data.Syutsuba_info_kyoso_syubetsu_code),
                        
                        # juushouhassoujun  重勝発売要項
                        
                        tokusouhonsuu = int(data.Tokubetsu_kyoso_kaisuu) if data.Tokubetsu_kyoso_hukuhon_kubun == '1' and int(data.Tokubetsu_kyoso_kaisuu) else None,
                        tokusoumeihon = data.Tokubetsu_kyoso_hondai.rstrip(),
                        tokusoufukusuu = int(data.Tokubetsu_kyoso_kaisuu) if data.Tokubetsu_kyoso_hukuhon_kubun == '2' and int(data.Tokubetsu_kyoso_kaisuu) else None,
                        tokusoumeifuku = data.Tokubetsu_kyoso_hukudai.rstrip(),
                        
                        guredo = Cmn.chk_master_Mst_Grade(data.Tokubetsu_kyoso_grade),
                        kyori = int(data.Syutsuba_info_kyori),
                        torakku= Cmn.chk_master_Mst_Track(data.Syutsuba_info_track_code),
                        honshoukin_1 = int(data.Syutsuba_info_honsyoukin1)*1000,
                        honshoukin_2 = int(data.Syutsuba_info_honsyoukin2)*1000,
                        honshoukin_3 = int(data.Syutsuba_info_honsyoukin3)*1000,
                        honshoukin_4 = int(data.Syutsuba_info_honsyoukin4)*1000,
                        honshoukin_5 = int(data.Syutsuba_info_honsyoukin5)*1000,
                        # fukashoukin  付加賞金額 出走馬名表では無し
                        rekokubun = Cmn.chk_master_Mst_Record(data.Recode_kubun),
                        refun = int(data.Course_recode_time[0:1]),
                        rebyo = int(data.Course_recode_time[1:3]),
                        remiri = int(data.Course_recode_time[3:4]),
                        rekobamei = self.make_rekobamei_FSIN(data),
                        shusuu = int(data.Syutsuba_info_syusso_tousuu),
                        hji = int(data.Syutsuba_info_hasso_time[0:2]),
                        hfun = int(data.Syutsuba_info_hasso_time[2:4]),
                        # zhji # 前日発売締切時刻（時） 出走馬名表では無し
                        # zhfun # 前日発売締切時刻（分） 出走馬名表では無し

                        jouken = self.get_jouken_FSIN(data),
                        kigou = Cmn.chk_master_Mst_Kyousou_kigou(data.Syutsuba_info_kyoso_kigou_code),
                        jyuuryoushubetsu = Cmn.chk_master_Mst_Juryo(data.Syutsuba_info_juryo_syubetsu_code),
                        # siokubun # 指定オープン区分 出走馬名表では無し

                    )

                    # FSIN_2 出走馬名表（競走馬情報）
                    FSIN2_list = Trn_FSIN_2_Syussouba_RaceHorse.objects.filter(FSIN_1=data)
                    for shussouba_data in FSIN2_list:

                        # M11 出馬表_出走情報を登録
                        if self.chk_blank(shussouba_data.Horse_name):
                            m11_obj = M11_shutsubahyou_shujouhou.objects.create(
                                m10 = m10_obj,
                                waku = int(shussouba_data.Horse_wakuban) if not shussouba_data.Horse_wakuban == '9' else None,
                                uma = int(shussouba_data.Horse_ban) if not shussouba_data.Horse_ban == '99' else None,
                                umakigou = Cmn.chk_master_Mst_Uma_kigou(shussouba_data.Horse_kigou_code),
                                tokushubagu = 'ブリンカー' if shussouba_data.Tokusyu_sougu == '1' else None,
                                fujuu = self.make_fujuu(shussouba_data.Kishu_hutan_juryo),
                                umajouhou = Cmn.chk_master_Mst_Horse(shussouba_data.Horse_ketto_touroku_number),
                                kijouhou = Cmn.chk_master_Mst_Kishu(shussouba_data.Kishu_code),
                                choujouhou = Cmn.chk_master_Mst_Choukyoushi(shussouba_data.Choukyoushi_code), 
                            )

                    edit_mddb_list.append(m10_obj)
                # FSIN 出走馬名表データレコード 中間DB登録完了
                return edit_mddb_list

            elif YOKUJITSU_SYUTSUBA_HYOU == datDataFileFlg or SHUUSEI_SYUTSUBA_HYOU == datDataFileFlg or TOUJITSU_SYUTSUBA_HYOU == datDataFileFlg:
                # I204_1 翌日出馬表／Z204_1 修正出馬表／I904_1 当日出馬表データレコードの場合、M10/M11を登録
                I2041_list = Trn_I204_1_Yokujitsu_syutsuba_hyou.objects.filter(Receive_filename=datfilename)
                # 番組情報を取得
                for data in I2041_list:
                    jou_obj = Cmn.chk_master_Mst_Jou(data.Bangumi_Jou_code)
                    kaisai_str = data.Bangumi_year + data.Syutsuba_info_tsukihi
                    kaisaibi = dt.strptime(kaisai_str, '%y%m%d').date()
                    rebangou = int(data.Bangumi_race_number)

                    # 既に同じレースのレコードが存在する場合は、更新ではなく一旦全削除→新規作成とする。
                    # （出走表の修正が選手数が減る形でされた場合に修正前の選手が残る可能性があるので）
                    M10_shutsubahyou.objects.filter(
                        joumei = jou_obj,
                        kaisaibi = kaisaibi, 
                        rebangou = rebangou
                    ).delete()

                    m10_obj = M10_shutsubahyou.objects.create(
                        joumei=jou_obj,
                        kaisuu = int(data.Bangumi_kai), 
                        kainichime=int(data.Bangumi_day),
                        # 本日送出予定：翌日出馬表データレコードの場合は「翌日」当日出馬表データレコードの場合は「前売」
                        shutsubakubun = '前売' if TOUJITSU_SYUTSUBA_HYOU == datDataFileFlg else '翌日',
                        kaisaibi = kaisaibi,
                        rebangou= rebangou,
                        shubetsu=Cmn.chk_master_Mst_Kyousou_shubetsu(data.Syutsuba_info_kyoso_syubetsu_code),
                        
                        # juushouhassoujun  重勝発売要項から取得
                        
                        tokusouhonsuu = int(data.Tokubetsu_kyoso_kaisuu) if data.Tokubetsu_kyoso_hukuhon_kubun == '1' and int(data.Tokubetsu_kyoso_kaisuu) else None,
                        tokusoumeihon = data.Tokubetsu_kyoso_hondai.rstrip(),
                        tokusoufukusuu = int(data.Tokubetsu_kyoso_kaisuu) if data.Tokubetsu_kyoso_hukuhon_kubun == '2' and int(data.Tokubetsu_kyoso_kaisuu) else None,
                        tokusoumeifuku = data.Tokubetsu_kyoso_hukudai.rstrip(),

                        guredo = Cmn.chk_master_Mst_Grade(data.Syutsuba_info_grade),
                        kyori = int(data.Syutsuba_info_kyori),
                        torakku= Cmn.chk_master_Mst_Track(data.Syutsuba_info_track_code),
                        honshoukin_1 = int(data.Honsyoukin1)*1000,
                        honshoukin_2 = int(data.Honsyoukin2)*1000,
                        honshoukin_3 = int(data.Honsyoukin3)*1000,
                        honshoukin_4 = int(data.Honsyoukin4)*1000,
                        honshoukin_5 = int(data.Honsyoukin5)*1000,
                        fukashoukin_1 = int(data.Hukasyoukin1)*100,
                        fukashoukin_2 = int(data.Hukasyoukin2)*100,
                        fukashoukin_3 = int(data.Hukasyoukin3)*100,
                        rekokubun = Cmn.chk_master_Mst_Record(data.Syutsuba_info_recode_kubun),
                        refun = data.Cource_recode_time[0:1],
                        rebyo = data.Cource_recode_time[1:3],
                        remiri = data.Cource_recode_time[3:4],
                        rekobamei = self.make_rekobamei_I204(data),
                        shusuu = int(data.Syutsuba_info_syusso_tousuu),
                        hji = int(data.Syutsuba_info_hasso_time[0:2]),
                        hfun = int(data.Syutsuba_info_hasso_time[2:4]),
                        zhji = int(data.Syutsuba_info_jogai_simekiri_zen[0:2]),
                        zhfun = int(data.Syutsuba_info_jogai_simekiri_zen[2:4]),

                        jouken = self.get_jouken_I204(data),
                        kigou = Cmn.chk_master_Mst_Kyousou_kigou(data.Syutsuba_info_kyoso_kigou_code),
                        jyuuryoushubetsu = Cmn.chk_master_Mst_Juryo(data.Syutsuba_info_juryo_syubetsu_code),
                        siokubun = data.Syutsuba_info_sitei_open_kubun,
                    )
                    # logger.info(f'm10_obj : {m10_obj}')

                    # I204_2 翌日出走馬表 出走馬情報／Z204_2 修正出馬表／I904_2 当日出馬表 出走馬情報（競走馬情報）
                    I2042_list = Trn_I204_2_Yokujitsu_syutsuba_horse_info.objects.filter(I204_1=data)
                    for shussouba_data in I2042_list:
                        # 馬マスタ、騎手マスタ、調教師マスタの更新用にdictを用意しておく

                        # M11 出馬表_出走情報を登録
                        if self.chk_blank(shussouba_data.Name):
                            m11_obj = M11_shutsubahyou_shujouhou.objects.create(
                                m10 = m10_obj,
                                waku = int(shussouba_data.Wakuban),
                                uma = int(shussouba_data.Umaban),
                                umakigou = Cmn.chk_master_Mst_Uma_kigou(shussouba_data.Horse_kigou_code),
                                tokushubagu = 'ブリンカー' if shussouba_data.Tokusyu_sougu == '1' else None,
                                fujuu = self.make_fujuu(shussouba_data.Kishu_hutan_juryo),
                                umajouhou = Cmn.chk_master_Mst_Horse(shussouba_data.Ketto_number),
                                kijouhou = Cmn.chk_master_Mst_Kishu(shussouba_data.Kishu_code),
                                choujouhou = Cmn.chk_master_Mst_Choukyoushi(shussouba_data.Choukyoushi_code),
                            )
                            # logger.info(f'm11_obj : {m11_obj}')

                    edit_mddb_list.append(m10_obj)
                # I204_2 翌日出走馬表 出走馬情報／Z204_2 修正出馬表／I904_2 当日出馬表 出走馬情報（競走馬情報） 中間DB登録完了
                return edit_mddb_list

            elif JUSYOU_HATSUBAI_YOUKOU == datDataFileFlg or JUSYOU_HATSUBAI_YOUKOU_HENKO == datDataFileFlg:
                if JUSYOU_HATSUBAI_YOUKOU == datDataFileFlg:
                    # K191 重勝発売要項
                    K191_or_K192_list = Trn_K191_Jusyou_hatsubai_youkou.objects.filter(Receive_filename=datfilename)
                if JUSYOU_HATSUBAI_YOUKOU_HENKO == datDataFileFlg:
                    # N192 重勝発売要項変更
                    K191_or_K192_list = Trn_N192_Jusyou_hatsubai_youkou_henko.objects.filter(Receive_filename=datfilename)
                for data in K191_or_K192_list:
                    for i in range(5):
                        renum = i + 1
                        if getattr(data, 'Toujitsu' + str(renum) + '_jou_code',False):
                            jou_obj = Cmn.chk_master_Mst_Jou(getattr(data, 'Toujitsu' + str(renum) + '_jou_code',None))
                            kaisuu = int(getattr(data, 'Toujitsu' + str(renum) + '_kai',None))
                            kainichime = int(getattr(data, 'Toujitsu' + str(renum) + '_day',None))
                            rebangou = int(getattr(data, 'Toujitsu' + str(renum) + '_race_no',None))

                            m10_list = M10_shutsubahyou.objects.filter(
                                joumei = jou_obj,
                                kaisuu = kaisuu, 
                                kainichime = kainichime,
                                rebangou = rebangou,
                            )
                            if not m10_list.exists():
                                logger.info(f'{kaisuu}回{kainichime}日目 {jou_obj.Jou_name} {rebangou}R の中間DB出馬表のデータが見つかりません')
                            else:
                                m10_obj = m10_list.last()
                                m10_obj.juushouhassoujun = renum
                                m10_obj.save()
                                edit_mddb_list.append(m10_obj)

                        if getattr(data, 'Zenjitsu' + str(renum) + '_jou_code',False):
                            jou_obj = Cmn.chk_master_Mst_Jou(getattr(data, 'Zenjitsu' + str(renum) + '_jou_code',None))
                            kaisuu = int(getattr(data, 'Zenjitsu' + str(renum) + '_kai',None))
                            kainichime = int(getattr(data, 'Zenjitsu' + str(renum) + '_day',None))
                            rebangou = int(getattr(data, 'Zenjitsu' + str(renum) + '_race_no',None))

                            m10_list = M10_shutsubahyou.objects.filter(
                                joumei = jou_obj,
                                kaisuu = kaisuu, 
                                kainichime = kainichime,
                                rebangou = rebangou,
                            )
                            if not m10_list.exists():
                                logger.info(f'{kaisuu}回{kainichime}日目 {jou_obj.Jou_name} {rebangou}R の中間DB出馬表のデータが見つかりません')
                            else:
                                m10_obj = m10_list.last()
                                m10_obj.juushouhassoujun = renum
                                m10_obj.save()
                                edit_mddb_list.append(m10_obj)

                return edit_mddb_list

            # elif SYUTSUBA_HYOU_KAIGAI == datDataFileFlg:
            #     # FSKS 出馬表詳細（国際）データレコード
            #     # TODO 2022/4/20 共同通信データがまだないので未検証
            #     FSKS_list = Trn_FSKS_Syutsuba_hyou_kaigai.objects.filter(Receive_filename=datfilename)
            #     # 番組情報を取得
            #     for data in FSKS_list:
            #         jou_obj = Cmn.chk_master_Mst_Jou(data.Bangumi_Jou_code)
            #         kaisaibi = dt.strptime(data.Bangumi_year + data.Tsukihi, '%y%m%d').date()
            #         rebangou = int(data.Bangumi_race_number)

            #         m10_obj, created = M10_shutsubahyou.objects.update_or_create(
            #             joumei=jou_obj,
            #             kaisuu = int(data.Bangumi_kai), 
            #             kainichime = int(data.Bangumi_day),
            #             kaisaibi = kaisaibi,
            #             rebangou= rebangou,
            #             defaults={
            #                 'shubetsu':Cmn.chk_master_Mst_Kyousou_shubetsu(data.Race_type),
            #                 'tokusoumeihon': data.Race_name.rstrip(),
            #                 'guredo' : Cmn.chk_master_Mst_Grade(data.Grade_code),
            #                 'kyori' : int(data.Kyori),
            #                 'torakku': Cmn.chk_master_Mst_Track(data.Track_ryaku_code),
            #                 'honshoukin_1' : int(data.Syoukin1)*1000,
            #                 'honshoukin_2' : int(data.Syoukin2)*1000,
            #                 'honshoukin_3' : int(data.Syoukin3)*1000,
            #                 'honshoukin_4' : int(data.Syoukin4)*1000,
            #                 'honshoukin_5' : int(data.Syoukin5)*1000,
            #                 'refun' : int(data.CR_time1[0:1]),
            #                 'rebyo' : int(data.CR_time1[1:3]),
            #                 'remiri' :int(data.CR_time1[3:4]),
            #                 'rekobamei' : data.CR_horse_name1.rstrip(),
            #                 'shusuu' : int(data.Syussou_tousu),
            #                 'hji' : int(data.hassou_time_JST[9:11]),
            #                 'hfun' : int(data.hassou_time_JST[11:13]),
            #             }
            #         )

            #         # M11 出馬表_出走情報を登録
            #         m11_obj = M11_shutsubahyou_shujouhou.objects.create(
            #             m10 = m10_obj,
            #             waku = int(data.Gate_number),
            #             uma = int(data.Horse_number),
            #             tokushubagu = 'ブリンカー' if data.Tokusyu_sougu == '1' else None,
            #             fujuu = self.make_fujuu(data.Kishu_hutan_juryo),
            #             umajouhou = Cmn.chk_master_Mst_Horse(data.Ketto_touroku_number),
            #             kijouhou = Cmn.chk_master_Mst_Kishu(data.Kishu_code),
            #             choujouhou = Cmn.chk_master_Mst_Choukyoushi(data.Choukyoushi_code)
            #         )

            #         edit_mddb_list.append(m10_obj)

            #     # FSKS 出馬表詳細（国際） 中間DB登録完了
            #     return edit_mddb_list
            return edit_mddb_list

        except Exception as e:
            Common_log.Out_Logs(log_err_msg_id, [failure(e)])
            logger.error(failure(e))
