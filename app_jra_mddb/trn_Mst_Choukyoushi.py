from logging import getLogger
import re
from app_jra.consts import *
import sys
import linecache
from app_jra.log_commons import *
from datetime import datetime as dt


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

class Trn_Mst_Choukyoushi():

    def chk_blank(self, record):
        # 受信データにはブランクのパターンがいくつかあるが、空データの場合は全てブランクとみなす処理
        blank_list = ['','　',None,[],{}]
        if record in blank_list or re.fullmatch('\s+', record):
            return False
        else:
            return True

    def make_name(self, rowname):
        # 余計な空白を削除して、「苗字　名前」形式にして返す
        return re.sub('[ 　]+', ' ', rowname.replace('　',' ').rstrip()).replace(' ','　')

    def make_3jichoumei(self, chouname):
        from app_jra.models import (
            Mst_3jichou,
            Mst_Choukyoushi,
        )
        # 調教師名から３字調教師名を作る

        chouname3ji = ''

        # 3文字調教師名にしたときに、名前も入れる調教師名の名字のリスト(日本人名)
        meiari_sei_jp_list = list(Mst_3jichou.objects.filter(Gaikoku_name_flg=False).values_list('Choukyoushi_name_sei', flat=True)) 

        # 3文字調教師名にしたときに、名前も入れる調教師名の名字のリスト(外国人名)
        meiari_sei_gai_list = list(Mst_3jichou.objects.filter(Gaikoku_name_flg=True).values_list('Choukyoushi_name_sei', flat=True)) 

        if '　' in chouname:
            # 日本人名の場合
            sei = self.make_name(chouname).split('　')[0]
            mei = self.make_name(chouname).split('　')[1]

            # Mst_3jichouにない名字の場合で、既に登録されている調教師の名字に同じものがあれば、Mst_3jichouに自動追加する
            # → 自動でMst_3jichouに追加にすると、苗字がダブってるにもかかわらず３字調教師名にする必要がない調教師も更新のたびに３字調教師名になってしまう
            # そのため、以下処理はコメント化
            # ★★★★★ここから★★★★★
            # if not sei in meiari_sei_jp_list:
            #     if Mst_Choukyoushi.objects.filter(Choukyoushi_name_sei=sei).exclude(Choukyoushi_name_mei=mei).exists():
            #         Mst_3jichou.objects.create(Choukyoushi_name_sei=sei,Gaikoku_name_flg=False)
            #         meiari_sei_jp_list.append(sei)
            #         # 新たに同じ名字の調教師が現れたので、元から居る調教師の３字略も修正する。
            #         dousei_chou_l = Mst_Choukyoushi.objects.filter(Choukyoushi_name_sei=sei).exclude(Choukyoushi_name_mei=mei)
            #         for chou_obj in dousei_chou_l:
            #             if len(sei) >= 2:
            #                 # 横山武
            #                 updatename = sei[0:2] + chou_obj.Choukyoushi_name_mei[0]
            #             elif len(sei) == 1:
            #                 # 武　豊
            #                 updatename = sei + '　' + chou_obj.Choukyoushi_name_mei[0]
            #             chou_obj.Choukyoushi_name = updatename
            #             chou_obj.save()
            #             logger.info(f'【調教師マスタ】3字調教師名 更新 {chou_obj.Choukyoushi_code} {chou_obj.Choukyoushi_name}')
            # ★★★★★ここまで★★★★★

            if sei in meiari_sei_jp_list:
                if len(sei) >= 2:
                    # 横山武
                    chouname3ji = sei[0:2] + mei[0]
                elif len(sei) == 1:
                    # 武　豊
                    chouname3ji = sei + '　' + mei[0]
            else:
                if len(sei) == 1:
                    # 　幸　
                    chouname3ji = '　' + sei + '　'
                elif len(sei) == 2:
                    # 福　永
                    chouname3ji = sei[0] + '　' + sei[1]
                elif len(sei) >= 3:
                    # 武士沢
                    chouname3ji = sei[0:3]
            
        else:
            # 外国人名の場合
            if '．' in chouname:
                firstname = chouname.split('．')[0] # Ｃ
                lastname = chouname.split('．')[1] # ルメール

                if lastname in meiari_sei_gai_list: # 外国人同姓対応
                    if len(lastname) == 1:
                        # Ｍ　デ　
                        chouname3ji = firstname[0] + '　' + lastname
                    elif len(lastname) == 2:
                        # Ｍデム
                        chouname3ji = firstname[0] + lastname
                    elif len(lastname) >= 3:
                        # Ｍデム
                        chouname3ji = firstname[0] + lastname[0:2]
                else:
                    if len(lastname) == 1:
                        # 　ル　
                        chouname3ji = '　' + lastname + '　'
                    elif len(lastname) == 2:
                        # ル　メ
                        chouname3ji = lastname[0] + '　' + lastname[1]
                    elif len(lastname) >= 3:
                        # ルメー
                        chouname3ji = lastname[0:3]

                    # 上記と同じ理由で、以下のMst_3jichouへの自動追加処理はコメント化
                    # ★★★★★ここから★★★★★
                    # Mst_3jichouに名前がないにもかかわらず、既に調教師マスタに登録されている外国人名調教師の中に、別人だけど3字略が同じ調教師がいた場合、この調教師のlastnameをMst_3jichouに自動追加し、デムーロ対応する
                    # if Mst_Choukyoushi.objects.filter(Choukyoushi_name=chouname3ji).exclude(Choukyoushi_name_sei=chouname).exists():
                    #     Mst_3jichou.objects.create(Choukyoushi_name_sei=lastname,Gaikoku_name_flg=True)
                    #     if len(lastname) == 1:
                    #         # Ｍ　デ
                    #         chouname3ji = firstname[0] + '　' + lastname
                    #     elif len(lastname) == 2:
                    #         # Ｍデム
                    #         chouname3ji = firstname[0] + lastname
                    #     elif len(lastname) >= 3:
                    #         # Ｍデム
                    #         chouname3ji = firstname[0] + lastname[0:2]

                    #     # 新たに同じ名字の調教師が現れたので、元から居る調教師の３字略も修正する。
                    #     dousei_chou_l = Mst_Choukyoushi.objects.filter(Choukyoushi_name=chouname3ji).exclude(Choukyoushi_name_sei=chouname)
                    #     for chou_obj in dousei_chou_l:
                    #         update_chouname = chou_obj.Choukyoushi_name_sei
                    #         u_firstname = update_chouname.split('．')[0] # Ｍ
                    #         u_lastname = update_chouname.split('．')[1] # デムーロ
                    #         if len(u_lastname) == 1:
                    #             # Ｍ　デ　
                    #             updatename = u_firstname[0] + '　' + u_lastname
                    #         elif len(u_lastname) == 2:
                    #             # Ｍデム
                    #             updatename = u_firstname[0] + u_lastname
                    #         elif len(u_lastname) >= 3:
                    #             # Ｍデム
                    #             updatename = u_firstname[0] + u_lastname[0:2]
                    #         chou_obj.Choukyoushi_name = updatename
                    #         chou_obj.save()
                    #         logger.info(f'【調教師マスタ】3字調教師名 更新 {chou_obj.Choukyoushi_code} {chou_obj.Choukyoushi_name}')
                    # ★★★★★ここまで★★★★★
            else:
                # '．'がない外国人名の場合で、既にいる調教師と3文字略がかぶった場合はどうしようもない（前例がなく3文字略する法則がないので、機械的に直せない。手で直すしかない）
                chouname3ji = chouname[0:3]

        return chouname3ji

    def make_sei(self, chouname):
        if '　' in chouname:
            sei = self.make_name(chouname).split('　')[0]
        else:
            sei = chouname # 外国時の場合はそのまま姓にする
        return sei

    def make_mei(self, chouname):
        if '　' in chouname:
            mei = self.make_name(chouname).split('　')[1]
        else:
            mei = None # 外国時の場合は名は無し
        return mei

    def insert_or_update_Mst_Choukyoushi(self, datfilename, datDataFileFlg):
        try:
            from app_jra.models import (
                Mst_JTRA_Choukyoushi,
                Mst_Choukyoushi,
            )
            Cmn = Common()
            update_num = 0

            if M_CHOUKYOUSHI == datDataFileFlg:
                # JTRA 調教師マスタ
                JTRA_list = Mst_JTRA_Choukyoushi.objects.filter(Receive_filename=datfilename)
                for data in JTRA_list:
                    chou_objs = Mst_Choukyoushi.objects.filter(Choukyoushi_code=data.Choukyoushi_code)
                    chouname = self.make_name(data.Choukyoushi_name) # 「姓　名」
                    if chou_objs.exists():
                        # 更新
                         chou_obj = chou_objs.last() # Choukyoushi_codeはunique
                         chou_obj.Choukyoushi_name = self.make_3jichoumei(chouname)
                         chou_obj.Choukyoushi_name_sei = self.make_sei(chouname)
                         chou_obj.Choukyoushi_name_mei = self.make_mei(chouname)
                         chou_obj.Kokuseki = Cmn.chk_master_Mst_Jou(data.Choukyoushi_shotai_kunimei_code)
                         chou_obj.Touzai = TOUZAI_KUBUN[data.Choukyoushi_touzai_kubun_code]
                         chou_obj.save()
                    else:
                        chou_obj = Mst_Choukyoushi.objects.create(
                            Choukyoushi_code=data.Choukyoushi_code,
                            Choukyoushi_name = self.make_3jichoumei(chouname),
                            Choukyoushi_name_sei = self.make_sei(chouname),
                            Choukyoushi_name_mei = self.make_mei(chouname),
                            Kokuseki=Cmn.chk_master_Mst_Jou(data.Choukyoushi_shotai_kunimei_code),
                            Touzai = TOUZAI_KUBUN[data.Choukyoushi_touzai_kubun_code]
                        )
                    update_num += 1
                logger.info(f'【調教師マスタ】{str(update_num)}件 登録・更新')

            return NORMAL

        except Exception as e:
            Common_log.Out_Logs(log_err_msg_id, [e])
            logger.error(failure(e))
            return ABNORMAL
