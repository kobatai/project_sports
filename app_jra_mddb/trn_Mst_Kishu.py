from logging import getLogger
import re
from app_jra.consts import *
import sys
import linecache
from app_jra.log_commons import *
from datetime import datetime as dt
import mojimoji

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

class Trn_Mst_Kishu():

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

    def make_3jikishumei(self, kishuname):
        from app_jra.models import (
            Mst_3jikishu,
            Mst_Kishu,
        )
        # 騎手名から３字騎手名を作る

        # <騎手名パターン>
        # 幸　英明　　　　幸　　姓１字
        # 福永祐一　　　福　永　姓２字
        # 武士沢友治　　武士沢　姓３字
        # 武豊　　　　　武　豊　姓　名
        # 横山武史　　　横山武　姓姓名
        # Ｃ．ルメール　ルメー　外姓
        # Ｍ．デムーロ　Ｍデム　外名姓

        kishuname3ji = ''

        # 3文字騎手名にしたときに、名前も入れる騎手名の名字のリスト(日本人名)
        # 2022/4現在 '横山','岩田','戸崎','内田','藤岡','吉田','田中','柴田','石橋','西村','木幡','江田','角田','荻野','秋山','藤田','国分','古川','鮫島','菅原','小林','小牧','北村','西谷','和田','武','森'
        meiari_sei_jp_list = list(Mst_3jikishu.objects.filter(Gaikoku_name_flg=False).values_list('Kishu_name_sei', flat=True)) 

        # 3文字騎手名にしたときに、名前も入れる騎手名の名字のリスト(外国人名)
        # 2022/4現在 'デムーロ'
        meiari_sei_gai_list = list(Mst_3jikishu.objects.filter(Gaikoku_name_flg=True).values_list('Kishu_name_sei', flat=True)) 

        if '　' in kishuname:
            # 日本人名の場合
            sei = self.make_name(kishuname).split('　')[0]
            mei = self.make_name(kishuname).split('　')[1]

            
            # Mst_3jikishuにない名字の場合で、既に登録されている騎手の名字に同じものがあれば、Mst_3jikishuに自動追加する
            # → 自動でMst_3jikishuに追加にすると、苗字がダブってるにもかかわらず３字騎手名にする必要がない騎手も更新のたびに３字騎手名になってしまう
            # そのため、以下処理はコメント化
            # ★★★★★ここから★★★★★
            # if not sei in meiari_sei_jp_list:
            #     if Mst_Kishu.objects.filter(Kishu_name_sei=sei).exclude(Kishu_name_mei=mei).exists():
            #         Mst_3jikishu.objects.create(Kishu_name_sei=sei,Gaikoku_name_flg=False)
            #         meiari_sei_jp_list.append(sei)
            #         # 新たに同じ名字の騎手が現れたので、元から居る騎手の３字略も修正する。
            #         dousei_kishu_l = Mst_Kishu.objects.filter(Kishu_name_sei=sei).exclude(Kishu_name_mei=mei)
            #         for kishu_obj in dousei_kishu_l:
            #             if len(sei) >= 2:
            #                 # 横山武
            #                 updatename = sei[0:2] + kishu_obj.Kishu_name_mei[0]
            #             elif len(sei) == 1:
            #                 # 武　豊
            #                 updatename = sei + '　' + kishu_obj.Kishu_name_mei[0]
            #             kishu_obj.Kishu_name = updatename
            #             kishu_obj.save()
            #             logger.info(f'【騎手マスタ】3字騎手名 更新 {kishu_obj.Kishu_code} {kishu_obj.Kishu_name}')
            # ★★★★★ここまで★★★★★

            if sei in meiari_sei_jp_list:
                if len(sei) >= 2:
                    # 横山武
                    kishuname3ji = sei[0:2] + mei[0]
                elif len(sei) == 1:
                    # 武　豊
                    kishuname3ji = sei + '　' + mei[0]
            else:
                if len(sei) == 1:
                    # 　幸　
                    kishuname3ji = '　' + sei + '　'
                elif len(sei) == 2:
                    # 福　永
                    kishuname3ji = sei[0] + '　' + sei[1]
                elif len(sei) >= 3:
                    # 武士沢
                    kishuname3ji = sei[0:3]
            
        else:
            # 外国人名の場合
            if '．' in kishuname:
                firstname = kishuname.split('．')[0] # Ｃ
                lastname = kishuname.split('．')[1] # ルメール

                if lastname in meiari_sei_gai_list: # 外国人同姓対応
                    if len(lastname) == 1:
                        # Ｍ　デ　
                        kishuname3ji = firstname[0] + '　' + lastname
                    elif len(lastname) == 2:
                        # Ｍデム
                        kishuname3ji = firstname[0] + lastname
                    elif len(lastname) >= 3:
                        # Ｍデム
                        kishuname3ji = firstname[0] + lastname[0:2]
                else:
                    if len(lastname) == 1:
                        # 　ル　
                        kishuname3ji = '　' + lastname + '　'
                    elif len(lastname) == 2:
                        # ル　メ
                        kishuname3ji = lastname[0] + '　' + lastname[1]
                    elif len(lastname) >= 3:
                        # ルメー
                        kishuname3ji = lastname[0:3]

                    # 上記と同じ理由で、以下のMst_3jikishuへの自動追加処理はコメント化
                    # ★★★★★ここから★★★★★
                    ## Mst_3jikishuに名前がないにもかかわらず、既に騎手マスタに登録されている外国人名騎手の中に、別人だけど3字略が同じ騎手がいた場合、この騎手のlastnameをMst_3jikishuに自動追加する

                    # if Mst_Kishu.objects.filter(Kishu_name=kishuname3ji).exclude(Kishu_name_sei=kishuname).exists():
                    #     Mst_3jikishu.objects.create(Kishu_name_sei=lastname,Gaikoku_name_flg=True)
                    #     if len(lastname) == 1:
                    #         # Ｍ　デ
                    #         kishuname3ji = firstname[0] + '　' + lastname
                    #     elif len(lastname) == 2:
                    #         # Ｍデム
                    #         kishuname3ji = firstname[0] + lastname
                    #     elif len(lastname) >= 3:
                    #         # Ｍデム
                    #         kishuname3ji = firstname[0] + lastname[0:2]

                    #     # 新たに同じ名字の騎手が現れたので、元から居る騎手の３字略も修正する。
                    #     dousei_kishu_l = Mst_Kishu.objects.filter(Kishu_name=kishuname3ji).exclude(Kishu_name_sei=kishuname)
                    #     for kishu_obj in dousei_kishu_l:
                    #         update_kishuname = kishu_obj.Kishu_name_sei
                    #         u_firstname = update_kishuname.split('．')[0] # Ｍ
                    #         u_lastname = update_kishuname.split('．')[1] # デムーロ
                    #         if len(u_lastname) == 1:
                    #             # Ｍ　デ　
                    #             updatename = u_firstname[0] + '　' + u_lastname
                    #         elif len(u_lastname) == 2:
                    #             # Ｍデム
                    #             updatename = u_firstname[0] + u_lastname
                    #         elif len(u_lastname) >= 3:
                    #             # Ｍデム
                    #             updatename = u_firstname[0] + u_lastname[0:2]
                    #         kishu_obj.Kishu_name = updatename
                    #         kishu_obj.save()
                    #         logger.info(f'【騎手マスタ】3字騎手名 更新 {kishu_obj.Kishu_code} {kishu_obj.Kishu_name}')
                    # ★★★★★ここまで★★★★★
            else:
                # '．'がない外国人名の場合で、既にいる騎手と3文字略がかぶった場合はどうしようもない（前例がなく3文字略する法則がないので、機械的に直せない。手で直すしかない）
                kishuname3ji = kishuname[0:3]

        return kishuname3ji

    def make_sei(self, kishuname):
        if '　' in kishuname:
            sei = self.make_name(kishuname).split('　')[0]
        else:
            sei = kishuname
        return sei

    def make_mei(self, kishuname):
        if '　' in kishuname:
            mei = self.make_name(kishuname).split('　')[1]
        else:
            mei = None # 外国時の場合は名は無し
        return mei

    def insert_or_update_Mst_Kishu(self, datfilename, datDataFileFlg):
        try:
            from app_jra.models import (
                Mst_JJOC_Kishu,
                Mst_Kishu,
            )
            Cmn = Common()
            update_num = 0

            if M_KISHU == datDataFileFlg:
                # JJOC 騎手マスタ
                JJOC_list = Mst_JJOC_Kishu.objects.filter(Receive_filename=datfilename)
                
                for data in JJOC_list:
                    kishu_objs = Mst_Kishu.objects.filter(Kishu_code=data.Kishu_code)
                    kishuname = self.make_name(data.Kishu_name)  # 「姓　名」

                    gaikokumei = None
                    gaikokusei = None
                    if '．' in kishuname:
                        gaikokuseimei = mojimoji.han_to_zen(data.Kishu_name_katakana.rstrip())
                        if '．' in gaikokuseimei:
                            gaikokusei = gaikokuseimei.split('．')[0]
                            gaikokumei = gaikokuseimei.split('．')[1]
                        elif '　' in gaikokuseimei:
                            gaikokusei = gaikokuseimei.split('　')[0]
                            gaikokumei = gaikokuseimei.split('　')[1]

                    if kishu_objs.exists():
                        # 更新
                         kishu_obj = kishu_objs.last() # Kishu_codeはunique
                         kishu_obj.Kishu_name = self.make_3jikishumei(kishuname)
                         kishu_obj.Kishu_name_sei = self.make_sei(kishuname) if not gaikokusei else gaikokusei
                         kishu_obj.Kishu_name_mei = self.make_mei(kishuname) if not gaikokumei else gaikokumei
                         kishu_obj.Kokuseki = Cmn.chk_master_Mst_Jou(data.Kishu_shotai_kunimei_code)
                         kishu_obj.Mikubun = Cmn.chk_master_Mst_Mikubun(data.Kishu_minarai_kigou_code)
                         kishu_obj.Touzai = TOUZAI_KUBUN[data.Kishu_touzai_kubun_code]
                         kishu_obj.save()
                    else:
                        kishu_obj = Mst_Kishu.objects.create(
                            Kishu_code=data.Kishu_code,
                            Kishu_name = self.make_3jikishumei(kishuname),
                            Kishu_name_sei = self.make_sei(kishuname) if not gaikokusei else gaikokusei,
                            Kishu_name_mei = self.make_mei(kishuname) if not gaikokumei else gaikokumei,
                            Kokuseki = Cmn.chk_master_Mst_Jou(data.Kishu_shotai_kunimei_code),
                            Mikubun=Cmn.chk_master_Mst_Mikubun(data.Kishu_minarai_kigou_code),
                            Touzai = TOUZAI_KUBUN[data.Kishu_touzai_kubun_code]
                        )
                    update_num += 1
                logger.info(f'【騎手マスタ】{str(update_num)}件 登録・更新')

            return NORMAL

        except Exception as e:
            Common_log.Out_Logs(log_err_msg_id, [e])
            logger.error(failure(e))
            return ABNORMAL
