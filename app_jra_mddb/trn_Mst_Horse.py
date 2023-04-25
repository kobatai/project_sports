from logging import getLogger
import re
from app_jra.consts import *
import sys
import linecache
from app_jra.log_commons import *
from datetime import datetime as dt
from django.db import transaction


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

class Trn_Mst_Horse():

    def chk_blank_zero(self, record):
        # 受信データにはブランクのパターンがいくつかあるが、ゼロの場合と空データの場合は全てブランクとみなす処理
        blank_list = ['','　',None,[],{},0,'0','０']
        if record in blank_list or re.fullmatch('\s+', record) or re.fullmatch('　', record) or re.fullmatch('0', record) or re.fullmatch('０', record):
            return False
        else:
            return True
    def get_bamei(self, row_name, row_name_EN):
        name = None
        name_9 = None
        if self.chk_blank_zero(row_name):
            name = row_name.rstrip()
            name_9 = row_name[0:9].rstrip().ljust(9, '　')
        elif self.chk_blank_zero(row_name_EN):
            name = row_name_EN.rstrip()
            name_9 = row_name_EN[0:9].rstrip().ljust(9, '　')
        return name, name_9



    def insert_or_update_Mst_Horse(self, datfilename, datDataFileFlg):
        try:
            from app_jra.models import (
                Mst_JHOS_RaceHorse,
                Mst_Horse,
            )
            Cmn = Common()
            update_num = 0

            if M_RACEHORSE == datDataFileFlg:
                # JHOS 競走馬マスタ
                JHOS_list = Mst_JHOS_RaceHorse.objects.filter(Receive_filename=datfilename)

                if JHOS_list.count() > 5000:
                    # 蓄積マスタの超でかいデータを扱うときは、以下の一括処理
                    logger.info(f'競走馬マスタ 登録開始 {str(JHOS_list.count())}件 一括登録します。')
                    id_num = Mst_Horse.objects.last().id + 1 if Mst_Horse.objects.last() else 1
                    create_records = []
                    update_records = []
                    update_table_fields = ["Bamei","Bamei_9","Kyuu_Bamei","Kyuu_Bamei_9","Gaikokuseki","Seibetsu","Keiro","Kigou","Chichi","Haha","Hahachichi","Hahahaha","Seinen","Umanushi","Sanchi","Seisansha","Ruihonkin","Ruifukakin","Hirashukin","Shoushukin","JRA_Geneki_flg","CK_Geneki_flg","Kaigai_flg"]
                    for data in JHOS_list:
                        horse_num = data.Horse_ketto_number
                        if self.chk_blank_zero(horse_num):
                            horse_objs = Mst_Horse.objects.filter(Number=horse_num)
                            if horse_objs.exists():
                                horse_obj = horse_objs.last()
                                horse_obj.Bamei = self.get_bamei(data.Horse_name, data.Horse_name_EN)[0]
                                horse_obj.Bamei_9 = self.get_bamei(data.Horse_name, data.Horse_name_EN)[1]
                                horse_obj.Kyuu_Bamei = self.get_bamei(data.Horse_mae_horse_name, data.Horse_mae_horse_name_EN)[0]
                                horse_obj.Kyuu_Bamei_9 = self.get_bamei(data.Horse_mae_horse_name, data.Horse_mae_horse_name_EN)[1]
                                horse_obj.Gaikokuseki = Cmn.chk_master_Mst_Jou(data.Horse_shotai_kunimei_code)
                                horse_obj.Seibetsu = Cmn.chk_master_Mst_Seibetsu(data.Horse_seibetu_code)
                                horse_obj.Keiro = Cmn.chk_master_Mst_Keiro(data.Horse_keiro_code)
                                horse_obj.Kigou = Cmn.chk_master_Mst_Uma_kigou(data.Horse_kigou_code)
                                horse_obj.Chichi = self.get_bamei(data.Horse_dad_name, data.Horse_dad_name_EN)[0]
                                horse_obj.Haha = self.get_bamei(data.Horse_mom_name, data.Horse_mom_name_EN)[0]
                                horse_obj.Hahachichi = self.get_bamei(data.Horse_grandpa_name, data.Horse_grandpa_name_EN)[0]
                                horse_obj.Hahahaha = self.get_bamei(data.Horse_grandma_name, data.Horse_grandma_name_EN)[0]
                                horse_obj.Seinen = dt.strptime(data.Horse_seinengappi, '%Y%m%d').date()
                                horse_obj.Umanushi = Cmn.chk_master_Mst_Umanushi(data.Horse_banushi_number)
                                horse_obj.Sanchi = data.Horse_sanchi.rstrip()
                                horse_obj.Seisansha = Cmn.chk_master_Mst_Seisansha(data.Horse_producer_code)
                                horse_obj.Ruihonkin = (int(data.Horse_heichi_nyuchaku_honsyokin) + int(data.Horse_syogai_nyuchaku_honsyokin))*100
                                horse_obj.Ruifukakin = int(data.Horse_heichi_nyuchaku_hukasyokin)*100
                                horse_obj.Hirashukin = int(data.Horse_new_heichi_syokin_old)*100
                                horse_obj.Shoushukin = int(data.Horse_new_syogai_syokin)*100
                                horse_obj.JRA_Geneki_flg = data.Horse_massho_kubun == '0'
                                horse_obj.CK_Geneki_flg = (data.Horse_shotai_kubun_code == '1' or data.Horse_shotai_kubun_code == '3')
                                horse_obj.Kaigai_flg = (data.Horse_shotai_kubun_code == '2' or data.Horse_shotai_kubun_code == '4')
                                update_records.append(horse_obj)
                            else:
                                create_fields = {
                                    "id" : id_num,
                                    "Number" : data.Horse_ketto_number,
                                    "Bamei" : self.get_bamei(data.Horse_name, data.Horse_name_EN)[0],
                                    "Bamei_9" : self.get_bamei(data.Horse_name, data.Horse_name_EN)[1],
                                    "Kyuu_Bamei" : self.get_bamei(data.Horse_mae_horse_name, data.Horse_mae_horse_name_EN)[0],
                                    "Kyuu_Bamei_9" : self.get_bamei(data.Horse_mae_horse_name, data.Horse_mae_horse_name_EN)[0],
                                    "Gaikokuseki" : Cmn.chk_master_Mst_Jou(data.Horse_shotai_kunimei_code),
                                    "Seibetsu" : Cmn.chk_master_Mst_Seibetsu(data.Horse_seibetu_code),
                                    "Keiro" : Cmn.chk_master_Mst_Keiro(data.Horse_keiro_code),
                                    "Kigou" : Cmn.chk_master_Mst_Uma_kigou(data.Horse_kigou_code),
                                    "Chichi" : self.get_bamei(data.Horse_dad_name, data.Horse_dad_name_EN)[0],
                                    "Haha" : self.get_bamei(data.Horse_mom_name, data.Horse_mom_name_EN)[0],
                                    "Hahachichi" : self.get_bamei(data.Horse_grandpa_name, data.Horse_grandpa_name_EN)[0],
                                    "Hahahaha" : self.get_bamei(data.Horse_grandma_name, data.Horse_grandma_name_EN)[0],
                                    "Seinen" : dt.strptime(data.Horse_seinengappi, '%Y%m%d').date(),
                                    "Umanushi" : Cmn.chk_master_Mst_Umanushi(data.Horse_banushi_number),
                                    "Sanchi" : data.Horse_sanchi.rstrip(),
                                    "Seisansha" : Cmn.chk_master_Mst_Seisansha(data.Horse_producer_code),
                                    "Ruihonkin" : (int(data.Horse_heichi_nyuchaku_honsyokin) + int(data.Horse_syogai_nyuchaku_honsyokin))*100,
                                    "Ruifukakin" : int(data.Horse_heichi_nyuchaku_hukasyokin)*100,
                                    "Hirashukin" : int(data.Horse_new_heichi_syokin_old)*100,
                                    "Shoushukin" : int(data.Horse_new_syogai_syokin)*100,
                                    "JRA_Geneki_flg" : data.Horse_massho_kubun == '0',
                                    "CK_Geneki_flg" : (data.Horse_shotai_kubun_code == '1' or data.Horse_shotai_kubun_code == '3'),
                                    "Kaigai_flg" : (data.Horse_shotai_kubun_code == '2' or data.Horse_shotai_kubun_code == '4')
                                }
                                create_record = Mst_Horse(**create_fields)
                                create_records.append(create_record)
                                id_num += 1

                            # DB　ファイル登録
                            with transaction.atomic():
                                update_num += 1
                                if len(create_records) + len(update_records) >= 2000:
                                    # 2000件ごとに一括登録
                                    if create_records:
                                        Mst_Horse.objects.bulk_create(create_records, batch_size=500)
                                    if update_records:
                                        Mst_Horse.objects.bulk_update(update_records, fields=update_table_fields, batch_size=500)
                                    logger.info(f'{str(update_num)}件までの登録 完了')
                                    # 後始末
                                    create_record = {}
                                    create_records = []
                                    update_records = []
                        
                    # 余りの残りを一括登録
                    if create_records:
                        Mst_Horse.objects.bulk_create(create_records, batch_size=500)
                    if update_records:
                        Mst_Horse.objects.bulk_update(update_records, fields=update_table_fields, batch_size=500)
                    # 後始末
                    create_records = []
                    update_records = []

                else:
                    # JHOS 競走馬マスタ
                    JHOS_list = Mst_JHOS_RaceHorse.objects.filter(Receive_filename=datfilename)
                    for data in JHOS_list:
                        # 通常は以下の処理
                        uma_objs = Mst_Horse.objects.filter(Number=data.Horse_ketto_number)
                        if uma_objs.exists():
                            # 更新
                            uma_obj = uma_objs.last() # Numberはunique
                            uma_obj.Bamei = self.get_bamei(data.Horse_name, data.Horse_name_EN)[0]
                            uma_obj.Bamei_9 = self.get_bamei(data.Horse_name, data.Horse_name_EN)[1]
                            uma_obj.Kyuu_Bamei = self.get_bamei(data.Horse_mae_horse_name, data.Horse_mae_horse_name_EN)[0]
                            uma_obj.Kyuu_Bamei_9 = self.get_bamei(data.Horse_mae_horse_name, data.Horse_mae_horse_name_EN)[1]
                            uma_obj.Gaikokuseki = Cmn.chk_master_Mst_Jou(data.Horse_shotai_kunimei_code)
                            uma_obj.Seibetsu = Cmn.chk_master_Mst_Seibetsu(data.Horse_seibetu_code)
                            uma_obj.Keiro = Cmn.chk_master_Mst_Keiro(data.Horse_keiro_code)
                            uma_obj.Kigou = Cmn.chk_master_Mst_Uma_kigou(data.Horse_kigou_code)
                            uma_obj.Chichi = self.get_bamei(data.Horse_dad_name, data.Horse_dad_name_EN)[0]
                            uma_obj.Haha = self.get_bamei(data.Horse_mom_name, data.Horse_mom_name_EN)[0]
                            uma_obj.Hahachichi = self.get_bamei(data.Horse_grandpa_name, data.Horse_grandpa_name_EN)[0]
                            uma_obj.Hahahaha = self.get_bamei(data.Horse_grandma_name, data.Horse_grandma_name_EN)[0]
                            uma_obj.Seinen = dt.strptime(data.Horse_seinengappi, '%Y%m%d').date()
                            uma_obj.Umanushi = Cmn.chk_master_Mst_Umanushi(data.Horse_banushi_number)
                            uma_obj.Sanchi = data.Horse_sanchi.rstrip()
                            uma_obj.Seisansha = Cmn.chk_master_Mst_Seisansha(data.Horse_producer_code)
                            uma_obj.Ruihonkin = (int(data.Horse_heichi_nyuchaku_honsyokin) + int(data.Horse_syogai_nyuchaku_honsyokin))*100
                            uma_obj.Ruifukakin = int(data.Horse_heichi_nyuchaku_hukasyokin)*100
                            uma_obj.Hirashukin = int(data.Horse_new_heichi_syokin_old)*100
                            uma_obj.Shoushukin = int(data.Horse_new_syogai_syokin)*100
                            uma_obj.JRA_Geneki_flg = data.Horse_massho_kubun == '0'
                            uma_obj.CK_Geneki_flg = (data.Horse_shotai_kubun_code == '1' or data.Horse_shotai_kubun_code == '3')
                            uma_obj.Kaigai_flg = (data.Horse_shotai_kubun_code == '2' or data.Horse_shotai_kubun_code == '4')
                            uma_obj.save()
                        else:
                            # 新規作成
                            uma_obj = Mst_Horse.objects.create(
                                Number = data.Horse_ketto_number,
                                Bamei = self.get_bamei(data.Horse_name, data.Horse_name_EN)[0],
                                Bamei_9 = self.get_bamei(data.Horse_name, data.Horse_name_EN)[1],
                                Kyuu_Bamei = self.get_bamei(data.Horse_mae_horse_name, data.Horse_mae_horse_name_EN)[0],
                                Kyuu_Bamei_9 = self.get_bamei(data.Horse_mae_horse_name, data.Horse_mae_horse_name_EN)[1],
                                Gaikokuseki = Cmn.chk_master_Mst_Jou(data.Horse_shotai_kunimei_code),
                                Seibetsu = Cmn.chk_master_Mst_Seibetsu(data.Horse_seibetu_code),
                                Keiro = Cmn.chk_master_Mst_Keiro(data.Horse_keiro_code),
                                Kigou = Cmn.chk_master_Mst_Uma_kigou(data.Horse_kigou_code),
                                Chichi = self.get_bamei(data.Horse_dad_name, data.Horse_dad_name_EN)[0],
                                Haha = self.get_bamei(data.Horse_mom_name, data.Horse_mom_name_EN)[0],
                                Hahachichi = self.get_bamei(data.Horse_grandpa_name, data.Horse_grandpa_name_EN)[0],
                                Hahahaha = self.get_bamei(data.Horse_grandma_name, data.Horse_grandma_name_EN)[0],
                                Seinen = dt.strptime(data.Horse_seinengappi, '%Y%m%d').date(),
                                Umanushi = Cmn.chk_master_Mst_Umanushi(data.Horse_banushi_number),
                                Sanchi = data.Horse_sanchi.rstrip(),
                                Seisansha = Cmn.chk_master_Mst_Seisansha(data.Horse_producer_code),
                                Ruihonkin = (int(data.Horse_heichi_nyuchaku_honsyokin) + int(data.Horse_syogai_nyuchaku_honsyokin))*100,
                                Ruifukakin = int(data.Horse_heichi_nyuchaku_hukasyokin)*100,
                                Hirashukin = int(data.Horse_new_heichi_syokin_old)*100,
                                Shoushukin = int(data.Horse_new_syogai_syokin)*100,
                                JRA_Geneki_flg = data.Horse_massho_kubun == '0',
                                CK_Geneki_flg = (data.Horse_shotai_kubun_code == '1' or data.Horse_shotai_kubun_code == '3'),
                                Kaigai_flg = (data.Horse_shotai_kubun_code == '2' or data.Horse_shotai_kubun_code == '4')
                            )
                        update_num += 1

            logger.info(f'【競走馬マスタ】{str(update_num)}件 登録・更新')
            return NORMAL

        except Exception as e:
            Common_log.Out_Logs(log_err_msg_id, [e])
            logger.error(failure(e))
            return ABNORMAL
