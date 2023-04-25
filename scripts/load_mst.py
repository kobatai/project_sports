# docker-compose exec web bashの中で
# python manage.py runscript restore_DB
import glob
import os
import subprocess
import shutil
import time

from app_jra_receive.receive_proc import Receive_proc
from app_jra.models import Tran_Chikuseki_Mst

def loaddata(date):
    # 蓄積系マスタデータをロードする

    date_str = '20220614' # 初期値は、蓄積マスタの初期データ
    if date:
        date_str = str(date)
    print(f"{date_str}")
    base_dir = "蓄積マスタ/" + date_str

    # 騎手名、調教師名は修正済みマスタから取得する
    for mst in ["Mst_3jichou","Mst_3jikishu","Mst_Choukyoushi","Mst_Kishu"]:
        print(f"{mst} ロード開始")
        cmd = ["python", "manage.py", "loaddata", base_dir + "/" + mst + "_" + "modified" + ".json"]
        subprocess.run(cmd)

    # それ以外は20220614マスタから取得する
    for mst in ["Mst_Seisansha","Mst_Umanushi","Mst_Horse","Mst_Kyousou_seiseki"]:
        print(f"{mst} ロード開始")
        cmd = ["python", "manage.py", "loaddata", base_dir + "/" + mst + "_" + date_str + ".json"]
        subprocess.run(cmd)
    
    # datdataを過去から順番にロードしていく
    dat_base_dir = "蓄積マスタ/datdata/"
    if os.path.exists(dat_base_dir):
        print(f"{dat_base_dir}にあるdatdataを順番にロードします")
        receive_dir = "app_jra/input/Receive/"
        resisterd_list = []
        for datname in ['jrsjoc', 'jrstra', 'jrsown', 'jrsbrd', 'jrshos', 'jrsres', 'chires']:
            # 順番を担保する必要がある。特にjrshos→jrsres→chires
            dat_dir = dat_base_dir + datname
            # Receiveファイルに全てのデータをコピーする
            filelist = os.listdir(dat_dir)
            if len(filelist):
                for filename in filelist:
                    # 日付指定がある場合、その日までのデータのみ投入する
                    if date:
                        if int(filename.split('_')[1].split('.')[0]) <= date:
                            shutil.copy(os.path.join(dat_dir, filename), receive_dir)
                    else:
                        # 日付指定が無い場合は、最新状態にするために最新版まで全部入れる
                        shutil.copy(os.path.join(dat_dir, filename), receive_dir)
                    
                # Receiveフォルダからの受信処理を走らせる。
                try:
                    # print(f"{datname} の受信処理開始")
                    receivedfile_list = Receive_proc().run_reception_processing()
                except Exception as e:
                    print(f"{datname}のロードでエラーが発生しました。{e}")
                finally:
                    print(f"{datname} 受信処理完了")
                    resisterd_list.extend(receivedfile_list)
        if resisterd_list:
            print(f"全受信処理を完了しました。受信データを整形し、各種マスタテーブルに登録していきます。")
            final_filename = resisterd_list[-1]
            mst_waiting_fig = True
            print(f"マスタ登録処理が完了するまでしばらくお待ちください。10分くらい。 ({final_filename} のデータが登録されたら完了)")
            while mst_waiting_fig:
                if Tran_Chikuseki_Mst.objects.filter(Receive_filename=final_filename).exists():
                    mst_waiting_fig = False
                else:
                    print(f"登録中...")
                    time.sleep(55)


def run(date=None):
    '''
    開始
    '''
    print(f"蓄積系マスタデータをロードします。たぶん数分かかります。完了したらここに完了って出ます。")
    loaddata(date)
    print(f"蓄積系マスタデータ データロード完了")
