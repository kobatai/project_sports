# docker-compose exec web bashの中で
# python manage.py runscript restore_DB
import glob
import os
import subprocess
from datetime import datetime as dt
from pathlib import Path

def dumpdata():
    # 蓄積系マスタデータをjsonに出力する

    base_model = "app_jra."
    today_str = dt.now().strftime('%Y%m%d')
    base_dir = "蓄積マスタ/" + today_str

    dir = Path(base_dir)
    if not dir.exists():
        # 指定のディレクトリが存在しない場合のみ新規作成
        os.makedirs(base_dir)

    for mst in ["Mst_Choukyoushi","Mst_Kishu","Mst_Seisansha","Mst_Umanushi","Mst_Horse","Mst_Kyousou_seiseki"]:
        print(f"{mst} 出力開始")
        cmd_str = "python manage.py dumpdata "+ base_model + mst + " > " + base_dir + "/" + mst + "_" + today_str + ".json"
        subprocess.run(cmd_str, shell=True)

def run():
    '''
    開始
    '''
    print(f"蓄積系マスタデータを出力します。たぶん数分かかります。完了したらここに完了って出ます。")
    dumpdata()
    print(f"蓄積系マスタデータ　出力完了")
