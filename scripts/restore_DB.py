# docker-compose exec web bashの中で
# python manage.py runscript restore_DB
import glob
import os
import subprocess

BASE_DIR = os.path.dirname(os.path.dirname(__file__))  # このファイルの場所によって変更


def clean_migration():
    migration_files = glob.iglob('**/migrations/[0-9][0-9][0-9][0-9]*.py', recursive=True)
    for migration_file in migration_files:
        os.remove(os.path.join(BASE_DIR, migration_file))
        print(f"Deleted {migration_file}")


def reset_db():
    # yes　で開始
    cmd = ["python", "manage.py", "reset_db"]
    subprocess.run(cmd)


def makemigrations():
    cmd = ["python", "manage.py", "makemigrations"]
    subprocess.run(cmd)


def migrate():
    cmd = ["python", "manage.py", "migrate"]
    subprocess.run(cmd)


def createsuperuser():
    cmd = ["python", "manage.py", "createsuperuser"]
    subprocess.run(cmd)


def loaddata():
    cmd = ["python", "manage.py", "loaddata", "app_jra.json"]
    subprocess.run(cmd)
    # 蓄積系マスタデータをロードする
    # cmd = ["python", "manage.py", "loaddata", "Mst_Choukyoushi.json"]
    # subprocess.run(cmd)
    # cmd = ["python", "manage.py", "loaddata", "Mst_Kishu.json"]
    # subprocess.run(cmd)
    # cmd = ["python", "manage.py", "loaddata", "Mst_Seisansha.json"]
    # subprocess.run(cmd)
    # cmd = ["python", "manage.py", "loaddata", "Mst_Umanushi.json"]
    # subprocess.run(cmd)
    # cmd = ["python", "manage.py", "loaddata", "Mst_Horse.json"]
    # subprocess.run(cmd)

def run():
    '''
    開始
    '''
    clean_migration()

    reset_db()

    makemigrations()

    migrate()

    loaddata()
    # こちらでスーバーユーザー作成
    createsuperuser()
