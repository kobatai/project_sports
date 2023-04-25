# docker-compose exec web bashの中で
# python manage.py runscript migrate
import glob
import os
import subprocess

BASE_DIR = os.path.dirname(os.path.dirname(__file__))  # このファイルの場所によって変更


def clean_migration():
    migration_files = glob.iglob('**/migrations/[0-9][0-9][0-9][0-9]*.py', recursive=True)
    for migration_file in migration_files:
        os.remove(os.path.join(BASE_DIR, migration_file))
        print(f"Deleted {migration_file}")


def makemigrations():
    cmd = ["python", "manage.py", "makemigrations"]
    subprocess.run(cmd)


def migrate():
    cmd = ["python", "manage.py", "migrate"]
    subprocess.run(cmd)


def run():
    '''
    開始
    '''
    # clean_migration()

    makemigrations()

    migrate()
