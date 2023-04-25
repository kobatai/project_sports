# docker-compose exec web bashの中で
# python manage.py runscript auto_migration
import os
import subprocess

# BASE_DIR = os.path.dirname(os.path.dirname(__file__)) # このファイルの場所によって変更

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
    makemigrations()

    migrate()

