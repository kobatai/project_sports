# coding: utf-8
# docker-compose exec web bashの中で
# python manage.py runscript script_name
# import glob
import os
from pathlib import Path

BASE_DIR = os.path.dirname(os.path.dirname(__file__))  # このファイルの場所によって変更
SIZE_IN_MB = 2
# def clean_migration():
#     migration_files = glob.iglob('**/migrations/[0-9][0-9][0-9][0-9]*.py', recursive=True)
#     for migration_file in migration_files:
#         os.remove(os.path.join(BASE_DIR, migration_file))
#         print(f"Deleted {migration_file}")


def search_files(path, size_min_in_byte):
    """指定されたパスの下にある指定されたサイズ以上のファイル名を一覧表示する
    """
    size_min_in_mb = size_min_in_byte << 20

    p = Path(path)

    # 指定されたパス以下のファイルを再帰的にチェックする
    # 指定されたサイズ以上のファイルは「 10MB  ファイル名」といった感じに表示する
    for file in p.iterdir():
        if file.is_dir():
            search_files(file, size_min_in_byte)
        elif file.is_file():
            size = file.stat().st_size
            if size >= size_min_in_mb:
                # resolve() を使って絶対パスを表示する
                print('{:.1f}MB\t{}'.format(size >> 20, file.resolve()))


def run():
    '''
    開始
    '''
    search_files(BASE_DIR, SIZE_IN_MB)
