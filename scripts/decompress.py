import re
import lhafile
import zipfile
import os
import logging.config

class decompress():

    def unzip_lzh_for_directory(self, lzh_dir:str, unzip_lzh_dir:str) -> bool:
        '''
        lzh形式で圧縮されたファイルをディレクトリ単位で解凍する
        
        Parameters
        ----------
            lzh_dir : lzhで圧縮されたファイルのおかれているディレクトリ
            unzip_lzh_dir : 解凍したファイルを置くためのディレクトリ

        戻り値
        ----------
            全処理が成功した場合、True
            例外が発生した場合にはFalseを返す
        '''
        logger = logging.getLogger("debug")

        try :
            # LZHファイルのリストを取得
            lzh_file_list = os.listdir(lzh_dir)

            # ファイルの数だけ処理を繰り返す
            for lzh_file_name in lzh_file_list:

                # 拡張子が lzh のファイルに対してのみ実行
                if re.search(".lzh", lzh_file_name):

                    # 解凍したファイルを保存する場所を新たに指定
                    new_file_dir = unzip_lzh_dir + lzh_file_name[:-4] + "/"

                    # ファイルを格納するフォルダを作成
                    os.makedirs(new_file_dir, exist_ok=True)

                    file = lhafile.Lhafile(lzh_dir + lzh_file_name)

                    # 解凍したファイルの名前を取得
                    info = file.infolist()
                    name = info[0].filename

                    # 解凍したファイルの保存
                    open(unzip_lzh_dir + name, "wb").write(file.read(name))

                    logger.debug(unzip_lzh_dir + lzh_file_name + " を解凍しました")

            # ファイルの数だけ処理を繰り返す
            for lzh_file_name in lzh_file_list:

                # 拡張子が lzh のファイルに対してのみ実行
                if re.search(".lzh", lzh_file_name):
                    # 解凍の完了したlzhファイルを削除する
                    os.remove(lzh_file_name)

            return True

        except Exception as e:
            logger.error("エラー %s " % e)
            return False


    def unzip_lzh_for_file(self, lzh_file:str) -> tuple[bool, str] :
        '''
        lzh形式で圧縮されたファイルをファイル単位で解凍する
        
        Parameters
        ----------
            lzh_file : lzhで圧縮されたファイルパス

        戻り値
        ----------
            bool : 処理が成功した場合はTrue、例外が発生した場合にはFalseを返す
            str : lzh解凍後のファイルパス（例外発生時はNone）
        '''
        logger = logging.getLogger("debug")
        unzip_lzh_filename : str = lzh_file

        try :
            # 拡張子が lzh のファイルに対してのみ実行
            if re.search(".lzh", lzh_file):

                file = lhafile.Lhafile(lzh_file)

                # 解凍したファイルの名前を取得
                info = file.infolist()
                name = info[0].filename

                # 解凍したファイルの保存
                unzip_lzh_filename = os.path.dirname(lzh_file) + '/' + name
                open(unzip_lzh_filename, "wb").write(file.read(name))

                logger.debug(unzip_lzh_filename + " を解凍しました")

            return True , unzip_lzh_filename

        except Exception as e:
            logger.error("エラー %s " % e)
            return False , None


    def unzip_for_file(self, zip_file:str) -> tuple[bool, str] :
        '''
        zip形式で圧縮されたファイルをファイル単位で解凍する
        
        Parameters
        ----------
            zip_file : zipで圧縮されたファイルパス

        戻り値
        ----------
            bool : 処理が成功した場合はTrue、例外が発生した場合にはFalseを返す
            str : zip解凍後のファイルパス（例外発生時はNone）
        '''
        logger = logging.getLogger("debug")
        unzip_filename : str = zip_file

        try :
            # 拡張子が zip のファイルに対してのみ実行
            if re.search(".zip", zip_file):

                file = zipfile.ZipFile(zip_file)

                # 解凍したファイルの名前を取得
                info = file.infolist()
                name = info[0].filename

                # 解凍したファイルの保存
                unzip_filename = os.path.dirname(zip_file) + '/' + name
                open(unzip_filename, "wb").write(file.read(name))

                logger.debug(unzip_filename + " を解凍しました")

            return True , unzip_filename

        except Exception as e:
            logger.error("エラー %s " % e)
            return False , None
