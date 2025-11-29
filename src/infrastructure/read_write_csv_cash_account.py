# Libraries
import glob
from logging import getLogger

import polars as pl


class ReadWriteCsvCashAcount:
    """
    ゆうちょ銀行からダウンロードしたcsvから必要な列を抽出する。
    """

    def __init__(self) -> None:
        self.logger = getLogger(self.__class__.__name__)

    def read_csv_raw(self, file_path: str, target_ym: str):
        """
        ゆうちょ銀行のcsvはヘッダーが8行目にあるため、skip_rows=7を設定する
        Args:
            file_path: 処理するデータのパス
            target_ym: 処理するデータの対象年月
            cash_record.csv: 取引履歴のcsvファイル名

        Returns:
            Polars_DataFrame: 処理するデータのDataFrame
        """
        csv_path = glob.glob(file_path + f"/{target_ym}" + "/*.csv")
        try:
            _df_csv = pl.read_csv(
                csv_path[0],
                skip_rows=7,
                encoding="shift-jis",
            )
            self.logger.info("処理するデータの読み込み")
        except Exception as e:
            self.logger.error(f"読み込みエラーが発生しました: {e}")
            raise e

        return _df_csv

    def write_csv(self, file_path: str, target_ym: str, pl_df):
        """
        処理したデータをデータマートとして保存する機能
        Args:
            file_path: 保存するディレクトリ
            target_ym: 保存するデータの対象年月
            pl_df: 処理したデータ
        """
        _save_path = file_path + f"/{target_ym}" + "/cash_record_tmp.csv"
        try:
            with open(_save_path, mode="w", encoding="shift-jis") as _f:
                _f.write(pl_df.write_csv())
            self.logger.info("処理したデータの書き込み")
        except Exception as e:
            self.logger.error(f"書き込みエラーが発生しました: {e}")
            raise e

    def read_csv(self, file_path: str, target_ym: str):
        """
        加工し保存したcsvデータを読み込む。
        Args:
            file_path: 処理するデータのパス
            target_ym: 処理するデータの対象年月
            cash_record.csv: 取引履歴のcsvファイル名

        Returns:
            Polars_DataFrame: 処理するデータのDataFrame
        """
        csv_path = glob.glob(file_path + f"/{target_ym}" + "/*.csv")
        try:
            _df_csv = pl.read_csv(
                csv_path[0],
                encoding="shift-jis",
            )
            self.logger.info("処理するデータの読み込み")
        except Exception as e:
            self.logger.error(f"読み込みエラーが発生しました: {e}")
            raise e

        return _df_csv
