# Libraries
from datetime import datetime, timedelta
from logging import getLogger

import polars as pl
import yaml

from src.infrastructure.read_write_csv_cash_account import ReadWriteCsvCashAcount


class CreateDatamart:
    """
    抽出したデータを加工し、可視化しやすい形に整形し、保存する。
    """

    def __init__(self, config_path: str, target_ym: str) -> None:
        self.logger = getLogger(self.__class__.__name__)
        self.target_ym = target_ym
        self.__rwc = ReadWriteCsvCashAcount()
        # configファイルの読み込み
        with open(config_path) as _f:
            self.config = yaml.load(_f, Loader=yaml.FullLoader)

    def run_all(self) -> None:
        # 加工を一括で行う。
        self.create_date()  # 日付の生成
        self.date_to_cashtrade()  # データの加工
        self.save_trade_data()  # 加工データの保存

    def create_date(self) -> None:
        """
        1ヶ月分の日付を生成する。
        Args:
            target_ym: 対象年月

        Returns:
            df_date: 月日
        """
        _year = int(self.target_ym[:4])
        _month = int(self.target_ym[4:6])
        _start_date = datetime(_year, _month, 1)
        _next_month = (_month % 12) + 1
        _next_year = _year + (1 if _next_month == 1 else 0)
        _end_date = datetime(_next_year, _next_month, 1) - timedelta(days=1)

        # その月のリストを生成する
        date_list = [
            (_start_date + timedelta(days=i)).strftime("%Y%m%d")
            for i in range((_end_date - _start_date).days + 1)
        ]

        # DataFrame に縦に並べる
        self.df_date = pl.DataFrame({"date": date_list})

    def date_to_cashtrade(self) -> None:
        """
        1ヶ月の取引履歴のデータを作成する
        Args:
            df_date: 日付のグリッド
            cash_trade: 取引履歴

        Returns:
            df_cash_trade_days: 取引履歴を含んだグリッド
        """
        _file_path = self.config["cashtrade"]["file_path"]
        # データを読み込み、取引日をstr型に変換する
        _cash_trade = self.__rwc.read_csv_raw(_file_path, self.target_ym).with_columns(
            pl.col("取引日").cast(pl.Utf8).alias("取引日")
        )

        # グリッドデータを作成する
        self.closs_joined_date = (
            self.df_date.select(pl.col("date").alias("取引日")).join(
                _cash_trade,
                on=["取引日"],
                how="left",
            )
            # 当日の取引履歴であるため、直前の値でnullは埋める
            .with_columns(pl.col("現在（貸付）高").fill_null(strategy="forward"))
        )
        self._closs_joined_date_fillnull = (
            self.closs_joined_date.with_columns(
                [
                    pl.col("払出金額（円）").fill_null(0),
                    pl.col("受入金額（円）").fill_null(0),
                ]
            )
            # 直前の取引履歴がない場合は、(直後の残高) + (直前の払出金額) - (直前の受入金額)
            .with_columns(
                [
                    pl.col("現在（貸付）高").shift(-1).alias("次_現在（貸付）高"),
                    pl.col("払出金額（円）").shift(-1).alias("次_払出金額（円）"),
                    pl.col("受入金額（円）").shift(-1).alias("次_受入金額（円）"),
                ]
            )
            .with_columns(
                pl.when(pl.col("現在（貸付）高").is_null())
                .then(
                    pl.col("次_現在（貸付）高")
                    + pl.col("次_払出金額（円）")
                    + pl.col("次_受入金額（円）")
                )
                .otherwise(pl.col("現在（貸付）高"))
                .alias("残高")
            )
            .with_columns(
                pl.when(pl.col("残高").is_null())
                .then(pl.col("残高").drop_nulls().first())
                .otherwise(pl.col("残高"))
            )
            # 収支の計算
            .with_columns(
                (pl.col("受入金額（円）") - pl.col("払出金額（円）")).alias("収支")
            )
        )

    # csvファイルとして保存する
    def save_trade_data(self) -> None:
        """
        1ヶ月分の取引履歴から必要なカラムを抽出し、tmpファイルとして保存する
        Args:
            self._closs_joined_date_fillnull: 残高の穴埋めを行ったデータ
        """

        _save_path = self.config["tmp"]["file_path"]
        self._save_data = self._closs_joined_date_fillnull.select(
            pl.col("取引日"),
            pl.col("受入金額（円）").alias("収入"),
            pl.col("払出金額（円）").alias("支出"),
            pl.col("詳細１").alias("種類"),
            pl.col("詳細２").alias("対象"),
            pl.col("残高"),
            pl.col("収支"),
        )
        self.__rwc.write_csv(_save_path, self.target_ym, self._save_data)
