from logging import getLogger

import dash
import plotly.graph_objects as go
import polars as pl
import yaml
from dash import dcc, html
from dash.dependencies import Input, Output

from src.infrastructure.read_write_csv_cash_account import \
    ReadWriteCsvCashAcount


class DashBoard:
    def __init__(self, config_path: str, target_ym: str):
        self.logger = getLogger(self.__class__.__name__)
        self.external_stylesheets = ["https://codepen.io/chriddyp/pen/bWLwgP.css"]
        self.__app = dash.Dash(__name__, external_stylesheets=self.external_stylesheets)
        self._server = self.__app.server
        self.__rwc = ReadWriteCsvCashAcount()
        self.target_ym = target_ym
        # configファイルの読み込み
        with open(config_path) as _f:
            self.config = yaml.load(_f, Loader=yaml.FullLoader)
        # データの読み込み
        self._pl_df = self.__rwc.read_csv(
            self.config["tmp"]["file_path"], self.target_ym
        )

    def run_all(self):
        self.data()  # 図の作成
        self.createdash()  # ダッシュボードの作成
        self.__app.run_server(debug=True)  # サーバーを起動

    def update_graph(self, tragde_types):
        if tragde_types is None or tragde_types == []:
            return None
        title = ""
        self.fig = go.Figure()
        for trade_type in tragde_types:
            self.fig.add_trace(
                go.Scatter(
                    x=self._pl_df_balance["取引日"],
                    y=self._pl_df_balance[trade_type],
                    name=trade_type,
                )
            )
            title += trade_type + ", "
        title = title[:-2]
        self.fig.update_layout(title=title + "の残高推移", showlegend=True)
        self.fig.update_layout(title="残高の推移")
        self.fig.update_layout(
            xaxis=dict(title="取引日", tickangle=45),
            yaxis=dict(
                tickprefix="¥",  # 円の記号を追加する場合
                ticksuffix="万",  # 万円単位を追加
            ),
        )
        return dcc.Graph(figure=self.fig)

    def data(self):
        """
        ダッシュボードで使用するデータの作成を行う
        """
        self._pl_df_balance = (
            self._pl_df.group_by(pl.col("取引日"))
            .agg(
                [
                    pl.col("残高").last().alias("残高"),
                    pl.col("収入").sum().alias("収入"),
                    pl.col("支出").sum().alias("支出"),
                    pl.col("収支").sum().alias("収支"),
                ]
            )
            .sort(pl.col("取引日"))
            .select(
                pl.col("取引日").cast(pl.Utf8),
                (pl.col("残高") / 10000).alias("残高"),
                (pl.col("収入") / 10000).alias("収入"),
                -(pl.col("支出") / 10000).alias("支出"),
                (pl.col("収支") / 10000).alias("収支"),
            )
            .with_columns(
                [
                    pl.col("収入").cumsum().alias("累計収入"),
                    pl.col("支出").cumsum().alias("累計支出"),
                    pl.col("収支").cumsum().alias("累計収支"),
                ]
            )
        ).to_pandas()

    def createdash(self):
        """
        ダッシュボードを作成するクラス
        """
        self.__app.layout = html.Div(
            [
                html.H1("Python Dash"),
                html.H2("Jupyter Notebook"),
                html.P("ここではDashで口座管理ダッシュボードを作成します."),
                dcc.Dropdown(
                    id="stock_chart_dropdown",
                    options=[
                        {"label": "残高", "value": "残高"},
                        {"label": "収入", "value": "収入"},
                        {"label": "支出", "value": "支出"},
                        {"label": "収支", "value": "収支"},
                        {"label": "累計収入", "value": "累計収入"},
                        {"label": "累計支出", "value": "累計支出"},
                        {"label": "累計収支", "value": "累計収支"},
                    ],
                    multi=True,
                    style={"width": "50%"},
                ),
                html.Div(id="stock_chart"),
            ],
            style={"margin-left": "10%", "margin-right": "10%"},
        )

        self.__app.callback(
            Output(component_id="stock_chart", component_property="children"),
            Input(component_id="stock_chart_dropdown", component_property="value"),
        )(self.update_graph)
