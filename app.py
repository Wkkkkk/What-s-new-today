# Run this app with `python app.py` and
# visit http://127.0.0.1:8050/ in your web browser.

import dash
from dash import dcc, html
from dash.dependencies import Input, Output

import plotly.express as px
import pandas as pd
import argparse
import random
import time
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

from influxdb import DataFrameClient

app = dash.Dash(__name__)

client = DataFrameClient('localhost', 8086)
client.switch_database('pyexample')

def read_from_db():
    result = client.query('select * from twitters where time > now() - 2m')
    if not result:
        return

    df = result['twitters']
    df["annotation"] = df["annotation"].astype(float)
    df["count_sum"] = df["count_sum"].astype(float) * 0.01
    return df


dataframe = pd.DataFrame([], columns=["created_at", "annotation", "count_sum", "source", "text"])
fig = px.scatter(dataframe, x="created_at", y="annotation",
                 size="count_sum", color="source", hover_name="text",
                 size_max=60)

app.layout = html.Div([
        html.H1('What\'s New On Twitter', style = {'textAlign':'center',\
                                            'marginTop':40,'marginBottom':40}),
        dcc.Graph(
            id='graph',
            figure=fig
        ),
        # Hidden div inside the app that stores the intermediate value
        html.Div(id='intermediate-value', style={'display': 'none'}, children=""),
        dcc.Interval(
            id='interval-component',
            interval=2*1000, # 2 seconds
            n_intervals=0
        )
    ])


@app.callback(Output('graph', 'figure'),
              [Input('interval-component', 'n_intervals')])
def update_global_var(n):
    df = read_from_db()

    fig = px.scatter(df, x="created_at", y="annotation",
                     size="count_sum", color="source", hover_name="text",
                     size_max=60)

    return fig


if __name__ == '__main__':
    app.run_server(debug=True)

