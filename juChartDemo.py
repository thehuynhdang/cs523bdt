#!/usr/bin/env python
# coding: utf-8

from urllib.request import urlopen
import json
import happybase
import pandas as pd
import plotly.express as px
import matplotlib.pyplot as plt
from matplotlib.pyplot import figure
import datetime
import plotly.graph_objs as go

connection = happybase.Connection(host='localhost', port=9090, autoconnect=True)
table = connection.table('USCovid19CasesByDate')

cases = pd.DataFrame(columns=['Cases', 'Deaths', 'Date', 'State'])
for key, value in table.scan(row_prefix=b''):
    nbrCases = int.from_bytes(value[b'NbrStatic:nbrCases'], byteorder='big') 
    nbrDeaths = int.from_bytes(value[b'NbrStatic:nbrDeaths'], byteorder='big')
    dtTracked = value[b'StateInfo:dtTracked'].decode('utf-8')
    state = value[b'StateInfo:state'].decode('utf-8')
    cases = cases.append({'Cases':nbrCases,'Deaths':nbrDeaths,'Date':dtTracked,'State':state},ignore_index=True)


state_cases = cases[['State', 'Cases', 'Date','Deaths']].groupby(['State','Date'])['Cases', 'Deaths'].max().reset_index();
state_cases["Date"] = "US"

fig = px.treemap(state_cases, path=['Date','State'], values='Cases',
                  color='Deaths', hover_data=['State'],
                  color_continuous_scale='matter')
fig.show()

#chart2
f = plt.figure(figsize=(15,10))
f.add_subplot(111)

plt.axes(axisbelow=True)
plt.barh(cases.groupby(["State"]).sum().sort_values('Cases')["Cases"].index[-10:],
cases.groupby(["State"]).sum().sort_values('Cases')["Cases"].values[-10:],color="cyan")
plt.tick_params(size=3,labelsize = 13)
plt.xlabel("Confirmed Cases",fontsize=18)
plt.title("Top 10 Counties: USA (Confirmed Cases)",fontsize=20)
plt.grid(True)
plt.savefig('Top 10 Counties_USA (Confirmed Cases).png')


