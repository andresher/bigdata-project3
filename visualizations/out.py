import plotly.plotly as py
from plotly.graph_objs import *
import datetime

f = open('../out/keywords.txt', 'r')

sentiment = []
for line in f:
    line = line[:-1]
    line = eval(line)
    sentiment.append((line['timestamp'], line['positive']/(line['positive']+line['negative'])))

f.close()

trace_list = []
x_list = []
y_list = []
for tup in sentiment:
    x_list.append(tup[0])
    y_list.append(tup[1])

trace_list.append(Scatter(
    x=x_list,
    y=y_list,
    name="Percentage of Positive Tweets"
))

data = Data(trace_list)

py.plot(data, filename = 'hastags-trace')
