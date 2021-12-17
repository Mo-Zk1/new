import json
from datetime import datetime
import csv
import pandas as pd
import requests
import pymysql
import traceback

json_file = open("AMRN_flowchart.txt", "r")
d = json.load(json_file)  # convert json to dict
data = d["chart"]["result"][0]
indicators = data["indicators"]
quote = indicators["quote"]
real_data = quote[0]  # same
real_data = d["chart"]["result"][0]["indicators"]["quote"][0]
json_file.close()


def parseTimestamp(input_result_dic):
    timestamp_ls = input_result_dic["timestamp"]
    calendertime = []

    for ts in timestamp_ls:
        dt = datetime.fromtimestamp(ts)
        calendertime.append(dt.strftime("%Y-%m-%d"))
    return calendertime


def parseTimestampHour(input_result_dic):
    timestamp_ls = input_result_dic["timestamp"]
    calendertime = []

    for ts in timestamp_ls:
        dt = datetime.fromtimestamp(ts)
        calendertime.append(dt.strftime("%Y-%m-%d-%h"))
    return calendertime


def parseTimestampMin(input_result_dic):
    timestamp_ls = input_result_dic["timestamp"]
    calendertime = []

    for ts in timestamp_ls:
        dt = datetime.fromtimestamp(ts)
        calendertime.append(dt.strftime("%Y-%m-%d-%h-%m"))
    return calendertime


calendertime_ls = parseTimestamp(data)
print("length is {}".format(len(calendertime_ls)))
print(calendertime_ls)
rows = []
i = 0
while i < len(calendertime_ls):
    row = [calendertime_ls[i], real_data["open"][i], real_data["high"][i], real_data["low"][i], real_data["close"][i],
           real_data["volume"][i]]
    rows.append(row)
    i += 1

header = ["date", "open", "high", "low", "close", "volume"]
df = pd.DataFrame(rows, columns=header)
df.to_csv('AMRN.csv', index=False, na_rep='Unknown')


def parseTimestamp(input_result_dic):
    timestamp_ls = input_result_dic["timestamp"]
    calendertime = []

    for ts in timestamp_ls:
        dt = datetime.fromtimestamp(ts)
        calendertime.append(dt.strftime("%Y-%m-%d"))

    return calendertime


# dic: dict from response.txt
def dic2rows(dic, timeSpan):
    real_data = dic["chart"]["result"][0]["indicators"]["quote"][0]
    time_data = dic["chart"]["result"][0]
    if timeSpan == "1d":
        calendertime_ls = parseTimestamp(time_data)
    elif timeSpan == "1h":
        calendertime_ls = parseTimestampHour(time_data)
    elif timeSpan == "1m":
        calendertime_ls = parseTimestampMin(time_data)
    rows = []
    i = 0
    while i < len(calendertime_ls):
        row = [calendertime_ls[i], real_data["open"][i], real_data["high"][i], real_data["low"][i],
               real_data["close"][i], real_data["volume"][i]]
        rows.append(row)
        i += 1
    header = ["date", "open", "high", "low", "close", "volume"]
    return rows


def response2rows(response_txt, timeSpan):
    data = json.loads(response_txt)  # convert json to dict
    return dic2rows(data, timeSpan)


# begin_date: "05/12/2018"
# range: One of the following is allowed 1d|5d|1mo|3mo|6mo|1y|2y|5y|10y|ytd|max.
# Do not use together with period1 and period2

def get_stock_response_txt(stock_name, stock_range, begin_date, time):
    url = "https://yh-finance.p.rapidapi.com/stock/v2/get-chart"
    datetime_format = datetime.strptime(begin_date, "%d/%m/%Y")
    # convert datetime to posix
    data_posix = int(datetime.timestamp(datetime_format))
    querystring = {"interval": time, "symbol": stock_name, "range": stock_range, "region": "US", "period1": data_posix}
    headers = {
        'x-rapidapi-host': "yh-finance.p.rapidapi.com",
        'x-rapidapi-key': ""
    }
    response = requests.request("GET", url, headers=headers, params=querystring)
    print(response.text)
    return response.text


def get_all_stocks(stock_name, stock_range, begin_date, time):
    for name in stock_name:
        get_stock_response_txt(name, stock_range, begin_date, time)


def insert_data2mysql(host, user, password, database, rows, table):
    mydb = pymysql.connect(
        host=host,
        user=user,
        password=password,
        db=database,
    )
    try:
        mycursor = mydb.cursor()
        sql = "insert into  " + table + " values (%s, %s, %s, %s, %s, %s);"
        for row in rows:
            val = (row[0], row[1], row[2], row[3], row[4], row[5])
            mycursor.execute(sql, val)
        mydb.commit()
        mydb.close()
        print(len(rows), "record inserted.")
    except:
        mydb.close()
        print("failed")
        traceback.print_exc()


def API2Sql(stock_name, stock_range, begin_date, table, timeSpan):
    text = get_stock_response_txt(stock_name, stock_range, begin_date, timeSpan)
    rows = response2rows(text, timeSpan)
    host = ""
    user = ""
    password = ""
    database = ""
    insert_data2mysql(host, user, password, database, rows, table)


API2Sql("^GSPC", "1y", "05/11/2018", "SP500_stock_1day", "1d")
