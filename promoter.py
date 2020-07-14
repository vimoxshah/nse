import json
import os
import datetime
import sys

import pandas as pd
import requests


# from_date = "13-06-2020"
# to_date = "13-07-2020"
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.162 Safari/537.36'}
path = "/Users/vimoxshah/Documents/promoters"
today_date = datetime.datetime.now().strftime('%d-%m-%Y')
today_path = f"{path}/{today_date}"


def download_overall_csv(from_date, to_date):
    url = f"https://www.nseindia.com/api/corporates-pit?index=equities&from_date={from_date}&to_date={to_date}&csv=true"
    print(f"Downloading overall data csv {url}")
    response = requests.request("GET", url, headers=headers)

    os.makedirs(f"{today_path}", exist_ok=True)
    with open(f"{path}/{today_date}/CF-Insider-Trading-equities-{from_date}-{to_date}.csv", 'wb') as f:
        f.write(response.content)


def get_json_data(from_date, to_date):
    url = f"https://www.nseindia.com/api/corporates-pit?index=equities&from_date={from_date}&to_date={to_date}&json=true"

    response = requests.request("GET", url, headers=headers)
    kaam_ka_data = list()
    if response.status_code == 200:
        required_data = json.loads(response.content)
        if required_data:
            data = required_data.get("data")

            for d in data:
                if d.get("personCategory") in ["Promoter Group", "Promoters"] and d.get("acqMode") == "Market Purchase":
                    obj = {"symbol": d.get("symbol"), "company": d.get("company"), "secVal": d.get("secVal")}
                    kaam_ka_data.append(obj)

    return kaam_ka_data


def consolidate_data(data):
    require_data=dict()
    sorted_data = list()
    for d in data:
        stock = d.get("symbol")
        if stock not in require_data:
            new_value = int(d.get("secVal"))
        else:
            stored_sec_value = require_data.get(stock)
            new_value= stored_sec_value + int(d.get("secVal"))

        require_data.update({stock: new_value})
    sorted_value = sorted(require_data.items(), key=lambda kv: int(kv[1]), reverse=True)

    for tuple in sorted_value:
        sorted_data.append({"symbol": tuple[0], "secVal": int(tuple[1])})
    return sorted_data


def pick_gt(data, threshold_value=10000000):
    final_data = list()
    for obj in data:
        if int(obj.get("secVal")) > threshold_value:
            final_data.append({"symbol": obj.get("symbol"), "secVal": obj.get("secVal")})

    return final_data


def download_csv_stock(final_data,from_date, to_date):
    for obj in final_data:
        symbol = obj.get("symbol")
        if symbol:
            url = f"https://www.nseindia.com/api/corporates-pit?index=equities&from_date={from_date}&to_date={to_date}&csv=true&symbol={symbol}"
            print(f"Fetching stock data csv {url}")
            response = requests.request("GET", url, headers=headers)

            os.makedirs(f"{path}/{today_date}", exist_ok=True)
            with open(f"{path}/{today_date}/{symbol}-{from_date}-{to_date}.csv", 'wb') as f:
                f.write(response.content)


def analyse_stock_data(final_data,from_date, to_date):
    symbol_avg_val = list()
    for obj in final_data:
        symbol = obj.get("symbol")
        if symbol:
            secAcq = 0
            secVal = 0
            promoter_pledging = 0.0
            promoter_shareholding = 0.0
            url = f"https://www.nseindia.com/api/corporates-pit?index=equities&from_date={from_date}&to_date={to_date}&symbol={symbol}"
            print(f"Fetching stock data from {url}")
            response = requests.request("GET", url, headers=headers)
            if response.status_code == 200:
                res_data = json.loads(response.content)
                if res_data:
                    data = res_data.get("data")
                    # all_buy = all([d.get("acqMode") == "Market Purchase" for d in data])
                    # if all_buy:
                    for d in data:
                        if d.get("personCategory") in ["Promoter Group", "Promoters"] and d.get("acqMode") == "Market Purchase":
                            secAcq+=int(d.get("secAcq"))
                            secVal+=int(d.get("secVal"))

            corp_info = f"https://www.nseindia.com/api/quote-equity?symbol={symbol}&section=corp_info"
            print(f"Fetching coporate info: {corp_info}")
            corp_response = requests.request("GET", corp_info, headers=headers)
            if corp_response.status_code == 200:
                corp_response_data = json.loads(corp_response.content)
                corporate = corp_response_data.get("corporate")
                if corporate:
                    shareholding_pattern = corporate.get('shareholdingPatterns')
                    pledgedetails = corporate.get('pledgedetails')
                    if shareholding_pattern.get("data"):
                        data = shareholding_pattern.get("data")
                        cols = shareholding_pattern.get("cols")
                        for d in data:
                            if d.get("name") in "Promoter & Promoter Group":
                                recent_date = cols[0]
                                promoter_shareholding = d.get(recent_date)
                    if pledgedetails:
                        promoter_pledging = pledgedetails[0].get("per3")
            if secVal > 0 and secAcq:
                symbol_avg_val.append({"symbol": symbol, "secAvgVal": round(secVal/secAcq, 2), "promoter_shareholding": promoter_shareholding,
                                       "promoter_pledging": promoter_pledging})
    return symbol_avg_val


def final_selection(avg_value_data):
    temp_list = list()
    for d in avg_value_data:
        if float(d.get("promoter_pledging")) <= 0:
            temp_list.append(d)

    final_data = sorted(temp_list, key=lambda x: int(float(x.get("promoter_shareholding"))), reverse=True)

    return final_data


def build_sheet(writer, data, sheet_name, columns=None):
    df = pd.read_json(json.dumps(data))
    worksheet = df_to_excel(df, writer,
                            sheet_name=sheet_name,columns=columns)
    set_col_width(worksheet)


def df_to_excel(dataframe, writer, sheet_name,columns):
    dataframe.to_excel(writer, sheet_name=sheet_name,
                       index=False,columns=columns)
    return writer.sheets[sheet_name]


def set_col_width(worksheet):
    for col in worksheet.columns:
        max_length = 0
        column = col[0].column_letter
        for cell in col:
            # for empty cell handling require try-except
            try:
                if len(str(cell.value)) > max_length:
                    max_length = len(cell.value)
            except:
                pass
        adjusted_width = (max_length + 2) * 1.1
        worksheet.column_dimensions[column].width = adjusted_width


from_date = None

def main(from_date, to_date):
    download_overall_csv(from_date, to_date)
    file_name = f"{today_date}.xls"
    writer = pd.ExcelWriter(path=f"{today_path}/{file_name}", engine="openpyxl")
    kaam_ka_data = get_json_data(from_date, to_date)
    build_sheet(writer, kaam_ka_data, "overall_filtered_data", columns=["company","symbol","secVal"])
    sorted_data = consolidate_data(kaam_ka_data)
    build_sheet(writer, sorted_data, "sorted_data", columns=["symbol","secVal"])
    final_data = pick_gt(sorted_data)
    build_sheet(writer, final_data, "selected_stocks",columns=["symbol","secVal"])
    download_csv_stock(final_data,from_date, to_date)
    symbol_avg_val = analyse_stock_data(final_data,from_date, to_date)
    build_sheet(writer, symbol_avg_val, "avg_value",columns=["symbol","secAvgVal", "promoter_shareholding", "promoter_pledging"])
    final_stocks = final_selection(symbol_avg_val)
    build_sheet(writer, final_stocks, "final_stocks",
                columns=["symbol", "secAvgVal", "promoter_shareholding", "promoter_pledging"])

    writer.save()
    writer.close()


if __name__ == "__main__":
    from_date = sys.argv[1]
    to_date = sys.argv[2]
    main(from_date, to_date)