import pandas as pd

def main():

    url_confirmed = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv'
    url_deaths = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv'
    url_recovered = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv'

    # Downloadable from here:
    # https://www.bsg.ox.ac.uk/research/research-projects/oxford-covid-19-government-response-tracker
    url_stringency = 'https://ocgptweb.azurewebsites.net/CSVDownload'

    (
        url_confirmed,
        url_deaths,
        url_recovered,
        url_stringency,
        ) = tmp_download()

    df_confirmed = pd.read_csv(url_confirmed)

    df_deaths = pd.read_csv(url_deaths)

    df_recovered = pd.read_csv(url_recovered)

    df_stringency = pd.read_csv(url_stringency).fillna(0)

    # print(df_confirmed)
    # print(df_deaths)
    # print(df_recovered)
    # print(df_stringency)

    # S1_School closing
    # S2_Workplace closing
    # S3_Cancel public events
    # S4_Close public transport
    # S5_Public information campaigns
    # S6_Restrictions on internal movement
    # S7_International travel controls
    # S8_Fiscal measures
    # S9_Monetary measures
    # S10_Emergency investment in health care
    # S11_Investment in Vaccines
    # S12_Testing framework
    # S13_Contact tracing

    # df = df_stringency[df_stringency['CountryCode'] == 'KOR'].copy()
    df = df_stringency
    print(df)

    weights = {
        'S1': 1.00,  # S1_School closing
        'S2': 2.50,  # S2_Workplace closing
        'S3': 1.00,  # S3_Cancel public events
        'S4': 0.50,  # S4_Close public transport
        'S5': 0.30,  # S5_Public information campaigns
        'S6': 2.50,  # S6_Restrictions on internal movement
        'S7': 1.00,  # S7_International travel controls
        'S12': 0.50,  # S12_Testing framework
        'S13': 0.70,  # S13_Contact tracing
        }
    functions = {
        'S1': factors2values3,
        'S2': factors2values3,
        'S3': factors2values3,
        'S4': factors2values3,
        'S5': factors2values2,
        'S6': factors2values3,
        'S7': factors1values4,
        'S12': func12,
        'S13': func13,
        }

    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.apply.html
    for s in weights.keys():
        args = [_ for _ in df.columns if _.startswith(s+'_') and not _.endswith('_Notes')]
        func = functions[s]
        print(s, args)
        df[s] = weights[s] * df.apply(
            func,
            axis=1,  # apply function to each row
            # Positional arguments to pass to func in addition to the array/series.
            args = args,
            )
        print(df[s].values)

    df['cbf'] = df[weights.keys()].sum(axis=1) / 10
    print(df[['cbf', 'CountryName', 'Date'] + list(weights.keys())])

    return


def factors2values2(series, col1, col2):

    if int(series[col1]) == 1 and int(series[col2]) == 1:
        return 10
    elif int(series[col1]) == 1 and int(series[col2]) == 0:
        return 5
    elif int(series[col1]) == 0 and int(series[col2]) == 0:
        return 0
    elif int(series[col1]) == 0 and int(series[col2]) == 1:
        return 0


def factors2values3(series, col1, col2):

    if int(series[col1]) == 2 and int(series[col2]) == 1:
        return 10
    elif int(series[col1]) == 2 and int(series[col2]) == 0:
        return 5
    elif int(series[col1]) == 1 and int(series[col2]) == 1:
        return 7.5
    elif int(series[col1]) == 1 and int(series[col2]) == 0:
        return 2.5
    elif int(series[col1]) == 0 and int(series[col2]) == 0:
        return 0
    elif int(series[col1]) == 0 and int(series[col2]) == 1:
        return 0


def factors1values4(series, col1):

    if int(series[col1]) == 3:
        return 10
    elif int(series[col1]) == 2:
        return 7
    elif int(series[col1]) == 1:
        return 3
    elif int(series[col1]) == 0:
        return 0



def func12(series, col1):

    if int(series[col1]) == 3:
        return 2
    elif int(series[col1]) == 2:
        return 4
    elif int(series[col1]) == 1:
        return 7
    elif int(series[col1]) == 0:
        return 10


def func13(series, col1):

    if int(series[col1]) == 2:
        return 2
    elif int(series[col1]) == 1:
        return 6
    elif int(series[col1]) == 0:
        return 10


def tmp_download():

    # temporary function, so I don't have to download the files all the time.

    url_confirmed = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv'
    url_deaths = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv'
    url_recovered = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv'

    url_stringency = 'https://ocgptweb.azurewebsites.net/CSVDownload'

    import requests
    import os
    for url in (
        url_confirmed,
        url_deaths,
        url_recovered,
        url_stringency,
        ):
        path = os.path.basename(url)
        if os.path.exists(path):
            continue
        r = requests.get(url)
        with open(path, 'wb') as f:
            f.write(r.content)

    url_confirmed = os.path.basename(url_confirmed)
    url_deaths = os.path.basename(url_deaths)
    url_recovered = os.path.basename(url_recovered)
    url_stringency = os.path.basename(url_stringency)

    return (
        url_confirmed,
        url_deaths,
        url_recovered,
        url_stringency,
        )


if __name__ == '__main__':
    main()