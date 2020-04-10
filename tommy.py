import pandas as pd
import pycountry
import iso3166
import json
from datetime import datetime

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

    calculate_iERPScoreB(df_stringency)

    d_name2alpha = prepare_country_dict(df_stringency)

    # Check completeness and correctness of country dictionary.
    set1 = set(df_confirmed['Country/Region'].values)
    set2 = set(d_name2alpha.keys())
    set3 = set(['MS Zaandam', 'Diamond Princess'])  # Cruise ships
    _ = set1 - set2 - set3
    assert len(_) == 0, _

    # for country in df_confirmed['Country/Region'].unique():
    for country in df_stringency['CountryName'].unique():
        # df_merged = merge_data_frames(
        #     country, df_confirmed, df_deaths, df_recovered)
        alpha3 = d_name2alpha[country]
        do_json(
            country,
            alpha3,
            # df_merged,
            df_stringency[df_stringency['CountryCode'] == alpha3],
            )

    return


def do_json(country, alpha3, df_stringency):

    d = {}
    d['limitations'] = []

    for column in ('S1', 'S2', 'S3', 'S4', 'S5', 'S6', 'S7', 'S12', 'S13'):
        print(country, column)
        value = float(df_stringency[column].tail(1) / 10)
        name, value, icon, colorB, colorT = limitations_translation(column, value)
        d['limitations'].append({
            'name': name,
            'value': value,
            'icon': icon,
            'colorB': colorB,
            'colorT': colorT,
            })

    d['scores'] = {'iERPScoreBNow': float(df_stringency['iERPScoreB'].tail(1))}

    d['graphs'] = {
        'iERPScoreB': {'history': []},
        'cases': {'history': []},
        'deaths': {'history': []},
        }
    for Date, iERPScoreB, cases, deaths in zip(
        df_stringency['Date'],
        df_stringency['iERPScoreB'],
        df_stringency['ConfirmedCases'],
        df_stringency['ConfirmedDeaths'],
        ):
        dateISO = datetime.strptime(str(Date), '%Y%m%d').strftime('%Y-%m-%d')
        d['graphs']['iERPScoreB']['history'].append(
            {'d': dateISO, 'iERPScoreB': iERPScoreB})
        d['graphs']['cases']['history'].append(
            {'d': dateISO, 'cases': deaths})
        d['graphs']['deaths']['history'].append(
            {'d': dateISO, 'deaths': deaths})

    with open('country-data-{}.txt'.format(alpha3), 'w') as f:
        json.dump(d, f, indent=4)

    return


def limitations_translation(column, value):

    # placeholder: https://github.com/iERP-ai/businesswithcovid-generator/issues/1#issue-597577465
    # Wait for further instructions.

    d_names = {
        'S1': 'School closing',
        'S2': 'Workplace closing',
        'S3': 'Cancel public events',
        'S4': 'Close public transport',
        'S5': 'Public information campaigns',
        'S6': 'Restrictions on internal movement',
        'S7': 'International travel controls',
        'S12': 'Testing framework',
        'S13': 'Contact tracing',
    }

    name = d_names[column]
    value = value
    icon = 'RocketOutlined'
    colorB = '#fff'
    colorT = '#000'

    return name, value, icon, colorB, colorT


def merge_data_frames(country, df_confirmed, df_deaths, df_recovered):

    df_confirmed_sum = df_confirmed.groupby('Country/Region').sum().reset_index()
    df_deaths_sum = df_deaths.groupby('Country/Region').sum().reset_index()
    df_recovered_sum = df_recovered.groupby('Country/Region').sum().reset_index()
    df = pd.concat([
        df_confirmed_sum[df_confirmed_sum['Country/Region'] == country].drop(['Lat', 'Long', 'Country/Region'], axis=1).transpose(),
        df_deaths_sum[df_deaths_sum['Country/Region'] == country].drop(['Lat', 'Long', 'Country/Region'], axis=1).transpose(),
        df_recovered_sum[df_recovered_sum['Country/Region'] == country].drop(['Lat', 'Long', 'Country/Region'], axis=1).transpose(),
        ], axis=1)
    print(df_confirmed)
    print(country)
    df.columns = ['confirmed', 'deaths', 'recovered']

    return df


def prepare_country_dict(df_stringency):

    # Create a dictionary that points from both official and common name to alpha3 code.
    # https://en.wikipedia.org/wiki/ISO_3166-1_alpha-3
    # Use the two modules pycountry and iso3166 to mitigate discrepancies.
    # pycountry = 'Slovak Republic', 'Korea, Republic of', 'Republic of the Congo', ...
    # iso3166 = 'Slovakia', 'Korea, Republic of', 'Congo', 'Kosovo', ...

    # check serbia and france and uk...
    d_name2alpha = {}

    for country in pycountry.countries:
        try:
            d_name2alpha[country.official_name] = country.alpha_3
        except AttributeError:
            pass
        try:
            d_name2alpha[country.common_name] = country.alpha_3
        except AttributeError:
            pass
        d_name2alpha[country.name] = country.alpha_3

    for country in iso3166.countries:
        if 'Congo' in country.name:
            print(country)
        d_name2alpha[country.name] = country.alpha3
        d_name2alpha[country.apolitical_name] = country.alpha3

    for CountryCode, CountryName in zip(
        df_stringency['CountryCode'].unique(),
        df_stringency['CountryName'].unique(),
        ):
        d_name2alpha[CountryName] = CountryCode

    # Manually correct for errors in Johns Hopkins dataset; e.g. the country "US" does not exist.
    d_name2alpha['US'] = d_name2alpha['United States']
    d_name2alpha['Korea, South'] = d_name2alpha['South Korea']
    d_name2alpha['Taiwan*'] = d_name2alpha['Taiwan']
    d_name2alpha['Holy See'] = d_name2alpha['Holy See (Vatican City State)']
    d_name2alpha["Cote d'Ivoire"] = d_name2alpha["Côte d'Ivoire"]
    d_name2alpha['Burma'] = d_name2alpha['Myanmar']
    d_name2alpha['West Bank and Gaza'] = d_name2alpha['Palestine, State of']
    d_name2alpha['Congo (Brazzaville)'] = d_name2alpha['Republic of the Congo']
    d_name2alpha['Congo (Kinshasa)'] = d_name2alpha['Congo, The Democratic Republic of the']

    # for country in df_confirmed[~df_confirmed['Province/State'].isnull()]['Country/Region'].unique():
    #     print(df_confirmed[df_confirmed['Country/Region'] == country])

    return d_name2alpha


def calculate_iERPScoreB(df):

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

    for s in weights.keys():
        args = [_ for _ in df.columns if all((
            _.startswith(s + '_'),
            not _.endswith('_Notes'),
            ))]
        func = functions[s]
        df[s] = weights[s] * df.apply(
            func,
            axis=1,  # apply function to each row
            args=args,
            )

    df['iERPScoreB'] = df[weights.keys()].sum(axis=1) / 10

    return df


def factors2values2(series, col1, col2):

    if int(series[col1]) == 1 and int(series[col2]) == 1:
        return 10
    elif int(series[col1]) == 1 and int(series[col2]) == 0:
        return 5
    elif int(series[col1]) == 0 and int(series[col2]) == 0:
        return 0
    elif int(series[col1]) == 0 and int(series[col2]) == 1:
        return 0
    else:
        print(series)
        print('func2')
        print(series[col1])
        print(series[col2])
        exit()


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
    else:
        print(series)
        print('func3')
        print(series[col1])
        print(series[col2])
        exit()


def factors1values4(series, col1):

    if int(series[col1]) == 3:
        return 10
    elif int(series[col1]) == 2:
        return 7
    elif int(series[col1]) == 1:
        return 3
    elif int(series[col1]) == 0:
        return 0
    else:
        print(series)
        print('func4')
        print(series[col1])
        print(series[col2])
        exit()


def func12(series, col1):

    if int(series[col1]) == 3:
        return 2
    elif int(series[col1]) == 2:
        return 4
    elif int(series[col1]) == 1:
        return 7
    elif int(series[col1]) == 0:
        return 10
    else:
        print('func12')
        print(series[col1])
        print(series[col2])
        exit()


def func13(series, col1):

    if int(series[col1]) == 2:
        return 2
    elif int(series[col1]) == 1:
        return 6
    elif int(series[col1]) == 0:
        return 10
    else:
        print('func13')
        print(series[col1])
        print(series[col2])
        exit()


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
