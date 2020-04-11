#!/usr/bin/env python
# -*- coding: utf-8 -*-

# built-in
import itertools
import operator
import sys
from datetime import datetime
from datetime import timedelta
import json

# not built-in
import pandas as pd
from scipy.optimize import curve_fit
import numpy as np

import pycountry
import iso3166


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

    print('reading', url_confirmed, file=sys.stderr)
    df_confirmed = pd.read_csv(url_confirmed)
    print('reading', url_deaths, file=sys.stderr)
    df_deaths = pd.read_csv(url_deaths)
    print('reading', url_recovered, file=sys.stderr)
    df_recovered = pd.read_csv(url_recovered)

    print('reading', url_stringency, file=sys.stderr)
    df_stringency = pd.read_csv(url_stringency).fillna(method='ffill').fillna(0)

    # # Only include data older than today.
    intDateToday = int(datetime.today().strftime('%Y%m%d'))
    # df_stringency = df_stringency[
    #     df_stringency['Date'] < intDateToday]

    # # Find most recent common date across countries.
    intDateCommon = df_stringency.groupby('CountryName').tail(1)['Date'].min()
    # df_stringency = df_stringency[
    #     df_stringency['Date'] <= intDateCommon]

    # Exclude data newer than today or newer than most recent common date across countries.
    df_stringency = df_stringency[
        df_stringency['Date'] <= max(intDateToday - 1, intDateCommon)]

    print('calculating iERPScoreB', file=sys.stderr)
    calculate_iERPScoreB(df_stringency)

    d_name2alpha = prepare_country_dict(df_stringency)
    d_alpha2name = {}
    for country in df_confirmed['Country/Region'].unique():
        # Skip cruise line ships.
        if country in ('Diamond Princess', 'MS Zaandam'):
            continue
        d_alpha2name[d_name2alpha[country]] = country

    # Check completeness and correctness of country dictionary.
    set1 = set(df_confirmed['Country/Region'].values)
    set2 = set(d_name2alpha.keys())
    set3 = set(['MS Zaandam', 'Diamond Princess'])  # Cruise ships
    _ = set1 - set2 - set3
    assert len(_) == 0, _

    df_merged = merge_data_frames(
        df_confirmed, df_deaths, df_recovered)

    print('creating json files for each country', file=sys.stderr)
    do_json_across_countries(
        df_merged,
        df_stringency,
        d_alpha2name,
        )

    print('creating json file across countries', file=sys.stderr, end='\n\n')
    for country in df_confirmed['Country/Region'].unique():
        # Skip cruise line ships.
        if country in ('Diamond Princess', 'MS Zaandam'):
            continue
        alpha3 = d_name2alpha[country]
        if alpha3 not in df_stringency['CountryCode'].unique():
            continue
        print('country', country, file=sys.stderr)
        do_json_per_country(
            country,
            alpha3,
            df_stringency[df_stringency['CountryCode'] == alpha3],
            )
        bool_figure = True
        if bool_figure is True:
            s = df_stringency[df_stringency['CountryCode'] == alpha3]['iERPScoreB']
            ax = s.plot.line()
            fig = ax.get_figure()
            fig.savefig('{}.png'.format(alpha3))
            fig.clf()

    print('\nall done - happy days', file=sys.stderr)

    return


def do_json_across_countries(df_merged, df_stringency, d_alpha2name):

    # print(df_stringency)

    d = {}

    top5 = df_stringency.groupby('CountryCode').tail(1).set_index('CountryCode')['iERPScoreB'].nlargest(5)
    d['topCountriesImpacted'] = dict(top5)

    d['topCountriesByGDP'] = {}
    d['map'] = {}

    for CountryCode in df_stringency['CountryCode'].unique():

        # Skip Aruba (Netherlands), Bermuda (UK), Hong Kong (China), Lesotho, Macau (China), Puerto Rico (US)
        if CountryCode not in d_alpha2name.keys():
            continue

        df = df_stringency[df_stringency['CountryCode'] == CountryCode][['Date', 'iERPScoreB']].tail(8)

        if CountryCode in ('USA', 'CHN', 'JPN', 'DEU', 'IND'):
            _ = float(df.head(1)['iERPScoreB']) - float(df.tail(1)['iERPScoreB'])
            if _ > 0:
                icon = 'FallOutlined'
                color = '#444'
            elif _ < 0:
                icon = 'RiseOutlined'
                color = '#000'
            else:
                icon = 'MinusOutlined'
                color = '#000'
            d['topCountriesByGDP'][CountryCode] = {
                'iERPScoreB': float(df.tail(1)['iERPScoreB']),
                'icon': icon,
                'color': color,
                }

        countryName = d_alpha2name[CountryCode]
        d['map'][CountryCode] = {
            'iERPScoreB': round(float(df.tail(1)['iERPScoreB'], 3)),
            'cases': int(df_merged[countryName, 'confirmed'].tail(1)),
            'deaths': int(df_merged[countryName, 'deaths'].tail(1)),
            'recoveries': int(df_merged[countryName, 'recovered'].tail(1)),
            }

    with open('homepage-data.json', 'w') as f:
        json.dump(d, f, indent=4)

    return


def logistic(x, a, b, c):

    # a is maximum
    # b is steepness/inclination
    # c is midpoint

    y = a / (1 + np.exp(-b * (x - c)))

    return y


def predict_logistic(values, dates):

    dateLatest = datetime.strptime(str(
        dates.tail(1).iat[0]), '%Y%m%d')
    valueLatest = values.tail(1).iat[0]
    yield valueLatest, dateLatest.strftime('%Y-%m-%d')

    # Guess seeding values.
    guessMaximum = values.tail(1).iat[0] * 2
    guessSteepness = 0.25
    guessMidpoint = len(values)
    p0 = [guessMaximum, guessSteepness, guessMidpoint]

    try:
        popt, pcov = curve_fit(
            logistic,
            range(len(values)),
            values,
            p0=p0,
            )
        perr = np.sqrt(np.diag(pcov))
        fitMaximum, fitSteepness, fitMidpoint = popt
    except RuntimeError:
        fitMaximum = 0

    # Revert to linear fit, if fit to logistic function fails.
    if fitMaximum < valueLatest:
        for valuePredicted, dateISO in predict_linear(values, dates):
            yield valuePredicted, dateISO
        return

    # Get fitted values 3 weeks into the future.
    for days in range(1, 21 + 1):
        dateISO = operator.add(
            dateLatest,
            timedelta(days=days),
            ).strftime('%Y-%m-%d')
        valuePredicted = logistic(len(values) + days, *popt)
        yield valuePredicted, dateISO

    return


def predict_linear(cases, dates):
    s = pd.Series(cases)

    change = s.pct_change(periods=3).tail(1).iat[0]
    lastValue = cases.tail(1).iat[0]
    lastDate = dates.tail(1).iat[0]
    reduction = change / 100 * 3

    predictions = []

    if (lastValue < 30):
        for x in range(0, 21):
            dateISO = operator.add(
                datetime.strptime(str(lastDate), '%Y%m%d'),
                timedelta(days=x),
                ).strftime('%Y-%m-%d')
            predictions.append([lastValue, dateISO])
            lastDate = lastDate + 1

    else:
        for x in range(0, 21):
            dateISO = operator.add(
                datetime.strptime(str(lastDate), '%Y%m%d'),
                timedelta(days=x),
                ).strftime('%Y-%m-%d')
            predictions.append([lastValue, dateISO])

            change = change - reduction
            lastValue = lastValue + (lastValue * change)

    return predictions


def append_predictions(dateLast, value, deltaDays1, deltaDays2):

    predictions = []

    for days in range(deltaDays1, deltaDays2 + 1):
        dateISO = operator.add(
            dateLast,
            timedelta(days=days),
            ).strftime('%Y-%m-%d')
        predictions.append([value, dateISO])

    return predictions


def predict_scores(scores, dates):

    # https://github.com/iERP-ai/businesswithcovid-generator/issues/5

    dateLast = datetime.strptime(str(
        dates.tail(1).iat[0]), '%Y%m%d')
    valueLast = scores.tail(1).iat[0]

    predictions = [[valueLast, dateLast.strftime('%Y-%m-%d')]]

    scoresConsecutive, daysConsecutive = zip(*(
        (k, len(list(g))) for k, g in itertools.groupby(scores)))

    assert scoresConsecutive[-1] == valueLast

    val1 = scoresConsecutive[-1]
    val2 = scoresConsecutive[-2]
    len1 = daysConsecutive[-1]
    len2 = daysConsecutive[-2]

    # 1. if today's business score is less than 3 ->
    # today+1 - today+21 = will be equal current business score
    if val1 < 3:
        predictions += append_predictions(dateLast, val1, 1, 21)

    # 2. if "val1" is more than 8 and "len1" is higher than 14 ->
    # today+1 - today+4 = today's business score
    elif val1 > 8 and len1 > 14:
        predictions += append_predictions(dateLast, val1, 1, 4)
        # if "val2" is less than 7 -> today+5 - today+21 = val2
        if val2 < 7:
            predictions += append_predictions(dateLast, val2, 5, 21)
        # if "val2" is more or equal 7 -> today+5 - today+21 = (val2 - 1)
        else:
            predictions += append_predictions(dateLast, val2 - 1, 5, 21)

    # 3. if "val1" < "val2" and "len1" is higher than 5 days ->
    # today+1 - today+4 = today's business score
    # today+5 - today+21 = val1*0.6
    elif val1 < val2 and len1 > 5:
        predictions += append_predictions(dateLast, val1, 1, 4)
        predictions += append_predictions(dateLast, val1 * 0.6, 5, 21)

    # 4. if "val1" is more than 8 and "len1" is lower than 14 ->
    # today+1 - today+(14-"len1") = today's business score
    elif val1 > 8 and len1 <= 14:
        predictions += append_predictions(dateLast, val1, 1, 14 - len1)
        # if "val2" is less than 7 -> today+(14-"len1") - today+21 = val2
        if val2 < 7:
            assert len1 < 14  # Make Jozef aware it can throw an assertion error!
            predictions += append_predictions(dateLast, val2, 14 - len1 + 1, 21)
        # if "val2" is more or equal 7 -> today+(14-"len1") - today+21 = (val2 - 1)
        else:
            predictions += append_predictions(dateLast, val2 - 1, 14 - len1 + 1, 21)

    # 5. if "val1" > "val2" and val1 is between 3 and 5 ->
    # today+1 - today+4 = today's business score
    # today+5 - today+21 = 8
    elif val1 > val2 and 3 <= val1 <= 5:
        predictions += append_predictions(dateLast, val1, 1, 4)
        predictions += append_predictions(dateLast, 8, 5, 21)

    # 6. if "val1" > "val2" and val1 is between 5 and 8 ->
    # today+1 - today+4 = today's business score
    # today+5 - today+21 = 8.2
    elif val1 > val2 and 5 <= val1 <= 8:
        predictions += append_predictions(dateLast, val1, 1, 4)
        predictions += append_predictions(dateLast, 8.2, 5, 21)

    # 7.
    elif val1 < val2 and len1 <= 5:
        predictions += append_predictions(dateLast, val1, 1, 4)
        predictions += append_predictions(dateLast, val1 * 0.8, 5, 21)

    # This should not happen.
    else:
        print(list(scores))
        print(val1, val2)
        print(len1, len2)
        exit()

    # Check that a prediction is not made for the same date twice.
    assert len(predictions) == len(set(list(zip(*predictions))[1])), predictions
    # Check that predictions are for the latest date and the next 3 weeks.
    assert len(predictions) == 1 + 21, predictions

    return predictions


def do_json_per_country(country, alpha3, df_stringency):

    # https://github.com/iERP-ai/businesswithcovid-generator/issues/1

    d = {}
    d['limitations'] = []

    for column in ('S1', 'S2', 'S3', 'S4', 'S5', 'S6', 'S7', 'S12', 'S13'):
        value = float(df_stringency[column + 'raw'].tail(1))
        # print(country, column, value)
        name, value, icon, colorB, colorT = limitations_translation(column, value)
        d['limitations'].append({
            'name': name,
            'value': value,
            'icon': icon,
            'colorB': colorB,
            'colorT': colorT,
            })

    d['scores'] = {'iERPScoreBNow': round(float(df_stringency['iERPScoreB'].tail(1)), 3)}

    d['graphs'] = {
        'iERPScoreB': {'history': [], 'forecast': []},
        'cases': {'history': [], 'forecast': []},
        'deaths': {'history': [], 'forecast': []},
        }

    dateLatest = df_stringency['Date'].tail(1).iat[0]
    for i, (scorePredicted, dateISO) in enumerate(
        predict_scores(
            df_stringency['iERPScoreB'],
            df_stringency['Date'],
            )):
        d['graphs']['iERPScoreB']['forecast'].append(
            {'d': dateISO, 'iERPScoreB': round(scorePredicted, 3)})
        if i in (7, 14, 21):
            d['scores']['iERPScoreBDays{}'.format(i)] = round(scorePredicted, 3)

    for k1, k2 in (('ConfirmedCases', 'cases'), ('ConfirmedDeaths', 'deaths')):
        for valuePredicted, dateISO in predict_logistic(
            df_stringency[k1],
            df_stringency['Date'],
            ):
            d['graphs'][k2]['forecast'].append(
                {'d': dateISO, k2: int(valuePredicted)})

    for Date, iERPScoreB, cases, deaths in zip(
        df_stringency['Date'],
        df_stringency['iERPScoreB'],
        df_stringency['ConfirmedCases'],
        df_stringency['ConfirmedDeaths'],
        ):
        dateISO = datetime.strptime(str(Date), '%Y%m%d').strftime('%Y-%m-%d')
        d['graphs']['iERPScoreB']['history'].append(
            {'d': dateISO, 'iERPScoreB': round(iERPScoreB, 3)})
        d['graphs']['cases']['history'].append(
            {'d': dateISO, 'cases': cases})
        d['graphs']['deaths']['history'].append(
            {'d': dateISO, 'deaths': deaths})

    with open('country-data-{}.json'.format(alpha3), 'w') as f:
        json.dump(d, f, indent=4)

    return


def limitations_translation(column, value):

    # placeholder: https://github.com/iERP-ai/businesswithcovid-generator/issues/1#issue-597577465
    # Wait for further instructions.

    # It's safer (rounding errors) to work with integers than floats,
    # if they are to be used as dictionary keys.
    # It would be better to use a string for each case,
    # if there is a likelihood the hard coded scores will be adjusted in the future.
    value = int(10 * value)

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

    d_values = {
        'S1': {
            100: 'All schools closed in whole country',
            75: 'All schools recommended to be closed in whole country',
            50: 'Schools closed in some regions',
            25: 'Schools recommended to be closed in some regions',
            0: 'All schools open',
            },
        'S2': {
            100: 'Workplaces require closing in whole country',
            75: 'Workplaces recommended to be closed in whole country',
            50: 'Workplaces require closing in some regions',
            25: 'Workplaces recommended to be closed in some regions',
            0: 'No restrictions on workplaces closing',
            },
        'S3': {
            100: 'Public events cancelled in whole country',
            75: 'Public events recommended to be cancelled in whole country',
            50: 'Public events cancelled in some regions',
            25: 'Public events recommended to be cancelled in some regions',
            0: 'No restrictions on Public events',
            },
        'S4': {
            100: 'Public transportation closed in whole country',
            75: 'Public transportation recommended to be closed in whole country',
            50: 'Public transportation closed in some regions',
            25: 'Public transportation recommended to be closed in some regions',
            0: 'No restrictions on Public transportation',
            },
        'S5': {
            100: 'COVID-19 public information campaign launched in whole country',
            50: 'COVID-19 public information campaign launched in some regions',
            0: 'No COVID-19 public information campaign launched',
            },
        'S6': {
            100: 'Restricted public movement in whole country',
            75: 'Restricted public movement recommended in whole country',
            50: 'Restricted public movement in some regions',
            25: 'Restricted public movement recommended in some regions',
            0: 'No restrictions on public movement',
            },
        'S7': {
            100: 'Travel ban on high-risk regions',
            70: 'Quarantine on travel from high-risk regions',
            30: 'Screening on travel from high-risk regions',
            0: 'No travel controls',
            },
        'S12': {
            100: 'No testing policy on COVID-19',
            70: 'Selective testing on COVID-19',
            40: 'Testing of anyone showing COVID-19 symptoms',
            20: 'Open public testing available',
            },
        'S13': {
            100: 'No contact tracing of COVID-19 infected individuals',
            60: 'Limited contact tracing of COVID-19 infected individuals',
            20: 'Comprehensive contact tracing of COVID-19 infected individuals for all cases',
            },
        }

    d_icons = {
        'S1': 'ExperimentOutlined',
        'S2': 'ContactsOutlined',
        'S3': 'TeamOutlined',
        'S4': 'SendOutlined',
        'S5': 'UsergroupAddOutlined',
        'S6': 'CarOutlined',
        'S7': 'CarOutlined',
        'S12': 'BugOutlined',
        'S13': 'ContactsOutlined',
        }

    name = d_names[column]
    value = d_values[column][value]
    icon = d_icons[column]
    colorB = '#fff'
    colorT = '#000'

    return name, value, icon, colorB, colorT


def merge_data_frames(df_confirmed, df_deaths, df_recovered):

    df_confirmed['key'] = 'confirmed'
    df_deaths['key'] = 'deaths'
    df_recovered['key'] = 'recovered'

    # Sum across countries with overseas territories such as:
    # UK, Denmark, Netherlands, France

    # Do merge, grouping, sum and transpose.
    df = pd.concat([
        df_confirmed.drop(['Lat', 'Long'], axis=1),
        df_deaths.drop(['Lat', 'Long'], axis=1),
        df_recovered.drop(['Lat', 'Long'], axis=1),
        ]).groupby(['Country/Region', 'key']).sum().transpose()

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
    # d_name2alpha["Cote d'Ivoire"] = d_name2alpha["Côte d'Ivoire"]
    d_name2alpha["Cote d'Ivoire"] = 'CIV'
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
        df[s + 'raw'] = df.apply(
            func,
            axis=1,  # apply function to each row
            args=args,
            )
        df[s + 'weighted'] = weights[s] * df[s + 'raw']

    df['iERPScoreB'] = df[[_ + 'weighted' for _ in weights.keys()]].sum(axis=1) / 10

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
