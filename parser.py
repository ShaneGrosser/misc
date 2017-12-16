import numpy as np
import pandas as pd
import re
import calendar
import datetime

# df = pd.read_excel('G:/GAS & POWER/19. Gas trading/Analysis/LNG/Argus Tender Data/LNG_tender_data.xlsx' ,encoding='utf-8')
df = pd.read_excel('P:/Ad Hoc/LNG_tender_data.xlsx', encoding='utf-8')
df = df.drop_duplicates(subset=['Type', 'Supply', 'First reported'])
# df_countries = pd.read_excel('G:/GAS & POWER/19. Gas trading/Analysis/LNG/Argus Tender Data/country_mapping_fixed.xlsx')
df_countries = pd.read_excel('P:/Ad Hoc/country_mapping_fixed.xlsx')
df = df.drop('Unnamed: 0', axis=1)

"""Mapping"""
month_dict = {'January': 'Jan', 'February': 'Feb', 'March': 'Mar', 'April': 'Apr', 'May': 'May', 'June': 'Jun',
              'July': 'Jul', 'August': 'Aug', 'September': 'Sep', 'October': 'Oct', 'November': 'Nov',
              'December': 'Dec'}
month2num_dict = {name: num for num, name in enumerate(calendar.month_abbr) if num}
countries_dict = {}
nationalities_dict = {}
project_dict = {}
export_dict = {}
for index, row in df_countries.iterrows():
    countries_dict[row['Abb']] = row['Country']
    nationalities_dict[row['Abb']] = row['Nationality']
    project_dict[row['Abb']] = [row['LNG Project 1'], row['LNG Project 2'], row['LNG Project 3'], row['LNG Project 4'],
                                row['LNG Project 5'], row['LNG Project 6'], row['LNG Project 7'], row['LNG Project 8'],
                                row['LNG Project 9'], row['LNG Project 10'], row['LNG Project 11'],
                                row['LNG Project 12'], row['LNG Project 13'], row['LNG Project 14'],
                                row['LNG Project 15'], row['LNG Project 16'], row['LNG Project 17']]
    export_dict[row['Abb']] = row[
        ['LNG Export 1', 'LNG Export 2', 'LNG Export 3', 'LNG Export 4', 'LNG Export 5', 'LNG Export 6', 'LNG Export 7',
         'LNG Export 8', 'LNG Export 9', 'LNG Export 10', 'LNG Export 11', 'LNG Export 12', 'LNG Export 13',
         'LNG Export 14', 'LNG Export 15', 'LNG Export 16', 'LNG Export 17', 'LNG Export 18', 'LNG Export 19',
         'LNG Export 20', 'LNG Export 21', 'LNG Export 22', 'LNG Export 23', 'LNG Export 24', 'LNG Export 25',
         'LNG Export 26', 'LNG Export 27', 'LNG Export 28', 'LNG Export 29', 'LNG Export 30', 'LNG Export 31',
         'LNG Export 32', 'LNG Export 33', 'LNG Export 34', 'LNG Export 35', 'LNG Export 36', 'LNG Export 37',
         'LNG Export 38', 'LNG Export 39', 'LNG Export 40', 'LNG Export 41', 'LNG Export 42', 'LNG Export 43',
         'LNG Export 44', 'LNG Export 45', 'LNG Export 46', 'LNG Export 47', 'LNG Export 48', 'LNG Export 49']]

"""Define patterns to search for"""
pattern_number = r"(\d+)((\s?-\s?)(\d+))?"
pattern_reload = r"re-?(export|load)"
pattern_perm = r"/month"
pattern_pery = r"/y(ea)?r"
pattern_tpery = r"t/y(ea)?r"
pattern_loadingm = r"\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b((.+)\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b)?"
pattern_loadingm_long = r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\b((.+)(January|February|March|April|May|June|July|August|September|October|November|December)\b)?"
pattern_loadingy = r"(\d\d\d\d)((.+)(\d\d\d\d))?"
pattern_cancelled = r"(Cancel|Withdrawn|Declin|Delay|unawarded|not awarded)"

"""Start looping over DataFrame"""
for index, row in df.iterrows():

    """ Define strings to work on """
    string_supply = str(row['Supply'].encode('utf-8'))
    string_loading = str(row['Loading period'])
    string_comments = str(row['Comments'])

    """Cargoe/Volume matching functions"""
    number_match = re.search(pattern_number, string_supply, re.I)
    reload_match = re.search(pattern_reload, string_supply, re.I)
    cancelled_match = re.search(pattern_cancelled, string_comments, re.I)
    perm_match = re.search(pattern_perm, string_supply, re.I)
    pery_match = re.search(pattern_pery, string_supply, re.I)
    tpery_match = re.search(pattern_tpery, string_supply, re.I)

    if perm_match:
        df.loc[index, 'PerM'] = 1
    if pery_match:
        df.loc[index, 'PerY'] = 1
    if tpery_match:
        df.loc[index, 'TPerM'] = 1

    if number_match:
        df.loc[index, 'Cargoes_1'] = number_match.group(1)
        df.loc[index, 'Cargoes_2'] = number_match.group(4)
    if reload_match:
        df.loc[index, 'Reload'] = 1
    if cancelled_match:
        df.loc[index, 'Cancelled'] = 1

    """Loading period functions"""
    loadingm_match = re.search(pattern_loadingm, string_loading, re.I)
    loadingm_match_long = re.search(pattern_loadingm_long, string_loading, re.I)
    loadingy_match = re.search(pattern_loadingy, string_loading, re.I)

    if loadingm_match:
        df.loc[index, 'Loading_month_1'] = loadingm_match.group(1)
        df.loc[index, 'Loading_month_1_index'] = loadingm_match.start(1)
        df.loc[index, 'Loading_month_2'] = loadingm_match.group(4)

    if loadingm_match_long:
        df.loc[index, 'Loading_month_3'] = month_dict[loadingm_match_long.group(1)]
        df.loc[index, 'Loading_month_3_index'] = loadingm_match_long.start(1)
        if loadingm_match_long.group(4) in calendar.month_name[1:13]:
            df.loc[index, 'Loading_month_4'] = month_dict[loadingm_match_long.group(4)]

    if loadingy_match:
        df.loc[index, 'Loading_year_1'] = loadingy_match.group(1)
        df.loc[index, 'Loading_year_2'] = loadingy_match.group(4)

"""Change type and break up Report Date"""
df.Cargoes_1 = df.Cargoes_1.replace('', ).astype(float)
df.Cargoes_2 = df.Cargoes_2.replace('', ).astype(float)
df['Report_year'] = df['First reported'].apply(lambda x: x.year)
df['Report_month'] = df['First reported'].apply(lambda x: x.month)

"""Loading month to, and from, logic"""
df['Loading_month_from'] = df.apply(
    lambda row: row['Loading_month_1'] if pd.isnull(row['Loading_month_3']) else row['Loading_month_1'] if row[
                                                                                                               'Loading_month_1_index'] <
                                                                                                           row[
                                                                                                               'Loading_month_3_index'] else
    row['Loading_month_3'], axis=1)
df['Loading_month_to'] = df.apply(
    lambda row: row['Loading_month_1'] if row['Loading_month_1_index'] > row['Loading_month_3_index'] else row[
        'Loading_month_3'] if row['Loading_month_1_index'] < row['Loading_month_3_index'] else row[
        'Loading_month_2'] if pd.isnull(row['Loading_month_4']) else row['Loading_month_4'], axis=1)
df['Loading_month_from'] = df.apply(lambda row: month2num_dict[row['Loading_month_from']] if row[
                                                                                                 'Loading_month_from'] in calendar.month_abbr else 'NaN',
                                    axis=1)
df['Loading_month_to'] = df.apply(
    lambda row: month2num_dict[row['Loading_month_to']] if row['Loading_month_to'] in calendar.month_abbr else 'NaN',
    axis=1)

"""Cargoe logic"""
df['Cargoes_final'] = df.apply(
    lambda row: row['Cargoes_1'] if pd.isnull(row['Cargoes_2']) else (row['Cargoes_1'] + row['Cargoes_2']) / 2.0,
    axis=1)

"""Clean"""
df = df.drop(['Loading_month_1', 'Loading_month_2', 'Loading_month_3', 'Loading_month_4', 'Cargoes_1', 'Cargoes_2'],
             axis=1)
df['Loading_year_2'] = df['Loading_year_2'].fillna(value=np.nan)

"""Change type"""
df.Loading_month_to = df.Loading_month_to.replace('', ).astype(float)
df.Loading_month_from = df.Loading_month_from.replace('', ).astype(float)

"""Loop to find implied loading year"""
for index, row in df.iterrows():
    """Implied year from report date"""
    if row['Report_month'] <= row['Loading_month_from']:
        df.loc[index, 'Loading_year_3'] = row['Report_year']
    else:
        df.loc[index, 'Loading_year_3'] = row['Report_year'] + 1
    if row['Report_month'] <= row['Loading_month_to']:
        df.loc[index, 'Loading_year_4'] = row['Report_year']
    else:
        df.loc[index, 'Loading_year_4'] = row['Report_year'] + 1

"""Loading year to, and from, logic"""
df['Loading_year_from'] = df.apply(
    lambda row: row['Loading_year_3'] if pd.isnull(row['Loading_year_1']) else row['Loading_year_1'], axis=1)
df['Loading_month_to'] = df.apply(
    lambda row: 1 if (pd.isnull(row['Loading_month_to']) and not pd.isnull(row['Loading_year_2'])) else row[
        'Loading_month_to'], axis=1)
df['Loading_year_to'] = df.apply(
    lambda row: row['Loading_year_4'] if pd.isnull(row['Loading_year_2']) else row['Loading_year_2'], axis=1)

"""Change type and clean"""
df.Loading_year_to = df.Loading_year_to.replace('', ).astype(float)
df.Loading_year_from = df.Loading_year_from.replace('', ).astype(float)
df = df.drop(['Loading_year_1', 'Loading_year_2', 'Loading_year_3', 'Loading_year_4', 'Report_year', 'Report_month'],
             axis=1)

df['Loading_year_to'] = df.apply(
    lambda row: row['Loading_year_from'] if row['Loading_year_to'] < row['Loading_year_from'] else row[
        'Loading_year_to'], axis=1)
df['Loading_day'] = '1'
df['Loading_month_to'] = df.apply(
    lambda row: 1 if (pd.isnull(row['Loading_month_to']) and pd.isnull(row['Loading_month_from'])) else row[
        'Loading_month_to'], axis=1)
df['Loading_month_from'] = df.apply(
    lambda row: 1 if pd.isnull(row['Loading_month_from']) else row['Loading_month_from'], axis=1)

"""Change type"""
df['Loading_month_from'] = df['Loading_month_from'].astype(str)
df['Loading_year_from'] = df['Loading_year_from'].astype(str)
df['Loading_month_to'] = df['Loading_month_to'].astype(str)
df['Loading_year_to'] = df['Loading_year_to'].astype(str)
df['Comments'] = df['Comments'].astype(str)

"""Form date logic"""
df['Date_from'] = pd.to_datetime(df['Loading_year_from'] + df['Loading_month_from'] + df['Loading_day'],
                                 errors='coerce')
df['Date_to'] = pd.to_datetime(df['Loading_year_to'] + df['Loading_month_to'] + df['Loading_day'], errors='coerce')

"""Drop redundant columns"""
df = df.drop(['Loading_month_from', 'Loading_year_from', 'Loading_month_to', 'Loading_year_to', 'Loading_day'], axis=1)

"""Extract countries from Supply field"""


def is_word_in_string(dict, string):
    for k, v in dict.items():
        if v in string:
            return k


def is_list_in_string(dict, string):
    for k, v in dict.items():
        for value in v:
            if str(value) in string:
                return k


for index, row in df.iterrows():
    df.loc[index, 'Country_1'] = is_word_in_string(countries_dict, row['Supply'])
    df.loc[index, 'Country_2'] = is_word_in_string(nationalities_dict, row['Supply'])
    df.loc[index, 'Country_3'] = is_list_in_string(project_dict, row['Supply'])
    df.loc[index, 'Country_4'] = is_list_in_string(export_dict, row['Supply'])

df['Country'] = df.apply(
    lambda row: row['Country_1'] if not pd.isnull(row['Country_1']) else row['Country_2'] if not pd.isnull(
        row['Country_2']) else row['Country_3'] if not pd.isnull(row['Country_3']) else row['Country_4'], axis=1)

"""Extract countries from Comments field"""
for index, row in df.iterrows():
    df.loc[index, 'Country2_1'] = is_word_in_string(countries_dict, row['Comments'])
    df.loc[index, 'Country2_2'] = is_word_in_string(nationalities_dict, row['Comments'])

df['Country2'] = df.apply(lambda row: row['Country2_1'] if not pd.isnull(row['Country2_1']) else row['Country2_2'],
                          axis=1)
df['Country2'] = df.apply(lambda row: "" if row['Country2'] == row['Country'] else row['Country2'], axis=1)

df['Cargoes_final'] = df.apply(lambda row: 0 if row['TPerM'] == 1 else row['Cargoes_final'], axis=1)
df['Cargoes_final'] = df.apply(
    lambda row: row['Cargoes_final'] * row['PerM'] if row['PerM'] == 1 else row['Cargoes_final'], axis=1)
df['Cargoes_final'] = df.apply(
    lambda row: row['Cargoes_final'] * row['PerY'] if row['PerY'] == 1 else row['Cargoes_final'], axis=1)

"""Deal with per month/year"""
df['Cargoes_final'] = df.apply(lambda row: (((row['Date_to'] - row['Date_from']) / 30).days) * row['Cargoes_final'] if (
        (not pd.isnull(row['Date_to'])) and row['PerM'] == 1) else row['Cargoes_final'], axis=1)
df['Cargoes_final'] = df.apply(
    lambda row: (((row['Date_to'] - row['Date_from']) / 365).days) * row['Cargoes_final'] if (
            (not pd.isnull(row['Date_to'])) and row['PerY'] == 1) else row['Cargoes_final'], axis=1)
df = df.drop(['TPerM', 'PerM', 'PerY'], axis=1)

df = df.drop(['Country_1', 'Country_2', 'Country_3', 'Country_4', 'Country2_1', 'Country2_2'], axis=1)

df_new = df.loc[:, ['Date_from', 'Date_to', 'Cargoes_final', 'Country', 'Country2', 'Reload', 'Cancelled', 'Type']]

df_new = df_new[df_new['Reload'] != 1]

df_new.to_excel('P:/Spyder/Parsing/Output.xlsx')
