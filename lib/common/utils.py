import pandas as pd


def get_country_dataframes():
    countries_df = pd.read_csv('datasets/CountryCodes.csv')
    countries_df = countries_df[['Country','alpha3']]
    countries_df.set_index('Country', inplace=True)
    return countries_df


def get_country_df_alpha3():
    countries_df = pd.read_csv('datasets/CountryCodes.csv')
    countries_df = countries_df[['Country','alpha3']]
    countries_df.set_index('alpha3', inplace=True)
    return countries_df