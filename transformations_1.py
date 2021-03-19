import pandas as pd
import requests
from random import randint
import time
import random
import ast
import numpy as np
start = time.time()
import re


df = pd.read_csv('RANTHESE.csv')

def process_leader_name(letter, x):
    """ parses the data for:
    first name,
    last name,
    credentials,
    salutations in the full name strings within the json file"""
    from nameparser import HumanName
    credentials = ['PhD', 'PHD', 'DO', 'MS', 'DNP', 'DMD', 'DDS', 'OD', 'DPM', 'DC', 'PA', 'JD', 'PharmD', 'PHARMD',
                   'RPh', 'RPH', 'BSN', 'RN',
                   'MSN', 'MS', 'MBA', 'MA', 'MFA', 'LPN', 'CRNA', 'CPhT', 'CPHT', 'DPT', 'OT', 'SLP', 'DVM', 'M.D.',
                   'Ph.D', 'CEO', 'FACS', 'FAAN', 'ESQ',
                   'MPH', 'C.G.M.A', 'LLP', 'CPA', 'EdD', 'DD', 'Md', 'Rn', 'Phd', 'Jd', 'IOM', 'CAE']
    salutations = ['Mr', 'MR', 'Ms', 'Mrs', 'Miss', 'Prof', 'Dr', 'DR', 'Senator', 'SENATOR', 'MISS', 'MRS', 'MS',
                   'PROF', 'CONGRESSMAN', 'Congressman',
                   'Congresswoman', 'CONGRESSWOMAN', 'Professor', 'PROFESSOR']
    try:
        if letter == 'c':
            credentialing = []
            for cred in credentials:
                if cred in x and cred not in credentialing:
                    credentialing.append(cred)
            credentialing = str(credentialing).upper().replace('[', '').replace(']', '').replace('.', '')

            return credentialing
        if letter == 's':
            for sal in salutations:
                if sal in x:
                    return sal.capitalize()

        # calls name parser library
        r = HumanName(x)

        last_name = str(r.last).capitalize()
        if letter == 'l':
            return last_name

        first_name = str(r.first).capitalize()
        if letter == 'f':
            return first_name

        middle_letter = r.middle
        if letter == 'm':
            return ""

        suffix = str(r.suffix).capitalize()
        if letter == 'su':
            return suffix

    except:
        pass


def leadership_ids_columns(seed_df, receiving_df):
    # handles creation of columns related to leadership if fields are available in the data
    receiving_df['LASTNAME'] = seed_df['data.operations.leader_name'].apply(lambda x: process_leader_name('l', x)).replace(
        'RMInputNeeded', '')
    receiving_df['FIRSTNAME'] = seed_df['data.operations.leader_name'].apply(lambda x: process_leader_name('f', x)).replace(
        'RMInputNeeded', '')
    receiving_df['SALUTATION'] = seed_df['data.operations.leader_name'].apply(lambda x: process_leader_name('s', x)).replace(
        np.nan, '')
    receiving_df['MIDDLELETTER'] = seed_df['data.operations.leader_name'].apply(
        lambda x: process_leader_name('m', x)).replace(np.nan, '')
    receiving_df['SUFFIX'] = seed_df['data.operations.leader_name'].apply(lambda x: process_leader_name('su', x)).replace(
        np.nan, '')
    receiving_df['CREDENTIALS'] = seed_df['data.operations.leader_name'].str.upper().apply(
        lambda x: process_leader_name('c', x)).replace(np.nan, '').str.replace("'", '').str.replace('.', '')

    def apply_roles(x):
        # adds the job title for the leadership
        x = str(x)
        roles = ['Executive Director', 'Executive', 'Board Chair', 'Board Member', 'CEO', 'President', 'Chairperson']
        for role in roles:
            if role in x:
                return role

    receiving_df['ROLE'] = seed_df['data.operations.leader_profile'].apply(lambda x: apply_roles(x)).replace(np.nan,
                                                                                                        'RMInputNeeded')


def create_ext_vod(seed_df):
    # custom field created for matching multiple documents and instances of software pages
    x = seed_df['FIRSTNAME'].astype(str) + ' ' + seed_df['LASTNAME'].astype(str) + '-' + seed_df['EIN'].astype(
        str).str.replace('-', '') + '-' + seed_df['ROLE'].replace(np.nan, 'Unavailable').astype(str)
    return x.str.upper()


def get_dictionary_phone(x):
    # "unwinds" the phone number from within the json file and then edits it to remove the dashes into proper format
    try:
        x_list = ast.literal_eval(x)
        x = [x_dict['telephone_number'] for x_dict in x_list][0].replace('(', '').replace(')', '').replace('-',
                                                                                                           '').replace(
            '.', '')
        x = x[:3] + '-' + x[3:6] + '-' + x[6:]
        if len(x) != 12:
            x = ""
        return x
    except:
        pass


def main_df_factory(df):

    # creates the main dataframe where all data will be stored and prepared for breaking up into individual files
    main_df = pd.DataFrame()
    df = df[df['code'] == 200]
    main_df['EIN'] = df['data.summary.ein']
    main_df['EIN2'] = df['data.summary.ein'].replace(np.nan, '')
    main_df['CHARITYNAME'] = df['data.summary.organization_name'].replace(np.nan, '').str.title()
    main_df['MISSION'] = df['data.summary.mission'].replace(np.nan, '').str.capitalize().apply(lambda x: re.sub(r'[^\x00-\x7f]',r'', x))
    main_df['WEBSITEURL'] = df['data.summary.website_url'].replace(np.nan, '').str.lower()
    main_df['GENERALEMAIL'] = df['data.operations.organization_email'].replace(np.nan, '')
    main_df['IRSCLASSIFICATION'] = df['data.summary.ntee_code'].replace(np.nan, '').apply(lambda x: re.sub(r'[^\x00-\x7f]',r'', x))
    main_df['IRSLATEST990'] = df['data.financials.most_recent_year_financials.fiscal_year'].replace(np.nan, '')
    main_df['IRSSUBSECTION'] = '501(c)(3)'
    main_df['TOTALREVENUE'] = df['data.financials.most_recent_year_financials.total_revenue'].replace(np.nan, '')
    leadership_ids_columns(df, main_df)
    main_df['EXTERNALVODID'] = create_ext_vod(main_df)
    main_df['MAILINGCITY'] = df['data.summary.city'].str.title().str.replace(', Dc', '').str.replace(', Il', '')
    main_df['MAILINGCOUNTRY'] = 'United States'
    main_df['ZIPCODE'] = df['data.summary.zip'].replace(np.nan, '').astype(str)
    main_df['STATE'] = df['data.summary.state'].replace(np.nan, '')
    main_df['STREETADDRESS'] = df['data.summary.address_line_1'].replace(np.nan, '')
    main_df['STREETADDRESSLINE2'] = df['data.summary.address_line_2'].replace(np.nan, '')
    main_df['EIN#'] = df['data.summary.ein'].replace(np.nan, '')
    main_df['EIN'] = df['data.summary.ein'].astype(str).str.replace('-', '')
    main_df['PHONENUMBER'] = df['data.summary.telephone_numbers'].apply(lambda x: get_dictionary_phone(x)).replace(
        np.nan,
        'RMInputNeeded')
    return main_df


def create_load_files(feed_df):
    """ makes the csv files required for the load of data into platform"""
    organization_account = pd.DataFrame()
    organization_account['External_ID_Vod_c'] = feed_df['EIN']
    organization_account['Name'] = feed_df['CHARITYNAME']
    organization_account['Mission_c'] = feed_df['MISSION']
    organization_account['Website'] = feed_df['WEBSITEURL']
    organization_account['POL_KOL_Phone_C'] = feed_df['PHONENUMBER']
    organization_account['POL_KOL_Email_C'] = feed_df['GENERALEMAIL']
    organization_account['Organization_Type_c'] = feed_df['IRSCLASSIFICATION']
    organization_account['Latest990_C'] = feed_df['IRSLATEST990']
    organization_account['KRM_Legal_Entity_c'] = feed_df['IRSSUBSECTION']
    organization_account['Total_Revenue_000_c'] = feed_df['TOTALREVENUE']
    organization_account['KRM_CHANGE_STATUS_C'] = 'Published'
    organization_account['KRM_CHANGE_DATE_C'] = '3/10/2020'
    organization_account['RECORDTYPEID'] = '01203000000CijdAAC'
    organization_account['LASTNAME'] = feed_df['LASTNAME']
    organization_account['FIRSTNAME'] = feed_df['FIRSTNAME']
    organization_account['SALUTATION'] = feed_df['SALUTATION']
    organization_account['EXTERNAL_ID_VOD_C'] = feed_df['EXTERNALVODID']
    organization_account['MIDDLE_VOD_C'] = feed_df['MIDDLELETTER']
    organization_account['SUFFIX_VOD_C'] = ''
    organization_account['KOL_CREDENTIALS_OLR_C'] = feed_df['CREDENTIALS']
    organization_account['RECORDTYPEID'] = '01203000000CijdAAC'
    organization_account['City_vod_c'] = feed_df['MAILINGCITY']
    organization_account['Country_vod_c'] = feed_df['MAILINGCOUNTRY']
    organization_account['Zip_vod_c'] = feed_df['ZIPCODE']
    organization_account['State_vod_c'] = feed_df['STATE']
    organization_account['Name'] = feed_df['STREETADDRESS']
    organization_account['Address_line_2_vod_c'] = feed_df['STREETADDRESSLINE2']
    organization_account['External_Id_Vod_c'] = 'EIN-' + feed_df['EIN'].astype(str)
    organization_account['EIN'] = feed_df['EIN']
    organization_account['Account_Vod_C'] = ''
    organization_account['Leader_Name'] = feed_df['EXTERNALVODID']
    organization_account['FROM_ACCOUNT_VOD_C'] = ''
    organization_account['ROLE_VOD_C'] = feed_df['ROLE']
    organization_account['EIN'] = feed_df['EIN']
    organization_account['TO_ACCOUNT_VOD_C'] = ''
    organization_account.to_csv('organization_account.csv', index=False)

process_df = main_df_factory(df)
write_em = create_load_files(process_df)
end = time.time()
print('Run Time: ', end-start)
