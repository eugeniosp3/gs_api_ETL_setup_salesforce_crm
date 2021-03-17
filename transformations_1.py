import pandas as pd
import ast
import numpy as np

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
                   'MPH', 'C.G.M.A', 'LLP', 'CPA', 'EdD', 'DD', 'Md', 'Rn', 'Phd', 'Jd']
    salutations = ['Mr', 'MR', 'Ms', 'Mrs', 'Miss', 'Prof', 'Dr', 'DR', 'Senator', 'SENATOR', 'MISS', 'MRS', 'MS',
                   'PROF', 'CONGRESSMAN', 'Congressman',
                   'Congresswoman', 'CONGRESSWOMAN', 'Professor', 'PROFESSOR']
    try:
        if letter == 'c':
            credentialing = []
            for cred in credentials:
                if cred in x:
                    credentialing.append(cred)
            credentialing = str(credentialing).replace('[', '').replace(']', '').replace('.', '').capitalize()

            return credentialing
        if letter == 's':
            for sal in salutations:
                if sal in x:
                    return sal

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
    main_df['LASTNAME'] = seed_df['data.operations.leader_name'].apply(lambda x: process_leader_name('l', x)).replace(
        np.nan, '')
    main_df['FIRSTNAME'] = seed_df['data.operations.leader_name'].apply(lambda x: process_leader_name('f', x)).replace(
        np.nan, '')
    main_df['SALUTATION'] = seed_df['data.operations.leader_name'].apply(lambda x: process_leader_name('s', x)).replace(
        np.nan, '')
    main_df['MIDDLELETTER'] = seed_df['data.operations.leader_name'].apply(
        lambda x: process_leader_name('m', x)).replace(np.nan, '')
    main_df['SUFFIX'] = seed_df['data.operations.leader_name'].apply(lambda x: process_leader_name('su', x)).replace(
        np.nan, '')
    main_df['CREDENTIALS'] = seed_df['data.operations.leader_name'].apply(
        lambda x: process_leader_name('c', x)).replace(np.nan, '')

    def apply_roles(x):
        # adds the job title for the leadership
        x = str(x)
        roles = ['Executive Director', 'Executive', 'Board Chair', 'Board Member', 'CEO', 'President', 'Chairperson']
        for role in roles:
            if role in x:
                return role

    main_df['ROLE'] = seed_df['data.operations.leader_profile'].apply(lambda x: apply_roles(x)).replace(np.nan,
                                                                                                            'Unavailable')


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
    df = df[df['Da_code'] == 200]
    main_df['Da_EIN'] = df['data.summary.ein']
    main_df['Da_EIN2'] = df['data.summary.ein'].replace(np.nan, '')
    main_df['Da_CHARITYNAME'] = df['data.summary.organization_name'].replace(np.nan, '')
    main_df['Da_MISSION'] = df['data.summary.mission'].replace(np.nan, '')
    main_df['Da_WEBSITEURL'] = df['data.summary.website_url'].replace(np.nan, '')
    main_df['Da_GENERALEMAIL'] = df['data.operations.organization_email'].replace(np.nan, '')
    main_df['Da_IRSCLASSIFICATION'] = df['data.summary.ntee_code'].replace(np.nan, '')
    main_df['Da_IRSLATEST990'] = df['data.financials.most_recent_year_financials.fiscal_year'].replace(np.nan, '')
    main_df['Da_IRSSUBSECTION'] = df['data.summary.subsection_description'].replace(np.nan, '')
    main_df['Da_TOTALREVENUE'] = df['data.financials.most_recent_year_financials.total_revenue'].replace(np.nan, '')
    leadership_ids_columns(df, main_df)
    main_df['Da_EXTERNALVODID'] = create_ext_vod(main_df)
    main_df['Da_MAILINGCITY'] = df['data.summary.city']
    main_df['Da_MAILINGCOUNTRY'] = 'United States'
    main_df['Da_ZIPCODE'] = df['data.summary.zip'].replace(np.nan, '').astype('str')
    main_df['Da_STATE'] = df['data.summary.state'].replace(np.nan, '')
    main_df['Da_STREETADDRESS'] = df['data.summary.address_line_1'].replace(np.nan, '')
    main_df['Da_STREETADDRESSLINE2'] = df['data.summary.address_line_2'].replace(np.nan, '')
    main_df['Da_EIN#'] = df['data.summary.ein'].replace(np.nan, '')
    main_df['Da_EIN'] = df['data.summary.ein'].astype(str).str.replace('-', '')
    main_df['Da_PHONENUMBER'] = df['data.summary.telephone_numbers'].apply(lambda x: get_dictionary_phone(x)).replace(
        np.nan, 'Unavailable')

def create_load_files():
    """ makes the csv files required for the load of data into platform"""
    organization_account = pd.DataFrame()
    organization_account['Da_ext_ID_Vod_Da_'] = main_df['EIN']
    organization_account['Da_Name_Da_'] = main_df['CHARITYNAME']
    organization_account['Da_Mission_Da_'] = main_df['MISSION']
    organization_account['Da_Website_Da_'] = main_df['WEBSITEURL']
    organization_account['Da_Phone_Da_'] = main_df['PHONENUMBER']
    organization_account['Da_Email_Da_'] = main_df['GENERALEMAIL']
    organization_account['Da_Organization_Type_Da_'] = main_df['IRSCLASSIFICATION']
    organization_account['Da_Latest990_Da_'] = main_df['IRSLATEST990']
    organization_account['Da_KRM_Legal_Entity_Da_'] = main_df['IRSSUBSECTION']
    organization_account['Da_Total_Revenue_000_Da_'] = main_df['TOTALREVENUE']
    organization_account['Da_KRM_CHANGE_STATUS_Da_'] = ''
    organization_account['Da_KRM_CHANGE_DATE_Da_'] = ''
    organization_account.to_csv('Da_ORGANIZATION_ACCOUNT.csv')

    organization_leadership_account = pd.DataFrame()
    organization_leadership_account['Da_LastNAME'] = main_df['LASTNAME']
    organization_leadership_account['Da_First_NAME'] = main_df['FIRSTNAME']
    organization_leadership_account['Da_SALUTATION'] = main_df['SALUTATION']
    organization_leadership_account['Da_EXTERNAL_ID_Da_'] = main_df['EXTERNALVODID']
    organization_leadership_account['Da_MIDDLE_Da_'] = main_df['MIDDLELETTER']
    organization_leadership_account['Da_SUFFIX_Da_'] = main_df['SUFFIX']
    organization_leadership_account['Da_KOL_CREDENTIALS_Da_'] = main_df['CREDENTIALS']
    organization_leadership_account.to_csv('Da_ORGANIZATION_LEADERSHIP_ACCOUNT.csv')

    address = pd.DataFrame()
    address['Da_City_Da_'] = main_df['MAILINGCITY']
    address['Da_Country_Da_'] = main_df['MAILINGCOUNTRY']
    address['Da_Zip_Da_'] = main_df['ZIPCODE']
    address['Da_State_Da_'] = main_df['STATE']
    address['Da_Name'] = main_df['STREETADDRESS']
    address['Da_Address_line_2_Da_'] = main_df['STREETADDRESSLINE2']
    address['Da_External_Id_Da_'] = main_df['EIN2']
    address['Da_EIN'] = main_df['EIN']
    address['Da_Account_Da_'] = ''
    address.to_csv('Da_ADDRESS.csv')

    affiliation = pd.DataFrame()
    affiliation['Da_Leader_Name'] = main_df['EXTERNALVODID']
    affiliation['Da_FROM_ACCOUNT_Da_'] = ''
    affiliation['Da_ROLE_Da_'] = main_df['ROLE']
    affiliation['Da_EIN'] = main_df['EIN']
    affiliation['Da_TO_ACCOUNT_Da_'] = ''
    affiliation.to_csv('Da_AFFILIATION.csv')






