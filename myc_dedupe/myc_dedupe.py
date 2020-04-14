import civis
import pandas as pd
import os
from civis.io import read_civis_sql
import pickle
import os.path
import logging
import sys

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

STATE = os.getenv('STATE')
GOOGLE_OAUTH_CREDS = os.getenv('GOOGLE_OAUTH_CREDS')

### Commenting these out so folks can populate with their own values or load as environment variables if need be
### SPREADSHEET_ID = ''
### COMMITTEE_ID = ''

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
RANGE = '!A:H'

SQL = '''

    with dupes as (

            select
            *,
            md5(lower(first_name) || lower(last_name) || date_of_birth::varchar || state_code::varchar) as dupe_key
            from phoenix_demssanders20_vansync.person_records_myc
            where date_part(y, datetime_created) in (2019,2020)
            and myc_van_id is not null
            and (voter_type_id::integer > 5 or voter_type_id::integer is null)
            and person_committee_id = ''' + COMMITTEE_ID + '''
            and dupe_key
            in (
                select
                dupe_key
                from (
                    select
                    md5(lower(first_name) || lower(last_name) || date_of_birth::varchar|| state_code::varchar) as dupe_key,
                    count(*) as record_count
                    from phoenix_demssanders20_vansync.person_records_myc
                    where date_part(y, datetime_created) in (2019,2020)
                    and myc_van_id is not null
                    and (voter_type_id::integer > 5 or voter_type_id::integer is null)
                    and person_committee_id =  ''' + COMMITTEE_ID + '''
                    and state_code = \'''' + STATE + '''\'
                    group by 1
                    having record_count > 1

                )
            )

        )


    select
    *
    from (
    	select
    	dupe_key,
    	max(case when quality_rank = 1 then myc_van_id else null end) over (partition by dupe_key) as good_mycampaign_id,
    	max(case when quality_rank > 1 then myc_van_id else null end) over (partition by dupe_key, quality_rank) as bad_mycampaign_ids,
    	first_name as first_name,
    	last_name as last_name,
    	state_code as state_code,
    	date_of_birth as date_of_birth,
    	max(case when quality_rank = 1 then van_precinct_id else null end) over (partition by dupe_key) as van_precinct_id
    	from (
    			select
    			*,
    			row_number() over (partition by dupe_key order by info_completeness desc) as quality_rank
    			from (
    				select
    				myc_van_id,
    				dupe_key,
    				count(state_code)
    				+ count(myv_van_id)
    				+ count(first_name)
    				+ count(middle_name)
    				+ count(last_name)
    				+ count(suffix)
    				+ count(us_cong_district)
    				+ count(state_house_district)
    				+ count(state_senate_district)
    				+ count(van_precinct_id)
    				+ count(bad_voting_address)
    				+ count(contacts_address_id)
    				+ count(phone_id)
    				+ count(date_of_birth)
    				+ count(sex)
    				+ count(datetime_created)
    				+ count(email_id)
    				+ count(person_committee_id)
    				+ count(voter_type_id)
    				+ count(datetime_modified)
    				+ count(created_by_user_id)
    				+ count(is_deceased)
    				+ count(employer_name)
    				+ count(occupation_name) as info_completeness,
    				max(first_name) as first_name,
    				max(last_name) as last_name,
    				max(state_code) as state_code,
    				max(date_of_birth) as date_of_birth,
    				max(van_precinct_id) as van_precinct_id
    				from dupes
    				where myc_van_id is not null
    				group by 1, 2
    				)
    	    )
    	)
    	where bad_mycampaign_ids is not null
'''

def service():

    # Copy-paste from https://developers.google.com/sheets/api/quickstart/python
    """Shows basic usage of the Sheets API.
    Prints values from a sample spreadsheet.
    """

    print('Running OAuth + instantiating Google Sheets Service object...',file=sys.stdout)
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()
    return sheet

def create_sheet(sheet):

    current_sheets = [wks['properties']['title'] for wks in sheet.get(spreadsheetId=SPREADSHEET_ID).execute()['sheets']]
    is_new = False

    if STATE not in current_sheets:

        print('Creating new sheet for state %s...',file=sys.stdout)

        batch_update_spreadsheet_request_body = {
        'requests': [
            {
                "addSheet":{
                    "properties": {
                        "title": STATE
                    }
                }
            }
            ]
        }
        request = sheet.batchUpdate(spreadsheetId=SPREADSHEET_ID, body=batch_update_spreadsheet_request_body)
        response = request.execute()

        is_new = True

    return is_new

def clear_sheet(sheet, is_new):

    if not is_new:

        print('Clearing existing sheet for state {}...'.format(STATE),file=sys.stdout)

        batch_clear_values_request_body = {
            'ranges': [STATE + RANGE]
        }
        request = sheet.values().batchClear(spreadsheetId=SPREADSHEET_ID, body=batch_clear_values_request_body)
        response = request.execute()

def update_sheet(sheet, df):

    print('Updating data for state {}...'.format(STATE),file=sys.stdout)

    range_ = STATE + RANGE

    body = [list(df.columns)]
    for index, row in df.iterrows():
        body.append(list(row.fillna('N/A')))

    value_input_option = 'RAW'
    value_range_body = {
            "majorDimension":"ROWS",
            'values':
                body
    }
    request = sheet.values().update(spreadsheetId = SPREADSHEET_ID, range = range_, valueInputOption = value_input_option, body = value_range_body)
    request.execute()

def dedupe():

    print('Initiating de-duplication script...',file=sys.stdout)
    print('Querying database and finding dupes...',file=sys.stdout)
    dupes = read_civis_sql(SQL,"Bernie 2020",use_pandas=True)
    sheet = service()
    is_new = create_sheet(sheet)
    clear_sheet(sheet, is_new)
    update_sheet(sheet,dupes)
    print('Done!',file=sys.stdout)

if __name__ == "__main__":
    dedupe()
