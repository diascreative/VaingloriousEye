"""
Converts zip codes to state codes.  Also converts state codes to state
names.
"""

# From data at:
# http://www.novicksoftware.com/udfofweek/Vol2/T-SQL-UDF-Vol-2-Num-48-udf_Addr_Zip5ToST.htm
def zip_to_state(zip):
    """Convert the zip or postal code to a state code.

    This returns None if it can't be converted"""
    if isinstance(zip, basestring):
        zip = zip.split('-', 1)[0]
        try:
            zip = int(zip, 10)
        except ValueError:
            return None
    if zip >= 99501 and zip <= 99950:
        return 'AK'
    elif zip >= 35004 and zip <= 36925:
        return 'AL'
    elif zip >= 71601 and zip <= 72959:
        return 'AR'
    elif zip >= 75502 and zip <= 75502:
        return 'AR'
    elif zip >= 85001 and zip <= 86556:
        return 'AZ'
    elif zip >= 90001 and zip <= 96162:
        return 'CA'
    elif zip >= 80001 and zip <= 81658:
        return 'CO'
    elif zip >=  6001 and zip <=  6389:
        return 'CT'
    elif zip >=  6401 and zip <=  6928:
        return 'CT'
    elif zip >= 20001 and zip <= 20039:
        return 'DC'
    elif zip >= 20042 and zip <= 20599:
        return 'DC'
    elif zip >= 20799 and zip <= 20799:
        return 'DC'
    elif zip >= 19701 and zip <= 19980:
        return 'DE'
    elif zip >= 32004 and zip <= 34997:
        return 'FL'
    elif zip >= 30001 and zip <= 31999:
        return 'GA'
    elif zip >= 39901 and zip <= 39901:
        return 'GA'
    elif zip >= 96701 and zip <= 96898:
        return 'HI'
    elif zip >= 50001 and zip <= 52809:
        return 'IA'
    elif zip >= 68119 and zip <= 68120:
        return 'IA'
    elif zip >= 83201 and zip <= 83876:
        return 'ID'
    elif zip >= 60001 and zip <= 62999:
        return 'IL'
    elif zip >= 46001 and zip <= 47997:
        return 'IN'
    elif zip >= 66002 and zip <= 67954:
        return 'KS'
    elif zip >= 40003 and zip <= 42788:
        return 'KY'
    elif zip >= 70001 and zip <= 71232:
        return 'LA'
    elif zip >= 71234 and zip <= 71497:
        return 'LA'
    elif zip >=  1001 and zip <=  2791:
        return 'MA'
    elif zip >=  5501 and zip <=  5544:
        return 'MA'
    elif zip >= 20331 and zip <= 20331:
        return 'MD'
    elif zip >= 20335 and zip <= 20797:
        return 'MD'
    elif zip >= 20812 and zip <= 21930:
        return 'MD'
    elif zip >=  3901 and zip <=  4992:
        return 'ME'
    elif zip >= 48001 and zip <= 49971:
        return 'MI'
    elif zip >= 55001 and zip <= 56763:
        return 'MN'
    elif zip >= 63001 and zip <= 65899:
        return 'MO'
    elif zip >= 38601 and zip <= 39776:
        return 'MS'
    elif zip >= 71233 and zip <= 71233:
        return 'MS'
    elif zip >= 59001 and zip <= 59937:
        return 'MT'
    elif zip >= 27006 and zip <= 28909:
        return 'NC'
    elif zip >= 58001 and zip <= 58856:
        return 'ND'
    elif zip >= 68001 and zip <= 68118:
        return 'NE'
    elif zip >= 68122 and zip <= 69367:
        return 'NE'
    elif zip >=  3031 and zip <=  3897:
        return 'NH'
    elif zip >=  7001 and zip <=  8989:
        return 'NJ'
    elif zip >= 87001 and zip <= 88441:
        return 'NM'
    elif zip >= 88901 and zip <= 89883:
        return 'NV'
    elif zip >=  6390 and zip <=  6390:
        return 'NY'
    elif zip >= 10001 and zip <= 14975:
        return 'NY'
    elif zip >= 43001 and zip <= 45999:
        return 'OH'
    elif zip >= 73001 and zip <= 73199:
        return 'OK'
    elif zip >= 73401 and zip <= 74966:
        return 'OK'
    elif zip >= 97001 and zip <= 97920:
        return 'OR'
    elif zip >= 15001 and zip <= 19640:
        return 'PA'
    elif zip >=  2801 and zip <=  2940:
        return 'RI'
    elif zip >= 29001 and zip <= 29948:
        return 'SC'
    elif zip >= 57001 and zip <= 57799:
        return 'SD'
    elif zip >= 37010 and zip <= 38589:
        return 'TN'
    elif zip >= 73301 and zip <= 73301:
        return 'TX'
    elif zip >= 75001 and zip <= 75501:
        return 'TX'
    elif zip >= 75503 and zip <= 79999:
        return 'TX'
    elif zip >= 88510 and zip <= 88589:
        return 'TX'
    elif zip >= 84001 and zip <= 84784:
        return 'UT'
    elif zip >= 20040 and zip <= 20041:
        return 'VA'
    elif zip >= 20040 and zip <= 20167:
        return 'VA'
    elif zip >= 20042 and zip <= 20042:
        return 'VA'
    elif zip >= 22001 and zip <= 24658:
        return 'VA'
    elif zip >=  5001 and zip <=  5495:
        return 'VT'
    elif zip >=  5601 and zip <=  5907:
        return 'VT'
    elif zip >= 98001 and zip <= 99403:
        return 'WA'
    elif zip >= 53001 and zip <= 54990:
        return 'WI'
    elif zip >= 24701 and zip <= 26886:
        return 'WV'
    elif zip >= 82001 and zip <= 83128:
        return 'WY'
    return None

def unabbreviate_state(abbrev):
    """Given a state abbreviation, return the full state name"""
    return abbrev_to_state[abbrev].capitalize()

state_to_abbrev = {
    'ALABAMA': 'AL',
    'ALASKA': 'AK',
    'AMERICAN SAMOA': 'AS',
    'ARIZONA': 'AZ',
    'ARKANSAS': 'AR',
    'CALIFORNIA': 'CA',
    'COLORADO': 'CO',
    'CONNECTICUT': 'CT',
    'DELAWARE': 'DE',
    'DISTRICT OF COLUMBIA': 'DC',
    'FEDERATED STATES OF MICRONESIA': 'FM',
    'FLORIDA': 'FL',
    'GEORGIA': 'GA',
    'GUAM': 'GU',
    'HAWAII': 'HI',
    'IDAHO': 'ID',
    'ILLINOIS': 'IL',
    'INDIANA': 'IN',
    'IOWA': 'IA',
    'KANSAS': 'KS',
    'KENTUCKY': 'KY',
    'LOUISIANA': 'LA',
    'MAINE': 'ME',
    'MARSHALL ISLANDS': 'MH',
    'MARYLAND': 'MD',
    'MASSACHUSETTS': 'MA',
    'MICHIGAN': 'MI',
    'MINNESOTA': 'MN',
    'MISSISSIPPI': 'MS',
    'MISSOURI': 'MO',
    'MONTANA': 'MT',
    'NEBRASKA': 'NE',
    'NEVADA': 'NV',
    'NEW HAMPSHIRE': 'NH',
    'NEW JERSEY': 'NJ',
    'NEW MEXICO': 'NM',
    'NEW YORK': 'NY',
    'NORTH CAROLINA': 'NC',
    'NORTH DAKOTA': 'ND',
    'NORTHERN MARIANA ISLANDS': 'MP',
    'OHIO': 'OH',
    'OKLAHOMA': 'OK',
    'OREGON': 'OR',
    'PALAU': 'PW',
    'PENNSYLVANIA': 'PA',
    'PUERTO RICO': 'PR',
    'RHODE ISLAND': 'RI',
    'SOUTH CAROLINA': 'SC',
    'SOUTH DAKOTA': 'SD',
    'TENNESSEE': 'TN',
    'TEXAS': 'TX',
    'UTAH': 'UT',
    'VERMONT': 'VT',
    'VIRGIN ISLANDS': 'VI',
    'VIRGINIA': 'VA',
    'WASHINGTON': 'WA',
    'WEST VIRGINIA': 'WV',
    'WISCONSIN': 'WI',
    'WYOMING': 'WY',
    }

abbrev_to_state = {}
for name, abbrev in state_to_abbrev.items():
    abbrev_to_state[abbrev] = name
del name, abbrev
