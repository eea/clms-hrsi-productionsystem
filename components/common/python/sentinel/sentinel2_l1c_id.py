from datetime import datetime
from os.path import basename
from re import RegexFlag
import re

class Sentinel2L1cId(object):
    '''
    Contains the different components of a Sentinel-2 L1C product ID.
    See https://sentinel.esa.int/web/sentinel/user-guides/sentinel-2-msi/naming-convention

    E.g., the following filename S2A_MSIL1C_20170105T013442_N0204_R031_T53NMJ_20170105T013443.SAFE
    Identifies a Level-1C product acquired by Sentinel-2A on the 5th of January, 2017 at 1:34:42 AM.
    It was acquired over Tile 53NMJ during Relative Orbit 031, and processed with PDGS Processing Baseline 02.04.

    :param full_id: full product ID
    :param mission: S2A or S2B
    :param instrument: always MSI
    :param level: always L1C
    :param start_time: (datetime) datatake sensing start time
    :param baseline: processing baseline number (e.g. N0204)
    :param orbit: relative orbit number (R001 - R143)
    :param tile: tile number field
    :param discriminator: (datetime) this second date is 15 characters in length, and is used to distinguish
    between different end user products from the same datatake. Depending on the instance, the time in this
    field can be earlier or slightly later than the datatake sensing time.
    '''

    def __init__(self, l1c_path):
        '''
        Constructor from a Sentinel-2 L1C product path in the DIAS.

        :param l1c_path: e.g. /eodata/.../S2A_MSIL1C_20170105T013442_N0204_R031_T53NMJ_20170105T013443.SAFE
        '''

        # Extract filename from full path if necessary
        l1c_basename = basename(l1c_path)

        # Regex and datetime formats
        datetime_regex = r'(\d{4}\d{2}\d{2}T\d{2}\d{2}\d{2})'
        datetime_strptime = '%Y%m%dT%H%M%S'

        # Build and match the regular expression
        # MMM_MSIL1C_YYYYMMDDHHMMSS_Nxxyy_ROOO_Txxxxx_<Product Discriminator>.SAFE
        # Use parenthesis to match the substrings.
        # TODO fix mission parsing (S2C|S2D) migth arrive in the future
        regex = (r''
                 '(S2A|S2B)_' +
                 '(MSI)(L1C)_' +
                 datetime_regex + '_' +
                 r'N(\d{2}\d{2})_' +
                 r'R(\d{3})_' +
                 r'T([A-Z0-9]{5})_' +
                 datetime_regex +
                 '.SAFE'
                 )
        result = re.match(regex, l1c_basename, RegexFlag.IGNORECASE)

        # Check result
        if (not result) or (len(result.groups()) != 8):
            raise Exception(
                'Sentinel-2 product ID: %s\n'
                'Does not match regular expression: %s' % (l1c_basename, regex))

        # Remove '.SAFE' extension
        full_id = l1c_basename[:-len('.SAFE')]
        # Init class parameters
        self.full_id = full_id
        self.mission = result.group(1)
        self.instrument = result.group(2)
        self.level = result.group(3)
        self.start_time = datetime.strptime(result.group(4), datetime_strptime)
        self.baseline = result.group(5)
        self.orbit = result.group(6)
        self.tile = result.group(7)
        self.discriminator = datetime.strptime(result.group(8), datetime_strptime)
