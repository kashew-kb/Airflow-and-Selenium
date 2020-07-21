# General Python Pacakages
import os
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

# Airflow Related Packages
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators import PythonOperator
from airflow.operators.selenium_plugin import SeleniumOperator
from selenium_scripts.scrape_trade_yearly_export import get_df as get_df

from elasticsearch import Elasticsearch, TransportError

data_folder = "downloads"


country_dict = {
#     '1': 'AFGHANISTAN TIS',
    # '3': 'ALBANIA',
    # '11': 'ANGOLA',
    # '12': 'ANGUILLA',
    # '14': 'ANTARTICA',
    # '13': 'ANTIGUA',
    # '15': 'ARGENTINA',
    # '16': 'ARMENIA',
    # '20': 'ARUBA',
    # '17': 'AUSTRALIA',
    # '19': 'AUSTRIA',
    # '21': 'AZERBAIJAN',
    # '23': 'BAHAMAS',
    # '25': 'BAHARAIN IS',
    # '27': 'BANGLADESH PR',
    # '29': 'BARBADOS',
    # '55': 'BELARUS',
    # '33': 'BELGIUM',
    # '31': 'BELIZE',
    # '35': 'BENIN',
    # '37': 'BERMUDA',
    # '38': 'BHUTAN',
    # '39': 'BOLIVIA',
    # '40': 'BOSNIA-HRZGOVIN',
    # '41': 'BOTSWANA',
    # '45': 'BR VIRGN IS',
    # '43': 'BRAZIL',
    '47': 'BRUNEI',
    # '49': 'BULGARIA',
    # '50': 'BURKINA FASO',
    # '53': 'BURUNDI',
    # '67': 'C AFRI REP',
    '56': 'CAMBODIA',
    # '57': 'CAMEROON',
    # '59': 'CANADA',
    # '61': 'CANARY IS',
    # '63': 'CAPE VERDE IS',
    # '65': 'CAYMAN IS',
    # '69': 'CHAD',
    # '71': 'CHANNEL IS',
    # '73': 'CHILE',
    # '77': 'CHINA P RP',
    # '79': 'CHRISTMAS IS.',
    # '81': 'COCOS IS',
    # '83': 'COLOMBIA',
    # '85': 'COMOROS',
    # '459': 'CONGO D. REP.',
    # '87': 'CONGO P REP',
    # '89': 'COOK IS',
    # '91': 'COSTA RICA',
    # '199': 'COTE D\' IVOIRE',
    # '92': 'CROATIA',
    # '93': 'CUBA',
    # '276': 'CURACAO',
    # '95': 'CYPRUS',
    # '98': 'CZECH REPUBLIC',
    # '101': 'DENMARK',
    # '102': 'DJIBOUTI',
    # '103': 'DOMINIC REP',
    # '105': 'DOMINICA',
    # '109': 'ECUADOR',
    # '111': 'EGYPT A RP',
    # '113': 'EL SALVADOR',
    # '117': 'EQUTL GUINEA',
    # '116': 'ERITREA',
    # '114': 'ESTONIA',
    # '115': 'ETHIOPIA',
    # '123': 'FALKLAND IS',
    # '121': 'FAROE IS.',
    # '127': 'FIJI IS',
    # '125': 'FINLAND',
    # '131': 'FR GUIANA',
    # '133': 'FR POLYNESIA',
    # '135': 'FR S ANT TR',
    # '129': 'FRANCE',
    # '141': 'GABON',
    # '143': 'GAMBIA',
    # '145': 'GEORGIA',
    # '147': 'GERMANY',
    # '149': 'GHANA',
    # '151': 'GIBRALTAR',
    # '155': 'GREECE',
    # '157': 'GREENLAND',
    # '159': 'GRENADA',
    # '161': 'GUADELOUPE',
    # '163': 'GUAM',
    # '165': 'GUATEMALA',
    # '124': 'GUERNSEY',
    # '167': 'GUINEA',
    # '169': 'GUINEA BISSAU',
    # '171': 'GUYANA',
    # '175': 'HAITI',
    # '176': 'HEARD MACDONALD',
    # '177': 'HONDURAS',
    # '179': 'HONG KONG',
    # '181': 'HUNGARY',
    # '185': 'ICELAND',
    # '187': 'INDONESIA',
    # '189': 'IRAN',
    # '191': 'IRAQ',
    # '193': 'IRELAND',
    # '195': 'ISRAEL',
    # '197': 'ITALY',
    # '203': 'JAMAICA',
    # '205': 'JAPAN',
    # '122': 'JERSEY',
    # '207': 'JORDAN',
    # '212': 'KAZAKHSTAN',
    # '213': 'KENYA',
    # '214': 'KIRIBATI REP',
    # '215': 'KOREA DP RP',
    # '217': 'KOREA RP',
    # '219': 'KUWAIT',
    # '216': 'KYRGHYZSTAN',
    '223': 'LAO PD RP',
    # '224': 'LATVIA',
    # '225': 'LEBANON',
    # '227': 'LESOTHO',
    # '229': 'LIBERIA',
    # '231': 'LIBYA',
    # '233': 'LIECHTENSTEIN',
    # '234': 'LITHUANIA',
    # '235': 'LUXEMBOURG',
    # '239': 'MACAO',
    # '240': 'MACEDONIA',
    # '241': 'MADAGASCAR',
    # '243': 'MALAWI',
    # '245': 'MALAYSIA',
    # '247': 'MALDIVES',
    # '249': 'MALI',
    # '251': 'MALTA',
    # '252': 'MARSHALL ISLAND',
    # '253': 'MARTINIQUE',
    # '255': 'MAURITANIA',
    # '257': 'MAURITIUS',
    # '34': 'MAYOTTE',
    # '259': 'MEXICO',
    # '256': 'MICRONESIA',
    # '260': 'MOLDOVA',
    # '262': 'MONACO',
    # '261': 'MONGOLIA',
    # '356': 'MONTENEGRO',
    # '263': 'MONTSERRAT',
    # '265': 'MOROCCO',
    # '267': 'MOZAMBIQUE',
    '258': 'MYANMAR',
    # '294': 'N. MARIANA IS.',
    # '269': 'NAMIBIA',
    # '271': 'NAURU RP',
    # '273': 'NEPAL',
    # '275': 'NETHERLAND',
    # '277': 'NETHERLANDANTIL',
    # '279': 'NEUTRAL ZONE',
    # '281': 'NEW CALEDONIA',
    # '285': 'NEW ZEALAND',
    # '287': 'NICARAGUA',
    # '289': 'NIGER',
    # '291': 'NIGERIA',
    # '293': 'NIUE IS',
    # '295': 'NORFOLK IS',
    # '297': 'NORWAY',
    # '301': 'OMAN',
    # '307': 'PACIFIC IS',
    # '309': 'PAKISTAN IR',
    # '310': 'PALAU',
    # '313': 'PANAMA C Z',
    # '311': 'PANAMA REPUBLIC',
    # '315': 'PAPUA N GNA',
    # '317': 'PARAGUAY',
    # '319': 'PERU',
    # '323': 'PHILIPPINES',
    # '321': 'PITCAIRN IS.',
    # '325': 'POLAND',
    # '327': 'PORTUGAL',
    # '331': 'PUERTO RICO',
    # '335': 'QATAR',
    # '339': 'REUNION',
    # '343': 'ROMANIA',
    # '344': 'RUSSIA',
    # '345': 'RWANDA',
    # '347': 'SAHARWI A.DM RP',
    # '447': 'SAMOA',
    # '346': 'SAN MARINO',
    # '349': 'SAO TOME',
    # '351': 'SAUDI ARAB',
    # '353': 'SENEGAL',
    # '352': 'SERBIA',
    # '355': 'SEYCHELLES',
    # '357': 'SIERRA LEONE',
    # '359': 'SINGAPORE',
    # '278': 'SINT MAARTEN (DUTCH PART)',
    # '358': 'SLOVAK REP',
    # '360': 'SLOVENIA',
    # '361': 'SOLOMON IS',
    # '363': 'SOMALIA',
    # '365': 'SOUTH AFRICA',
    # '382': 'SOUTH SUDAN ',
    # '367': 'SPAIN',
    # '369': 'SRI LANKA DSR',
    # '371': 'ST HELENA',
    # '373': 'ST KITT N A',
    # '375': 'ST LUCIA',
    # '377': 'ST PIERRE',
    # '379': 'ST VINCENT',
    # '196': 'STATE OF PALEST',
    # '381': 'SUDAN',
    # '383': 'SURINAME',
    # '6': 'SVALLBARD AND J',
    # '385': 'SWAZILAND',
    # '387': 'SWEDEN',
    # '389': 'SWITZERLAND',
    # '391': 'SYRIA',
    # '75': 'TAIWAN',
    # '393': 'TAJIKISTAN',
    # '395': 'TANZANIA REP',
    # '397': 'THAILAND',
    # '329': 'TIMOR LESTE',
    # '399': 'TOGO',
    # '401': 'TOKELAU IS',
    # '403': 'TONGA',
    # '405': 'TRINIDAD',
    # '407': 'TUNISIA',
    # '409': 'TURKEY',
    # '410': 'TURKMENISTAN',
    # '411': 'TURKS C IS',
    # '413': 'TUVALU',
    # '419': 'U ARAB EMTS',
    # '421': 'U K',
    # '423': 'U S A',
    # '417': 'UGANDA',
    # '422': 'UKRAINE',
    # '354': 'UNION OF SERBIA & MONTENEGRO',
    # '599': 'UNSPECIFIED',
    # '427': 'URUGUAY',
    # '424': 'US MINOR OUTLYING ISLANDS               ',
    # '430': 'UZBEKISTAN',
    # '431': 'VANUATU REP',
    # '198': 'VATICAN CITY',
    # '433': 'VENEZUELA',
    # '437': 'VIETNAM SOC REP',
    # '439': 'VIRGIN IS US',
    # '443': 'WALLIS F IS',
    # '453': 'YEMEN REPUBLC',
    # '461': 'ZAMBIA',
    # '463': 'ZIMBABWE',
    }
hslevel_list = ['2','4','6','8']
currency_list = ['RS','USD']


args = {
    'owner': 'AIDatabases',
    'depends_on_past': True,
    'email_on_failure': False,
    'email_on_retry': False,
    # 'email': 'kashish@aidatabases.in',
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
    'provide_context':True
}

def transform_data(**context):
    pkl_filename = "import.pkl"
    path = os.path.join(data_folder, pkl_filename)
 
    if (os.path.exists(path)):
        dfname = pd.read_pickle(path)
        print(str(dfname))
    
    import_df = dfname.T
    import_df['Year'] = import_df.index
    import_df.Year = import_df.Year.str.slice(0,4)
    import_df = import_df.set_index('Year')  
    import_df.fillna("",inplace=True)
    
    return import_df

def load_data(countryname, hslevel, currency, countrycode, **context):
    elastic=Elasticsearch([{'host':'142.93.213.146','port':9200}])
    print(elastic)
    INDEX_NAME = 'trade_yearly_v3'
    TYPE_NAME = '_doc'
    uuid = int(Variable.get("uuid"))

    
    if currency == 'USD':
        units = 'Million Dollars'
    else:
        units = 'Lakh Rupees'

    Doc_Source={
            "Country": countryname,
            "Datasources": "Ministry of Commerce",
            "Description": "India exported this commodity from " + countryname,
            "Frequency": "Yearly",
            "Mode":"Export",
            "HSCodeLevel": hslevel,
            "Value": currency,
            "Units": units,
            
            }
    x = 'Transform_Data_' + countrycode + '_' + hslevel + '_' + currency
    df = context['task_instance'].xcom_pull(task_ids= x)
    print(str(df))

    for column in df:
        df_Temp=df[[column]]
        hscode = column.split('_')[0]
        hsname = column.split('_')[1]
        hscodename = hscode + '_' + hsname
        df_Temp = df_Temp.rename(columns={column:"Variable"})
        df_Temp_JSON = df_Temp.to_dict()
    
        
        doc_source = {
            "HSCodeName":column,
            "HSCode":hscode,
            "HSName":hsname,
#             "GstRate":Gst_rate,
#             "Cess":Cess,
#             "Chapter":Chapter,
#             "Section":Section,
#             "AppliedMFNAverageAvDuties":AppliedMfnAverage,
#             "AppliedMFNMinimumAvDuty":AppliedMfnMinimum,
#             "AppliedMFNMaximumAvDuty":AppliedMfnMaximum,
#             "BoundDataAverageAvDuties":BoundDataAverage,
#             "BoundDataMinimumAvDuty":BoundDataMinimum,
#             "BoundDataMaximumAvDuty":BoundDataMaximum,
            
            "data": df_Temp_JSON["Variable"]
        }
        Doc_Source.update(doc_source)
        
        response=elastic.index(index=INDEX_NAME,doc_type=TYPE_NAME,id=uuid,body=Doc_Source)
        print('uuid is',str(uuid))
        uuid += 1
        
    Variable.set("uuid", uuid)    



with DAG(dag_id='ETL_Export_Yearly', description='Trade', start_date=datetime(2020,6, 15),end_date= None, schedule_interval='@yearly', default_args=args) as dag:
    
    
    for countrycode, countryname in country_dict.items():
        for hslevelcode in hslevel_list:
            for currency in currency_list:
                
                task1 = SeleniumOperator(
                script = get_df,
                script_args = [data_folder, countrycode, hslevelcode, currency],
                task_id = 'Extract_Data_' + countrycode + '_' + hslevelcode + '_' + currency)

                task2 = PythonOperator(
                task_id = 'Transform_Data_' + countrycode + '_' + hslevelcode + '_' + currency,
                # op_kwargs={'countrycode': countrycode},
                python_callable = transform_data)

                task3 = PythonOperator(
                task_id = 'Load_Data_' + countrycode + '_' + hslevelcode + '_' + currency,
                op_kwargs = {'countryname': countryname, 'hslevel': hslevelcode, 'currency': currency, 'countrycode': countrycode},
                python_callable = load_data)

                # Set Dependencies
                task1 >> task2 >> task3
