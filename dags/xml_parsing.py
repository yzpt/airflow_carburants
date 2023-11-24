import requests
from datetime import datetime
import zipfile
import xml.etree.ElementTree as ET
import psycopg2
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def download_file():
    url = 'https://donnees.roulez-eco.fr/opendata/instantane'
    output_folder = './instantanes_files/xml/'
    try:
        response = requests.get(url)
        if response.status_code == 200:
            zip_path = './instantanes_files/zip/instantane_' + datetime.now().strftime('%Y%m%d_%H%M%S') + '.zip'
            with open(zip_path, 'wb') as f:
                f.write(response.content)
                logging.info('File saved successfully.')

            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(output_folder)
                
            logging.info('File unzipped successfully.')
        else:
            logging.error('Request status code: ' + str(response.status_code))
    except Exception as e:
        logging.error(e)

def parse_xml(xml_file):
    try:
        tree = ET.parse('instantanes_files/xml/PrixCarburants_instantane.xml')
        root = tree.getroot()
        logging.info('XML parsed successfully.')
        return ET.tostring(root, encoding='unicode') 
    except Exception as e:
        logging.error(e)

def db_insert(root_string):
    try:
        root = ET.fromstring(root_string)    
        
        conn = psycopg2.connect(
            database="psql_db_name",
            user="postgres",
            password="postgres",
            host="localhost",
            port="5434"
        )
        cursor = conn.cursor()
        logging.info('Connected to ' + conn.dsn + '.')
        
        dt_now = datetime.now()
        logging.info('Loading ...')

        cursor.execute("""
               INSERT INTO records (record_timestamp, id, adresse, ville) VALUES (%s, %s, %s, %s);
               """ , (dt_now, 198632453, '35 rue de l''égalité', 'Leforest'))
        conn.commit()
        # for item in root.findall('pdv'):
        #     id                  = int(item.get('id')) if item.get('id') is not None else None
        #     record_timestamp    = dt_now
        #     latitude            = float(item.get('latitude')) if item.get('latitude') is not None else None
        #     longitude           = float(item.get('longitude')) if item.get('longitude') is not None else None
        #     cp                  = item.get('cp') if item.get('cp') is not None else None
        #     pop                 = item.get('pop') if item.get('pop') is not None else None
        #     adresse             = item.find('adresse').text if item.find('adresse') is not None else None
        #     ville               = item.find('ville').text if item.find('ville') is not None else None
        #     horaires            = item.get('horaires') if item.get('horaires') is not None else None
        #     services            = item.get('services') if item.get('services') is not None else None
        #     gazole_maj          = datetime.strptime(item.find("prix[@nom='Gazole']").get('maj'), '%Y-%m-%d %H:%M:%S') if item.find("prix[@nom='Gazole']") is not None else None
        #     gazole_prix         = float(item.find("prix[@nom='Gazole']").get('valeur')) if item.find("prix[@nom='Gazole']") is not None else None
        #     sp95_maj            = datetime.strptime(item.find("prix[@nom='SP95']").get('maj'), '%Y-%m-%d %H:%M:%S') if item.find("prix[@nom='SP95']") is not None else None
        #     sp95_prix           = float(item.find("prix[@nom='SP95']").get('valeur')) if item.find("prix[@nom='SP95']") is not None else None
        #     e85_maj             = datetime.strptime(item.find("prix[@nom='E85']").get('maj'), '%Y-%m-%d %H:%M:%S') if item.find("prix[@nom='E85']") is not None else None
        #     e85_prix            = float(item.find("prix[@nom='E85']").get('valeur')) if item.find("prix[@nom='E85']") is not None else None
        #     gplc_maj            = datetime.strptime(item.find("prix[@nom='GPLc']").get('maj'), '%Y-%m-%d %H:%M:%S') if item.find("prix[@nom='GPLc']") is not None else None
        #     gplc_prix           = float(item.find("prix[@nom='GPLc']").get('valeur')) if item.find("prix[@nom='GPLc']") is not None else None
        #     e10_maj             = datetime.strptime(item.find("prix[@nom='E10']").get('maj'), '%Y-%m-%d %H:%M:%S') if item.find("prix[@nom='E10']") is not None else None
        #     e10_prix            = float(item.find("prix[@nom='E10']").get('valeur')) if item.find("prix[@nom='E10']") is not None else None
        #     sp98_maj            = datetime.strptime(item.find("prix[@nom='SP98']").get('maj'), '%Y-%m-%d %H:%M:%S') if item.find("prix[@nom='SP98']") is not None else None
        #     sp98_prix           = float(item.find("prix[@nom='SP98']").get('valeur')) if item.find("prix[@nom='SP98']") is not None else None
            
            
        #     cursor.execute(f'''
        #         INSERT INTO records (
        #                 id, record_timestamp, cp, pop, adresse, ville, horaires, services, latitude, longitude, gazole_maj, gazole_prix, sp95_maj, sp95_prix, e85_maj, e85_prix, gplc_maj, gplc_prix, e10_maj, e10_prix, sp98_maj, sp98_prix
        #             ) VALUES (
        #                 %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s
        #             )
        #         ''', 
        #         (
        #             id, record_timestamp, cp, pop, adresse, ville, horaires, services, latitude, longitude, gazole_maj, gazole_prix, sp95_maj, sp95_prix, e85_maj, e85_prix, gplc_maj, gplc_prix, e10_maj, e10_prix, sp98_maj, sp98_prix
        #         )
        #     )

        # conn.commit()
        logging.info('Data inserted successfully.')
        cursor.close()
        conn.close()
        logging.info('Connection closed.')

    except Exception as e:
        logging.error(e)