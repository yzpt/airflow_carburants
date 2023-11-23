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
            database="carburants",
            user="yzpt",
            password="yzpt",
            host="localhost",
            port="5432"
        )
        cursor = conn.cursor()
        logging.info('Connected to database successfully.')
        
        dt_now = datetime.now()
        logging.info('Loading ...')
        # Rest of the code...
        conn.commit()
        logging.info('Data inserted successfully.')
        cursor.close()
        conn.close()
        logging.info('Connection closed.')

    except Exception as e:
        logging.error(e)