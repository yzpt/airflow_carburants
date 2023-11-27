import requests
from datetime import datetime
import zipfile
import xml.etree.ElementTree as ET


def download_file():
    try:
        response = requests.get('https://donnees.roulez-eco.fr/opendata/instantane')
        if response.status_code == 200:
            zip_path = './instantane.zip'
            with open(zip_path, 'wb') as f:
                f.write(response.content)
                print('File saved successfully.')

            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall('./data.xml')
                
            print('File unzipped successfully.')
        else:
            print('Request status code: ' + response.status_code)
    except Exception as e:
        print(e)

def parse_xml():
    try:
        tree = ET.parse('./data.xml')
        root = tree.getroot()
        print('XML parsed successfully.')
        return root
    except Exception as e:
        print(e)

def db_insert(root):
    try:    
        print('============================================')
        print('allo')
        print('============================================')
        dt_now = datetime.now()
        print('Loading ...')
        for item in root.findall('pdv'):
            id                  = int(item.get('id')) if item.get('id') is not None else None
            record_timestamp    = dt_now
            latitude            = float(item.get('latitude')) if item.get('latitude') is not None else None
            longitude           = float(item.get('longitude')) if item.get('longitude') is not None else None
            cp                  = item.get('cp') if item.get('cp') is not None else None
            pop                 = item.get('pop') if item.get('pop') is not None else None
            adresse             = item.find('adresse').text if item.find('adresse') is not None else None
            ville               = item.find('ville').text if item.find('ville') is not None else None
            horaires            = item.get('horaires') if item.get('horaires') is not None else None
            services            = item.get('services') if item.get('services') is not None else None
            gazole_maj          = datetime.strptime(item.find("prix[@nom='Gazole']").get('maj'), '%Y-%m-%d %H:%M:%S') if item.find("prix[@nom='Gazole']") is not None else None
            gazole_prix         = float(item.find("prix[@nom='Gazole']").get('valeur')) if item.find("prix[@nom='Gazole']") is not None else None
            sp95_maj            = datetime.strptime(item.find("prix[@nom='SP95']").get('maj'), '%Y-%m-%d %H:%M:%S') if item.find("prix[@nom='SP95']") is not None else None
            sp95_prix           = float(item.find("prix[@nom='SP95']").get('valeur')) if item.find("prix[@nom='SP95']") is not None else None
            e85_maj             = datetime.strptime(item.find("prix[@nom='E85']").get('maj'), '%Y-%m-%d %H:%M:%S') if item.find("prix[@nom='E85']") is not None else None
            e85_prix            = float(item.find("prix[@nom='E85']").get('valeur')) if item.find("prix[@nom='E85']") is not None else None
            gplc_maj            = datetime.strptime(item.find("prix[@nom='GPLc']").get('maj'), '%Y-%m-%d %H:%M:%S') if item.find("prix[@nom='GPLc']") is not None else None
            gplc_prix           = float(item.find("prix[@nom='GPLc']").get('valeur')) if item.find("prix[@nom='GPLc']") is not None else None
            e10_maj             = datetime.strptime(item.find("prix[@nom='E10']").get('maj'), '%Y-%m-%d %H:%M:%S') if item.find("prix[@nom='E10']") is not None else None
            e10_prix            = float(item.find("prix[@nom='E10']").get('valeur')) if item.find("prix[@nom='E10']") is not None else None
            sp98_maj            = datetime.strptime(item.find("prix[@nom='SP98']").get('maj'), '%Y-%m-%d %H:%M:%S') if item.find("prix[@nom='SP98']") is not None else None
            sp98_prix           = float(item.find("prix[@nom='SP98']").get('valeur')) if item.find("prix[@nom='SP98']") is not None else None
            

    except Exception as e:
        print(e)

# if __name__ == '__main__':

#     url = 'https://donnees.roulez-eco.fr/opendata/instantane'
#     output_folder = './instantanes_files/xml/'
#     table = 'records'

#     download_file(url, output_folder)
#     root = parse_xml(output_folder + 'PrixCarburants_instantane.xml')
#     db_insert(root)