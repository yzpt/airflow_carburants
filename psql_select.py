import psycopg2

try:    
        conn = psycopg2.connect(
            database="carburants",
            user="yzpt",
            password="yzpt",
            host="localhost",
            port="5432"
        )
        cursor = conn.cursor()
        print('Connected to database successfully.')

        sql = '''
            SELECT id, record_timestamp, adresse, cp, ville, latitude, longitude 
            from records 
            ORDER BY record_timestamp DESC
            LIMIT 5;
        '''
        cursor.execute(sql)
        records = cursor.fetchall()
        print('Data selected successfully.')
        for row in records:
            print(row)
        cursor.close()
        conn.close()
        print('Connection closed.')
except Exception as e:
    print(e)

