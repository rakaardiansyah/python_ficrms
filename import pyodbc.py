import pyodbc
import schedule
import time
import requests



# SQL Server configuration
server = 'LINK_SERVER'
database = 'NAME_DB'
username = 'USERNAME'
password = 'PASSWORD'
driver = '{ODBC Driver 17 for SQL Server}'



def run_sql_query_and_send_message():
    try:

        connection = pyodbc.connect(
            f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'
        )

        cursor = connection.cursor()

        cursor.execute("select chat_id, CONVERT(varchar, Tgl_System, 106 ), Jumlah_Data from SGFUAT.dbo.Monitoring")

        result = cursor.fetchall()
        for r in result:
            send_telegram_message (str(r))  
        # formatted_result = "\n".join(map(str, result))
        # send_telegram_message(formatted_result)

        cursor.close()
        connection.close()

    except Exception as r:
        print(f'Error: (str(r))')



def send_telegram_message(message):
    
    token = "TOKEN_BOT_TELEGERAM"
    #chat_id = "239082367"
    chat_id = [
         '1433.426386', 
         '239082367', 
         '5777.620081',
         '20554.12595',
         '98424.1102'
     ]
    for cid in chat_id:
        print(cid)
        print("https://api.telegram.org/bot"+token+"/sendMessage"+"?chat_id="+ cid + "&text=" + message)
        cursor = "https://api.telegram.org/bot"+token+"/sendMessage"+"?chat_id="+ cid + "&text=" + message
        cursor = requests.get(cursor)
        
schedule.every().day.at("16:23").do(run_sql_query_and_send_message)


while True:
    schedule.run_pending()
    time.sleep(1)
