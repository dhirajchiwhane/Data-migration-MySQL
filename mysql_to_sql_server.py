import sqlalchemy as sal
import pymysql
import pyodbc
import pandas as pd
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import logging

logging.basicConfig(filename='server.log', level=logging.INFO, filemode='w',
                    format='%(levelname)s:%(asctime)s:%(name)s:%(message)s',
                    datefmt='%d:%b:%y %H:%M:%S')


def data_migrate():
    try:
        with open('config_files\mysql_config.json') as fp:
            cred = json.load(fp)
        mysql_connect = sal.create_engine(
            f"mysql+pymysql://{cred['username']}:{cred['password']}@{cred['host']}:{cred['port']}/{cred['database']}")
        mysql_conn = mysql_connect.connect()
        logging.info("Successfully connect mysql database")

        with open('config_files\sqlserver_config.json') as s_fp:
            data = json.load(s_fp)
        # sql_server_engine = sal.create_engine(f"mssql+pyodbc://{data['host']}/{data['database']}?driver=ODBC+Driver+17+for+SQL+Server")
        sql_server_engine = sal.create_engine("mssql+pyodbc://DHIRAJ/bank_records?driver=ODBC+Driver+17+for+SQL+Server")
        sql_server_conn = sql_server_engine.connect()
        logging.info("Successfully connect sql_server database")

        query = "select * from sbi_bank_customers"
        df = pd.read_sql(query, mysql_conn)
        df.to_sql("sbi_cust_targt", sql_server_conn, if_exists='replace', index=False)
        logging.info("Data successfully transfer to  mysql database")
        mysql_conn.close()
        sql_server_conn.close()
        logging.info("connection close")
        return True
    except Exception as e:
        print(f"Error :{e}")
        return False


def send_mail(status):
    with open('config_files\gmail_config.json') as g_fp:
        g_cred = json.load(g_fp)
    passward = f"{g_cred['apppass']}"
    sender_email = "chiwhane.d@gmail.com"
    reciver_email = "dhirajchiwhane08@gmail.com"

    subject = " Data Migration Status"

    mysql_db = "db_connect.sbi_bank_customers"
    mssql_db = "bank_records.sbi_cust_targt"

    if status:
        body = f"Hi team, \n\n{mysql_db} from MYSQL DB successfully load the data in mssql DB {mssql_db} "
        logging.info("Successfully send mail")
    else:
        body = f"Hi team, \n\n{mysql_db} from MYSQL DB fail to load the data in mssql DB {mssql_db}"
        logging.info("Fail to send mail")

    # Create THe email
    msg = MIMEMultipart()
    msg['Form'] = sender_email
    msg['To'] = reciver_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    try:
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(sender_email, passward)
            server.sendmail(sender_email, reciver_email, msg.as_string())
    except Exception as e:
        logging.error(f"fail to send mail {e}")


def main():
    st = data_migrate()
    send_mail(st)


if __name__ == "__main__":
    main()
