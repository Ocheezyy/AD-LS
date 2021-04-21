import os
import shutil
import ssl
import time
import smtplib
import sys
import itertools
import pyodbc
from logger import Logger
import openpyxl
import sqlite3 as sqlite
import pandas as pd
import datetime as dt
from email import encoders
from shutil import copyfile
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from datetime import datetime as dt2
from cryptography.fernet import Fernet
from email.mime.multipart import MIMEMultipart
import warnings

warnings.filterwarnings("ignore")
col_list = ["SS", "LN", "FN", "DB", "G", "F_DOD", "SRC", "Q_FACTOR"]
EMAIL_TO = ['####']
proj_root = os.path.dirname(os.path.abspath(__file__))


comp = dt.timedelta(days=365)
cwd = os.getcwd()


def main():

    print("Initialized")
    while True:
        t = dt2.now()
        log_name = f"{str(t.strftime('%b-%d'))}.log"
        log_path = os.path.join(proj_root, f"logs\\{log_name}")

        if not os.path.isdir(os.path.join(proj_root, "logs")):
            os.mkdir(os.path.join(proj_root, "logs"))
        logger = Logger(log_path, "LS360 Insert: ")

        emailed_path = os.path.join(
            proj_root, f"data/{t.strftime('%b-%d')}.xlsx")
        save_path = os.path.join("K:\\LS360\\", str(t.strftime('%Y')), str(
            t.strftime('%b')) + "-" + str(t.year)[2:] + "\\")

        filename = None
        files = os.listdir(save_path)
        for file in files:
            if "death_audit" in file:
                if ".csv" in file:
                    filename = file

        if filename is None:
            print("No new files found")
            time.sleep(600)
            continue
        else:
            sqlite_conn = sqlite.connect(
                os.path.join(proj_root, "data\\files.db"))
            sqlite_cursor = sqlite_conn.cursor()
            sqlite_cursor.execute(
                'SELECT filename FROM files ORDER BY filename DESC LIMIT 200')
            output_tup = sqlite_cursor.fetchall()
            output = list(itertools.chain(*output_tup))
            if filename in output:
                print(f"{t.strftime('%H:%M:%S')}: {filename} already processed")
                time.sleep(600)
                continue
            else:
                sqlite_cursor.execute(
                    f"INSERT INTO files (filename) VALUES ('{filename}')")
                sqlite_conn.commit()
                sqlite_cursor.close()

        conn = pyodbc.connect(get_conn(), autocommit=True)
        cursor = conn.cursor()
        path = os.path.join(save_path, filename)

        if not os.path.isfile(path):
            message("Unable to find referenced CSV file", logger,
                    log_path, log_name, t=t, critical=True)
            time.sleep(1209600)
            continue

        input_df = pd.read_csv(path, dtype={'SS': str})
        input_df_sorted = input_df.sort_values(by='Q_FACTOR', ascending=False)
        output_df = input_df_sorted.drop_duplicates(
            keep='first', subset=['SS'])
        output_df.to_csv(path)

        message("Formatting Dataframes", logger, log_path, log_name, t=t)
        df = pd.read_csv(path, parse_dates=True,
                         usecols=col_list, dtype={'SS': str})
        df['F_DOD'] = pd.to_datetime(df['F_DOD'])
        df['date_today'] = t
        df['date_diff'] = abs(df['date_today'] - df['F_DOD'])
        df_to_add_pc = df[(df.Q_FACTOR == 100) & (df.date_diff <= comp)]
        df_not_added = df[(df.Q_FACTOR != 100) | (df.date_diff >= comp)]
        df_to_add_pc['SS'] = df['SS'].apply(lambda x: '{0:0>9}'.format(x))
        df_not_added['SS'] = df['SS'].apply(lambda x: '{0:0>9}'.format(x))

        in_statement_not_added = ', '.join(
            [f"'{i}'" for i in df_not_added['SS']])
        check_query_not_added = f"SELECT DISTINCT SSN AS SS, EnteredDate, Died AS F_DOD FROM PersonalData WHERE EnteredDate IS NOT NULL AND SSN IN ({in_statement_not_added})"

        try:
            df_check_not_entered = pd.read_sql(check_query_not_added, conn)
        except Exception as e:
            message('Manual entry check query failed:\n' + str(e),
                    logger, log_path, log_name, t=t, exception=True)
            time.sleep(600)
            continue

        df_not_added_ae = df_difference_both(
            df_check_not_entered[['SS', 'F_DOD']], df_not_added[['SS', 'F_DOD']])
        df_not_added_te = df_difference_right(
            df_check_not_entered[['SS', 'F_DOD']], df_not_added[['SS', 'F_DOD']])
        df_not_added_manual = df_not_added_te[['SS']].merge(
            df_not_added[['SS', 'LN', 'FN', 'DB', 'G', 'F_DOD', 'SRC', 'Q_FACTOR']], how='inner', on='SS')

        in_statement = ', '.join([f"'{i}'" for i in df_to_add_pc['SS']])
        check_query = f"SELECT DISTINCT SSN AS SS, Died AS F_DOD FROM PersonalData WHERE EnteredDate IS NOT NULL AND SSN IN ({in_statement})"
        try:
            df_check_before = pd.read_sql(check_query, conn)
        except Exception as e:
            message('Before check query failed:\n' + str(e),
                    logger, log_path, log_name, t=t, exception=True)
            time.sleep(600)
            continue
        else:
            message('Connection to database successful',
                    logger, log_path, log_name)

        df_compared_right_before = df_difference_right(
            df_check_before[['SS', 'F_DOD']], df_to_add_pc[['SS', 'F_DOD']])
        already_entered_before = df_difference_both(
            df_check_before[['SS', 'F_DOD']], df_to_add_pc[['SS', 'F_DOD']])
        message("Entering Deaths", logger, log_path, log_name, t=t)
        time.sleep(2)

        try:
            for row in df_compared_right_before.itertuples():
                sp_viator_died = f"""SET NOCOUNT ON; exec sp_ViatorD 
                                        @SSN = '{str(row[1])}', 
                                        @D = '{row[3]}', 
                                        @SSDI = 0, 
                                        @Website = 0, 
                                        @WebsiteComment = ' ', 
                                        @Emails = 0, 
                                        @EmailsComment = ' ', 
                                        @Other = 1, 
                                        @OtherComment = 'LS360', 
                                        @UsrName = 'AutoScript', 
                                        @DMFMatching = 0, 
                                        @Comment = ' ', 
                                        @ComServ = 0;"""
                cursor.execute(sp_viator_died)
        except Exception as e:
            message('Failed to execute stored procedure sp_ViatorDied:\n' +
                    str(e), logger, log_path, log_name, t=t, exception=True)
            time.sleep(600)
            continue
        else:
            message('sp_ViatorDied executed', logger, log_path, log_name, t=t)
            try:
                df_check_after = pd.read_sql(check_query, conn)
            except Exception as e:
                message('After check query failed:\n' + str(e), logger,
                        log_path, log_name, t=t, exception=True)

            try:
                mismatch_query = f"""SELECT SSN AS SS, Died AS DOD, EnteredDate FROM PersonalData WHERE 
                                        (EnteredDate IS NOT NULL AND Died IS NULL)"""
                df_mismatch_pre = pd.read_sql(mismatch_query, conn)
            except Exception as e:
                message("Failed to fetch mismatch death records:\n" +
                        str(e), logger, log_path, log_name, t=t, exception=True)

            df_compared_right_after = df_difference_right(
                df_check_after[['SS', 'F_DOD']], df_to_add_pc[['SS', 'F_DOD']])
            already_entered_after = df_difference_both(
                df_check_after[['SS', 'F_DOD']], df_to_add_pc[['SS', 'F_DOD']])
            df_already_entered_pc = pd.concat(
                [df_not_added_ae, already_entered_before], ignore_index=True, sort=False)
            df_already_entered_ac = df_already_entered_pc[(
                pd.notnull(df_already_entered_pc.F_DOD_x))]

            final_auto_enter = df_difference_right(already_entered_before[['SS', 'F_DOD_x']],
                                                   already_entered_after[['SS', 'F_DOD_x']])

            df_manual_no_dupes = df_not_added_manual.drop_duplicates('SS')

            auto_entered_num = str(len(final_auto_enter))
            already_entered_num = str(len(df_already_entered_ac.index))
            manual_not_entered_num = str(len(df_manual_no_dupes.index))
            entry_failed_num = str(len(df_compared_right_after.index))

            final_w_qfactor = final_auto_enter.merge(
                df[['SS', 'Q_FACTOR']], on='SS', how='inner')

            # Removing all duplicates
            final = final_w_qfactor.drop_duplicates(subset='SS', keep='first')
            already_entered_final = df_already_entered_ac.drop_duplicates(
                subset='SS', keep='first')
            df_mismatch = df_mismatch_pre.drop_duplicates(
                subset='SS', keep='first')

            dodv_in_statement = ', '.join(
                [f"'{i}'" for i in final_auto_enter['SS']])
            if dodv_in_statement == '':
                # time.sleep(600)
                continue
            else:
                try:
                    dod_prior_query = f"SELECT p.AVSRecNo, p.SSN FROM PersonalData p WHERE p.Died < p.InActiveFileDate AND p.EnteredDate > DATEADD(DD, -1, GETDATE())"
                    df_prior = pd.read_sql(dod_prior_query, conn)
                except Exception as e:
                    message('Failed to fetch dod prior records: ' + str(e),
                            logger, log_path, log_name, t=t, exception=True)
                    time.sleep(600)
                    continue
                else:
                    message('DODPrior records fetched successfully',
                            logger, log_path, log_name, t=t)

                try:
                    write_to_excel(df_manual_no_dupes, final[['SS', 'F_DOD_x_y', 'Q_FACTOR']],
                                   df_compared_right_after, already_entered_final[['SS', 'F_DOD_x', 'F_DOD_y']], df_prior, df_mismatch, emailed_path)
                except Exception as e:
                    message('Failed to output to XLSX: ' + str(e),
                            logger, log_path, log_name, t=t)
                    time.sleep(600)
                    continue
                else:
                    message('XLSX created successfully',
                            logger, log_path, log_name, t=t)

                dod_prior_num = str(len(df_prior.index))
            try:
                copyfile(emailed_path, save_path + 'Script_Output.xlsx')
            except Exception as e:
                message('Failed to copy file to K drive', logger,
                        log_path, log_name, t=t, exception=True)
                time.sleep(600)
                continue
            else:
                message('File copied to K drive successfully',
                        logger, log_path, log_name, t=t)
            time.sleep(1)
            message("Entries Complete", logger, log_path, log_name, t=t)

            message("Sending E-mail", logger, log_path, log_name, t=t)
            try:
                sendmail(message, auto_entered_num, already_entered_num, manual_not_entered_num, entry_failed_num,
                         dod_prior_num, emailed_path, t)
            except Exception as e:
                message(str(e), logger, log_path, log_name, t=t)
                time.sleep(600)
                continue


def message(msg, logger, log_path, log_name, t, warning=False, exception=False, critical=False):
    print("LS360 Script: " + msg)
    if warning:
        logger.log_warning(msg)
    elif exception:
        logger.log_error(msg)
        if msg.find("E-Mail") == -1:
            sendmail(path=log_path, error=True, filename=log_name, t=t)
    elif critical:
        logger.log_critical(msg)
        sendmail(path=log_path, error=True, filename=log_name, t=t)
    else:
        logger.log_info(msg)


def df_difference_right(df1, df2):
    # Find rows which are different between two DataFrames.
    comparison_df = df1.merge(df2, indicator=True, how='outer', on='SS')
    right_only = comparison_df[(comparison_df._merge == 'right_only')]
    return right_only


def df_difference_both(df1, df2):
    # Find rows that are identical between two DataFrames
    comparison_df = df1.merge(df2, indicator=True, how='outer', on='SS')
    both = comparison_df[(comparison_df._merge == 'both')]
    return both


def write_to_excel(df1, df2, df3, df4, df5, df6, emailed_path):
    with pd.ExcelWriter(emailed_path) as writer:
        df1.to_excel(writer, sheet_name='Not Entered', index=False)
        df2.to_excel(writer, sheet_name='Auto Entered', index=False)
        df3.to_excel(writer, sheet_name='Auto Enter Failed', index=False)
        df4.to_excel(writer, sheet_name='Already Entered', index=False)
        df5.to_excel(writer, sheet_name='DOD Prior to Completed', index=False)
        df6.to_excel(writer, sheet_name='Death Mismatches', index=False)


def get_conn():
    encrypted_str = open(os.path.join(
        proj_root, 'conn\connectionlive.key'), 'rb')
    encrypted = encrypted_str.read()

    kr = open(os.path.join(proj_root, 'data/tmp/key.key'), 'rb')
    k = kr.read()
    f = Fernet(k)
    decrypted = f.decrypt(encrypted)
    return decrypted.decode("utf-8")


def sendmail(logger, auto_entered, already_entered, not_entered, entry_failed, dod_prior, emailed_path, t):
    EMAIL = "####"
    PASS = "####"
    MAILSERVER = "####"
    PORTTLS = 25
    FROM = "####"
    SUBJECT = "AD-LS " + str(t.strftime('%m-%d-%Y'))

    # if entry_failed:
    #     EMAIL_TO = ['sodonnell@avsllc.com']
    #     body = "Failed to enter check log"
    # else:
    body = f"Automatically entered: {auto_entered}\nAlready entered: {already_entered}\nNot entered: {not_entered}\nFailed to automatically enter: {entry_failed}\n"

    msg = MIMEMultipart()
    msg["From"] = EMAIL
    msg["To"] = ', '.join(EMAIL_TO)
    msg["Subject"] = SUBJECT

    msg.attach(MIMEText(body, 'plain'))
    attachment = MIMEBase('application', "octet-stream")
    attachment.set_payload(open(emailed_path, "rb").read())
    encoders.encode_base64(attachment)
    attachment.add_header('Content-Disposition', 'attachment',
                          filename=f"{t.strftime('%b-%d')}.xlsx")  # or
    msg.attach(attachment)

    # Send the mail
    context = ssl.create_default_context()
    try:
        server = smtplib.SMTP(MAILSERVER, PORTTLS)
        server.ehlo()
        server.starttls(context=context)
        server.ehlo()
        server.login(EMAIL, PASS)
        server.sendmail(FROM, EMAIL_TO, msg.as_string())
    except smtplib.SMTPAuthenticationError as auth_error:
        logger('E-Mail authentication error', exception=True)
    except Exception as e:
        logger('Failed to send email', exception=True)
    else:
        logger('Email sent')
    finally:
        if server is not None:
            server.quit()


if __name__ == '__main__':
    main()
