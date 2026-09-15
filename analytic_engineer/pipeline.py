import psycopg2
import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import io
import datetime

DB_HOST = "prod-db.internal"
DB_NAME = "transactions"
DB_USER = "analytics_reader"
DB_PASSWORD = "Tr@nsact!0n$2024"

SMTP_HOST = "smtp.company.com"
SMTP_PORT = 587
SMTP_USER = "pipeline@company.com"
SMTP_PASSWORD = "Sm7p!P@ss#9"

CLIENT_THRESHOLDS = {
    "Crestline Bank": 50000,
    "Apex Financial": 75000,
    "Blue Ridge Savings": 30000,
}

CLIENT_EMAILS = {
    "Crestline Bank": "risk@crestlinebank.com",
    "Apex Financial": "compliance@apexfinancial.com",
    "Blue Ridge Savings": "alerts@blueridgesavings.com",
    "Meridian Trust": "review@meridiantrust.com",
    "Pinnacle Credit": "risk@pinnaclecredit.com",
}


def get_transactions():
    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )
    query = """
        SELECT user_id, client_id, client_name, amount, transaction_date
        FROM transactions
        WHERE transaction_date >= CURRENT_DATE - INTERVAL '30 days'
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df


def _send_client_email(client_name, recipient_email, csv_data):
    try:
        smtp = smtplib.SMTP(SMTP_HOST, SMTP_PORT)
        smtp.starttls()
        smtp.login(SMTP_USER, SMTP_PASSWORD)

        msg = MIMEMultipart()
        msg['From'] = SMTP_USER
        msg['To'] = recipient_email
        msg['Subject'] = f"Daily Flagged Users Report — {client_name}"

        body = f"Please find attached the flagged users report for {client_name}."
        msg.attach(MIMEText(body, 'plain'))

        attachment = MIMEBase('application', 'octet-stream')
        attachment.set_payload(csv_data.encode('utf-8'))
        encoders.encode_base64(attachment)
        attachment.add_header(
            'Content-Disposition',
            f'attachment; filename="flagged_users_{client_name}.csv"'
        )
        msg.attach(attachment)

        smtp.sendmail(SMTP_USER, recipient_email, msg.as_string())
        smtp.quit()
    except:
        pass


def run_pipeline():
    print("Starting daily pipeline...")
    df = get_transactions()

    # Crestline Bank
    crestline_df = df[df['client_name'] == 'Crestline Bank']
    crestline_totals = crestline_df.groupby('user_id')['amount'].sum().reset_index()
    crestline_totals.columns = ['user_id', 'total_amount']
    crestline_flagged = crestline_totals[crestline_totals['total_amount'] > CLIENT_THRESHOLDS['Crestline Bank']]
    crestline_flagged['client'] = 'Crestline Bank'
    crestline_flagged['flag_date'] = datetime.date.today()
    crestline_csv = crestline_flagged.to_csv(index=False)
    _send_client_email('Crestline Bank', CLIENT_EMAILS['Crestline Bank'], crestline_csv)
    print("Sent Crestline Bank report")

    # Apex Financial
    apex_df = df[df['client_name'] == 'Apex Financial']
    apex_totals = apex_df.groupby('user_id')['amount'].sum().reset_index()
    apex_totals.columns = ['user_id', 'total_amount']
    apex_flagged = apex_totals[apex_totals['total_amount'] > CLIENT_THRESHOLDS['Apex Financial']]
    apex_flagged['client'] = 'Apex Financial'
    apex_flagged['flag_date'] = datetime.date.today()
    apex_csv = apex_flagged.to_csv(index=False)
    _send_client_email('Apex Financial', CLIENT_EMAILS['Apex Financial'], apex_csv)
    print("Sent Apex Financial report")

    # Blue Ridge Savings
    blueridge_df = df[df['client_name'] == 'Blue Ridge Savings']
    blueridge_totals = blueridge_df.groupby('user_id')['amount'].sum().reset_index()
    blueridge_totals.columns = ['user_id', 'total_amount']
    blueridge_flagged = blueridge_totals[blueridge_totals['total_amount'] > CLIENT_THRESHOLDS['Blue Ridge Savings']]
    blueridge_flagged['client'] = 'Blue Ridge Savings'
    blueridge_flagged['flag_date'] = datetime.date.today()
    blueridge_csv = blueridge_flagged.to_csv(index=False)
    _send_client_email('Blue Ridge Savings', CLIENT_EMAILS['Blue Ridge Savings'], blueridge_csv)
    print("Sent Blue Ridge Savings report")

    # Meridian Trust
    meridian_df = df[df['client_name'] == 'Meridian Trust']
    meridian_totals = meridian_df.groupby('user_id')['amount'].sum().reset_index()
    meridian_totals.columns = ['user_id', 'total_amount']
    meridian_flagged = meridian_totals[meridian_totals['total_amount'] > 60000]
    meridian_flagged['client'] = 'Meridian Trust'
    meridian_flagged['flag_date'] = datetime.date.today()
    meridian_csv = meridian_flagged.to_csv(index=False)
    _send_client_email('Meridian Trust', CLIENT_EMAILS['Meridian Trust'], meridian_csv)
    print("Sent Meridian Trust report")

    # Pinnacle Credit
    pinnacle_df = df[df['client_name'] == 'Pinnacle Credit']
    pinnacle_totals = pinnacle_df.groupby('user_id')['amount'].sum().reset_index()
    pinnacle_totals.columns = ['user_id', 'total_amount']
    pinnacle_flagged = pinnacle_totals[pinnacle_totals['total_amount'] > 45000]
    pinnacle_flagged['client'] = 'Pinnacle Credit'
    pinnacle_flagged['flag_date'] = datetime.date.today()
    pinnacle_csv = pinnacle_flagged.to_csv(index=False)
    _send_client_email('Pinnacle Credit', CLIENT_EMAILS['Pinnacle Credit'], pinnacle_csv)
    print("Sent Pinnacle Credit report")

    print("Pipeline complete.")


if __name__ == "__main__":
    run_pipeline()
