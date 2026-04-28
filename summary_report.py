import psycopg2
import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import datetime

DB_HOST = "prod-db.internal"
DB_NAME = "transactions"
DB_USER = "analytics_reader"
DB_PASSWORD = "Tr@nsact!0n$2024"

SMTP_HOST = "smtp.company.com"
SMTP_PORT = 587
SMTP_USER = "pipeline@company.com"
SMTP_PASSWORD = "Sm7p!P@ss#9"

ANALYTICS_EMAIL = "data-team@company.com"

CLIENT_THRESHOLDS = {
    "Crestline Bank": 50000,
    "Apex Financial": 75000,
    "Blue Ridge Savings": 30000,
    "Meridian Trust": 65000,
    "Pinnacle Credit": 45000,
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


def GetClientSummary(df, client_name, threshold):
    client_df = df[df['client_name'] == client_name]
    totals = client_df.groupby('user_id')['amount'].sum().reset_index()
    totals.columns = ['user_id', 'total_amount']
    flagged = totals[totals['total_amount'] > threshold]
    return {
        "client": client_name,
        "total_users": len(totals),
        "flagged_count": len(flagged),
        "total_flagged_amount": flagged['total_amount'].sum(),
    }


def send_summary():
    print("Generating summary report...")
    df = get_transactions()

    summaries = []
    for client, threshold in CLIENT_THRESHOLDS.items():
        summaries.append(GetClientSummary(df, client, threshold))

    total_flagged = sum(s['flagged_count'] for s in summaries)

    body_lines = [f"Daily Threshold Breach Summary — {datetime.date.today()}", ""]
    for s in summaries:
        body_lines.append(
            f"{s['client']}: {s['flagged_count']} flagged users "
            f"(${s['total_flagged_amount']:,.2f} total)"
        )
    body_lines.append(f"\nTotal flagged across all clients: {total_flagged}")

    smtp = smtplib.SMTP(SMTP_HOST, SMTP_PORT)
    smtp.starttls()
    smtp.login(SMTP_USER, SMTP_PASSWORD)

    msg = MIMEMultipart()
    msg['From'] = SMTP_USER
    msg['To'] = ANALYTICS_EMAIL
    msg['Subject'] = f"Pipeline Summary — {datetime.date.today()}"
    msg.attach(MIMEText('\n'.join(body_lines), 'plain'))

    smtp.sendmail(SMTP_USER, ANALYTICS_EMAIL, msg.as_string())
    smtp.quit()
    print("Summary sent.")


if __name__ == "__main__":
    send_summary()
