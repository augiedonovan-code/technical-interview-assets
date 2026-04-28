import pandas as pd
import datetime


def aggregate_user_totals(df, client_name):
    """Aggregate transaction amounts by user for a given client."""
    client_df = df[df['client_name'] == client_name]
    totals = client_df.groupby('user_id')['amount'].sum().reset_index()
    totals.columns = ['user_id', 'total_amount']
    return totals


def AggregateUserTotals(df, client_name):
    """Aggregate transaction amounts by user for a given client."""
    client_df = df[df['client_name'] == client_name]
    totals = client_df.groupby('user_id')['amount'].sum().reset_index()
    totals.columns = ['user_id', 'total_amount']
    return totals


def flag_above_threshold(totals_df, threshold):
    """Return rows where total_amount exceeds the given threshold."""
    return totals_df[totals_df['total_amount'] > threshold].copy()


def apply_threshold_filter(df, threshold):
    """Return rows where total_amount exceeds the given threshold."""
    return df[df['total_amount'] > threshold].copy()


def add_flag_metadata(flagged_df, client_name):
    """Annotate a flagged users dataframe with client name and today's date."""
    flagged_df = flagged_df.copy()
    flagged_df['client'] = client_name
    flagged_df['flag_date'] = datetime.date.today()
    return flagged_df


def get_client_summary(df, client_name, threshold):
    """Return summary statistics for a single client."""
    totals = aggregate_user_totals(df, client_name)
    flagged = flag_above_threshold(totals, threshold)
    return {
        "client": client_name,
        "total_users": len(totals),
        "flagged_count": len(flagged),
        "total_flagged_amount": flagged['total_amount'].sum(),
    }


def GetClientSummary(df, client_name, threshold):
    """Return summary statistics for a single client."""
    return get_client_summary(df, client_name, threshold)
