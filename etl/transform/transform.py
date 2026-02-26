from typing import Tuple
import pandas as pd
import numpy as np
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

SUPPORTED_CURRENCIES = {"USD"}
SUPPORTED_TX_TYPES = {"purchase", "withdrawal", "deposit"}

def clean_transaction_data(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Clean transaction stream data and separate valid from rejected records.

    Returns:
        (clean_df, rejected_df)
    """
    logger.info(f"Starting transaction transformation: {len(df)} records")

    rejection_reasons = []
    df = df.copy()
    df.columns = df.columns.str.strip().str.lower()
    critical_fields = ["transaction_id", "customer_id", "amount", "timestamp"]
    missing_mask = df[critical_fields].isnull().any(axis=1)

    if missing_mask.any():
        rejected = df[missing_mask].copy()
        rejected["rejection_reason"] = "Missing critical field"
        rejection_reasons.append(rejected)
        df = df[~missing_mask]

    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    type_fail_mask = df[["amount", "timestamp"]].isnull().any(axis=1)

    if type_fail_mask.any():
        rejected = df[type_fail_mask].copy()
        rejected["rejection_reason"] = "Type conversion failed"
        rejection_reasons.append(rejected)
        df = df[~type_fail_mask]


    invalid_amount = df["amount"] <= 0
    invalid_currency = ~df.get("currency", "USD").isin(SUPPORTED_CURRENCIES)
    invalid_type = ~df["transaction_type"].isin(SUPPORTED_TX_TYPES)
    future_txn = df["timestamp"] > datetime.utcnow()

    invalid_mask = invalid_amount | invalid_currency | invalid_type | future_txn

    if invalid_mask.any():
        rejected = df[invalid_mask].copy()
        rejected["rejection_reason"] = np.select(
            [
                invalid_amount[invalid_mask],
                invalid_currency[invalid_mask],
                invalid_type[invalid_mask],
                future_txn[invalid_mask],
            ],
            [
                "Invalid amount (<=0)",
                "Unsupported currency",
                "Invalid transaction type",
                "Future timestamp",
            ],
            default="Business rule violation",
        )
        rejection_reasons.append(rejected)
        df = df[~invalid_mask]

    df["transaction_type"] = df["transaction_type"].str.strip().str.lower()
    df["region"] = (
        df.get("region", "Unknown")
        .str.strip()
        .str.title()
        .fillna("Unknown")
    )

    df["is_high_amount"] = df["amount"] >= 3000
    df["transaction_hour"] = df["timestamp"].dt.hour
    df["transaction_date"] = df["timestamp"].dt.date

    rejected_df = (
        pd.concat(rejection_reasons, ignore_index=True)
        if rejection_reasons
        else pd.DataFrame()
    )

    logger.info(
        f"Transaction transformation complete: "
        f"{len(df)} valid, {len(rejected_df)} rejected"
    )

    return df, rejected_df



def transform_credit_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardize credit bureau API data.
    """
    df = df.copy()
    df.columns = df.columns.str.strip().str.lower()

    df["credit_score"] = pd.to_numeric(df["credit_score"], errors="coerce")
    df["credit_score_band"] = pd.cut(
        df["credit_score"],
        bins=[0, 580, 670, 740, 800, 900],
        labels=["Poor", "Fair", "Good", "Very Good", "Excellent"],
    )

    df["credit_score_band"] = df["credit_score_band"].astype(str)
    df["bureau"] = df.get("bureau", "MockBureau")

    return df



def aggregate_daily_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate transactions for risk & fraud analysis.
    """
    logger.info("Computing daily transaction aggregates")

    if df.empty:
        return pd.DataFrame()

    agg = (
        df.groupby(["transaction_date", "customer_id"])
        .agg(
            total_transactions=("transaction_id", "count"),
            total_amount=("amount", "sum"),
            avg_transaction_amount=("amount", "mean"),
            high_amount_txns=("is_high_amount", "sum"),
        )
        .reset_index()
    )

    agg["total_amount"] = agg["total_amount"].round(2)
    agg["avg_transaction_amount"] = agg["avg_transaction_amount"].round(2)

    logger.info(f"Generated {len(agg)} daily risk aggregates")
    return agg

