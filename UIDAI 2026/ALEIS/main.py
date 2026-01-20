"""
ALEIS - Aadhaar Life-Event Intelligence System
Main Execution Pipeline

Author: UIDAI Hackathon Prototype
Purpose: End-to-end orchestration of data ingestion, processing,
indicator computation, validation, anomaly detection, and reporting.
"""

from pathlib import Path
import yaml
import pandas as pd
import sys

# -------------------------------------------------
# Base directory (robust path handling)
# -------------------------------------------------
# BASE_DIR = Path(__file__).resolve().parent
BASE_DIR = Path(__file__).resolve().parent
sys.path.append(str(BASE_DIR.parent))


# -------------------------------------------------
# Pipelines
# -------------------------------------------------
from ALEIS.pipelines.ingest import load_dataset
from ALEIS.pipelines.clean import clean_common_fields
from ALEIS.pipelines.transform import add_time_features
from ALEIS.pipelines.aggregate import aggregate_monthly
from ALEIS.pipelines.validate import validate_non_negative


# -------------------------------------------------
# Features
# -------------------------------------------------
from ALEIS.features.enrolment_features import enrolment_velocity
from ALEIS.features.demographic_features import update_diversity
from ALEIS.features.temporal_features import temporal_concentration


# -------------------------------------------------
# Analytics
# -------------------------------------------------
from ALEIS.analytics.anomaly_detection import detect_anomalies


# -------------------------------------------------
# Indicators
# -------------------------------------------------
from ALEIS.indicators.lepi import compute_lepi
from ALEIS.indicators.mobility_index import mobility_index


# -------------------------------------------------
# Validation
# -------------------------------------------------
from ALEIS.validation.sanity_checks import check_empty
from ALEIS.validation.regional_consistency import check_region_coverage


# -------------------------------------------------
# Reports
# -------------------------------------------------
from ALEIS.reports.monthly_policy_brief import generate_brief


# -------------------------------------------------
# Config Loader
# -------------------------------------------------
def load_config():
    config_path = BASE_DIR / "config" / "indicators.yaml"
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


# -------------------------------------------------
# Main ALEIS Pipeline
# -------------------------------------------------
def run_aleis_pipeline():
    print("🚀 Starting ALEIS Pipeline...")

    # ---- Load configuration ----
    config = load_config()
    lepi_weights = config["lepi"]
    mobility_weights = config["mobility"]
    anomaly_threshold = config["thresholds"]["anomaly_zscore"]

    # ---- Load datasets ----
    enrol_df = load_dataset(
        BASE_DIR / "data" / "raw" / "enrolment" / "enrolment.csv"
    )

    demo_df = load_dataset(
        BASE_DIR / "data" / "raw" / "demographic_updates" / "demographic.csv"
    )

    # ---- Basic validation ----
    check_empty(enrol_df)
    check_empty(demo_df)

    # ---- Cleaning ----
    enrol_df = clean_common_fields(enrol_df)
    demo_df = clean_common_fields(demo_df)

    # ---- Transformation ----
    enrol_df = add_time_features(enrol_df, "date")
    demo_df = add_time_features(demo_df, "date")

    # ---- Aggregation (monthly, district-level) ----
    enrol_agg = aggregate_monthly(
        enrol_df,
        group_cols=["state", "district", "year", "month"],
        value_col="enrolments"
    )

    demo_agg = aggregate_monthly(
        demo_df,
        group_cols=["state", "district", "year", "month"],
        value_col="total_updates"
    )

    # ---- Validation checks ----
    validate_non_negative(enrol_agg, "enrolments")
    validate_non_negative(demo_agg, "total_updates")
    check_region_coverage(enrol_agg, "district")

    # ---- Feature Engineering ----
    enrol_agg = enrolment_velocity(enrol_agg, "enrolments")

    demo_agg["temporal_concentration"] = (
        demo_agg
        .groupby(["state", "district"])["total_updates"]
        .transform(temporal_concentration)
    )

    # Row-level diversity feature (if columns exist)
    if {"address_updates", "mobile_updates"}.issubset(demo_df.columns):
        demo_df["update_diversity"] = demo_df.apply(update_diversity, axis=1)

    # ---- Indicator Computation (LEPI) ----
    demo_agg["lepi"] = compute_lepi(
        freq=demo_agg["total_updates"],
        diversity=1,  # aggregated proxy
        temporal=demo_agg["temporal_concentration"],
        weights=lepi_weights
    )

    # ---- Anomaly Detection ----
    demo_agg["anomaly_flag"] = detect_anomalies(
        demo_agg["lepi"],
        threshold=anomaly_threshold
    )

    # ---- Mobility Index (optional) ----
    if {"address_updates", "mobile_updates"}.issubset(demo_df.columns):
        demo_df["mobility_index"] = mobility_index(
            demo_df["address_updates"],
            demo_df["mobile_updates"],
            mobility_weights
        )

    # ---- Save processed outputs ----
    output_dir = BASE_DIR / "data" / "processed" / "monthly"
    output_dir.mkdir(parents=True, exist_ok=True)

    demo_agg.to_csv(
        output_dir / "demo_indicators.csv",
        index=False
    )

    # ---- Policy Brief Generation ----
    anomalies = demo_agg[demo_agg["anomaly_flag"]]

    insight_text = (
        f"{len(anomalies)} districts exhibit unusually high life-event intensity, "
        f"suggesting elevated migration, employment transitions, or administrative stress."
    )

    policy_brief = generate_brief(insight_text)

    report_path = BASE_DIR / "reports" / "monthly_policy_brief.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)

    with open(report_path, "w") as f:
        f.write(policy_brief)

    print("✅ ALEIS Pipeline Completed Successfully.")


# -------------------------------------------------
# Entry Point
# -------------------------------------------------
if __name__ == "__main__":
    run_aleis_pipeline()
