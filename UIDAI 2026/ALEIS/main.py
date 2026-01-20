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
    enrol_df = load_dataset(BASE_DIR / "data" / "raw" / "enrolment" / "enrolment.csv")
    demo_df = load_dataset(BASE_DIR / "data" / "raw" / "demographic_updates" / "demographic.csv")
    
    # Load Biometric dataset to ensure all three sources are reflected
    bio_path = BASE_DIR / "data" / "raw" / "biometric" / "biometric.csv"
    bio_df = load_dataset(bio_path) if bio_path.exists() else pd.DataFrame()

    # ---- Basic validation ----
    check_empty(enrol_df)
    check_empty(demo_df)

    # ---- Cleaning & Transformation ----
    enrol_df = add_time_features(clean_common_fields(enrol_df), "date")
    demo_df = add_time_features(clean_common_fields(demo_df), "date")
    if not bio_df.empty:
        bio_df = add_time_features(clean_common_fields(bio_df), "date")

    # ---- Aggregation (Summing columns to prevent empty values) ----
    # 1. New Enrolments from enrolment file
    enrol_agg = aggregate_monthly(
        enrol_df,
        group_cols=["state", "district", "year", "month"],
        value_col="enrolments"
    )

    # 2. Total Updates from demographic file (Renaming 'enrolments' to 'total_updates')
    demo_agg = aggregate_monthly(
        demo_df,
        group_cols=["state", "district", "year", "month"],
        value_col="enrolments"
    ).rename(columns={"enrolments": "total_updates"})

    # 3. Biometric Updates from biometric file
    if not bio_df.empty:
        bio_agg = aggregate_monthly(
            bio_df,
            group_cols=["state", "district", "year", "month"],
            value_col="total_updates"
        ).rename(columns={"total_updates": "biometric_updates"})
    else:
        bio_agg = pd.DataFrame()

    # ---- Merge everything into a single consolidated view ----
    consolidated_agg = pd.merge(
        enrol_agg, demo_agg, 
        on=["state", "district", "year", "month"], 
        how="outer"
    ).fillna(0)

    if not bio_agg.empty:
        consolidated_agg = pd.merge(
            consolidated_agg, bio_agg, 
            on=["state", "district", "year", "month"], 
            how="outer"
        ).fillna(0)

    # ---- Validation checks ----
    validate_non_negative(consolidated_agg, "enrolments")
    validate_non_negative(consolidated_agg, "total_updates")
    check_region_coverage(consolidated_agg, "district")

    # ---- Feature Engineering ----
    consolidated_agg = enrolment_velocity(consolidated_agg, "enrolments")

    consolidated_agg["temporal_concentration"] = (
        consolidated_agg
        .groupby(["state", "district"])["total_updates"]
        .transform(temporal_concentration)
    )

    # ---- Indicator Computation (LEPI) ----
    consolidated_agg["lepi"] = compute_lepi(
        freq=consolidated_agg["total_updates"],
        diversity=1, 
        temporal=consolidated_agg["temporal_concentration"],
        weights=lepi_weights
    )

    # ---- Anomaly Detection ----
    consolidated_agg["anomaly_flag"] = detect_anomalies(
        consolidated_agg["lepi"],
        threshold=anomaly_threshold
    )

    # ---- Save consolidated outputs ----
    output_dir = BASE_DIR / "data" / "processed" / "monthly"
    output_dir.mkdir(parents=True, exist_ok=True)

    consolidated_agg.to_csv(
        output_dir / "demo_indicators.csv",
        index=False
    )

    # ---- Policy Brief Generation ----
    anomalies = consolidated_agg[consolidated_agg["anomaly_flag"]]
    insight_text = (
        f"{len(anomalies)} districts exhibit unusually high life-event intensity, "
        f"suggesting elevated migration or administrative stress."
    )
    policy_brief = generate_brief(insight_text)

    with open(BASE_DIR / "reports" / "monthly_policy_brief.md", "w") as f:
        f.write(policy_brief)

    print(f"✅ ALEIS Pipeline Completed. Data saved to: {output_dir / 'demo_indicators.csv'}")


if __name__ == "__main__":
    run_aleis_pipeline()