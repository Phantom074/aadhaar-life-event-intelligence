import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parent
sys.path.append(str(BASE_DIR.parent))

from ALEIS.main import run_aleis_pipeline
from ALEIS.dashboards.national_dashboard import plot_trend
from ALEIS.analytics.spatial_analysis import region_share

def start_aleis_center():
    # 1. Run the main processing pipeline
    print("Step 1: Processing Raw Data...")
    run_aleis_pipeline()

    # 2. Load the newly created processed data
    print("Step 2: Loading Processed Indicators...")
    df = pd.read_csv("data/processed/monthly/demo_indicators.csv")

    # 3. Calculate Regional Insights
    print("\n--- Regional Contribution Insights ---")
    df_spatial = region_share(df, region_col="district", value_col="total_updates")
    print(df_spatial[['district', 'region_share']].drop_duplicates())

    # 4. Launch the Visual Dashboard
    print("\nStep 3: Launching Visual Dashboard...")
    plot_trend(df['lepi'], "National Life-Event Intensity (LEPI) Trend")

if __name__ == "__main__":
    start_aleis_center()