"""Convert raw USAspending CSVs to Parquet, keeping only the columns we need.

Used by download.py, which calls convert() once per unzipped fiscal-year file.
Output: <out_dir>, partitioned by calendar year.
"""
from pyspark.sql import functions as F

SHARED = [
    "action_date", "federal_action_obligation", "modification_number",
    "awarding_agency_name", "awarding_sub_agency_name", "funding_agency_name",
    "recipient_uei", "recipient_name", "recipient_state_code",
    "prime_award_transaction_recipient_cd_current",
    "primary_place_of_performance_state_code", "primary_place_of_performance_county_name",
    "prime_award_transaction_place_of_performance_cd_current",
    "action_type_code",
]
COLUMNS = {
    "contracts": SHARED + [
        "contract_transaction_unique_key", "contract_award_unique_key",
        "naics_code", "naics_description", "product_or_service_code",
        "award_type", "action_type", "extent_competed", "number_of_offers_received",
    ],
    "assistance": SHARED + [
        "assistance_transaction_unique_key", "assistance_award_unique_key",
        "cfda_number", "cfda_title", "assistance_type_code", "assistance_type_description",
        "action_type_description", "business_types_description",
    ],
}


def convert(spark, award_type, csv_path, out_dir):
    wanted = COLUMNS[award_type]
    # multiLine + escape: award descriptions contain quoted newlines and doubled quotes.
    df = spark.read.csv(csv_path, header=True, multiLine=True, escape='"')

    missing = [c for c in wanted if c not in df.columns]
    if missing:
        # Column names were written from memory of the data dictionary; fix any listed here.
        print(f"[{award_type}] NOT FOUND, skipped: {missing}")
    assert {"action_date", "federal_action_obligation"} <= set(df.columns), df.columns

    out = (
        df.select([c for c in wanted if c in df.columns])
        .withColumn("action_date", F.to_date("action_date"))
        .withColumn("federal_action_obligation", F.col("federal_action_obligation").cast("double"))
        .filter(F.col("action_date").between("2022-01-01", "2025-12-31"))
        .withColumn("year", F.year("action_date"))
    )
    out.write.mode("overwrite").partitionBy("year").parquet(out_dir)
