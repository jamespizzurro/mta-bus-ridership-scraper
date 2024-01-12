import argparse
import calendar
import datetime as dt
import re
import subprocess
from pathlib import Path

import boto3
import pandas as pd
from prefect import task
from prefect.flows import Flow


def clean_column_names(df):
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    return df


@task
def load_data(input_file):
    # Check if input file exists
    if not input_file.exists():
        print("Input file does not exist")
        return False
    ridership = pd.read_csv(input_file)
    ridership = clean_column_names(ridership)
    return ridership


def process_routes(served_routes):
    citylink_pattern = re.compile("CityLink ([A-Z]+)")
    # Split routes into a list, strip whitespace, and capitalize CityLink
    modified_routes = [
        citylink_pattern.sub(
            lambda match: "CityLink " + match.group(1).title(),
            route.strip(),
        )
        for route in served_routes.split(",")
    ]
    # Join the routes back together
    return ", ".join(modified_routes)


def get_num_days_in_month(date):
    # Get the last day of the month
    last_day = dt.datetime(
        date.year, date.month, calendar.monthrange(date.year, date.month)[1]
    )
    return last_day.day


@task
def process_data(rides):
    # process routes
    rides["route"] = rides["route"].apply(process_routes)
    # Convert date column to datetime format
    rides["date"] = pd.to_datetime(rides["date"], format="%m/%Y")
    rides["date_end"] = rides["date"] + pd.offsets.MonthEnd(0)
    # Group the data by route and date
    rides = rides.groupby(["route", "date", "date_end"]).sum().reset_index()
    # Drop rows where ridership is 0
    rides = rides[rides["ridership"] > 0]
    # Get number of days in the month
    rides["num_days_in_month"] = rides["date"].apply(get_num_days_in_month)
    # Get ridership per day
    rides["ridership_per_day"] = (
        rides["ridership"] / rides["num_days_in_month"]
    )

    return rides


@task
def save_data(rides, output_file, format="csv"):
    if format == "csv":
        # Save the cleaned data to a new CSV file
        rides.to_csv(output_file, index=False)
    if format == "parquet":
        # Save the cleaned data to a new Parquet file
        rides.to_parquet(output_file, index=False)


@task
def upload_to_s3(file_to_upload, s3_output_filename=None,s3_bucket=None):
    # Check if input file exists
    if not file_to_upload.exists():
        print("Input file does not exist")
        return False

    ridership = pd.read_csv(file_to_upload)

    # Convert DataFrame to parquet
    parquet_file = s3_output_filename
    ridership.to_parquet(parquet_file)

    # Initialize the S3 client
    s3 = boto3.client("s3")

    # Upload the parquet file to S3
    with open(parquet_file, "rb") as data:
        s3.upload_fileobj(data, s3_bucket, parquet_file)


@task
def run_node_script(script_path):
    # Check if Node.js script exists
    if not script_path.exists():
        print("Node.js script does not exist")
        return False
    # Check if installed Node.js
    try:
        subprocess.run(["node", "-v"], check=True)
    except Exception as e:
        print(f"Failed to run Node.js: {e}")
        return False

    try:
        subprocess.run(["node", script_path], check=True)
    except Exception as e:
        print(f"Failed to run Node.js script: {e}")
        return False
    return True


@task
def check_for_directories():
    # Check if raw data path exists
    if not Path("data/raw").exists():
        print("Creating data/raw directory")
        Path("data/raw").mkdir(parents=True)

    # Check if processed data path exists
    if not Path("data/processed").exists():
        print("Creating data/processed directory")
        Path("data/processed").mkdir(parents=True)


# Define the Flow
@Flow
def data_transform(
    node_script_path="node/index.js",
    input_path="data/raw/mta_bus_ridership.csv",
    output_path="data/processed/mta_bus_ridership.csv",
    s3_output_filename = None,
    s3_bucket = None,
):
    # Check for directories
    check_for_directories()
    # Run Node.js script
    run_node_script(node_script_path)

    # Run data transformation tasks
    data = load_data(input_path)
    processed_data = process_data(data)
    save_data(processed_data, output_path)
    if s3_output_filename and s3_bucket:
        upload_to_s3(output_path, s3_output_filename, s3_bucket)

# Main function to parse arguments and run the flow
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Data Transformation CLI Tool")
    parser.add_argument("--node_script_path", default="node/index.js", type=str, help="Path to Node.js script")
    parser.add_argument("--input_path", default="data/raw/mta_bus_ridership.csv", type=str, help="Path to input data file")
    parser.add_argument("--output_path", default="data/processed/mta_bus_ridership.csv", type=str, help="Path to output data file")
    parser.add_argument("--s3_output_filename", type=str, help="S3 output filename")
    parser.add_argument("--s3_bucket", type=str, help="S3 bucket name")
    args = parser.parse_args()

    data_transform(
        node_script_path=Path(args.node_script_path),
        input_path=Path(args.input_path),
        output_path=Path(args.output_path),
        s3_output_filename=args.s3_output_filename,
        s3_bucket=args.s3_bucket
    )
