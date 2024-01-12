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
def upload_to_s3(input_file, output_file="mta_bus_ridership.parquet"):
    # Check if input file exists
    if not input_file.exists():
        print("Input file does not exist")
        return False

    ridership = pd.read_csv(input_file)

    # Convert DataFrame to parquet
    parquet_file = output_file
    ridership.to_parquet(parquet_file)

    # Initialize the S3 client
    s3 = boto3.client("s3")

    # Upload the parquet file to S3
    with open(parquet_file, "rb") as data:
        s3.upload_fileobj(data, "transitscope-baltimore", parquet_file)


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
    to_s3=False,
):
    # Check for directories
    check_for_directories()
    # Run Node.js script
    run_node_script(node_script_path)

    # Run data transformation tasks
    data = load_data(input_path)
    processed_data = process_data(data)
    save_data(processed_data, output_path)
    if to_s3:
        upload_to_s3(input_file=output_path)


if __name__ == "__main__":
    # Run the Flow
    node_script_path = Path("node/index.js")

    input_path = Path("data/raw/mta_bus_ridership.csv")
    output_path = Path("data/processed/mta_bus_ridership.csv")
    data_transform(
        node_script_path=node_script_path,
        input_path=input_path,
        output_path=output_path,
        to_s3=True,
    )
