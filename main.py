import calendar
import datetime as dt
import re
import subprocess
from pathlib import Path

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
    rides = pd.read_csv(input_file)
    rides = clean_column_names(rides)
    return rides


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
    # Join the routes back together
    return ", ".join(modified_routes)


def get_business_days(date):
    # Get the first day of the month
    first_day = dt.datetime(date.year, date.month, 1)
    # Get the last day of the month
    last_day = dt.datetime(
        date.year, date.month, calendar.monthrange(date.year, date.month)[1]
    )
    # Get the number of business days in the month
    business_days = pd.bdate_range(first_day, last_day).shape[0]
    return business_days


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
    # Calculate business days in the month
    rides["business_days"] = rides["date"].apply(get_business_days)
    # Normalize the ridership by the number of business days in the month
    rides["ridership_weekday"] = rides["ridership"] / rides["business_days"]
    # Get number of days in the month
    rides["num_days_in_month"] = rides["date"].apply(get_num_days_in_month)
    # Get ridership per day
    rides["ridership_per_day"] = (
        rides["ridership"] / rides["num_days_in_month"]
    )
    # Calculate change vs. previous years
    for i in range(1, 4):
        rides[f"change_vs_{i}_years_ago"] = (
            rides["ridership"]
            / rides.groupby("route")["ridership"].shift(i * 12)
            - 1
        )
    return rides


@task
def save_data(rides, output_file):
    # Save the cleaned data to a new CSV file
    rides.to_csv(output_file, index=False)


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
    node_script_path,
    input_path,
    output_path,
):
    # Check for directories
    check_for_directories()
    # Run Node.js script
    run_node_script(node_script_path)

    # Run data transformation tasks
    data = load_data(input_path)
    processed_data = process_data(data)
    save_data(processed_data, output_path)

if __name__ == "__main__":
    # Run the Flow
    node_script_path = Path("node/index.js")

    input_path = Path("data/raw/mta_bus_ridership.csv")
    output_path = Path("data/processed/mta_bus_ridership.csv")
    data_transform(
        node_script_path=node_script_path,
        input_path=input_path,
        output_path=output_path,
    )
