from prefect import task
import pandas as pd
import datetime as dt
import calendar
import re


def clean_column_names(df):
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
    return df


@task
def load_data(input_file):
    rides = pd.read_csv(input_file)
    rides = clean_column_names(rides)
    return rides



def process_routes(served_routes):
    # Split routes into a list, strip whitespace, and capitalize CityLink
    modified_routes = [
        re.sub(
            "CityLink ([A-Z]+)",
            lambda match: "CityLink " + match.group(1).title(),
            route.strip(),
        )
        for route in served_routes.split(",")
    ]
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


@task
def process_data(rides):
    # process routes
    rides["route"] = rides["route"].apply(process_routes)
    # Convert date column to datetime format
    rides["date"] = pd.to_datetime(rides["date"], format="%m/%Y")
    rides["date_end"] = rides["date"] + pd.offsets.MonthEnd(0)
    # Group the data by route and date
    rides = rides.groupby(["route", "date","date_end"]).sum().reset_index()
    # Drop rows where ridership is 0
    rides = rides[rides["ridership"] > 0]
    # Calculate business days in the month
    rides["business_days"] = rides["date"].apply(get_business_days)
    # Normalize the ridership by the number of business days in the month
    rides["ridership_weekday"] = rides["ridership"] / rides["business_days"]
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

