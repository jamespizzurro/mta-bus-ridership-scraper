# MTA Bus Ridership Scraper

This project provides a comprehensive Python pipeline to process and analyze MTA bus ridership data. The pipeline includes data loading, cleaning, transformation, and storage, along with an integration of Node.js for additional tasks.

This is a fork of James Pizzurro's [MTA Bus Ridership Scraper](https://github.com/jamespizzurro/mta-bus-ridership-scraper) that essentially acts as a python wrapper to run the scraper and process the data.

The scraper is a Node.js script that uses Puppeteer to automate the process of downloading the MTA bus ridership data. The scraper is run as part of the data pipeline, and the data is processed and saved to a CSV file.

This fork adds steps to clean the data, standardize the names of the bus routes, convert dates to a standard format, and calculate the average daily ridership for each route by month.

## Data
The raw data is found at www.mta.maryland.gov/performance-improvement

After scaping the data, the raw data is stored in the `data/raw` directory. The processed data is stored in the `data/processed` directory.

`data/raw/mta_bus_ridership.csv` is not committed to the repo, but looks like this:
| **Date** | **Route** | **Ridership** |
|----------|-----------|---------------|
| 01/2023  | 103       | 3916          |
| 01/2023  | 105       | 3530          |
| 01/2023  | 115       | 4179          |
| 01/2023  | 120       | 3887          |
| 01/2023  | 150       | 1833          |

## Installation

To set up the project, run the `setup.sh` script. This script will check for Conda and Node.js installations, create a Conda environment, install Python dependencies, and set up the necessary directories.

```bash
./setup.sh
```

## Usage
To run the pipeline, execute main.py. This script orchestrates the data transformation process, including data loading, processing, and saving the processed data to a CSV file.
```bash
python main.py
```

## Main Components
`main.py`
- Data Cleaning: Standardizes column names in the data.
- Data Processing: Includes functions for route processing and date-related calculations.
- Data Transformation: Applies data processing tasks to the dataset.
- Data Saving: Outputs the processed data to a CSV file.
- Node.js Integration: Runs a Node.js script as part of the data pipeline.

`setup.sh`
- Sets up the Python environment.
- Installs Python and Node.js dependencies.
- Prepares the project directory structure.
## Dependencies
- Python 3.11
- Pandas
- Prefect (For running the pipeline)
- Node.js (To run the browser automation script)

## Project Structure
```
project/
│
├── main.py - Main script for running the data pipeline.
├── setup.sh - Script for setting up the environment and dependencies.
├── data/
│   ├── raw/ - Directory for storing raw data files.
│   └── processed/ - Directory for storing processed data.
└── node/
    └── index.js - Node.js script for scraping the MTA bus ridership data.
```

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## License
GNU General Public License v3.0

## Credits
James Pizzurro - [MTA Bus Ridership Scraper](https://github.com/jamespizzurro/mta-bus-ridership-scraper)
