#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# Define Python version and environment name
PYTHON_VERSION="3.11"
ENV_NAME="ridership"

# Check if Conda is installed
if ! command -v conda &> /dev/null
then
    echo "Conda could not be found. Please install Conda and rerun this script."
    exit 1
else
    echo "Conda is installed."
fi

# Setup Conda Environment
echo "Setting up Conda environment..."
conda create --name $ENV_NAME python=$PYTHON_VERSION -y
source activate $ENV_NAME

# Install Python dependencies from requirements.txt
echo "Installing Python dependencies..."
pip install -r requirements.txt

# Check if Node.js is installed and install it if it's missing
if ! command -v node &> /dev/null
then
    echo "Node.js could not be found, installing..."
    # The following command depends on your system (e.g., apt-get for Ubuntu, brew for macOS)
    sudo apt-get install nodejs
    sudo apt-get install npm
else
    echo "Node.js is already installed"
fi

# Create necessary directories
echo "Creating necessary directories..."
mkdir -p data/raw
mkdir -p data/processed

# (Optional) Add commands to place initial data files in data/raw directory
# e.g., cp /path/to/initial/data.csv data/raw/

echo "Setup completed successfully."
