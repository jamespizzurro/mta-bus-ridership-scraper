import subprocess
from prefect import task
from prefect.flows import Flow
from src.data_transformation import load_data, process_data, save_data
from pathlib import Path

# from prefect import Client


@task
def run_node_script(script_path):
    try:
        subprocess.run(["node", script_path], check=True)
    except Exception as e:
        print(f"Failed to run Node.js script: {e}")
        return False
    return True


# Define the Flow
@Flow
def data_transform(
    node_script_path,
    input_path,
    output_path,
):
    # Run Node.js script
    run_node_script(node_script_path)

    # Run data transformation tasks
    data = load_data(input_path)
    processed_data = process_data(data)
    save_data(processed_data, output_path)


# Run the Flow
node_script_path = Path("node/index.js")
input_path = Path("data/raw/mta_bus_ridership.csv")
output_path = Path("data/processed/mta_bus_ridership.csv")
data_transform(
    node_script_path=node_script_path,
    input_path=input_path,
    output_path=output_path,
)
