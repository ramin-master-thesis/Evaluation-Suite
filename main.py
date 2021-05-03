import os
import sys

import papermill as pm
import yaml
from yaml import SafeLoader


def sample_users():
    print(f"sampling {samples} users")
    pm.execute_notebook(
        "src/sample_users.ipynb",
        "output/notebooks/sample_users.ipynb",
        parameters={
            "data": params["dataset"]["path"],
            "output": user_sample_path,
            "sample": samples,
        }
    )


def fetch_recommendations(application):
    ### Fetch recommendations from API ###
    app_id = application["id"]
    hash_function = application["hash_function"]
    should_merge = bool(application["merge"])
    partition_port = application["partition_port"]
    print(f"fetch recommendations for {hash_function}, with partition port {partition_port}")
    pm.execute_notebook(
        "src/query_recommendations.ipynb",
        f"output/notebooks/query_{app_id}_recommendations.ipynb",
        parameters={
            "users": user_sample_path,
            "hash_function": hash_function,
            "partition_port": partition_port,
            "should_merge": should_merge,
            "output_recommendations": f"data/{app_id}_recommendations.json",
            "output_merge_recommendations": f"data/{app_id}_merge_recommendations.json"
        }
    )


def calculate_map_k(application):
    ### Fetch recommendations from API ###
    app_id = application["id"]
    hash_function = application["hash_function"]
    should_merge = bool(application["merge"])
    show_partitions = bool(application["show_partitions"])
    print(f"calculating MAP@K for {hash_function}")
    pm.execute_notebook(
        "src/APK_MAPK.ipynb",
        f"output/notebooks/APK_MAPK_{app_id}.ipynb",
        parameters={
            "users": user_sample_path,
            "hash_function": hash_function,
            "should_merge": should_merge,
            "show_partitions": show_partitions,
            "baseline_recommendations_path": "data/baseline_recommendations.json",
            "single_partition_recommendations_path": "data/single_recommendations.json",
            "recommendations_path": f"data/{hash_function}_recommendations.json",
            "merge_recommendations_path": f"data/{hash_function}_merge_recommendations.json",
            "output_map": f"data/{hash_function}_map@k.json",
            "output_diagram": f"output/{hash_function}_map@k.png",
            "output_map_at_10": f"output/{hash_function}_map@10.csv"
        }
    )


def get_stats(application):
    app_id = application["id"]
    hash_function = application["hash_function"]
    should_merge = bool(application["merge"])
    partition_port = application["partition_port"]
    print(f"getting stats for {hash_function}")
    pm.execute_notebook(
        "src/partition_stats.ipynb",
        f"output/notebooks/partition_stats_{app_id}.ipynb",
        parameters={
            "hash_function": hash_function,
            "should_merge": should_merge,
            "partition_port": partition_port,
            "output_status": f"output/{hash_function}_status.csv"
        }
    )


def main():
    if should_sample:
        sample_users()
    for application in params["applications"]:
        get_stats(application)
        fetch_recommendations(application)
        if application["calculate_map_k"]:
            calculate_map_k(application)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Please provide a yaml config file. Python3 -m main <path-to-config-file.yaml>")
        exit(1)

    if not os.path.exists("output/notebooks"):
        os.mkdir("output/notebooks")

    ### Parameters ###
    path_to_config_file = sys.argv[1]
    print(f"loading config file from {path_to_config_file}")
    with open(path_to_config_file, "r") as file:
        params = yaml.load(file, Loader=SafeLoader)

    ### Paths ###
    user_sample_path = "data/users.json"

    ### Samples ###
    samples = int(params["dataset"]["sample_size"])
    should_sample = bool(params["dataset"]["should_sample"])

    ### Compute variants ###
    salsa = params["salsa"]

    main()
