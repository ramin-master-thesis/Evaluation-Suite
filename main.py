import os
import sys

import papermill as pm
import yaml
from yaml import SafeLoader

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

def sample_users():
    min_interactions = params["dataset"]["min_interactions"]
    print(f"sampling {samples} users with min {min_interactions}")
    pm.execute_notebook(
        "src/sample_users.ipynb",
        "output/notebooks/sample_users.ipynb",
        parameters={
            "data": params["dataset"]["path"],
            "min_interactions": min_interactions,
            "output": user_sample_path,
            "sample": samples,
        }
    )


def generate_baseline():
    ### Fetch baseline from API ###
    port = 5000
    repeat_for_user = 10
    print(f"generating baseline with repeat for single user is {repeat_for_user}")
    pm.execute_notebook(
        "src/query_baseline.ipynb",
        f"output/notebooks/query_baseline.ipynb",
        parameters={
            "users": user_sample_path,
            "port": port,
            "repeat_for_user": repeat_for_user,
            "output_baseline_recommendations": f"data/baseline_recommendations.json"
        }
    )


def fetch_recommendations(application):
    ### Fetch recommendations from API ###
    app_id = application["id"]
    hash_function = application["hash_function"]
    should_merge_1 = bool(application["merge_1"])
    should_merge_2 = bool(application["merge_2"])
    should_merge_3 = bool(application["merge_3"])
    partition_port = application["partition_port"]
    print(f"fetch recommendations for {hash_function}, with partition port {partition_port}")
    pm.execute_notebook(
        "src/query_recommendations.ipynb",
        f"output/notebooks/query_{app_id}_recommendations.ipynb",
        parameters={
            "users": user_sample_path,
            "hash_function": hash_function,
            "partition_port": partition_port,
            "should_merge_1": should_merge_1,
            "should_merge_2": should_merge_2,
            "should_merge_3": should_merge_3,
            "output_latency": f"data/{app_id}_latency.json",
            "output_recommendations": f"data/{app_id}_recommendations.json",
            "output_merge_recommendations": f"data/{app_id}_merge_recommendations.json",
            "output_best_partition_recommendations": f"data/{hash_function}_best_partition_recommendations.json",
            "output_highest_degree_recommendations": f"data/{hash_function}_highest_degree_recommendations.json"
        }
    )


def calculate_map_k(application):
    app_id = application["id"]
    hash_function = application["hash_function"]
    show_partitions = bool(application["show_partitions"])
    show_merge_1 = bool(application["merge_1"])
    show_merge_2 = bool(application["merge_2"])
    show_merge_3 = bool(application["merge_3"])
    print(f"calculating MAP@K for {hash_function}")
    pm.execute_notebook(
        "src/APK_MAPK.ipynb",
        f"output/notebooks/APK_MAPK_{app_id}.ipynb",
        parameters={
            "users": user_sample_path,
            "hash_function": hash_function,
            "show_partitions": show_partitions,
            "show_merge_1": show_merge_1,
            "show_merge_2": show_merge_2,
            "show_merge_3": show_merge_3,
            "baseline_recommendations_path": "data/baseline_recommendations.json",
            "single_partition_recommendations_path": "data/single_recommendations.json",
            "recommendations_path": f"data/{hash_function}_recommendations.json",
            "merge_recommendations_path": f"data/{hash_function}_merge_recommendations.json",
            "best_partition_recommendations_path": f"data/{hash_function}_best_partition_recommendations.json",
            "highest_degree_recommendations_path": f"data/{hash_function}_highest_degree_recommendations.json",
            "output_map": f"data/{hash_function}_map@k.json",
            "output_diagram": f"output/{hash_function}_map@k.png",
            "output_map_at_k": f"output/{hash_function}_map@k.csv"
        }
    )


def calculate_rbo_k(application):
    app_id = application["id"]
    hash_function = application["hash_function"]
    show_partitions = bool(application["show_partitions"])
    show_merge_1 = bool(application["merge_1"])
    show_merge_2 = bool(application["merge_2"])
    show_merge_3 = bool(application["merge_3"])
    print(f"calculating RBO@K for {hash_function}")
    pm.execute_notebook(
        "src/RBO.ipynb",
        f"output/notebooks/RBO_{app_id}.ipynb",
        parameters={
            "users": user_sample_path,
            "hash_function": hash_function,
            "show_partitions": show_partitions,
            "show_merge_1": show_merge_1,
            "show_merge_2": show_merge_2,
            "show_merge_3": show_merge_3,
            "baseline_recommendations_path": "data/baseline_recommendations.json",
            "single_partition_recommendations_path": "data/single_recommendations.json",
            "recommendations_path": f"data/{hash_function}_recommendations.json",
            "merge_recommendations_path": f"data/{hash_function}_merge_recommendations.json",
            "best_partition_recommendations_path": f"data/{hash_function}_best_partition_recommendations.json",
            "highest_degree_recommendations_path": f"data/{hash_function}_highest_degree_recommendations.json",
            "output_rbo": f"data/{hash_function}_rbo@k.json",
            "output_diagram": f"output/{hash_function}_rbo@k.png",
            "output_rbo_at_k": f"output/{hash_function}_rbo@k.csv"
        }
    )


def get_stats(application):
    app_id = application["id"]
    hash_function = application["hash_function"]
    partition_port = application["partition_port"]
    print(f"getting stats for {hash_function}")
    pm.execute_notebook(
        "src/partition_stats.ipynb",
        f"output/notebooks/partition_stats_{app_id}.ipynb",
        parameters={
            "hash_function": hash_function,
            "partition_port": partition_port,
            "output_status": f"output/{hash_function}_status.csv"
        }
    )


def main():
    if should_sample:
        sample_users()
    if should_generate_baseline:
        generate_baseline()
    for application in params["applications"]:
        fetch_recommendations(application)
        get_stats(application)
        if application["calculate_map_k"]:
            calculate_map_k(application)
            calculate_rbo_k(application)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Please provide a yaml config file. Python3 -m main <path-to-config-file.yaml>")
        exit(1)

    if not os.path.exists(f"{ROOT_DIR}/output/notebooks"):
        os.makedirs(f"{ROOT_DIR}/output/notebooks")

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

    ### Baseline ###
    should_generate_baseline = bool(params["baseline"]["should_generate_baseline"])

    main()
