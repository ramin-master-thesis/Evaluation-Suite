import itertools
from time import sleep

import papermill as pm
import yaml
import os


if not os.path.exists("output/notebooks"):
    os.mkdir("output/notebooks")

### Parameters ###
with open("evaluation.yaml", "r") as file:
    params = yaml.load(file)

### Paths ###
user_sample_path = "data/users.json"

### Compute variants ###
salsa = params["salsa"]

### Samples ###
samples = 20


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
    print(f"calculating MAP@K for {hash_function}")
    pm.execute_notebook(
        "src/APK_MAPK.ipynb",
        f"output/notebooks/APK_MAPK_{app_id}.ipynb",
        parameters={
            "users": user_sample_path,
            "hash_function": hash_function,
            "should_merge": should_merge,
            "baseline_recommendations_path": "data/baseline_recommendations.json",
            "single_partition_recommendations_path": "data/single_recommendations.json",
            "recommendations_path": f"data/{hash_function}_recommendations.json",
            "merge_recommendations_path": f"data/{hash_function}_merge_recommendations.json",
            "output_map": f"data/{hash_function}_map@k.json",
            "output_diagram": f"output/{hash_function}_map@k.png"
        }
    )


def calculate_max_map_k(applications):
    hash_functions = [sub.get('hash_function') for sub in applications]
    hash_functions = list(filter(lambda x: x != "Single", hash_functions))
    print(f"calculating max MAP@K for {hash_functions}")
    pm.execute_notebook(
        "src/max_map_at_k.ipynb",
        f"output/notebooks/max_map_at_k_{hash_functions}.ipynb",
        parameters={
            "hash_functions": hash_functions,
            "data_folder": "data",
            "output_table_path": "output/map_10.png"
        }
    )


def main():
    sample_users()
    for application in params["applications"]:
        fetch_recommendations(application)
        if application["hash_function"] != "Single":
            calculate_map_k(application)
    calculate_max_map_k(params["applications"])


if __name__ == "__main__":
    main()
