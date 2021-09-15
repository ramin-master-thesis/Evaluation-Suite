import os
import sys

import papermill as pm
import yaml
from yaml import SafeLoader

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
Metric = {
    "MAP": "Mean Average Precision",
    "RBO": "Ranked-Biased Overlap",
}


def sample_users():
    samples = int(dataset["sample_size"])
    min_interactions = dataset["min_interactions"]
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
    repeat_for_user = int(params["baseline"]["repeat"])
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


def fetch_recommendations(app):
    ### Fetch recommendations from API ###
    app_id = app["id"]
    union_results = bool(app["union_results"])
    highest_hit = bool(app["highest_hit"])
    most_interactions = bool(app["most_interactions"])
    partition_port = app["partition_port"]
    print(f"fetch recommendations for {hash_function}, with partition port {partition_port}")
    pm.execute_notebook(
        "src/query_recommendations.ipynb",
        f"output/notebooks/query_{app_id}_recommendations.ipynb",
        parameters={
            "users": user_sample_path,
            "hash_function": hash_function,
            "partition_port": partition_port,
            "union_results": union_results,
            "highest_hit": highest_hit,
            "most_interactions": most_interactions,
            "output_recommendations": f"data/{app_id}_recommendations.json",
            "output_union_results": f"data/{app_id}_union_results.json",
            "output_highest_hit": f"data/{hash_function}_highest_hit.json",
            "output_most_interactions": f"data/{hash_function}_most_interactions.json",
            "output_latency": f"{save_folder}/{app_id}_latency.csv"
        }
    )


def compute_recommendation_quality(metric):
    app_id = application["id"]
    show_partitions = bool(application["show_partitions"])
    show_union_results = bool(application["union_results"])
    show_highest_hit = bool(application["highest_hit"])
    show_most_interactions = bool(application["most_interactions"])
    print(f"calculating {metric}@K for {hash_function}")
    pm.execute_notebook(
        "src/compute_recommendation_quality.ipynb",
        f"output/notebooks/compute_recommendation_quality_{app_id}.ipynb",
        parameters={
            "users": user_sample_path,
            "hash_function": hash_function,
            "metric": metric,
            "show_partitions": show_partitions,
            "show_union_results": show_union_results,
            "show_highest_hit": show_highest_hit,
            "show_most_interactions": show_most_interactions,
            "baseline_recommendations_path": "data/baseline_recommendations.json",
            "recommendations_path": f"data/{hash_function}_recommendations.json",
            "union_results_path ": f"data/{hash_function}_union_results.json",
            "highest_hit_path": f"data/{hash_function}_highest_hit.json",
            "most_interactions_path": f"data/{hash_function}_most_interactions.json",
            "output_diagram": f"{save_folder}/{hash_function}_{metric}@k.png",
            "output_metric_at_k": f"{save_folder}/{hash_function}_{metric}@k.csv"
        }
    )


def get_stats():
    app_id = application["id"]
    partition_port = application["partition_port"]
    print(f"getting stats for {hash_function}")
    pm.execute_notebook(
        "src/partition_stats.ipynb",
        f"output/notebooks/partition_stats_{app_id}.ipynb",
        parameters={
            "hash_function": hash_function,
            "partition_port": partition_port,
            "output_status": f"{save_folder}/{hash_function}_status.csv"
        }
    )


def main():
    if should_sample:
        sample_users()
    if should_generate_baseline:
        generate_baseline()

    fetch_recommendations(application)
    get_stats()
    for metric in application["metrics"]:
        compute_recommendation_quality(metric)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Please provide a yaml config file. Python3 -m main <path-to-config-file.yaml>")
        exit(1)

    if not os.path.exists(f"{ROOT_DIR}/output/notebooks"):
        os.mkdir(f"{ROOT_DIR}/output/notebooks")

    ### Parameters ###
    path_to_config_file = sys.argv[1]
    print(f"loading config file from {path_to_config_file}")
    with open(path_to_config_file, "r") as file:
        params = yaml.load(file, Loader=SafeLoader)

    ### Paths ###
    user_sample_path = "data/users.json"

    ### Samples ###
    dataset = params.get("dataset", {"should_sample": False})
    should_sample = bool(dataset.get("should_sample"))

    ### Compute variants ###
    salsa = params["salsa"]

    ### Baseline ###
    baseline = params.get("baseline", {"should_generate_baseline": False})
    should_generate_baseline = bool(baseline.get("should_generate_baseline"))

    application = params["application"]
    hash_function = application["hash_function"]
    save_folder = f"{ROOT_DIR}/output/{hash_function}"

    if not os.path.exists(save_folder):
        os.mkdir(save_folder)

    main()
