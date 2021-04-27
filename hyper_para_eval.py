import os

import papermill as pm

if not os.path.exists("output/notebooks"):
    os.mkdir("output/notebooks")

### configs ###
user_sample_path = "data/users.json"
app_id = "StarSpace"
hash_function = "StarSpace"
should_merge = True
partition_port = {"0": "5002", "1": "5003", "2": "5004", "3": "5005"}


def fetch_star_space_recommendations():
    ### Fetch recommendations from API ###
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


def calculate_map_k():
    ### Fetch recommendations from API ###
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
            "output_diagram": f"output/{hash_function}_map@k.png",
            "output_map_at_10": f"output/{hash_function}_map@10.csv"
        }
    )


def main():
    fetch_star_space_recommendations()
    calculate_map_k()


if __name__ == "__main__":
    main()
