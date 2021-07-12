import os

import papermill as pm

if not os.path.exists("output/notebooks"):
    os.mkdir("output/notebooks")

### configs ###
user_sample_path = "data/users.json"
app_id = "StarSpace"
hash_function = "StarSpace"
should_merge = True
partition_port = {"0": "5002", "1": "5003"}


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
            "should_merge_1": True,
            "should_merge_2": True,
            "should_merge_3": True,
            "output_latency": f"data/{app_id}_latency.json",
            "output_recommendations": f"data/{app_id}_recommendations.json",
            "output_merge_recommendations": f"data/{app_id}_merge_recommendations.json",
            "output_best_partition_recommendations": f"data/{hash_function}_best_partition_recommendations.json",
            "output_highest_degree_recommendations": f"data/{hash_function}_highest_degree_recommendations.json"
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
            "should_merge_1": True,
            "should_merge_2": True,
            "should_merge_3": True,
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


def get_stats():
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
    fetch_star_space_recommendations()
    calculate_map_k()
    get_stats()


if __name__ == "__main__":
    main()
