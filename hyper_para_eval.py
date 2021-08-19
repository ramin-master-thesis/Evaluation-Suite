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
            "union_results": True,
            "highest_hit": True,
            "most_interactions": True,
            "output_recommendations": f"data/{app_id}_recommendations.json",
            "output_union_results": f"data/{app_id}_union_results.json",
            "output_highest_hit": f"data/{hash_function}_highest_hit.json",
            "output_most_interactions": f"data/{hash_function}_most_interactions.json",
            "output_latency": f"output/{app_id}_latency.csv"
        }
    )


def compute_recommendation_quality():
    ### Fetch recommendations from API ###
    print(f"calculating MAP@K for {hash_function}")
    metric = "MAP"
    pm.execute_notebook(
        "src/compute_recommendation_quality.ipynb",
        f"output/notebooks/compute_recommendation_quality_{app_id}.ipynb",
        parameters={
            "users": user_sample_path,
            "hash_function": hash_function,
            "metric": metric,
            "show_union_results": True,
            "show_highest_hit": True,
            "show_most_interactions": True,
            "baseline_recommendations_path": "data/baseline_recommendations.json",
            "single_partition_recommendations_path": "data/single_recommendations.json",
            "recommendations_path": f"data/{hash_function}_recommendations.json",
            "union_results_path": f"data/{hash_function}_union_results.json",
            "highest_hit_path": f"data/{hash_function}_highest_hit.json",
            "most_interactions_path": f"data/{hash_function}_most_interactions.json",
            "output_map": f"data/{hash_function}_map@k.json",
            "output_diagram": f"output/{hash_function}_{metric}@k.png",
            "output_metric_at_k": f"output/{hash_function}_{metric}@k.csv"
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
    compute_recommendation_quality()
    get_stats()


if __name__ == "__main__":
    main()
