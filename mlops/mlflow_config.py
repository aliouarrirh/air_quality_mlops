import mlflow
import os
import logging
def setup_mlflow(experiment_name="Casa_AirQuality"):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)

    db_path = os.path.join(project_root,"mlruns.db")
    db_path_uri = db_path.replace("\\", "/")
    tracking_uri = f"sqlite:///{db_path_uri}"

    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(experiment_name)

    logging.info(f"mlflow connected ! experiment:{experiment_name} | BDD: {db_path}")

    return tracking_uri

