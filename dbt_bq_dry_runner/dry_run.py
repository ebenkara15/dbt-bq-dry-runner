from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Union

import yaml
from google.api_core.exceptions import BadRequest
from google.cloud.bigquery import Client, QueryJob, QueryJobConfig
from loguru import logger as logging


class BigQueryDryRunner:
    """A class to run dry run DBT models on BigQuery.

    Args:
        project (str): The Google Cloud project ID.
        dbt_target_dir (str, optional): The directory where DBT target files are located. Defaults to None.
        location (str, optional): The location of the BigQuery dataset. Defaults to "EU".

    Returns:
        BigQueryDryRunner: An instance of the BigQueryDryRunner class.
    """

    def __init__(self, project: str, dbt_target_dir: str = None, location="EU"):
        self.client = Client(project=project, location=location)
        self.dbt_project_config = self._get_dbt_project_config()
        self.root_path = Path(".").resolve()
        self.dbt_target_dir = self._get_dbt_target_dir(dbt_target_dir)

    def _trim_path_from(self, path: Path, parent_dir: str) -> Path:
        """Trim the path directory from the parent dir.

        Args:
            path (Path): The path to trim.
            parent_dir (str): The parent directory to trim from.

        Returns:
            pathlib.Path: A new Path object with only the filename and the parent folder(s) starting with parent_dir.

        Example:
            path = Path("path/to/parent_dir/child_dir/file.sql")
            parent_dir = "parent_dir"
            trim_path_from(path, parent_dir) -> Path("parent_dir/child_dir/file.sql")
        """
        parts = path.parts
        idx = parts.index(parent_dir)
        return Path(*parts[idx:])

    def _get_dbt_target_dir(self, target_dir: str = None) -> Path:
        """Return the dbt target directory path as a pathlib.Path object.

        Args:
            target_dir (str): The target directory to use. Defaults to None.

        Returns:
            pathlib.Path: The Path object representing the dbt target directory.
        """
        if not target_dir:
            return (
                self.root_path
                / "target"
                / "compiled"
                / self.dbt_project_config["name"]
                / "models"
            )
        else:
            return (
                self.root_path
                / "target"
                / target_dir
                / "compiled"
                / self.dbt_project_config["name"]
                / "models"
            )

    def _get_dbt_project_config(self, dbt_project_dir: str = None) -> dict[str, Any]:
        """Get the dbt project configuration from the dbt_project.yml file.

        Args:
            dbt_project_dir (str): The dbt project directory. Defaults to None.

        Returns:
            dict: The dbt project configuration as a dictionary.
        """
        dbt_project_dir = Path(dbt_project_dir) if dbt_project_dir else Path(".")
        with open(dbt_project_dir / "dbt_project.yml", "r") as f:
            return yaml.safe_load(f)

    def _get_compiled_models(self) -> list[Path]:
        """Get all compiled models in the dbt target directory.

        Returns:
            list: A list of pathlib.Path objects representing the compiled models
        """
        return [f for f in self.dbt_target_dir.rglob("*.sql") if f.is_file()]

    def run(self, sql: str, **opts) -> QueryJob:
        """Run a SQL query using the client and the dry run method.

        Args:
            sql (str): The SQL query to run.
            **opts: Additional options to pass to the QueryJobConfig object.

        Returns:
            QueryJob: The query job object.
        """
        job_config = QueryJobConfig(dry_run=True, **opts)

        try:
            results = self.client.query(sql, job_config=job_config)
            logging.info(f"Provided query is valid.")
            return results
        except BadRequest as e:
            logging.error(
                f"Error running the dry run for provided query. The sever returned an error: {e.errors[0]['message']}"
            )
            raise e

    def run_from_model(self, model_path: Union[str, Path], **opts) -> QueryJob:
        """Run a SQL query from a compiled DBT file using the client and the dry run method.

        Args:
            model_path (str): The path to the compiled model file.
            **opts: Additional options to pass to the QueryJobConfig object.

        Returns:
            QueryJob: The query job object.
        """
        if isinstance(model_path, str):
            model_path = Path(model_path)

        with open(str(model_path), "r") as f:
            sql = f.read()

        job_config = QueryJobConfig(dry_run=True, **opts)
        try:
            results = self.client.query(sql, job_config=job_config)
            logging.info(f"Model {self._trim_path_from(model_path, 'models')} is valid.")
            return results
        except BadRequest as e:
            logging.error(
                f"Error running the dry run for model {self._trim_path_from(model_path, 'models')}. The sever returned an error: {e.errors[0]['message']}"
            )
            raise e

    def run_all_models(self) -> None:
        """Run all compiled models in the dbt target directory.

        This method uses a ThreadPoolExecutor to run all models concurrently.
        """
        dbt_compiled_models = self._get_compiled_models()

        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(self.run_from_model, str(node))
                for node in dbt_compiled_models
            ]

            for future in as_completed(futures):
                future.result()

    def run_from_model_name(self, model_name: str, **opts) -> None:
        raise NotImplementedError(
            "This method is not implemented yet for BigQueryDryRunner"
        )


if __name__ == "__main__":
    runner = BigQueryDryRunner(project="c4-links-ecollab-dev")

    runner.run_all_models()
