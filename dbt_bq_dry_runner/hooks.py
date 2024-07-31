import sys

from dry_run import BigQueryDryRunner


def dry_run_incremental_models():
    dry_runner = BigQueryDryRunner(
        project="c4-links-ecollab-dev",
        dbt_target_dir="incremental",
    )

    dry_runner.run_all_models()


def dry_run_full_refresh_models():
    print("Running full refresh models")
    dry_runner = BigQueryDryRunner(
        project="c4-links-ecollab-dev",
        dbt_target_dir="full_refresh",
    )

    dry_runner.run_all_models()


if __name__ == "__main__":
    dry_run_incremental_models()
    if sys.argv[1] == "incremental":
        dry_run_incremental_models()
    elif sys.argv[1] == "full_refresh":
        dry_run_full_refresh_models()
    else:
        print("Please provide a valid argument: `incremental` or `full_refresh`")
        sys.exit(1)
