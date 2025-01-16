from pyspark.sql import SparkSession
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.profiler.dlt_generator import DQDltGenerator
from databricks.sdk import WorkspaceClient
import yaml
from databricks.labs.dqx.col_functions import is_not_null, is_not_null_and_not_empty, value_is_in_list
from databricks.labs.dqx.engine import DQEngine, DQRuleColSet, DQRule


def main(spark: SparkSession):
    df = spark.read.csv('s3://confessions-of-a-data-guy/trips/202412-divvy-tripdata.csv', header=True)
    df.display()

    # the default stats 
    ws = WorkspaceClient()
    profiler = DQProfiler(ws)
    summary_stats, profiles = profiler.profile(df)
    print(summary_stats)
    print(profiles)

    # generate the default validation rules
    generator = DQGenerator(ws)
    checks = generator.generate_dq_rules(profiles)  # with default level "error"
    print(yaml.safe_dump(checks))


    dq_engine = DQEngine(WorkspaceClient())

    checks = DQRuleColSet( # define rule for multiple columns at once
            columns=["rideable_type", "ride_id"], 
            criticality="error", 
            check_func=is_not_null).get_rules() + \
             [
                DQRule( # define rule for a single column
                name='started_at_is_null_or_empty',
                criticality='error', 
                check=is_not_null_and_not_empty('started_at')),
                DQRule( # name auto-generated if not provided       
                    criticality='warn', 
                    check=value_is_in_list('rideable_type', ['classic_bike', 'electric_bike']))
        ]

    # Option 1: apply quality rules on the dataframe and provide valid and invalid (quarantined) dataframes 
    valid_df, quarantined_df = dq_engine.apply_checks_and_split(df, checks)

    # Option 2: apply quality rules on the dataframe and report issues as additional columns (`_warning` and `_error`)
    valid_and_quarantined_df = dq_engine.apply_checks(df, checks)


if __name__ == '__main__':
    main()