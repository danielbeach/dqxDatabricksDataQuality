### Testing out DQX Data Quality from Databricks Labs

Recently `dqx` came out as a PySpark DataFrame Data Quality library developed by
Databricks Labs. This codebase was written in conjunction with a Substack
article on this topic.

https://dataengineeringcentral.substack.com/p/data-quality-with-databricks-labs

<img src="https://github.com/danielbeach/dqxDatabricksDataQuality/blob/main/imgs/dqx.png" width="300">

DQX provides the following main features for PySpark Dataframes

```
1. Data profiling and generate quality rule candidates with stats
2. Ability to define more checks and validations as code or config
3. Set criticality level and quarantine bad data
4. Works in a batch of streaming system
5. Provide a Dashboard
```

See code example for more detail.
