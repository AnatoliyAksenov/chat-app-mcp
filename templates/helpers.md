### Function descriptions


```py
generate_spark_submit_command(
        dag_name,
        gitlab_token, 
        git_url="http://10.0.0.100/data-engineering/airflow.git",
        script_path="/dags/spark_task.py", 
        master_url="spark://spark-master:7077",
        spark_configs=None, 
        additional_files=None, 
        executor_memory="512m")
    """
    Generate a spark-submit command for Docker CMD with git clone integration
    
    Args:
        dag_name (str): Airflow dag id (dag name)
        gitlab_token (str): GitLab authentication token
        git_url (str): Git repository URL
        script_path (str): Path to the Spark script in the cloned repository
        master_url (str): Spark master URL
        spark_configs (dict): Dictionary of Spark configurations (overrides defaults and adds custom configs)
        additional_files (list): List of additional Python files to include
        executor_memory (str): Executor memory setting
    
    Returns:
        list: Docker CMD command as a list
    """

generate_spark_sql(
        dag_name,
        gitlab_token, 
        git_url="http://10.0.0.100/data-engineering/airflow.git",
        sql_file="/sql/customers.sql", 
        master_url="spark://spark-master:7077",
        spark_configs=None, 
        executor_memory="512m")
    """
    Generate a spark-submit command for Docker CMD with git clone integration
    
    Args:
        dag_name (str): Airflow dag id (dag name)
        gitlab_token (str): GitLab authentication token
        git_url (str): Git repository URL
        sql_file (str): Path to the sql script in the cloned repository
        master_url (str): Spark master URL
        spark_configs (dict): Dictionary of Spark configurations (overrides defaults and adds custom configs)
        additional_files (list): List of additional Python files to include
        executor_memory (str): Executor memory setting
    
    Returns:
        list: Docker CMD command as a list
    """


generate_dag_name(dag_name:str, file_path:str):
    """
    Generate an Airflow DAG name based on folder naming conventions.
    
    Args:
        dag_name (str): The original name of the DAG
        file_path (str): The full file path to the DAG Python file   
    """
```
