import os
from pathlib import Path
from typing import Dict, List, Union, Optional

from airflow.operators.bash_operator import BashOperator


class WaterDropOperator(BashOperator):
    ui_color = "#2D75D2"

    def __init__(
            self,
            execute_path: str,
            config_save_path: str,
            config_save_name: str,
            *,
            spark_master: Optional[str] = None,
            spark_deploy_mode: Optional[str] = None,
            spark_deploy_queue: Optional[str] = None,
            app_name: Optional[str] = None,
            executor_instances: Union[str, int] = None,
            executor_cores: Union[str, int] = None,
            executor_memory: Optional[str] = None,
            sql_catalog: Optional[str] = None,
            dynamic_partition: bool = False,
            input_config: Optional[Dict] = None,
            filter_config: Optional[Dict] = None,
            output_config: Optional[Dict] = None,
            bash_env: Optional[Dict] = None,
            bash_output_encoding: str = "utf-8",
            **kwargs,
    ):
        """
        Feature:
        1、Use Spark configuration map instead of Class Arguments
        """
        self._execute_path = execute_path
        self._config_save_path = config_save_path
        self._config_save_name = config_save_name
        self._spark_master = spark_master or "local"
        self._spark_deploy_mode = spark_deploy_mode or "client"
        self._spark_queue_name = spark_deploy_queue or "default"
        self._app_name = f"spark.app.name = \"{app_name or 'waterdrop-operator'}\""
        self._executor_instances = (
            f"spark.executor.instances = {executor_instances or 1}"
        )
        self._executor_cores = f"spark.executor.cores = {executor_cores or 2}"
        self._executor_memory = f"spark.executor.memory = \"{executor_memory or '2g'}\""
        self._sql_catalog = f'spark.sql.catalogImplementation = "{sql_catalog or None}"'
        self._dynamic_partition = dynamic_partition
        if self._dynamic_partition:
            self._dynamic_partition_config = (
                f'spark.hadoop.hive.exec.dynamic.partition = "true"'
            )
            self._dynamic_partition_mode = (
                f'spark.hadoop.hive.exec.dynamic.partition.mode = "nonstrict"'
            )

        self._input_config = input_config or {}
        self._filter_config = filter_config or {}
        self._output_config = output_config or {}

        # Generate Command
        self._execute_config = None
        command = self._generate_execute_command()
        # Call BashOperator
        super(WaterDropOperator, self).__init__(
            bash_command=command,
            env=bash_env,
            output_encoding=bash_output_encoding,
            **kwargs,
        )

    def pre_execute(self, context):
        # Generate WaterDrop Configuration
        self._execute_config = self._generate_config()
        print(f"Config Files Content: {self._execute_config}")
        # Save WaterDrop Configuration
        self._save_config()
        self._check_execute_path()

    # Generate whole config
    def _generate_config(self) -> str:
        config = """spark {spark_config}

input {input_config}

filter {filter_config}

output {output_config}"""
        config = config.format(
            spark_config=self._generate_spark_config(),
            input_config=self._generate_input_config(),
            filter_config=self._generate_filter_config(),
            output_config=self._generate_output_config(),
        )
        return config

    @staticmethod
    def _core_generate_config(generate_config: Union[List[Dict], Dict]):
        config = ""

        def _inner_generate(generate_cfg: Dict):
            cfg = ""
            for k, v in generate_cfg.items():
                if isinstance(v, dict):
                    inner_config = ""
                    for inner_key, inner_value in v.items():
                        inner_config += f'\t{inner_key} = "{inner_value}"\n\t'
                    cfg += f"{k} " + "{\n\t" + inner_config + "}"
                else:
                    cfg += f"{k}={v}"
            return cfg

        # 兼容如果传入的 config 是一个列表的情况
        if isinstance(generate_config, Dict):
            config = _inner_generate(generate_cfg=generate_config)
        elif isinstance(generate_config, List):
            suffix = ",\n\t"
            for i, c in enumerate(generate_config):
                if i == len(generate_config) - 1:
                    suffix = ""
                config += f"{_inner_generate(generate_cfg=c)}{suffix}"

        return "{\n\t" + config + "\n}"

    # Generate Spark Config
    def _generate_spark_config(self):
        if self._dynamic_partition:
            return (
                    "{" + f"\n\t{self._app_name}"
                          f"\n\t{self._executor_instances}"
                          f"\n\t{self._executor_cores}"
                          f"\n\t{self._executor_memory}"
                          f"\n\t{'' if 'None' in self._sql_catalog else self._sql_catalog}"
                    + f"\n\t{self._dynamic_partition_config}"
                    + f"\n\t{self._dynamic_partition_mode}"
                    + "\n}"
            )
        else:
            return (
                    "{" + f"\n\t{self._app_name}"
                          f"\n\t{self._executor_instances}"
                          f"\n\t{self._executor_cores}"
                          f"\n\t{self._executor_memory}"
                          f"\n\t{'' if 'None' in self._sql_catalog else self._sql_catalog}"
                    + "\n}"
            )

    # Generate Input Config
    def _generate_input_config(self):
        return self._core_generate_config(generate_config=self._input_config)

    # Generate Filter Config
    def _generate_filter_config(self):
        return self._core_generate_config(generate_config=self._filter_config)

    # Generate Output Config
    def _generate_output_config(self):
        return self._core_generate_config(generate_config=self._output_config)

    # Save generate config
    def _save_config(self):
        if self._config_save_path == "":
            self._config_save_path = os.getcwd()
        path = Path(f"{self._config_save_path}/{self._config_save_name}")
        if path.exists():
            os.remove(path)
        with open(path, "w") as f:
            f.write(self._execute_config)

    # Check start-waterdrop.sh exist
    def _check_execute_path(self):
        path = Path(self._execute_path)
        if path.is_file() is not True:
            raise RuntimeError("Waterdrop 启动脚本不存在!")

    # Generate execute command
    def _generate_execute_command(self) -> str:
        command = (
            f"source /etc/profile && "
            f"{self._execute_path} "
            f"--master {self._spark_master} "
            f"--deploy-mode {self._spark_deploy_mode} "
            f"--queue {self._spark_queue_name} "
            f"--config {self._config_save_path}/{self._config_save_name}"
        )
        return command
