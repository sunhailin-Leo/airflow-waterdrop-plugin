import unittest
from unittest import mock

from airflow_waterdrop_plugin.operators.waterdrop_operator import WaterDropOperator


class WaterDropOperatorTestCase(unittest.TestCase):
    def test(self):
        test_app_name = "waterdrop_test"
        op = WaterDropOperator(
            task_id="_",
            execute_path="/data/waterdrop/waterdrop-1.5.1/bin/start-waterdrop.sh",
            config_save_path="/waterdrop/config",
            config_save_name="etl.config",
            app_name=test_app_name,
        )
        config_result = op._generate_config()
        self.assertIn(test_app_name, config_result, msg="test_app_name is in config_result")


if __name__ == '__main__':
    unittest.main()
