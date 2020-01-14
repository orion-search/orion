from unittest import TestCase
from orion.core.airflow_utils.misctools import get_config
from orion.core.airflow_utils.misctools import find_filepath_from_pathstub


class TestMiscTools(TestCase):
    def test_get_config(self):
        get_config("orion_config.config", "postgresdb")
        with self.assertRaises(KeyError):
            get_config("mysqldb.config", "invalid")
        with self.assertRaises(KeyError):
            get_config("not_found.config", "mysqldb")

    def test_find_filepath_from_pathstub(self):
        find_filepath_from_pathstub("orion/packages")
        with self.assertRaises(FileNotFoundError):
            find_filepath_from_pathstub("orion/package")
