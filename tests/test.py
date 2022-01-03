import os
import unittest
from pathlib import Path
from unittest.mock import patch
import sys
import pkg_resources
import shutil

import invoke

from dbt_invoke import properties
from dbt_invoke.internal import _utils

PARENT_DIR = Path(__file__).parent


class TestDbtInvoke(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """
        Overrides unittest.TestCase.setUpClass

        :return: None
        """
        cls.logger = _utils.get_logger('dbt-invoke', level='DEBUG')
        cls.config_path = Path(PARENT_DIR, 'test_config.yml')
        cls.config = _utils.parse_yaml(cls.config_path)

        # for backward compatibility, select the correct dbt_project.yml file
        if pkg_resources.get_distribution("dbt-core").version >= '1.0.0':
            shutil.copy(
                Path(PARENT_DIR, 'dbt_project_files/dbt_project.yml'),
                Path(
                    PARENT_DIR, cls.config['project_name'], 'dbt_project.yml'
                ),
            )
        else:
            shutil.copy(
                Path(
                    PARENT_DIR, 'dbt_project_files/dbt_project_pre_dbt_v1.yml'
                ),
                Path(
                    PARENT_DIR, cls.config['project_name'], 'dbt_project.yml'
                ),
            )

        cls.project_dir = Path(PARENT_DIR, cls.config['project_name'])
        cls.profiles_dir = Path(PARENT_DIR, cls.config['project_name'])
        cls.expected_properties = cls.config['expected_properties']
        cls.expected_dbt_ls_results = cls.config['expected_dbt_ls_results']
        cls.ctx = invoke.Context()
        _utils.get_project_info(cls.ctx, project_dir=cls.project_dir)
        cls.macro_name = '_log_columns_list'
        cls.macro_value = _utils.MACROS[cls.macro_name]
        cls.macro_path = Path(
            cls.ctx.config['macro_paths'][0],
            f'{cls.macro_name}.sql',
        )
        cls.dbt_clean = (
            'dbt clean'
            f' --project-dir {cls.project_dir}'
            f' --profiles-dir {cls.project_dir}'
        )
        cls.dbt_compile = (
            'dbt compile'
            f' --project-dir {cls.project_dir}'
            f' --profiles-dir {cls.project_dir}'
        )
        invoke.run(cls.dbt_clean)
        invoke.run(cls.dbt_compile)

    def setUp(self):
        """
        Overrides unittest.TestCase.setUp

        :return: None
        """
        if self.macro_path.exists():
            os.remove(self.macro_path)
        with patch('builtins.input', return_value='y'):
            properties.delete(
                self.ctx,
                project_dir=self.project_dir,
                profiles_dir=self.profiles_dir,
            )

    def tearDown(self):
        """
        Overrides unittest.TestCase.tearDown

        :return: None
        """
        if self.macro_path.exists():
            os.remove(self.macro_path)

    @classmethod
    def tearDownClass(cls):
        """
        Overrides unittest.TestCase.tearDownClass

        :return: None
        """
        invoke.run(cls.dbt_clean)


if __name__ == '__main__':
    loader = unittest.TestLoader()
    suite = loader.discover(PARENT_DIR)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(not result.wasSuccessful())
