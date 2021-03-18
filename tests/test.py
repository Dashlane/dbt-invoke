import os
import unittest
from pathlib import Path
from unittest.mock import patch

import invoke

from dbt_invoke import utils, properties

PARENT_DIR = Path(__file__).parent


class TestDbtInvoke(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """
        Overrides unittest.TestCase.setUpClass
        :return: None
        """
        cls.logger = utils.get_logger('dbt-invoke', level='DEBUG')
        cls.config_path = Path(PARENT_DIR, 'test_config.yml')
        cls.config = utils.parse_yaml(cls.config_path)
        cls.project_dir = Path(PARENT_DIR, cls.config['project_name'])
        cls.profiles_dir = Path(PARENT_DIR, cls.config['project_name'])
        cls.expected_properties = cls.config['expected_properties']
        cls.expected_dbt_ls_results = cls.config['expected_dbt_ls_results']
        cls.ctx = invoke.Context()
        utils.get_project_info(cls.ctx, project_dir=cls.project_dir, logger=cls.logger)
        cls.macro_name = '_log_columns_list'
        cls.macro_value = utils.MACROS[cls.macro_name]
        cls.macro_path = Path(cls.ctx.config['macro_paths'][0], f'{cls.macro_name}.sql')
        invoke.run(f'dbt clean --project-dir {cls.project_dir} --profiles-dir {cls.profiles_dir}')
        invoke.run(f'dbt compile --project-dir {cls.project_dir} --profiles-dir {cls.profiles_dir}')

    def setUp(self):
        """
        Overrides unittest.TestCase.setUp
        :return: None
        """
        if self.macro_path.exists():
            os.remove(self.macro_path)
        with patch('builtins.input', return_value='y'):
            properties.delete(self.ctx, project_dir=self.project_dir, profiles_dir=self.profiles_dir)

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
        invoke.run(f'dbt clean --project-dir {cls.project_dir} --profiles-dir {cls.profiles_dir}')


if __name__ == '__main__':
    loader = unittest.TestLoader()
    suite = loader.discover(PARENT_DIR)
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)
