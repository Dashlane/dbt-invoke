import unittest
from pathlib import Path
from unittest.mock import patch

from dbt_invoke import properties
from dbt_invoke.internal import _utils
from test import TestDbtInvoke

PARENT_DIR = Path(__file__).parent
SUPPORTED_RESOURCE_TYPES = properties._SUPPORTED_RESOURCE_TYPES


class TestUtils(TestDbtInvoke):
    def test_add_macro(self):
        """
        Test the automatic addition of a macro to a dbt project

        :return: None
        """
        with patch('builtins.input', return_value='n'):
            try:
                _utils.add_macro(self.ctx, self.macro_name, logger=self.logger)
            except SystemExit:
                pass
        with patch('builtins.input', return_value='y'):
            _utils.add_macro(self.ctx, self.macro_name, logger=self.logger)
        with open(self.macro_path, 'r') as f:
            lines = f.read()
        self.assertEqual(lines, self.macro_value)

    def test_dbt_ls(self):
        """
        Test the "dbt ls" command with different arguments

        :return: None
        """
        for db_ls_kwarg, values in self.expected_dbt_ls_results.items():
            for value, expected_result_parts in values.items():
                dbt_ls_kwargs = {db_ls_kwarg: value}
                result_lines = _utils.dbt_ls(
                    self.ctx,
                    project_dir=self.project_dir,
                    profiles_dir=self.profiles_dir,
                    supported_resource_types=SUPPORTED_RESOURCE_TYPES,
                    output='path',
                    logger=self.logger,
                    **dbt_ls_kwargs,
                )
                result_parts = [
                    list(Path(line).parts) for line in result_lines
                ]
                self.assertCountEqual(result_parts, expected_result_parts)


if __name__ == '__main__':
    unittest.main()
