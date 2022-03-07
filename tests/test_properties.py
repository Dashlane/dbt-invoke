import unittest
from pathlib import Path
from unittest.mock import patch
import filecmp
import shutil
import os

from dbt_invoke import properties
from dbt_invoke.internal import _utils
from test import TestDbtInvoke

PARENT_DIR = Path(__file__).parent
DESCRIPTION = 'A fake test description.'
COL_TESTS = ['not_null']


class TestProperties(TestDbtInvoke):
    def test_create_update_delete_property_files(self):
        """
        Test the create -> update -> delete cycle of property files

        :return: None
        """
        # Create property files
        with patch('builtins.input', return_value='y'):
            properties.update(
                self.ctx,
                project_dir=self.project_dir,
                profiles_dir=self.profiles_dir,
                log_level='DEBUG',
            )
        # Check that the property files contain the expected contents
        all_files_actual_properties = dict()
        for file_location, exp_props in self.expected_properties.items():
            full_file_path = Path(self.project_dir, file_location)
            actual_props = _utils.parse_yaml(full_file_path)
            self.assertEqual(exp_props, actual_props)
            # Simulate a manual update of the property files
            for section in actual_props:
                if section.lower() != 'version':
                    actual_props[section][0]['description'] = DESCRIPTION
                    actual_props[section][0]['columns'][0]['tests'] = COL_TESTS
            all_files_actual_properties[full_file_path] = actual_props
            _utils.write_yaml(full_file_path, actual_props)
        # Automatically update property files, using threads
        properties.update(
            self.ctx,
            project_dir=self.project_dir,
            profiles_dir=self.profiles_dir,
            threads=2,
            log_level='DEBUG',
        )
        # Check that the automatic update did not overwrite the
        # previous manual update
        for full_file_path, exp_props in all_files_actual_properties.items():
            actual_props = _utils.parse_yaml(full_file_path)
            self.assertEqual(exp_props, actual_props)
        # Initiate then abort deletion of property files
        with patch('builtins.input', return_value='n'):
            properties.delete(
                self.ctx,
                project_dir=self.project_dir,
                profiles_dir=self.profiles_dir,
                log_level='DEBUG',
            )
        # Check that the property files still exist
        for full_file_path in all_files_actual_properties:
            self.assertTrue(full_file_path.exists())
        # Delete property files
        with patch('builtins.input', return_value='y'):
            properties.delete(
                self.ctx,
                project_dir=self.project_dir,
                profiles_dir=self.profiles_dir,
                log_level='DEBUG',
            )
        # Check that the property files no longer exist
        for full_file_path in all_files_actual_properties:
            self.assertFalse(full_file_path.exists())

    def test_multiline(self):
        self.edit_update_compare(
            "customers_multiline.yml", target_model="customers"
        )

    def test_multiline_missing_column(self):
        self.edit_update_compare(
            "customers_multiline_missing_column.yml",
            target_model="customers",
            expected_file="customers_multiline.yml",
        )

    def test_multiline_pipe(self):
        self.edit_update_compare(
            "customers_multiline_pipe.yml", target_model="customers"
        )

    def test_multiline_quotes(self):
        self.edit_update_compare(
            "customers_multiline_quotes.yml",
            target_model="customers",
            expected_file="customers_multiline_quotes_expected.yml",
        )

    def test_long_string(self):
        self.edit_update_compare(
            "customers_long_string.yml",
            target_model="customers",
            expected_file="customers_long_string_expected.yml",
        )

    def test_comment(self):
        self.edit_update_compare(
            "customers_comment.yml", target_model="customers"
        )

    def edit_update_compare(
        self, source_file, target_model="customers", expected_file=None
    ):
        target_path = Path(
            self.project_dir, "models", "marts", "core", f"{target_model}.yml"
        )
        source_path = Path(
            self.test_base_dir, "test_property_files", source_file
        )
        if expected_file:
            expected_path = Path(
                self.test_base_dir, "test_property_files", expected_file
            )
        else:
            expected_path = source_path
        shutil.copy(source_path, target_path)
        with patch('builtins.input', return_value='y'):
            properties.update(
                self.ctx,
                select=target_model,
                project_dir=self.project_dir,
                profiles_dir=self.profiles_dir,
                log_level='DEBUG',
            )
            # check the content
        self.logger.info(f"Comparing content of files {target_path} and {expected_path}")
        with open(target_path) as f:
            content = '\n'.join(f.readlines())
        self.logger.debug(f"Target content is \n{content}")
        with open(expected_path) as f:
            content = '\n'.join(f.readlines())
        self.logger.debug(f"Expected content is \n{content}")
        self.assertTrue(filecmp.cmp(target_path, expected_path))

        # clean up
        os.remove(target_path)


if __name__ == '__main__':
    unittest.main()
