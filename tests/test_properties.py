import unittest
from pathlib import Path
from unittest.mock import patch

from dbt_invoke import properties, utils
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
            actual_props = utils.parse_yaml(full_file_path)
            self.assertEqual(exp_props, actual_props)
            # Simulate a manual update of the property files
            for section in actual_props:
                if section.lower() != 'version':
                    actual_props[section][0]['description'] = DESCRIPTION
                    actual_props[section][0]['columns'][0]['tests'] = COL_TESTS
            all_files_actual_properties[full_file_path] = actual_props
            utils.write_yaml(full_file_path, actual_props)
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
            actual_props = utils.parse_yaml(full_file_path)
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


if __name__ == '__main__':
    unittest.main()
