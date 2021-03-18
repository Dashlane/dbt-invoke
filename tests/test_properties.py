import unittest
from pathlib import Path
from unittest.mock import patch

from dbt_invoke import properties, utils
from test import TestDbtInvoke

PARENT_DIR = Path(__file__).parent


class TestProperties(TestDbtInvoke):
    def test_create_update_delete_property_files(self):
        """
        Test the create -> update -> delete cycle of property files
        :return: None
        """
        # Create property files
        with patch('builtins.input', return_value='y'):
            properties.update(self.ctx, project_dir=self.project_dir, profiles_dir=self.profiles_dir, log_level='DEBUG')
        # Check that the property files contain the expected contents
        all_files_actual_properties = dict()
        for file_location, expected_properties in self.expected_properties.items():
            full_file_path = Path(self.project_dir, file_location)
            actual_properties = utils.parse_yaml(full_file_path)
            self.assertEqual(expected_properties, actual_properties)
            # Simulate a manual update of the property files
            for section in actual_properties:
                if section.lower() != 'version':
                    actual_properties[section][0]['description'] = 'A fake test description.'
                    actual_properties[section][0]['columns'][0]['tests'] = ['not_null']
            all_files_actual_properties[full_file_path] = actual_properties
            utils.write_yaml(full_file_path, actual_properties)
        # Automatically update property files
        properties.update(self.ctx, project_dir=self.project_dir, profiles_dir=self.profiles_dir, log_level='DEBUG')
        # Check that the automatic update did not overwrite the previous manual update
        for full_file_path, expected_properties in all_files_actual_properties.items():
            actual_properties = utils.parse_yaml(full_file_path)
            self.assertEqual(expected_properties, actual_properties)
        # Initiate then abort deletion of property files
        with patch('builtins.input', return_value='n'):
            properties.delete(self.ctx, project_dir=self.project_dir, profiles_dir=self.profiles_dir, log_level='DEBUG')
        # Check that the property files still exist
        for full_file_path in all_files_actual_properties:
            self.assertTrue(full_file_path.exists())
        # Delete property files
        with patch('builtins.input', return_value='y'):
            properties.delete(self.ctx, project_dir=self.project_dir, profiles_dir=self.profiles_dir, log_level='DEBUG')
        # Check that the property files no longer exist
        for full_file_path in all_files_actual_properties:
            self.assertFalse(full_file_path.exists())


if __name__ == '__main__':
    unittest.main()
