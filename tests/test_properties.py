import unittest
from pathlib import Path
from unittest.mock import patch
import shutil

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

    def migrate_wrapper(self, resource_type, *args, **kwargs):
        """
        Helper method for testing different migration patterns

        :param resource_type: An argument for listing dbt resources
            (run "dbt ls --help" for details)
        :param args: The names of files to consider for migration
        :param kwargs: The keys are post-migration relative locations
            from the test project directory (using '__' in place of
            '/'). The values are of type bool, representing whether to
            compare migration results and expectations files. If False,
            a test will be conducted to assert that the file does not
            exist (this is useful for checking that empty migration
            files are deleted appropriately).
        :return: None
        """
        # Copy migration property files to the test project
        migration_base_path = Path(
            self.test_base_dir,
            'test_property_files',
            'migration',
        )
        migration_file_names = [*args]
        migration_paths = {
            Path(
                migration_base_path,
                file_name,
            ): Path(
                self.project_dir,
                'models',
                file_name,
            )
            for file_name in migration_file_names
        }
        for source_path, target_path in migration_paths.items():
            shutil.copy(source_path, target_path)
        # Run migration
        properties.migrate(
            self.ctx,
            resource_type=resource_type,
            project_dir=self.project_dir,
            profiles_dir=self.profiles_dir,
            log_level='DEBUG',
        )
        # Compare migration results to expectations
        expected_base_path = Path(migration_base_path, 'expected')
        relative_paths = {
            Path(*relative_location.split('__')).with_suffix('.yml'): compare
            for relative_location, compare in kwargs.items()
        }
        for relative_path, compare in relative_paths.items():
            migrated_path = Path(self.project_dir, relative_path)
            if compare:
                expected_path = Path(expected_base_path, relative_path)
                self.assertTrue(
                    self.compare_files(migrated_path, expected_path)
                )
            else:
                self.assertFalse(migrated_path.exists())
        # Delete the new property files
        with patch('builtins.input', return_value='y'):
            properties.delete(
                self.ctx,
                project_dir=self.project_dir,
                profiles_dir=self.profiles_dir,
                log_level='DEBUG',
            )
        for target_path in migration_paths.values():
            try:
                target_path.unlink()
            except FileNotFoundError:
                continue

    def test_partial_migrate(self):
        """
        Test the partial migration of structure from one property file
        for multiple resources to one property file per resource

        :return: None
        """
        self.migrate_wrapper(
            'model',
            'migration_leftover_comment.yml',
            analyses__revenue_by_daily_cohort=False,
            data__items=False,
            models__marts__core__customers=True,
            models__marts__core__orders=True,
            models__migration_leftover_comment=True,
            snapshots__items_snapshot=False,
        )

    def test_full_migrate(self):
        """
        Test the full migration of structure from one property file for
        multiple resources to one property file per resource

        :return: None
        """
        self.migrate_wrapper(
            None,
            'migration_leftover_comment.yml',
            'migration_no_leftover_comment.yml',
            analyses__revenue_by_daily_cohort=True,
            data__items=True,
            models__marts__core__customers=True,
            models__marts__core__orders=True,
            models__migration_leftover_comment=True,
            models__migration_no_leftover_comment=False,
            snapshots__items_snapshot=True,
        )

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

    def test_keep_list_format(self):
        self.edit_update_compare(
            "customers_keep_list_format.yml", target_model="customers"
        )

    def test_keep_empty_line(self):
        self.edit_update_compare(
            "customers_keep_empty_line.yml", target_model="customers"
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
        self.logger.info(
            f"Comparing content of files {target_path} and {expected_path}"
        )

        self.assertTrue(self.compare_files(target_path, expected_path))

        # clean up
        target_path.unlink()


if __name__ == '__main__':
    unittest.main()
