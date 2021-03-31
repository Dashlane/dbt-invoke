import os
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from pathlib import Path

from invoke import task

from dbt_invoke import utils

LOGGER = utils.get_logger('dbt-invoke')
PARENT_DIR = Path(__file__).parent
SUPPORTED_RESOURCE_TYPES = utils.PROPERTIES['supported_resource_types']
RESOURCE_SELECTION_ARGUMENTS = utils.PROPERTIES['resource_selection_arguments']
UPDATE_AND_DELETE_HELP = {
    arg_name: utils.DBT_LS_ARG_HELP for arg_name in utils.DBT_CLI_LS_ARGS
}
UPDATE_AND_DELETE_HELP['log-level'] = (
    "One of Python's standard logging levels"
    " (DEBUG, INFO, WARNING, ERROR, CRITICAL)"
)
THREADS_HELP = {
    'threads': (
        "Maximum number of concurrent threads to use in"
        " collecting resources' column information from the data warehouse"
        " and in creating/updating the corresponding property files. Each"
        " thread will run dbt's get_columns_in_query macro against the"
        " data warehouse."
    )
}
MACRO_NAME = '_log_columns_list'


@task(
    default=True,
    help={**UPDATE_AND_DELETE_HELP, **THREADS_HELP},
    auto_shortflags=False,
)
def update(
    ctx,
    resource_type=None,
    select=None,
    models=None,
    exclude=None,
    selector=None,
    project_dir=None,
    profiles_dir=None,
    profile=None,
    target=None,
    vars=None,
    bypass_cache=None,
    state=None,
    log_level=None,
    threads=1,
):
    """
    Update property file(s) for the specified set of resources

    :param ctx: An Invoke context object
    :param resource_type: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param select: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param models: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param exclude: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param selector: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param project_dir: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param profiles_dir: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param profile: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param target: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param vars: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param bypass_cache: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param state: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param log_level: One of Python's standard logging levels
        (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    :param threads: Maximum number of concurrent threads to use in
        collecting resources' column information from the data warehouse
        and in creating/updating the corresponding property files. Each
        thread will run dbt's get_columns_in_query macro against the
        data warehouse.
    :return: None
    """
    if log_level:
        LOGGER.setLevel(log_level.upper())
    assert_supported_resource_type(resource_type)
    utils.get_project_info(ctx, project_dir=project_dir, logger=LOGGER)
    common_dbt_kwargs = {
        'project_dir': project_dir or ctx.config['project_path'],
        'profiles_dir': profiles_dir,
        'profile': profile,
        'target': target,
        'vars': vars,
        'bypass_cache': bypass_cache,
    }
    # Get the paths and resource types of the
    # resources for which to create property files
    transformed_ls_results = transform_ls_results(
        ctx,
        resource_type=resource_type,
        select=select,
        models=models,
        exclude=exclude,
        selector=selector,
        state=state,
        **common_dbt_kwargs,
    )
    create_all_property_files(
        ctx, transformed_ls_results, threads=threads, **common_dbt_kwargs
    )


@task(
    help=UPDATE_AND_DELETE_HELP,
    auto_shortflags=False,
)
def delete(
    ctx,
    resource_type=None,
    select=None,
    models=None,
    exclude=None,
    selector=None,
    project_dir=None,
    profiles_dir=None,
    profile=None,
    target=None,
    vars=None,
    bypass_cache=None,
    state=None,
    log_level=None,
):
    """
    Delete property file(s) for the specified set of resources

    :param ctx: An Invoke context object
    :param resource_type: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param select: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param models: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param exclude: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param selector: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param project_dir: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param profiles_dir: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param profile: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param target: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param vars: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param bypass_cache: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param state: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param log_level: One of Python's standard logging levels
        (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    :return: None
    """
    if log_level:
        LOGGER.setLevel(log_level.upper())
    assert_supported_resource_type(resource_type)
    utils.get_project_info(ctx, project_dir=project_dir, logger=LOGGER)
    common_dbt_kwargs = {
        'project_dir': project_dir or ctx.config['project_path'],
        'profiles_dir': profiles_dir,
        'profile': profile,
        'target': target,
        'vars': vars,
        'bypass_cache': bypass_cache,
    }
    # Get the paths of the property files to delete
    transformed_ls_results = transform_ls_results(
        ctx,
        resource_type=resource_type,
        select=select,
        models=models,
        exclude=exclude,
        selector=selector,
        state=state,
        **common_dbt_kwargs,
    )
    delete_all_property_files(ctx, transformed_ls_results)


@task
def echo_macro(ctx):
    """
    Print out the configured macro for the user to
    copy to their dbt project

    :param ctx: An Invoke context object
    :return: None
    """
    LOGGER.info(
        f'Copy and paste the following macro into your dbt project:'
        f'\n{utils.get_macro(MACRO_NAME)}'
    )


def transform_ls_results(ctx, **kwargs):
    """
    Run the "dbt ls" command to select resources and determine their
    resource type and path. Then filter out unsupported resource types
    and return the results in a dictionary.

    :param ctx: An Invoke context object
    :param kwargs: Arguments for listing dbt resources
        (run "dbt ls --help" for details)
    :return: Dictionary where the key is the resource path
        and the value is dictionary form of the resource's json
    """
    # Run dbt ls to retrieve resource path and json information
    LOGGER.info('Searching for matching resources...')
    result_lines_path = utils.dbt_ls(
        ctx,
        supported_resource_types=SUPPORTED_RESOURCE_TYPES,
        logger=LOGGER,
        **kwargs,
    )
    result_lines_dict = utils.dbt_ls(
        ctx,
        supported_resource_types=SUPPORTED_RESOURCE_TYPES,
        logger=LOGGER,
        output='json',
        **kwargs,
    )
    results = dict(zip(result_lines_path, result_lines_dict))
    # Filter dictionary for existing files and supported resource types
    results = {
        k: v
        for k, v in results.items()
        if v['resource_type'] in SUPPORTED_RESOURCE_TYPES
        and Path(ctx.config['project_path'], k).exists()
    }
    LOGGER.info(
        f"Found {len(results)} matching resources in dbt project"
        f' "{ctx.config["project_name"]}"'
    )
    if LOGGER.level <= 10:
        for resource in results:
            LOGGER.debug(resource)
    return results


def create_all_property_files(
    ctx, transformed_ls_results, threads=1, **kwargs
):
    """
    For each resource from dbt ls, create or update a property file

    :param ctx: An Invoke context object
    :param transformed_ls_results: Dictionary where the key is the
        resource path and the value is the dictionary form of the
        resource's json
    :param threads: Maximum number of concurrent threads to use in
        collecting resources' column information from the data warehouse
        and in creating/updating the corresponding property files. Each
        thread will run dbt's get_columns_in_query macro against the
        data warehouse.
    :param kwargs: Additional arguments for utils.dbt_run_operation
        (run "dbt run-operation --help" for details)
    :return: None
    """
    transformed_ls_results_length = len(transformed_ls_results)
    partial_create_property_file = partial(
        create_property_file,
        ctx,
        total=transformed_ls_results_length,
        **kwargs,
    )
    counters = range(1, 1 + transformed_ls_results_length)
    with ThreadPoolExecutor(max_workers=threads) as executor:
        column_lists = executor.map(
            partial_create_property_file,
            transformed_ls_results.keys(),
            transformed_ls_results.values(),
            counters,
        )
        list(column_lists)


def delete_all_property_files(ctx, transformed_ls_results):
    """
    For each resource from dbt ls,
    delete the property file if user confirms

    :param ctx: An Invoke context object
    :param transformed_ls_results: Dictionary where the key is the
        resource path and the value is dictionary form of the
        resource's json
    :return: None
    """
    resource_paths = [
        Path(ctx.config['project_path'], resource_location)
        for resource_location in transformed_ls_results
    ]
    property_paths = [
        rp.with_suffix('.yml')
        for rp in resource_paths
        if rp.with_suffix('.yml').exists()
    ]
    LOGGER.info(
        f'{len(property_paths)} of {len(resource_paths)}'
        f' have existing property files'
    )
    # Delete the selected property paths
    if len(property_paths) > 0:
        deletion_message_yml_paths = '\n'.join(
            [str(property_path) for property_path in property_paths]
        )
        deletion_message_prefix = '\nThe following files will be deleted:\n\n'
        deletion_message_suffix = (
            f'\n\nAre you sure you want to delete these'
            f' {len(property_paths)} file(s) (answer: y/n)?\n'
        )
        deletion_confirmation = input(
            f'{deletion_message_prefix}'
            f'{deletion_message_yml_paths}'
            f'{deletion_message_suffix}'
        )
        # User confirmation
        while deletion_confirmation.lower() not in ['y', 'n']:
            deletion_confirmation = input(
                '\nPlease enter "y" to confirm deletion'
                ' or "n" to abort deletion.\n'
            )
        if deletion_confirmation.lower() == 'y':
            for file in property_paths:
                os.remove(file)
            LOGGER.info('Deletion confirmed.')
        else:
            LOGGER.info('Deletion aborted.')
    else:
        LOGGER.info('There are no files to delete.')


def create_property_file(
    ctx, resource_location, resource_dict, counter, total, **kwargs
):
    """
    Create a property file

    :param ctx: An Invoke context object
    :param resource_location: The location of the file representing the
        resource
    :param resource_dict: A dictionary representing the json output for
        this resource from the "dbt ls" command
    :param counter: An integer assigned to this file (for logging the
        progress of file creation)
    :param total: An integer representing the total number of files to
        be created (for logging the progress of file creation)
    :param kwargs: Additional arguments for utils.dbt_run_operation
        (run "dbt run-operation --help" for details)
    :return: None
    """
    property_path = Path(
        ctx.config['project_path'], resource_location
    ).with_suffix('.yml')
    LOGGER.info(f'Starting: {counter}/{total}... {property_path.name}')
    columns = get_columns(ctx, resource_location, resource_dict, **kwargs)
    property_file_dict = structure_property_file_dict(
        property_path, resource_dict, columns
    )
    utils.write_yaml(property_path, property_file_dict)
    LOGGER.info(f'Completed: {counter}/{total}... {property_path.name}')


def get_columns(ctx, resource_location, resource_dict, **kwargs):
    """
    Get a list of the column names in a resource

    :param ctx: An Invoke context object
    :param resource_location: The location of the file representing the
        resource
    :param resource_dict: A dictionary representing the json output for
        this resource from the "dbt ls" command
    :param kwargs: Additional arguments for utils.dbt_run_operation
        (run "dbt run-operation --help" for details)
    :return: A list of the column names in the resource
    """
    resource_path = Path(resource_location)
    materialized = resource_dict['config']['materialized']
    resource_type = resource_dict['resource_type']
    resource_name = resource_dict['name']
    if materialized != 'ephemeral' and resource_type != 'analysis':
        result_lines = utils.dbt_run_operation(
            ctx,
            MACRO_NAME,
            hide=True,
            logger=LOGGER,
            resource_name=resource_name,
            **kwargs,
        )
    else:
        resource_path = Path(ctx.config['compiled_path'], resource_path)
        with open(resource_path, 'r') as f:
            lines = f.readlines()
        # Get and clean the SQL code
        lines = [line.strip() for line in lines if line.strip()]
        sql = "\n".join(lines)
        result_lines = utils.dbt_run_operation(
            ctx, MACRO_NAME, hide=True, logger=LOGGER, sql=sql, **kwargs
        )
    result_list_strings = [
        s for s in result_lines if s.startswith('[') and s.endswith(']')
    ]
    # Take the last line that started with '[' and ended with ']'
    # (just in case there is multi-line output)
    columns_string = result_list_strings[-1].strip('][').split(', ')
    columns = [col.strip("'") for col in columns_string]
    return columns


def structure_property_file_dict(location, resource_dict, columns_list):
    """
    Structure a dictionary that will be used to create a property file

    :param location: The location in which to create the property file
    :param resource_dict: A dictionary representing the json output for
        this resource from the "dbt ls" command
    :param columns_list: A list of columns to include in the
        property file
    :return: None
    """
    resource_type = resource_dict['resource_type']
    resource_name = resource_dict['name']
    # If the property file already exists, read it into a dictionary.
    if location.exists():
        property_file_dict = utils.parse_yaml(location)
    # Else create a new dictionary that
    # will be used to create a new property file.
    else:
        property_file_dict = get_property_header(resource_name, resource_type)
    # Get the sub-dictionaries of each existing column
    resource_type_plural = SUPPORTED_RESOURCE_TYPES[resource_type]
    columns_dict = {
        item['name']: item
        for item in property_file_dict[resource_type_plural][0]['columns']
    }
    # For each column we want in the property file,
    # reuse the sub-dictionary if it exists
    # or else create a new sub-dictionary
    property_file_dict[resource_type_plural][0]['columns'] = list()
    for column in columns_list:
        column_dict = columns_dict.get(column, get_property_column(column))
        property_file_dict[resource_type_plural][0]['columns'].append(
            column_dict
        )
    return property_file_dict


def get_property_header(resource, resource_type):
    """
    Create a dictionary representing resources properties

    :param resource: The name of the resource for which to create a
        property header
    :param resource_type: The type of the resource (model, seed, etc.)
    :return: A dictionary representing resource properties
    """
    header_dict = {
        'version': 2,
        SUPPORTED_RESOURCE_TYPES[resource_type]: [
            {'name': resource, 'description': "", 'columns': []}
        ],
    }
    return header_dict


def get_property_column(column_name):
    """
    Create a dictionary representing column properties

    :param column_name: Name of column
    :return: A dictionary representing column properties
    """
    column_dict = {'name': column_name, 'description': ""}
    return column_dict


def assert_supported_resource_type(resource_type):
    """
    Assert that the given resource type is in the list of supported
        resource types

    :param resource_type: A dbt resource type
    :return: None
    """
    try:
        assert (
            resource_type is None
            or resource_type.lower() in SUPPORTED_RESOURCE_TYPES
        )
    except AssertionError:
        msg = (
            f'Sorry, this tool only supports the following resource types:'
            f' {list(SUPPORTED_RESOURCE_TYPES.keys())}'
        )
        LOGGER.exception(msg)
        raise
