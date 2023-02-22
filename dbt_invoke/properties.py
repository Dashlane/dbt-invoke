import os
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import ast
from collections import defaultdict
import json

from invoke import task

from dbt_invoke.internal import _utils

_LOGGER = _utils.get_logger('dbt-invoke')
_MACRO_NAME = '_log_columns_list'
_SUPPORTED_RESOURCE_TYPES = {
    'model': 'models',
    'seed': 'seeds',
    'snapshot': 'snapshots',
    'analysis': 'analyses',
}
_PROGRESS_PADDING = 9  # Character padding to align progress logs

_update_and_delete_help = {
    arg.replace('_', '-'): details['help']
    for arg, details in _utils.DBT_LS_ARGS.items()
}
_update_and_delete_help['log-level'] = (
    "One of Python's standard logging levels"
    " (DEBUG, INFO, WARNING, ERROR, CRITICAL)"
)


@task(
    default=True,
    help={
        **_update_and_delete_help,
        'threads': (
            "Maximum number of concurrent threads to use in"
            " collecting resources' column information from the data warehouse"
            " and in creating/updating the corresponding property files. Each"
            " thread will run dbt's get_columns_in_query macro against the"
            " data warehouse."
        ),
    },
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
    common_dbt_kwargs, transformed_ls_results = _initiate_alterations(
        ctx,
        resource_type=resource_type,
        select=select,
        models=models,
        exclude=exclude,
        selector=selector,
        project_dir=project_dir,
        profiles_dir=profiles_dir,
        profile=profile,
        target=target,
        vars=vars,
        bypass_cache=bypass_cache,
        state=state,
        log_level=log_level,
    )
    _create_all_property_files(
        ctx,
        transformed_ls_results,
        threads=threads,
        **common_dbt_kwargs,
    )


@task(
    help=_update_and_delete_help,
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
    _, transformed_ls_results = _initiate_alterations(
        ctx,
        resource_type=resource_type,
        select=select,
        models=models,
        exclude=exclude,
        selector=selector,
        project_dir=project_dir,
        profiles_dir=profiles_dir,
        profile=profile,
        target=target,
        vars=vars,
        bypass_cache=bypass_cache,
        state=state,
        log_level=log_level,
    )
    _delete_all_property_files(ctx, transformed_ls_results)


@task
def echo_macro(ctx):
    """
    Print out the configured macro for the user to
    copy to their dbt project

    :param ctx: An Invoke context object
    :return: None
    """
    _LOGGER.info(
        f'Copy and paste the following macro into your dbt project:'
        f'\n{_utils.get_macro(_MACRO_NAME)}'
    )


def _read_manifest(target_path):
    """
    Read the dbt manifest file into something we can parse
    :param target_path: The dbt target_path for the project
        https://docs.getdbt.com/reference/project-configs/target-path
    :return: dict representing a dbt manifest
        https://docs.getdbt.com/reference/artifacts/manifest-json
    """
    with open(
        Path(target_path, 'manifest').with_suffix('.json'),
        "r",
        encoding='utf-8',
    ) as manifest_json:
        return json.loads(manifest_json.read())


@task(
    help={
        **_update_and_delete_help,
    },
    auto_shortflags=False,
)
def migrate(
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
    Change structure from multiple resources per property file to one
    resource per property file

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
    # Run _initiate_alterations to retrieve the locations of the
    # resources for which properties will be migrated.
    # Importantly, this also updates ctx to contain the dbt project
    # path.
    _, transformed_ls_results = _initiate_alterations(
        ctx,
        resource_type=resource_type,
        select=select,
        models=models,
        exclude=exclude,
        selector=selector,
        project_dir=project_dir,
        profiles_dir=profiles_dir,
        profile=profile,
        target=target,
        vars=vars,
        bypass_cache=bypass_cache,
        state=state,
        log_level=log_level,
    )
    # Parse nodes from the manifest file
    nodes = _read_manifest(ctx['target_path'])['nodes']
    # Create a migration_map dict to keep track of existing property
    # files from which properties for one or more resources will be
    # migrated. Structure of migration_map:
    # {
    #     <existing_property_path1>: [
    #         {
    #             'name': '<resource1_name>',
    #             'resource_type': '<resource1_resource_type>',
    #             'resource_type_plural': '<resource1_resource_type_plural>',
    #             'resource_path': <Path of resource1>,
    #             'property_path': <intended Path of resource1 properties>,
    #         },
    #         {
    #             'name': '<resource2_name>',
    #             ...
    #         },
    #         ...
    #     ],
    #     <existing_property_path2>: [
    #         ...
    #     ],
    #     ...
    # }
    migration_map = defaultdict(list)
    # Using the nodes from the manifest create a data structure that
    # keeps track of what existing yaml files we have and what
    # resources are defined in each.
    for node, metadata in nodes.items():
        # Skip if node is not a selected node.
        # This is not only for efficiency, but also to confirm that the
        # node comes from the correct dbt project
        # (transformed_ls_results should already only contain nodes from
        # the correct dbt project).
        if metadata['original_file_path'] not in transformed_ls_results:
            continue
        # Skip if node is not present in any existing property file
        elif not metadata.get('patch_path'):
            continue
        existing_property_path = Path(
            ctx.config['project_path'],
            metadata.get('patch_path').split('//')[-1],
        )
        resource_path = Path(
            ctx.config['project_path'],
            metadata['original_file_path'],
        )
        # Add data for to-be-created property files to the migration_map
        migration_map[existing_property_path].append(
            {
                'name': metadata['name'],
                'resource_type': metadata['resource_type'],
                'resource_type_plural': _SUPPORTED_RESOURCE_TYPES.get(
                    metadata['resource_type']
                ),
                'resource_path': resource_path,
                'property_path': resource_path.with_suffix('.yml'),
            }
        )
    # Loop through the migration_map to perform the migration
    for existing_property_path, resource_list in migration_map.items():
        # Keep track of items to remove from the existing property file
        indices_to_remove = defaultdict(list)
        # Create a set of the resource types for which at least one
        # resource will be migrated from the existing property file
        relevant_resource_types_plural = set(
            [resource['resource_type_plural'] for resource in resource_list]
        )
        # Read each relevant property file once to keep things speedy
        existing_property_file_dict = _utils.parse_yaml(existing_property_path)
        # For each relevant resource type for this migration, collect
        # the properties and index position of all resources within the
        # existing property file
        existing_properties = {
            properties['name']: {
                'resource_type_plural': k,
                'index': i,
                'properties': properties,
            }
            for k, v in existing_property_file_dict.items()
            if k in relevant_resource_types_plural
            for i, properties in enumerate(v)
        }
        # For each resource within the existing property path in the
        # migration_map copy the properties to their intended
        # destination
        for resource in resource_list:
            # Skip if the properties are already in the correct location
            if existing_property_path == resource['property_path']:
                continue
            _LOGGER.info(
                f"""Moving "{resource['name']}" definition from"""
                f""" {str(existing_property_path.resolve())} to"""
                f""" {str(resource['property_path'].resolve())}"""
            )
            # Try to create the new property file. Upon success, save
            # the index of the resource to remove from the migration
            # property file.
            try:
                property_file_dict = _get_property_header(
                    resource['name'],
                    resource['resource_type'],
                    existing_properties[resource['name']]['properties'],
                )
                _utils.write_yaml(
                    resource['property_path'],
                    property_file_dict,
                    mode='x',
                )
                indices_to_remove[resource['resource_type_plural']].append(
                    existing_properties[resource['name']]['index']
                )
                _LOGGER.info(f"Created {resource['property_path']}")
            except Exception:
                _LOGGER.exception(
                    f"Failed to create {resource['property_path']}"
                )
        # Remove migrated resources from migration_property_file_dict
        removed_counter = 0
        for resource_type_plural, indices in indices_to_remove.items():
            for i in sorted(indices, reverse=True):
                existing_property_file_dict[resource_type_plural].pop(i)
                removed_counter += 1
            # Remove resource type heading if it no longer contains any
            # resources
            if len(existing_property_file_dict[resource_type_plural]) == 0:
                existing_property_file_dict.pop(resource_type_plural)
        # Overwrite the migration property file with its remaining
        # post-migration contents.
        try:
            _utils.write_yaml(
                existing_property_path,
                existing_property_file_dict,
            )
            _LOGGER.info(
                f'Removed {str(removed_counter)} migrated resources from'
                f' {str(existing_property_path.resolve())}'
            )
        except Exception:
            _LOGGER.exception(
                f"Failed to update {str(existing_property_path.resolve())}"
            )
        # Delete the existing property file if it only contains version info
        if existing_property_path.read_text().strip().lower() == "version: 2":
            try:
                existing_property_path.unlink()
                _LOGGER.info(
                    f"Deleted {str(existing_property_path.resolve())}"
                )
            except Exception:
                _LOGGER.exception(
                    f"Failed to delete {str(existing_property_path.resolve())}"
                )
        _LOGGER.info('Migration successful')


def _initiate_alterations(ctx, **kwargs):
    """
    Retrieve the dbt keyword arguments that are common to multiple dbt
    commands as well as the transformed results of the "dbt ls" command

    :param ctx: An Invoke context object
    :param kwargs:
    :return: A 2-tuple of:
        1. The dbt keyword arguments that are common to multiple dbt
        commands
        2. The transformed results of the "dbt ls" command
    """
    if kwargs.get('log_level'):
        _LOGGER.setLevel(kwargs.get('log_level').upper())
    resource_type = kwargs.get('resource_type')
    _assert_supported_resource_type(resource_type)
    project_dir = kwargs.get('project_dir')
    _utils.get_project_info(ctx, project_dir=project_dir)
    common_dbt_kwargs = {
        'project_dir': project_dir or ctx.config['project_path'],
        'profiles_dir': kwargs.get('profiles_dir'),
        'profile': kwargs.get('profile'),
        'target': kwargs.get('target'),
        'vars': kwargs.get('vars'),
        'bypass_cache': kwargs.get('bypass_cache'),
    }
    # Get the paths and resource types of the
    # resources for which to create property files
    transformed_ls_results = _transform_ls_results(
        ctx,
        resource_type=resource_type,
        select=kwargs.get('select'),
        models=kwargs.get('models'),
        exclude=kwargs.get('exclude'),
        selector=kwargs.get('selector'),
        state=kwargs.get('state'),
        **common_dbt_kwargs,
    )
    return common_dbt_kwargs, transformed_ls_results


def _transform_ls_results(ctx, **kwargs):
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
    _LOGGER.info('Searching for matching resources...')
    potential_results = _utils.dbt_ls(
        ctx,
        supported_resource_types=_SUPPORTED_RESOURCE_TYPES,
        logger=_LOGGER,
        output='json',
        **kwargs,
    )
    potential_result_paths = None
    results = dict()
    for i, potential_result in enumerate(potential_results):
        if 'original_file_path' in potential_result:
            potential_result_path = potential_result['original_file_path']
        # Before dbt version 0.20.0, original_file_path was not
        # included in the json response of "dbt ls". For older
        # versions of dbt, we need to run "dbt ls" with the
        # "--output path" argument in order to retrieve paths
        else:
            if potential_result_paths is None:
                potential_result_paths = _utils.dbt_ls(
                    ctx,
                    supported_resource_types=_SUPPORTED_RESOURCE_TYPES,
                    logger=_LOGGER,
                    output='path',
                    **kwargs,
                )
                assert len(potential_result_paths) == len(
                    potential_results
                ), 'Length of results differs from length of result details'
            potential_result_path = potential_result_paths[i]
        if Path(ctx.config['project_path'], potential_result_path).exists():
            results[potential_result_path] = potential_result
    _LOGGER.info(
        f"Found {len(results)} matching resources in dbt project"
        f' "{ctx.config["project_name"]}"'
    )
    if _LOGGER.level <= 10:
        for resource in results:
            _LOGGER.debug(resource)
    return results


def _create_all_property_files(
    ctx,
    transformed_ls_results,
    threads=1,
    **kwargs,
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
    :param kwargs: Additional arguments for _utils.dbt_run_operation
        (run "dbt run-operation --help" for details)
    :return: None
    """
    # Run a check that will fail if the _MACRO_NAME macro does not exist
    if not _utils.macro_exists(ctx, _MACRO_NAME, logger=_LOGGER, **kwargs):
        _utils.add_macro(ctx, _MACRO_NAME, logger=_LOGGER)
    # Handle the creation of property files in separate threads
    transformed_ls_results_length = len(transformed_ls_results)
    with ThreadPoolExecutor(max_workers=threads) as executor:
        futures = {
            executor.submit(
                _create_property_file,
                ctx,
                k,
                v,
                i + 1,
                transformed_ls_results_length,
                **kwargs,
            ): {'index': i + 1, 'resource_location': k}
            for i, (k, v) in enumerate(transformed_ls_results.items())
        }
        # Log success or failure for each thread
        successes = 0
        failures = 0
        for future in as_completed(futures):
            index = futures[future]['index']
            resource_location = futures[future]['resource_location']
            progress = (
                f'Resource {index} of {transformed_ls_results_length},'
                f' {resource_location}'
            )
            if future.exception() is not None:
                _LOGGER.error(f'{"[FAILURE]":>{_PROGRESS_PADDING}} {progress}')
                failures += 1
                # Store exception message for later when all tracebacks
                # for failed futures will be logged
                e = future.exception()
                exception_lines = traceback.format_exception(
                    type(e), e, e.__traceback__
                )
                futures[future][
                    'exception_message'
                ] = f'{progress}\n{"".join(exception_lines)}'
            else:
                _LOGGER.info(f'{"[SUCCESS]":>{_PROGRESS_PADDING}} {progress}')
                successes += 1
    # Log traceback for all failures at the end
    if failures:
        exception_messages = list()
        # Looping through futures instead of as_completed(futures) so
        # that the failed futures are displayed in order of submission,
        # rather than completion
        for future in futures:
            exception_message = futures[future].get('exception_message')
            if exception_message:
                exception_messages.append(exception_message)
        if exception_messages:
            exception_messages = '\n'.join(exception_messages)
            _LOGGER.error(
                f'Tracebacks for all failures:\n\n{exception_messages}'
            )
    # Log result summary
    _LOGGER.info(
        f'{"[DONE]":>{_PROGRESS_PADDING}}'
        f' Total: {successes + failures},'
        f' Successes: {successes},'
        f' Failures: {failures}'
    )


def _delete_all_property_files(ctx, transformed_ls_results):
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
    _LOGGER.info(
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
            _LOGGER.info('Deletion confirmed.')
        else:
            _LOGGER.info('Deletion aborted.')
    else:
        _LOGGER.info('There are no files to delete.')


def _create_property_file(
    ctx,
    resource_location,
    resource_dict,
    counter,
    total,
    **kwargs,
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
    :param kwargs: Additional arguments for _utils.dbt_run_operation
        (run "dbt run-operation --help" for details)
    :return: None
    """
    _LOGGER.info(
        f'{"[START]":>{_PROGRESS_PADDING}}'
        f' Resource {counter} of {total},'
        f' {resource_location}'
    )
    columns = _get_columns(ctx, resource_location, resource_dict, **kwargs)
    property_path = Path(
        ctx.config['project_path'], resource_location
    ).with_suffix('.yml')
    property_file_dict = _structure_property_file_dict(
        property_path,
        resource_dict,
        columns,
    )
    _utils.write_yaml(
        property_path,
        property_file_dict,
    )


def _get_columns(ctx, resource_location, resource_dict, **kwargs):
    """
    Get a list of the column names in a resource

    :param ctx: An Invoke context object
    :param resource_location: The location of the file representing the
        resource
    :param resource_dict: A dictionary representing the json output for
        this resource from the "dbt ls" command
    :param kwargs: Additional arguments for _utils.dbt_run_operation
        (run "dbt run-operation --help" for details)
    :return: A list of the column names in the resource
    """
    resource_path = Path(resource_location)
    materialized = resource_dict['config']['materialized']
    resource_type = resource_dict['resource_type']
    resource_name = resource_dict['name']
    if materialized != 'ephemeral' and resource_type != 'analysis':
        result_lines = _utils.dbt_run_operation(
            ctx,
            _MACRO_NAME,
            hide=True,
            logger=_LOGGER,
            resource_name=resource_name,
            **kwargs,
        )
    # Ephemeral and analysis resource types are not materialized in the
    # data warehouse, so the compiled versions of their SQL statements
    # are used instead
    else:
        resource_path = Path(ctx.config['compiled_path'], resource_path)
        with open(resource_path, 'r') as f:
            lines = f.readlines()
        # Get and clean the SQL code
        lines = [line.strip() for line in lines if line.strip()]
        sql = "\n".join(lines)
        result_lines = _utils.dbt_run_operation(
            ctx, _MACRO_NAME, hide=True, logger=_LOGGER, sql=sql, **kwargs
        )

    relevant_lines = list(
        filter(
            lambda x: x.get(
                # dbt-core>=1.0,<1.4
                #   run-operation logs contain structure
                #   {
                #       'code': 'M011',
                #       'msg': ['column1', 'column2', ...]
                #   }
                'code',
                # dbt-core>=1.4
                #   run-operation logs contain structure
                #   {
                #       'info': {
                #           'code': 'M011',
                #           'msg': "['column1', 'column2', ...]" # string value
                #       }
                #   }
                x.get('info', dict()).get('code'),
            )
            == 'M011',
            result_lines,
        )
    )
    if len(relevant_lines) >= 1:
        relevant_line = relevant_lines[-1]
        columns = relevant_line.get(
            'msg',
            relevant_line.get('info', dict()).get('msg'),
        )
    else:
        # for older dbt-core versions, we need to cross fingers a little harder
        relevant_lines = result_lines[1:]
        # also, the message key is different
        columns = relevant_lines[-1].get('message')
    # In some version of dbt columns are not passed as valid json but as
    # a string representation of a list
    is_string_list = (
        isinstance(columns, str)
        and columns.startswith('[')
        and columns.endswith(']')
    )
    if is_string_list:
        columns = ast.literal_eval(columns)
    return columns


def _structure_property_file_dict(location, resource_dict, columns_list):
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
        property_file_dict = _utils.parse_yaml(location)
        # check if properties files has content
        if not property_file_dict:
            property_file_dict = _get_property_header(
                resource_name, resource_type
            )
    # Else create a new dictionary that
    # will be used to create a new property file.
    else:
        property_file_dict = _get_property_header(resource_name, resource_type)
    # Get the sub-dictionaries of each existing column
    resource_type_plural = _SUPPORTED_RESOURCE_TYPES[resource_type]
    existing_columns_dict = {
        item['name']: item
        for item in property_file_dict[resource_type_plural][0]['columns']
    }
    # For each column we want in the property file,
    # reuse the sub-dictionary if it exists
    # or else create a new sub-dictionary
    property_file_dict[resource_type_plural][0]['columns'] = list()
    for column in columns_list:
        column_dict = existing_columns_dict.get(
            column, _get_property_column(column)
        )
        property_file_dict[resource_type_plural][0]['columns'].append(
            column_dict
        )
    return property_file_dict


def _get_property_header(resource, resource_type, properties=None):
    """
    Create a dictionary representing resources properties

    :param resource: The name of the resource for which to create a
        property header
    :param resource_type: The type of the resource (model, seed, etc.)
    :param properties: Dictionary containing existing properties to
        which to add a header (name, description, columns, etc.)
    :return: A dictionary representing resource properties
    """
    if not properties:
        properties = {'name': resource, 'description': "", 'columns': []}
    header_dict = {
        'version': 2,
        _SUPPORTED_RESOURCE_TYPES[resource_type]: [properties],
    }
    return header_dict


def _get_property_column(column_name):
    """
    Create a dictionary representing column properties

    :param column_name: Name of column
    :return: A dictionary representing column properties
    """
    column_dict = {'name': column_name, 'description': ""}
    return column_dict


def _assert_supported_resource_type(resource_type):
    """
    Assert that the given resource type is in the list of supported
        resource types

    :param resource_type: A dbt resource type
    :return: None
    """
    try:
        assert (
            resource_type is None
            or resource_type.lower() in _SUPPORTED_RESOURCE_TYPES
        )
    except AssertionError:
        msg = (
            f'Sorry, this tool only supports the following resource types:'
            f' {list(_SUPPORTED_RESOURCE_TYPES.keys())}'
        )
        _LOGGER.exception(msg)
        raise
