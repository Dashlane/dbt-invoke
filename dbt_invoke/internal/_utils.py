import json
import logging
from pathlib import Path
import sys
import platform

import yaml
from dbt.task.base import get_nearest_project_dir

MACROS = {
    '_log_columns_list': (
        "\n{# This macro is intended for use by dbt-invoke #}"
        "\n{% macro _log_columns_list(sql=none, resource_name=none) %}"
        "\n    {% if sql is none %}"
        "\n        {% set sql = 'select * from ' ~ ref(resource_name) %}"
        "\n    {% endif %}"
        "\n    {% if execute %}"
        "\n        {{ log(get_columns_in_query(sql), info=True) }}"
        "\n    {% endif %}"
        "\n{% endmacro %}\n"
    )
}
DBT_GLOBAL_ARGS = {
    'log-format': 'json',
}
DBT_LS_ARG_HELP = (
    'An argument for listing dbt resources (run "dbt ls --help" for details)'
)
DBT_LS_ARGS = {
    'resource_type': {'help': DBT_LS_ARG_HELP, 'resource_selector': True},
    'select': {'help': DBT_LS_ARG_HELP, 'resource_selector': True},
    'models': {'help': DBT_LS_ARG_HELP, 'resource_selector': True},
    'exclude': {'help': DBT_LS_ARG_HELP, 'resource_selector': True},
    'selector': {'help': DBT_LS_ARG_HELP, 'resource_selector': True},
    'project_dir': {'help': DBT_LS_ARG_HELP, 'resource_selector': False},
    'profiles_dir': {'help': DBT_LS_ARG_HELP, 'resource_selector': False},
    'profile': {'help': DBT_LS_ARG_HELP, 'resource_selector': False},
    'target': {'help': DBT_LS_ARG_HELP, 'resource_selector': False},
    'vars': {'help': DBT_LS_ARG_HELP, 'resource_selector': False},
    'bypass_cache': {'help': DBT_LS_ARG_HELP, 'resource_selector': False},
    'state': {'help': DBT_LS_ARG_HELP, 'resource_selector': False},
}


def get_logger(name, level='INFO'):
    """
    Create a logger

    :param name: The name of the logger to create
    :param level: One of Python's standard logging levels
        (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    :return: A logging.Logger object
    """
    logger = logging.getLogger(name)
    if logger.hasHandlers():
        logger.handlers.clear()
    handler = logging.StreamHandler(stream=sys.stdout)
    formatter = logging.Formatter(
        '{name} | {levelname:^8} | {message}', style='{'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(level.upper())
    return logger


def parse_yaml(location):
    """
    Parse a yaml file

    :param location: The location of the yaml file to parse
    :return: The contents of the yaml file
    """
    with open(location, 'r') as stream:
        try:
            parsed_yaml = yaml.safe_load(stream)
            return parsed_yaml
        except yaml.YAMLError as exc:
            sys.exit(exc)


def write_yaml(location, data):
    """
    Write a yaml file

    :param location: The location to which to write the yaml file
    :param data: The object which will be written to the yaml file
    :return: None
    """
    try:
        with open(location, 'w') as stream:
            yaml.safe_dump(data, stream, sort_keys=False)
    except yaml.YAMLError as exc:
        sys.exit(exc)


def get_project_info(ctx, project_dir=None):
    """
    Get project level configurations for a dbt project
    and store them in ctx (an Invoke context object)

    :param ctx: An Invoke context object
    :param project_dir: A directory containing a dbt_project.yml file
    :return: None
    """
    project = Project(project_dir)
    project_path = get_nearest_project_dir(project)
    project_yml_path = Path(project_path, 'dbt_project.yml')
    # Get project configuration values from dbt_project.yml
    # (or use dbt defaults)
    project_yml = parse_yaml(project_yml_path)
    project_name = project_yml.get('name')
    target_path = Path(project_path, project_yml.get('target-path', 'target'))
    compiled_path = Path(target_path, 'compiled', project_name)
    macro_paths = [
        Path(project_path, macro_path)
        for macro_path in project_yml.get('macro-paths', ['macros'])
    ]
    # Set context config key-value pairs
    ctx.config['project_path'] = project_path
    ctx.config['project_name'] = project_name
    ctx.config['target_path'] = target_path
    ctx.config['compiled_path'] = compiled_path
    ctx.config['macro_paths'] = macro_paths


def dbt_ls(
    ctx,
    supported_resource_types=None,
    hide=True,
    output='json',
    logger=None,
    **kwargs,
):
    """
    Run the "dbt ls" command with options

    :param ctx: An Invoke context object
    :param supported_resource_types: A list of supported resource types
        to default to if no resource selection arguments are given
        (resource_type, select, models, exclude, selector)
    :param hide: Whether to suppress command line logs
    :param output: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param logger: A logging.Logger object
    :param kwargs: Additional arguments for listing dbt resources
        (run "dbt ls --help" for details)
    :return: A list of lines from stdout
    """
    if not logger:
        logger = get_logger('')
    resource_selection_arguments = {
        arg: kwargs.get(arg)
        for arg, details in DBT_LS_ARGS.items()
        if details['resource_selector']
    }
    # Use default arguments if no resource selection arguments are given
    default_arguments = list()
    if not any(resource_selection_arguments.values()):
        default_arguments.append(f'--select {ctx.config["project_name"]}')
        if supported_resource_types:
            for rt in supported_resource_types:
                default_arguments.append(f'{get_cli_kwargs(resource_type=rt)}')
    default_arguments = ' '.join(default_arguments)
    arguments = get_cli_kwargs(**kwargs)
    dbt_command_cli_args = f'{default_arguments} {arguments} --output {output}'
    dbt_global_cli_args = get_cli_kwargs(**DBT_GLOBAL_ARGS)
    command = f"dbt {dbt_global_cli_args} ls {dbt_command_cli_args}"
    logger.debug(f'Running command: {command}')
    result = ctx.run(command, hide=hide)
    result_lines = result.stdout.splitlines()
    result_lines_filtered = list()
    for line in result_lines:
        # Because we set the dbt global arg "--log-format json", if
        # line is valid json then it may be an actual result or it
        # may be some other output from dbt, like a warning.
        try:
            line_dict = json.loads(line)
        # If line is not valid json, then it should be an actual
        # result. This is because even when the "dbt ls" command
        # arg "--output" is not set to json, non-result logs will
        # still be in json format (due to the dbt global arg
        # "--log-format json").
        except ValueError:
            result_lines_filtered.append(line)
            continue
        # If 'resource_type' is in line_dict, then this is likely
        # an actual result and not something else like a warning.
        if 'resource_type' in line_dict:
            result_lines_filtered.append(line_dict)
        # Else, if 'resource_type' is not in line_dict, this may be
        # a warning from dbt, so log it.
        else:
            logger.warning(f'Extra output from "dbt ls" command: {line}')
    return result_lines_filtered


def get_cli_kwargs(**kwargs):
    """
    Transform Python keyword arguments to CLI keyword arguments

    :param kwargs: Keyword arguments
    :return: CLI keyword arguments
    """
    return ' '.join(
        [
            f'--{k.replace("_", "-")} {str(v).replace(",", " ")}'
            for k, v in kwargs.items()
            if v
        ]
    )


def dbt_run_operation(
    ctx,
    macro_name,
    project_dir=None,
    profiles_dir=None,
    profile=None,
    target=None,
    vars=None,
    bypass_cache=None,
    hide=True,
    logger=None,
    **kwargs,
):
    """
    Perform a dbt run-operation
    (see https://docs.getdbt.com/reference/commands/run-operation/)

    :param ctx: An Invoke context object
    :param macro_name: Name of macro that will be run
    :param project_dir: An argument for the dbt run-operation command
        (run "dbt run-operation --help" for details)
    :param profiles_dir: An argument for the dbt run-operation command
        (run "dbt run-operation --help" for details)
    :param profile: An argument for the dbt run-operation command
        (run "dbt run-operation --help" for details)
    :param target: An argument for the dbt run-operation command
        (run "dbt run-operation --help" for details)
    :param vars: An argument for the dbt run-operation command
        (run "dbt run-operation --help" for details)
    :param bypass_cache: An argument for the dbt run-operation command
        (run "dbt run-operation --help" for details)
    :param hide: Whether to suppress command line logs
    :param logger: A logging.Logger object
    :param kwargs: Arguments for defining macro's parameters
    :return: stdout in list where each item is one line of output
    """
    if not logger:
        logger = get_logger('')
    dbt_command_args = {
        'project_dir': project_dir or ctx.config['project_path'],
        'profiles_dir': profiles_dir,
        'profile': profile,
        'target': target,
        'vars': vars,
        'bypass_cache': bypass_cache,
    }
    dbt_command_cli_args = get_cli_kwargs(**dbt_command_args)
    dbt_global_cli_args = get_cli_kwargs(**DBT_GLOBAL_ARGS)
    macro_kwargs = json.dumps(kwargs, sort_keys=False)
    if platform.system().lower().startswith('win'):
        # Format YAML string for Windows Command Prompt
        macro_kwargs = macro_kwargs.replace('"', '\\"')
        macro_kwargs = macro_kwargs.replace('\\\\"', '"')
        macro_kwargs = macro_kwargs.replace('>', '^>')
        macro_kwargs = macro_kwargs.replace('<', '^<')
        macro_kwargs = f'"{macro_kwargs}"'
    else:
        # Format YAML string for Mac/Linux (bash)
        macro_kwargs = macro_kwargs.replace("'", """'"'"'""")
        macro_kwargs = f"'{macro_kwargs}'"
    command = (
        f"dbt {dbt_global_cli_args} run-operation {dbt_command_cli_args}"
        f" {macro_name} --args {macro_kwargs}"
    )
    logger.debug(f'Running command: {command}')
    result = ctx.run(command, hide=hide)
    result_lines = [json.loads(data) for data in result.stdout.splitlines()]
    return result_lines


def get_macro(macro_name):
    """
    Get the configured macro

    :param macro_name: The name of the macro to add
    :return: The macro itself in string form
    """
    return MACROS[macro_name]


def macro_exists(ctx, macro_name, logger=None, **kwargs):
    """
    Check if a given macro name exists in the dbt project

    :param ctx: An Invoke context object
    :param macro_name: The name of the macro to check for
    :param logger: A logging.Logger object
    :param kwargs: Additional arguments for dbt_run_operation
    :return: True if the macro exists, else False
    """
    if not logger:
        logger = get_logger('')
    try:
        dbt_run_operation(
            ctx,
            macro_name,
            logger=logger,
            sql=f'SELECT 1 AS __dbt_invoke_check_macro_{macro_name} LIMIT 0',
            **kwargs,
        )
    except Exception as exc:
        if all(
            [
                s in str(exc).lower()
                for s in ['runtime error', 'not', 'find', macro_name]
            ]
        ):
            return False
        else:
            logger.exception(exc)
    return True


def add_macro(ctx, macro_name, logger=None):
    """
    Add a macro to a dbt project if the user confirms

    :param ctx: An Invoke context object
    :param macro_name: The name of the macro to add
    :param logger: A logging.Logger object
    :return: None
    """
    if not logger:
        logger = get_logger('')
    location = Path(ctx.config['macro_paths'][0], f'{macro_name}.sql')
    logger.warning(
        f'This command requires the following macro:'
        f'\n{get_macro(macro_name)}'
    )
    question = (
        f'Would you like to add the macro "{macro_name}"'
        f' to the following location?:\n{location}'
    )
    prompt = (
        'Please enter "y" to confirm macro addition,'
        ' "n" to abort,'
        ' or "a" to provide an alternate location.'
    )
    add_confirmation = input(f'{question}\n{prompt}\n')
    while add_confirmation.lower() not in ['y', 'n', 'a']:
        add_confirmation = input(f'{prompt}\n')
    if add_confirmation.lower() == 'n':
        logger.info('Macro addition aborted.')
        sys.exit()
    elif add_confirmation.lower() == 'a':
        alternate_prompt = (
            'Please enter a path (ending in ".sql")'
            ' to a new or existing macro file'
            ' in one of your existing dbt macro-paths.\n'
        )
        location = Path(input(alternate_prompt))
        absolute_macro_paths = [
            mp.resolve() for mp in ctx.config['macro_paths']
        ]
        while (
            location.parent.resolve() not in absolute_macro_paths
            or location.suffix.lower() != '.sql'
        ):
            if location.parent.resolve() not in absolute_macro_paths:
                not_a_macro_path = (
                    f'{location.parent.resolve()}'
                    f' is not an existing macro path.'
                )
                existing_macro_paths_are = 'Your existing macro paths are:'
                existing_macro_paths = "\n".join(
                    [str(mp) for mp in absolute_macro_paths]
                )
                logger.warning(
                    f'{not_a_macro_path}'
                    f'\n{existing_macro_paths_are}'
                    f'\n{existing_macro_paths}'
                )
            if location.suffix.lower() != '.sql':
                logger.warning('File suffix must be ".sql".')
            location = Path(input(alternate_prompt))
    with location.open('a') as f:
        f.write(f'{get_macro(macro_name)}')
        logger.info(f'Macro "{macro_name}" added to {location.resolve()}')


class Project:
    """
    A placeholder class for use with get_nearest_project_dir
    """

    def __init__(self, project_dir=None):
        """
        Initialize a Project object

        :param project_dir:
        """
        self.project_dir = project_dir
