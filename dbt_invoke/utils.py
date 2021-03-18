import json
import logging
import sys
from pathlib import Path

import yaml
from dbt.task.base import get_nearest_project_dir

PROPERTIES = {
    'supported_resource_types': {
        'model': 'models',
        'seed': 'seeds',
        'snapshot': 'snapshots',
        'analysis': 'analyses'
    },
    'resource_selection_arguments': ['resource_type', 'select', 'models', 'exclude', 'selector']
}

MACROS = {
    '_log_columns_list': "\n"
                         "{# This macro is intended for use by dbt-invoke #}\n"
                         "{% macro _log_columns_list(sql=none, resource_name=none) %}\n"
                         "    {% if sql is none %}\n"
                         "        {% set sql = 'select * from ' ~ ref(resource_name) %}\n"
                         "    {% endif %}\n"
                         "    {% if execute %}\n"
                         "        {{ log(get_columns_in_query(sql), info=True) }}\n"
                         "    {% endif %}\n"
                         "{% endmacro %}\n"
}


def get_logger(name, level='INFO'):
    """
    Create a logger
    :param name: The name of the logger to create
    :param level: One of Python's standard logging levels (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    :return: A logging.Logger object
    """
    logger = logging.getLogger(name)
    if logger.hasHandlers():
        logger.handlers.clear()
    handler = logging.StreamHandler(stream=sys.stdout)
    formatter = logging.Formatter('{name}[{levelname}]:: {message}', style='{')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(level.upper())
    return logger


def parse_yaml(location):
    """
    Parse a YAML file
    :param location: The location of the YAML file to parse
    :return: The contents of the YAML file represented as a dictionary or list,
             depending on the structure of the YAML itself.
    """
    with open(location, 'r') as stream:
        try:
            parsed_yaml = yaml.safe_load(stream)
            return parsed_yaml
        except yaml.YAMLError as exc:
            sys.exit(exc)


def write_yaml(location, yml_representable_obj):
    """
    Write a YAML file
    :param location: The location to which to write the YAML file
    :param yml_representable_obj: A Python object like a dictionary or list that can be represented in YAML form
    :return: None
    """
    try:
        with open(location, 'w') as stream:
            yaml.safe_dump(yml_representable_obj, stream, sort_keys=False)
    except yaml.YAMLError as exc:
        sys.exit(exc)


def get_project_info(ctx, project_dir=None, logger=None):
    """
    Get project level configurations for a dbt project and store them in ctx (an Invoke context object)
    :param ctx: An Invoke context object
    :param project_dir: Location of a dbt project containing a dbt_project.yml file
    :param logger: A logging.Logger object
    :return: None
    """
    if not logger:
        logger = get_logger('')
    project = Project(project_dir)
    project_path = get_nearest_project_dir(project)
    project_yml_path = Path(project_path, 'dbt_project.yml')
    # Get project configuration values from dbt_project.yml (or use dbt defaults)
    project_yml = parse_yaml(project_yml_path)
    project_name = project_yml.get('name')
    target_path = Path(project_path, project_yml.get('target-path', 'target'))
    compiled_path = Path(target_path, 'compiled', project_name)
    macro_paths = [Path(project_path, macro_path) for macro_path in project_yml.get('macro-paths', ['macros'])]
    # Set context config key-value pairs
    ctx.config['project_path'] = project_path
    ctx.config['project_name'] = project_name
    ctx.config['target_path'] = target_path
    ctx.config['compiled_path'] = compiled_path
    ctx.config['macro_paths'] = macro_paths


def dbt_ls(ctx, supported_resource_types=None, hide=True, output='path', logger=None, **kwargs):
    """
    Run the "dbt ls" command with options
    :param ctx: An Invoke context object
    :param supported_resource_types: A list of supported resource types to default to if no resource selection arguments
                                     are given (resource_type, select, models, exclude, selector)
    :param hide: Whether to suppress command line logs
    :param output: Argument for listing dbt resources (run "dbt ls --help" for details)
    :param logger: A logging.Logger object
    :param kwargs: Additional arguments for listing dbt resources (run "dbt ls --help" for details)
    :return: stdout in list where each item is one line of output
    """
    if not logger:
        logger = get_logger('')
    resource_selection_arguments = {
        'resource_type': kwargs.get('resource_type'),
        'select': kwargs.get('select'),
        'models': kwargs.get('models'),
        'exclude': kwargs.get('exclude'),
        'selector': kwargs.get('selector')
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
    all_arguments = f'{default_arguments} {arguments} --output {output}'
    command = f"dbt ls {all_arguments}"
    logger.debug(f'Running command: {command}')
    result = ctx.run(command, hide=hide)
    result_lines = result.stdout.splitlines()
    if output == 'json':
        result_lines = [json.loads(result_json) for result_json in result_lines]
    return result_lines


def get_cli_kwargs(**kwargs):
    """
    Transform Python keyword arguments to CLI keyword arguments
    :param kwargs: Keyword arguments
    :return: CLI keyword arguments
    """
    return ' '.join([f'--{k.replace("_", "-")} {v}' for k, v in kwargs.items() if v])


def dbt_run_operation(ctx, macro_name, project_dir=None, profiles_dir=None, profile=None,
                      target=None, vars=None, bypass_cache=None, hide=True, logger=None, **kwargs):
    """
    Perform a dbt run-operation (see https://docs.getdbt.com/reference/commands/run-operation/)
    :param ctx: An Invoke context object
    :param macro_name: Name of macro that will be run
    :param project_dir: Argument for utils.dbt_run_operation (run "dbt run-operation --help" for details)
    :param profiles_dir: Argument for utils.dbt_run_operation (run "dbt run-operation --help" for details)
    :param profile: Argument for utils.dbt_run_operation (run "dbt run-operation --help" for details)
    :param target: Argument for utils.dbt_run_operation (run "dbt run-operation --help" for details)
    :param vars: Argument for utils.dbt_run_operation (run "dbt run-operation --help" for details)
    :param bypass_cache: Argument for utils.dbt_run_operation (run "dbt run-operation --help" for details)
    :param hide: Whether to suppress command line logs
    :param logger: A logging.Logger object
    :param kwargs: Arguments for defining macro's parameters
    :return: stdout in list where each item is one line of output
    """
    if not logger:
        logger = get_logger('')
    dbt_kwargs = {
        'project_dir': project_dir or ctx.config['project_path'],
        'profiles_dir': profiles_dir,
        'profile': profile,
        'target': target,
        'vars': vars,
        'bypass_cache': bypass_cache
    }
    dbt_cli_kwargs = get_cli_kwargs(**dbt_kwargs)
    macro_kwargs = json.dumps(kwargs, sort_keys=False)
    command = f"dbt run-operation {dbt_cli_kwargs} {macro_name} --args '{macro_kwargs}'"
    logger.debug(f'Running command: {command}')
    result = ctx.run(command, hide=hide, warn=True)
    if result.failed:
        # If error is because the configured macro is not found, prompt the user to add the macro
        if all([s in result.stdout.lower() for s in ['runtime error', 'not', 'find', macro_name]]):
            logger.warning(f'This command requires the following macro:\n{get_macro(macro_name)}')
            add_macro(ctx, macro_name, logger=logger)
            logger.debug(f'Running command: {command}')
            result = ctx.run(command, hide=hide, warn=True)
        else:
            logger.error(f'{result.stdout}')
            sys.exit()
    result_lines = result.stdout.splitlines()[1:]
    return result_lines


def get_macro(macro_name):
    """
    Get the configured macro
    :param macro_name: The name of the macro to add
    :return: The macro itself in string form
    """
    return MACROS[macro_name]


def add_macro(ctx, macro_name, logger=None):
    """
    Add a macro to a dbt project
    :param ctx: An Invoke context object
    :param macro_name: The name of the macro to add
    :param logger: A logging.Logger object
    :return:
    """
    if not logger:
        logger = get_logger('')
    location = Path(ctx.config['macro_paths'][0], f'{macro_name}.sql')
    question = f'Would you like to add the macro "{macro_name}" to the following location?:\n{location}'
    prompt = 'Please enter "y" to confirm macro addition, "n" to abort, or "a" to provide an alternate location.'
    add_confirmation = input(f'{question}\n{prompt}\n')
    while add_confirmation.lower() not in ['y', 'n', 'a']:
        add_confirmation = input(f'{prompt}\n')
    if add_confirmation.lower() == 'n':
        logger.info('Macro addition aborted.')
        sys.exit()
    elif add_confirmation.lower() == 'a':
        alternate_prompt = 'Please enter a path (ending in ".sql") to a new or existing macro file in one of your ' \
                           'existing dbt macro-paths.\n'
        location = Path(input(alternate_prompt))
        absolute_macro_paths = [mp.resolve() for mp in ctx.config['macro_paths']]
        while location.parent.resolve() not in absolute_macro_paths or location.suffix.lower() != '.sql':
            if location.parent.resolve() not in absolute_macro_paths:
                not_a_macro_path = f'{location.parent.resolve()} is not an existing macro path.'
                existing_macro_paths_are = 'Your existing macro paths are:'
                existing_macro_paths = "\n".join([str(mp) for mp in absolute_macro_paths])
                logger.warning(f'{not_a_macro_path}\n{existing_macro_paths_are}\n{existing_macro_paths}')
            if location.suffix.lower() != '.sql':
                logger.warning('File suffix must be ".sql".')
            location = Path(input(alternate_prompt))
    with location.open('a') as f:
        f.write(f'{get_macro(macro_name)}')
        logger.info(f'Macro "{macro_name}" added to {location.resolve()}')


class Project:
    def __init__(self, project_dir=None):
        self.project_dir = project_dir
