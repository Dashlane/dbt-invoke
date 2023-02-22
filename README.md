# dbt-invoke

dbt-invoke is a CLI (built with [Invoke](http://www.pyinvoke.org/)) for 
creating, updating, and deleting
[dbt](https://docs.getdbt.com/docs/introduction) 
[property files](https://docs.getdbt.com/reference/declaring-properties).


- Supported dbt resource types:
  - models
  - seeds
  - snapshots
  - analyses
  

- Under the hood, this tool works by combining the power of the 
  [dbt ls](https://docs.getdbt.com/reference/commands/list) and 
  [dbt run-operation](https://docs.getdbt.com/reference/commands/run-operation)
  commands with dbt's built-in `get_columns_in_query` macro.
  - This methodology allows the tool to work on 
    [ephemeral models](https://docs.getdbt.com/docs/building-a-dbt-project/building-models/materializations#ephemeral) 
    and [analyses](https://docs.getdbt.com/docs/building-a-dbt-project/analyses),
    which other approaches, such as those based on listing data warehouse 
    tables/views, can miss.


## Installation

```shell
pip install dbt-invoke
```
  

## Usage

- You must have previously executed `dbt run`/`dbt seed`/`dbt snapshot` on the
  resources for which you wish to create/update property files.
  - If you have made updates to your resources, execute the appropriate command
    (`dbt run`/`dbt seed`/`dbt snapshot`) before using this tool to 
    create/update property files.


- Property files will be created, updated, or deleted on a one-to-one basis in
  the same paths as the resource files they represent (the only change being a
  `.yml` file extension).
  - For example, given a resource file in the location 
    `models/marts/core/users.sql`, this tool will create, update, or delete a 
    property file in the location `models/marts/core/users.yml`.
  - If your dbt project defines properties for multiple resources per `.yml` file, see the
    [Migrating to One Resource Per Property File](#migrating-to-one-resource-per-property-file)
    section.

    
- Any newly generated property files are created with the correct resource 
  type, resource name, and columns.  A blank description field will be included
  for each column and for the resource itself.
  - For example, when generating a new property file for a model `users` with 
    column names `user_id` and `created_at`, the following yaml will be 
    generated:
    - ```yaml
      version: 2
      models:
      - name: users
        description: ''
        columns:
        - name: user_id
          description: ''
        - name: created_at
          description: ''
      ```

  
- When updating an already existing property file, new columns in the resource
  will be added, and columns that no longer exist will be removed.


- You may fill in the blank `description` properties and add other properties 
  (e.g. `tests`).  They will remain intact when updating existing property 
  files as long as the column/resource name to which they belong still exists.


### Creating/Updating Property Files

```shell
dbt-invoke properties.update <options>

# OR, because 'update' is set as the default command:
dbt-invoke properties <options>
```

- The first time you run this command, you should be prompted to add a short 
  macro called `_log_columns_list` to your dbt project.
  - You may accept the prompt to add it automatically.
  - Otherwise, copy/paste it into one your dbt project's macro-paths yourself.
  - To print the macro, at any time, run `dbt-invoke properties.echo-macro`.


- `<options>` primarily uses the same arguments as the `dbt ls` command to 
  allow flexibility in selecting the dbt resources for which you wish to 
  create/update property files (run `dbt ls --help` for details).
  - --resource-type
  - --models
  - --select
  - --selector
  - --exclude
  - --project-dir
  - --profiles-dir
  - --profile
  - --target
  - --vars
  - --bypass-cache
  - --state


- Notes: 
  - This tool supports only the long flags of `dbt ls` options (for 
  example: `--models`, and not short flags like `-m`).
  - Multiple values for the same argument can be passed as a comma separated
  string (Example: `--models modelA,modelB`)
    - Keep in mind that dbt may not support multiple values for certain 
      options.


- Two additional flags are made available.
  - `--log-level` to alter the verbosity of logs.
    - It accepts one of Python's standard logging levels (debug, info, warning,
      error, critical).
  - `--threads` to set a maximum number of concurrent threads to use in 
    collecting resources' column information from the data warehouse and in 
    creating/updating the corresponding property files. Each thread will run 
    dbt's get_columns_in_query macro against the data warehouse.
  

- Some examples:
  ```shell
  # Create/update property files for all supported resource types
  dbt-invoke properties
  
  # Create/update property files for all supported resource types, using 4 concurrent threads
  dbt-invoke properties --threads 4
  
  # Create/update property files for all models in a models/marts directory
  dbt-invoke properties --models marts
  
  # Create/update property files for a 'users' model and an 'orders' models
  dbt-invoke properties --models users,orders
  
  # Create/update property files for a 'users' model and all downstream models
  dbt-invoke properties --models users+
  
  # Create/update property files for all seeds
  dbt-invoke properties --resource-type seed
  
  # Create/update a property file for a snapshot called 'users_snapshot'
  dbt-invoke properties --resource-type snapshot --select users_snapshot
  
  # Create/update property files when your working directory is above your dbt project directory
  dbt-invoke properties --project-dir path/to/dbt/project/directory
  ```


### Deleting Property Files

```shell
dbt-invoke properties.delete <options>
```
- `<options>` uses the same arguments as for creating/updating property files,
  except for `--threads`.


### Migrating to One Resource Per Property File
- Two conventions for dbt property files are:
  1. One resource per property file
  2. Multiple resources per property file


- dbt-invoke abides by the "One resource per property file" convention.
- If your dbt project uses the "Multiple resources per property file"
  convention, you can migrate to one resource per property file by using
  dbt-invoke's `properties.migrate` command as shown here:
  ```shell
  dbt-invoke properties.migrate <options>
  ```
  - Successfully migrated properties will be removed from existing
    multiple-resource property files.
    - At then end of migration, property files that are newly empty (other than
      `version: 2`) will be automatically deleted.
  - `<options>` uses the same arguments as for creating/updating property
    files, except for `--threads`.

### Help

- To view the list of available commands and their short descriptions, run:
  ```shell
  dbt-invoke --list
  ```

- To view in depth command descriptions and available options/flags, run:
  ```shell
  dbt-invoke <command_name> --help
  ```

### Limitations

- dbt-invoke will try to preserve formatting and comments when updating
  existing files. If you want to preserve line-breaks, use `>` or `|` on your
  multiline strings, as recommended
  [here](https://stackoverflow.com/a/21699210/15202709).
- In order to collect or update the list of columns that should appear in 
  each property file, dbt's `get_columns_in_query` macro is run for each
  matching resource. As of the time of writing, `get_columns_in_query` uses a
  SELECT statement [limited to zero rows](https://github.com/fishtown-analytics/dbt/blob/2b48152da66dbd7f07272983bbc261f1b6924f20/core/dbt/include/global_project/macros/adapters/common.sql#L11).
  While this is not typically a performance issue for table or incremental 
  materializations, execution may be slow for complex analyses, views, or 
  ephemeral materializations. 
  - This may be partially remedied by increasing the value of the `--threads` 
    option in `dbt-invoke properties.update`.
- dbt-invoke is tested against:
  - dbt 0.18, 0.19, 1.1, and 1.4
  - macos-latest, windows-latest, ubuntu-latest
- dbt-invoke has not been tested across different types of data warehouses.
