# dbt-invoke
dbt-invoke is a CLI (built with [Invoke](http://www.pyinvoke.org/)) for creating, updating, and deleting 
[dbt](https://docs.getdbt.com/docs/introduction) [property files](https://docs.getdbt.com/reference/declaring-properties).


- Supported dbt resource types:
  - models
  - seeds
  - snapshots
  - analyses
  

- Under the hood, this tool works by combining the power of the [dbt ls](https://docs.getdbt.com/reference/commands/list) and 
  [dbt run-operation](https://docs.getdbt.com/reference/commands/run-operation) commands with dbt's built-in 
  `get_columns_in_query` macro.
  - This methodology allows the tool to work on [ephemeral models](https://docs.getdbt.com/docs/building-a-dbt-project/building-models/materializations#ephemeral) 
    and [analyses](https://docs.getdbt.com/docs/building-a-dbt-project/analyses), which other approaches, such as those 
    based on listing data warehouse tables/views, can miss.


## Installation

```shell
pip install dbt-invoke
```
  

## Usage

- You must have previously executed `dbt run`/`dbt seed`/`dbt snapshot` on the resources for which you wish to 
  create/update property files.
  - If you have made updates to your resources, execute the appropriate command (`dbt run`/`dbt seed`/`dbt snapshot`) 
    before using this tool to create/update property files.


- Property files will be created, updated, or deleted on a one-to-one basis in the same paths as the resource files 
  they represent (the only change being a `.yml` file extension).
  - For example, given a resource file in the location `models/marts/core/users.sql`, this tool will create, update, or 
    delete a property file in the location `models/marts/core/users.yml`.

    
- Any newly generated property files are created with the correct resource type, resource name, and columns.  A blank 
  description field will be included for each column and for the resource itself.
  - For example, when generating a new property file for a model `users` with column names `user_id` and `created_at`, 
    the following yaml will be generated:
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

  
- When updating an already existing property file, new columns in the resource will be added, and columns that no longer
  exist will be removed.


- You may fill in the blank `description` properties and add other properties (e.g. `tests`).  They will remain in-tact 
  when updating existing property files as long as the column/resource to which they belong still exists.

### Creating/Updating Property Files

```shell
dbt-invoke properties.update <options>

# OR, because 'update' is set as the default command:
dbt-invoke properties <options>
```

- The first time you run this command, you should be prompted to add a short macro called `_log_columns_list` to your 
  dbt project.
  - You may accept the prompt to add it automatically.
  - Otherwise, copy/paste it into one your dbt project's macro-paths yourself.
  - To print the macro, at any time, run `dbt-invoke properties.echo-macro`.


- `<options>` uses the same arguments as the `dbt ls` command to allow flexibility in selecting the dbt resources for 
  which you wish to create/update property files (run `dbt ls --help` for details).
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


- Note: This tool supports only the long flags of `dbt ls` options (for example: `--models`, and not short flags like 
  `-m`).
  

- An additional flag `--log-level` is made available to alter the verbosity of logs.
  - It accepts one of Python's standard logging levels (debug, info, warning, error, critical).
  

- Some examples:
  ```shell
  # Create/update property files for all supported resource types
  dbt-invoke properties
  
  # Create/update property files for all models in a models/marts directory
  dbt-invoke properties --models marts
  
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
- `<options>` uses the same arguments as for creating/updating property files.


### Help

- To view the list of available commands and their short descriptions, run:
  ```shell
  dbt-invoke --list
  ```

- To view in depth command descriptions and available options/flags, run:
  ```shell
  dbt-invoke <command_name> --help
  ```
