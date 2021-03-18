from invoke import Collection, Program

from dbt_invoke import properties, version

ns = Collection()
ns.add_collection(properties)
program = Program(namespace=ns, version=version.__version__)
