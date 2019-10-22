from .fixtures import (
    ProjectDefinition, rpc_server, Querier, project_dir, profiles_dir,
    postgres_profile, unique_schema, postgres_profile_data,
)


def test_rpc_basics(project_dir, profiles_dir, postgres_profile, unique_schema):
    project = ProjectDefinition(
        models={'my_model.sql': 'select 1 as id'}
    )
    project.write_to(project_dir)
    with rpc_server(
        project_dir=project_dir, schema=unique_schema, profiles_dir=profiles_dir
    ) as server:
        querier = Querier(server)

        token = querier.is_async_result(querier.run_sql('select 1 as id'))
        querier.is_result(querier.async_wait(token))

        token = querier.is_async_result(querier.run())
        querier.is_result(querier.async_wait(token))

        token = querier.is_async_result(querier.run_sql('select * from {{ ref("my_model") }}'))
        querier.is_result(querier.async_wait(token))

        token = querier.is_async_result(querier.run_sql('select * from {{ reff("my_model") }}'))
        querier.is_error(querier.async_wait(token))


def test_rpc_deps(project_dir, profiles_dir, postgres_profile, unique_schema):
    project = ProjectDefinition(
        models={
            'my_model.sql': 'select 1 as id',
        },
        packages={
            'packages': [
                {
                    'package': 'fishtown-analytics/dbt_utils',
                    'version': '0.2.1',
                },
            ],
        },
    )
    project.write_to(project_dir)
    with rpc_server(
        project_dir=project_dir, schema=unique_schema, profiles_dir=profiles_dir
    ) as server:
        querier = Querier(server)

        # we should be able to run sql queries at startup
        token = querier.is_async_result(querier.run_sql('select 1 as id'))
        querier.is_result(querier.async_wait(token))

        # the status should be something positive
        querier.is_result(querier.status())

        # deps should pass
        token = querier.is_async_result(querier.deps())
        querier.is_result(querier.async_wait(token))

        # queries should work after deps
        tok1 = querier.is_async_result(querier.run())
        tok2 = querier.is_async_result(querier.run_sql('select 1 as id'))

        querier.is_result(querier.async_wait(tok2))
        querier.is_result(querier.async_wait(tok1))

        # now break the project by giving an invalid URL (`dbt_util` instead of `utils`)
        project.packages['packages'][0]['package'] = 'fishtown-analytics/dbt_util'
        project.write_packages(project_dir, remove=True)

        # queries should still work because we haven't reloaded
        tok1 = querier.is_async_result(querier.run())
        tok2 = querier.is_async_result(querier.run_sql('select 1 as id'))

        querier.is_result(querier.async_wait(tok2))
        querier.is_result(querier.async_wait(tok1))

        # now run deps again, it should be sad
        token = querier.is_async_result(querier.deps())
        querier.is_error(querier.async_wait(token))
        # it should also not be running.
        result = querier.is_result(querier.ps(active=True, completed=False))
        assert result['rows'] == []

        # fix packages again
        project.packages['packages'][0]['package'] = 'fishtown-analytics/dbt_utils'
        project.write_packages(project_dir, remove=True)
        # keep queries broken, we haven't run deps yet
        querier.is_error(querier.run())

        # deps should pass now
        token = querier.is_async_result(querier.deps())
        querier.is_result(querier.async_wait(token))
        querier.is_result(querier.status())

        tok1 = querier.is_async_result(querier.run())
        tok2 = querier.is_async_result(querier.run_sql('select 1 as id'))

        querier.is_result(querier.async_wait(tok2))
        querier.is_result(querier.async_wait(tok1))
