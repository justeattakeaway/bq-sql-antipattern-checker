from pathlib import Path

import pytest
from jinja2 import Environment, FileSystemLoader, Template


@pytest.fixture
def jinja_env():
    template_path = (
        Path(__file__).parent.parent / "src" / "bq_sql_antipattern_checker" / "templates"
    )
    return Environment(loader=FileSystemLoader(template_path))


def test_render_with_limit(jinja_env):
    template = jinja_env.get_template("jobs_query.sql.j2")
    query = template.render(limit_row=10)
    jobs_query = query.format(
        query_project="my_project",
        region="us",
        date="'2024-06-01'",
    )
    assert "LIMIT 10" in jobs_query


def test_render_without_limit(jinja_env):
    template = jinja_env.get_template("jobs_query.sql.j2")
    query = template.render()
    jobs_query = query.format(
        query_project="my_project",
        region="us",
        date="'2024-06-01'",
    )
    assert "LIMIT 10" not in jobs_query
