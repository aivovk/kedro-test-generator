"""Generate Kedro Tests

Usage: `python generate_kedro_tests.py PATH_TO_KEDRO_PROJECT`

This scripts searches the kedro project for all pipelines. For a pipeline to be
found, it must meet the following conditions:

1) Be located in a `src/PROJECT_MODULE/pipelines/PIPELINE_FOLDER` folder or a
sub-directory of it.

2) The directory must contain a `pipeline.py` file.

3) The directory must contain a `__init__.py` file that imports the
`create_pipeline` function.

The script then creates a folder and boilerplate testing files. If one of the
files exists already, the files are not overwritten (to rewrite the boilerplate
files, delete the existing first). The testing directory structure mirrors the
project structure. I.e. if there was a `create_pipeline` function in the
PIPELINE_FOLDER module from above, the directory for the test files will be:
`src/tests/pipelines/PIPELINE_FOLDER`. Three testing files are created in that
directory:

1) `conftest.py` - this file contains starting code for python fixtures for
every input for the pipeline (including intermediate inputs). The fixture names
are modified to be valid python identifier names (only contain letters, numbers,
underscores, and do not start with letters). This file also contains a kedro
catalog fixture with an entry for every free input of the pipeline (excluding
intermediate inputs). The dataset names are mapped to memory datasets containing
the individual input fixtures.

2) `test_nodes.py` - this file contains function signatures for every node of
the pipeline. The function is called `test_` + the node function name. The
corresponding input fixtures are listed as the arguments.

TODO: there could be multiple nodes reusing the same node function, which will
currently create a naming conflict if they are part of the same pipeline. Each
node test will have unique input fixtures. This can be resolved either by
creating a unique name (or using the node name) - if this file is meant to test
the nodes in the context of the pipeline, OR create a single test for each node
function - if this file is meant to test the contents of the nodes.py
file. However, the nodes of the pipeline may use functions that are not in the
`nodes.py` file, in which case the import statement will be incorrect.

3) `test_pipeline.py` - this file contains a single test function that accepts
the catalog fixture as an argument and runs the entire pipeline.

"""
import importlib
import os
from pathlib import Path
import re
import sys

from kedro.framework.startup import bootstrap_project


INDENT = "    "  # four spaces


def clean_varname(s: str) -> str:
    """Replace an invalid character (not a letter, digit, or underscore) OR a
    digit at the start of the string, with underscores to make a valid Python
    variable name

    """
    return re.sub("\W|^(?=\d)", "_", s)


if __name__ == "__main__":
    project_dir = sys.argv[1]
    if not os.path.isabs(project_dir):
        project_root = Path.cwd() / project_dir
    else:
        project_root = Path(project_dir)

    metadata = bootstrap_project(project_root)
    project_module = metadata.package_name
    pipeline_path = metadata.source_dir / project_module / "pipelines"

    for path, _, files in os.walk(pipeline_path):
        if "pipeline.py" in files:
            pipeline_name = os.path.basename(path)
            # for some reason the import only works relative to the project
            # /src/ directory
            # [1:] removes the leading "/"
            module_name = path.replace(str(metadata.source_dir), "")[1:].replace(
                "/", "."
            )
            module = importlib.import_module(module_name)
            pipeline = module.create_pipeline()
        else:
            continue

        # generate conftest.py code
        fixtures_code = """\"\"\"

\"\"\"
import pytest
from kedro.io import DataCatalog, MemoryDataSet

"""
        for dataset in pipeline.all_inputs():
            var = clean_varname(dataset)
            fixtures_code += f"""
@pytest.fixture
def {var}():
    return None

"""
        fixtures_code += f"""
@pytest.fixture
def catalog({', '.join(clean_varname(dataset) for dataset in pipeline.inputs())}):
    return DataCatalog(
        data_sets={{
"""
        for dataset in pipeline.inputs():
            var = clean_varname(dataset)
            fixtures_code += (
                f'{INDENT}{INDENT}{INDENT}"{dataset}": MemoryDataSet({var}),\n'
            )
        fixtures_code += f"{INDENT}{INDENT}}}\n{INDENT})\n"

        # generate test_nodes.py code
        node_test_code = f"""\"\"\"

\"\"\"
from {project_module}.pipelines.{pipeline_name}.nodes import """
        node_test_code += ", ".join(node.func.__name__ for node in pipeline.nodes)
        node_test_code += "\n"

        for node in pipeline.nodes:
            arg_list = ", ".join(clean_varname(dataset) for dataset in node.inputs)
            node_test_code += f"""

def test_{node.func.__name__}({arg_list}):
    output = {node.func.__name__}({arg_list})
    assert False
"""

        # generate test_pipeline.py code
        pipeline_test_code = f"""\"\"\"

\"\"\"
from kedro.runner import SequentialRunner
from {project_module}.pipelines.{pipeline_name} import create_pipeline


def test_pipeline(catalog):
    runner = SequentialRunner()
    pipeline = create_pipeline()
    outputs = runner.run(pipeline, catalog)
    assert True
"""
        # write test files
        test_dir = Path(
            path.replace(
                str(project_root / "src" / metadata.package_name),
                str(project_root / "src" / "tests"),
            )
        )
        node_test_path = test_dir / "test_nodes.py"
        conftest_path = test_dir / "conftest.py"
        pipeline_test_path = test_dir / "test_pipeline.py"
        init_test_path = test_dir / "__init__.py"
        if not os.path.exists(test_dir):
            os.makedirs(test_dir)
        if os.path.exists(node_test_path):
            print(f"{node_test_path} already exists, skipping write to file")
        elif os.path.exists(conftest_path):
            print(f"{conftest_path} already exists, skipping write to file")
        elif os.path.exists(pipeline_test_path):
            print(f"{pipeline_test_path} already exists, skipping write to file")
        else:
            print(f"writing {pipeline_name} tests")
            with open(node_test_path, "w") as f:
                f.write(node_test_code)
            with open(conftest_path, "w") as f:
                f.write(fixtures_code)
            with open(pipeline_test_path, "w") as f:
                f.write(pipeline_test_code)
            with open(init_test_path, "w") as f:
                pass
