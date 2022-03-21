#!/bin/sh
poetry build
poetry run pip install --upgrade -t package dist/*.whl
cd package ; zip -r ../artifact.zip . -x '*.pyc'
cd ..
code_version=`poetry version | awk '{print $2}'`
cp artifact.zip artifact-$code_version.zip
echo \*\* artifact-$code_version.zip created \*\*