#!/bin/sh
poetry build
poetry run pip install --upgrade -t package dist/*.whl
cd package ; zip -r ../artifact.zip . -x '*.pyc'
cd ..
code_version=`python -c 'from cumulus_publish_cnm import __version__; print(__version__)'`
mv artifact.zip artifact-$code_version.zip