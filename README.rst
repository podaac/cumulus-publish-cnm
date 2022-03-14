====
cumulus-publish-cnm
====
This is the python code for the lambda `cumulus-publish-cmn`.
It takes in a list of cnm's (Cloud Notification Mechanism) and publishes each one to the specified ingestion SNS (provider-input-sns)

Build (as a zip to load to AWS Lambda)
====
`This <https://chariotsolutions.com/blog/post/building-lambdas-with-poetry/>`_ page contains some good info on the overall "building a zip with poetry that's compatible with AWS Lambda".

Auto build script
----
TL;DR ::

    chmod u+x build.sh;
    ./build.sh;
    chmod u-x build.sh;

to run the following commands in order and build the artifact.zip

Manual build
----
This command creates the ``dist/`` folder::

    poetry build

This command downloads the dependency files from the just created .whl file, along with the lambda_handler function in ``cumulus_publish_cnm/cumulus_publish_cnm.py``, and places them in the ``package`` folder::

    poetry run pip install --upgrade -t package dist/*.whl

Note that **boto3** and **moto** are not being pulled in. I've specifically excluded them via having them as ``tool.poetry.dev-dependencies`` only (via the ``pyproject.toml`` file)

The last command used is::

    cd package ; zip -r ../artifact.zip . -x '*.pyc'

Which zips and creates the ``artifact.zip`` file, containing all files found in ``package/`` excluding .pyc files

Then upload ``artifact.zip`` to any location you plan to use it

* Upload directly as a lambda with ``aws lambda update-function-code``
* Upload to your AWS S3 ``lambdas/`` folder so that your Cumulus Terraform Build can use it

Testing
====
Run the ``poetry run pytest`` command; you should see a similar output::

    platform darwin -- Python 3.8.9, pytest-7.0.1, pluggy-1.0.0
    rootdir: /Users/hryeung/PycharmProjects/jpl/cumulus_publish_cnm
    collected 3 items

    tests/test_cumulus_publish_cnm.py ... [100%]

    ====== 3 passed in 0.99s ======

