#!/bin/bash

cd "$KINGFISHER_PROCESS_DIR" || exit
PIPENV_IGNORE_VIRTUALENVS=1 pipenv run python ocdskingfisher-process-cli local-load --keep-collection-store-open "$1"  "$FILES_STORE"/colombia release_package
