#!/bin/bash

cd "$KINGFISHER_VIEWS_DIR" || exit
PIPENV_IGNORE_VIRTUALENVS=1 pipenv run python ocdskingfisher-views-cli refresh-views "$1"