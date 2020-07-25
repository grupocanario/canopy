#!/bin/bash

cd "$KINGFISHER_COLLECT_DIR" || exit
rm -r "${FILES_STORE}"
PIPENV_IGNORE_VIRTUALENVS=1 pipenv run scrapy crawl colombia -a from_date="$1"

