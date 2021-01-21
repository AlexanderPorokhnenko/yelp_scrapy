#!/usr/bin/env bash

PYTHONCMD=python3

#Find main project direcory
MAINDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "${MAINDIR}"
MAINDIR=$PWD

#Create new virtualenv
$PYTHONCMD -m venv env
source env/bin/activate
$PYTHONCMD -m pip install wheel
$PYTHONCMD -m pip install -r requirements.txt

