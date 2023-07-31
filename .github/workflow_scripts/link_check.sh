#!/bin/bash

set -ex

source $(dirname "$0")/env_setup.sh

setup_env

python3 get_broken_links.py
