#!/bin/bash

set -o xtrace

docker build -t benkl/playground .

(crontab -l ; echo "8 * * * * . $HOME/.profile; ${PWD}/extract.sh") | crontab -
