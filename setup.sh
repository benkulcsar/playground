set -o xtrace

docker build -t benkl/playground .

(crontab -l ; echo "8 * * * * ${PWD}/extract.sh") | crontab -
