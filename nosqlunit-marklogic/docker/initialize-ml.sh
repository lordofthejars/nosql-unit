#!/bin/bash
################################################################
# This script initializes MarkLogic and creates the admin account.
#
# Usage:
#
# initialize-ml.sh -u <admin-username> -p <admin-password> -r <secuity-realm>
#     -u  The admin username to create
#     -p  The password for the created admin username
#     -r  The desired security realm string, if any. Default: "public"
################################################################

################################################################
# The following are used to create the first MarkLogic
# Administrator account. The values are passed into this script
# from commandline arguements.
################################################################
USER=""
PASS=""
SEC_REALM="public"

################################################################
# Default curl authorization mode. MarkLogic uses "digest" as
# the default.
AUTH_MODE="digest"

# when MarkLogic restarts, how many times to check before deciding
# that the restart failed.
N_RETRY=5
# Sleep for this many seconds before trying to see if MarkLogic
# has restarted.
RETRY_INTERVAL=10

# Get the hostname of this server.
ML_HOST=$(hostname -f)

# Curl command for all requests. Suppress progress meter (-s),
# but still show errors (-S)
CURL="curl -s -S"
# Curl command when authentication is required, after security
# is initialized.
AUTH_CURL="${CURL} --${AUTH_MODE} --user ${USER}:${PASS}"

#######################################################
# Parse the command line. After MarkLogic initializes,
# an administrator account must be created. The desired
# username, password and realm for the admin account is
# given in commandline arguments.
########################################################

# Check that at least 2 arguments were given for the
# admin and password. If not, exit with error.
if [ $# -ge 2 ]; then 
  OPTIND=1
  while getopts ":r:p:u:" opt; do
    case "$opt" in
      r) SEC_REALM=$OPTARG ;;
      p) PASS=$OPTARG ;;
      u) USER=$OPTARG ;;
      \?) echo "Unrecognized option: -$OPTARG" >&2; exit 1 ;;
    esac
  done
  shift $((OPTIND-1))
else
  echo "ERROR: Desired admin username and password are required." >&2
  echo "USAGE: initialize-ml.sh -u <admin-username> -p <admin-password> [-r <security-realm-string>]" >&2
  exit 1
fi

#######################################################
# restart_check(hostname, baseline_timestamp, caller_lineno)
#
# Use the timestamp service to detect a server restart, given a
# a baseline timestamp. Use N_RETRY and RETRY_INTERVAL to tune
# the test length. Include authentication in the curl command
# so the function works whether or not security is initialized.
#   $1 :  The hostname to test against
#   $2 :  The baseline timestamp
#   $3 :  Invokers LINENO, for improved error reporting
# Returns 0 if restart is detected, exits with an error if not.
#
function restart_check {
  sleep 2
  LAST_START=`$CURL "http://$1:8001/admin/v1/timestamp"`
  echo "last_start=$LAST_START for host=$1"
  for i in `seq 1 ${N_RETRY}`; do
    if [ "$2" == "$LAST_START" ] || [ "$LAST_START" == "" ]; then
      sleep ${RETRY_INTERVAL}
      LAST_START=`$CURL "http://$1:8001/admin/v1/timestamp"`
      echo "last_start iteration $i=$LAST_START"
    else
      return 0
    fi
  done
  echo "ERROR: Line $3: Failed to restart $1"
  exit 1
}

TIMESTAMP=`$CURL "http://$ML_HOST:8001/admin/v1/timestamp"`
#echo "After ML start TIMESTAMP=$TIMESTAMP"

restart_check $ML_HOST $TIMESTAMP 102

#######################################################
#  Initialize this host
#  (1) POST /admin/v1/init (joining host)
#  GET /admin/v1/timestamp is used to confirm restarts.

echo "Initializing host: $ML_HOST..."

# (1) Initialize MarkLogic Server on the joining host
TIMESTAMP=`$CURL -X POST -d "" http://${ML_HOST}:8001/admin/v1/init`
#echo "TIMESTAMP returned=$TIMESTAMP"

restart_check $ML_HOST $TIMESTAMP 115

echo "$ML_HOST initialized."

echo "Configuring $ML_HOST Admin account..."
TIMESTAMP=`$CURL -X POST -H "Content-type: application/x-www-form-urlencoded" \
--data "admin-username=${USER}" --data "admin-password=${PASS}" \
--data "realm=${SEC_REALM}" \
http://${ML_HOST}:8001/admin/v1/instance-admin`
# echo "TIMESTAMP returned=$TIMESTAMP"

echo "$ML_HOST admin account created"

echo "$ML_HOST initialization completed!"