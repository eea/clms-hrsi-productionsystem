#!/usr/bin/env bash

# Be sure we fail on error and output debugging information
set -e
trap 'echo "$0: error on line $LINENO"' ERR

here="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

#-------------------------------------------------------------------------

echo ""
echo "Warning!"
echo ""
echo "This will erase all the data that are stored in the database"
echo "and this operation can't be undone."
echo ""
echo "Are you sure you want to clear the database?"
read -r -p "   type 'yes' if it is what you want: " response
if [[ "$response" =~ "yes" ]]
then
  echo "Proceed to clearing an initializing the database"
else
  echo "Leave the database in its current state"
  exit 0
fi

sudo systemctl stop postgrest
sudo systemctl stop postgresql
sudo pg_dropcluster --stop 12 main
sudo pg_createcluster --start 12 main
sudo systemctl start postgresql
sudo -u postgres psql -f $here/init_database.sql
sudo systemctl start postgrest

echo "The database is now cleared and initialized."