#!/bin/sh

if [[ "$1" == "" || "$2" == "" ]]; then
    echo "usage: $0 [ledger file] [database name]"
    exit 1
fi

ledger -f $1 csv | psql -c "delete from ledger; copy ledger from stdin with csv; analyze ledger;" $2
psql -f ledger.sql $2

