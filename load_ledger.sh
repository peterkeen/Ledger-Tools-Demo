#!/bin/sh

ledger -f $1 csv | psql -c "delete from ledger; copy ledger from stdin with csv; analyze ledger;" $2
psql -f ledger.sql $2

