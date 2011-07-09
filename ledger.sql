drop table aggregated_accounts;
create table aggregated_accounts as
SELECT
    xtn_date,
    date_trunc('month', xtn_date)::date as xtn_month,
    CASE
        WHEN (xtn_date >= '2010-12-05' and extract('day' from xtn_date) between 1 and 14) THEN 1
        WHEN (xtn_date < '2010-12-05' and (extract('day' from xtn_date) between 1 and 6 or extract('day' from xtn_date) between 22 and 31)) THEN 1
        ELSE 2
    END as pay_period,
    CASE
        WHEN (account ~ 'Expenses:Insurance')         THEN 'Expenses:Insurance'
        WHEN (account ~ 'Expenses:Taxes')             THEN 'Expenses:Taxes'
        WHEN (account ~ 'Expenses:Hous')              THEN 'Expenses:Housing'
        WHEN (account ~ 'Income' and account !~ 'Salary') THEN 'Income:Other'
        WHEN (account ~ 'Expenses:Food' and account !~ 'Groceries') THEN 'Expenses:Food:Out'
        ELSE account
    END AS account,
    amount,
    commodity
FROM
    ledger;
create index idx_aggregated_accounts_account on aggregated_accounts (account);
create index idx_aggregated_accounts_xtn_date on aggregated_accounts (xtn_date);
create index idx_aggregated_accounts_xtn_month on aggregated_accounts (xtn_month);
create index idx_aggregated_accounts_pay_period on aggregated_accounts (pay_period);

analyze aggregated_accounts;

delete from aggregated_accounts_by_month;
insert into aggregated_accounts_by_month
select 
    xtn_month,
    account,
    commodity,
    sum(amount)
from
    aggregated_accounts
group by
    xtn_month,
    account,
    commodity
;

analyze aggregated_accounts_by_month;

create or replace function this_month() returns date as $$
    select date_trunc('month', now())::date        
$$ language sql immutable;

create or replace function this_month(text) returns date as $$
    select date_trunc('month', $1::date)::date        
$$ language sql immutable;

create or replace function current_burn(text) returns numeric as $$
    select
        avg(amount)
    from (
        select
             xtn_month,
             sum(amount) as amount
        from
            aggregated_accounts_by_month
        where
            account ~ 'Expenses'
            and account !~ 'Depreciation'
            and account !~ 'Taxes'
            and account !~ 'Interest'
            and account !~ 'Travel'
            and xtn_month >= this_month($1) - '1 year'::interval
            and xtn_month != this_month($1)
        group by
            xtn_month
        order by xtn_month
    ) x
$$ language sql immutable;

create or replace function months_at_current_burn(text) returns text as $$
    select
        to_char(
            (
                select
                    sum(amount)
                from
                    aggregated_accounts_by_month
                where
                    account ~ 'Assets:Savings'
             ) / current_burn($1),
             '9.9'
         ) as months;
$$ language sql immutable;

commit;

