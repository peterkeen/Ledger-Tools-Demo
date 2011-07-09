#!/usr/bin/env python

import psycopg2
import argparse
from datetime import datetime

def render_table(id, columns, rows, classes=[]):

    def render_row(cells, celltype="td", cell_classes=[]):
        row = ["<tr>"]
        for i in xrange(len(cells)):
            try:
                cellclass = classes[i]
            except IndexError:
                cellclass = ''
                
            row.append("<{celltype} class={cellclass}>{data}</{celltype}>".format(data=cells[i],celltype=celltype,cellclass=cellclass))

        row.append("</tr>")
        return ''.join(row)

    header_row = render_row(columns, celltype="th")
    data_rows = []
    for row in rows:
        data_rows.append(render_row(row, cell_classes=classes))

    data_rows = '\n'.join(data_rows)

    table = """
<table cellpadding=0 cellspacing=0 border=0 class="display" id="{id}">
<thead>{header}</thead>
<tbody>{body}</tbody>
</table>
"""
    return table.format(
        header=header_row,
        body=data_rows,
        id=id)

def render_query(id, columns, query, binds, classes=[]):
    res = run_query(query, binds)
    return render_table(id, columns, res, classes=classes)

db_connect_string = ""
def run_query(query, binds):
    con = psycopg2.connect(db_connect_string)
    cur = con.cursor()
    cur.execute(query, binds)
    return cur

income_statement_query = """
select
    account as "Account",
    "3 years ago",
    "2 years ago",
    "1 year ago",
    "1 month ago",
    "This month"
from (    
    select
        account,
        sum(case when xtn_month = this_month(%(month)s) - interval '3 year'   then amount else 0 end) as "3 years ago",
        sum(case when xtn_month = this_month(%(month)s) - interval '2 year'   then amount else 0 end) as "2 years ago",
        sum(case when xtn_month = this_month(%(month)s) - interval '1 year'   then amount else 0 end) as "1 year ago",
        sum(case when xtn_month = this_month(%(month)s) - interval '1 month'  then amount else 0 end) as "1 month ago",
        sum(case when xtn_month = this_month(%(month)s)                       then amount else 0 end) as "This month"
    from
        aggregated_accounts_by_month
    where
        account !~ %(bracket)s
        and account !~ '401K'
        and (account ~* 'Income' or account ~* 'Expenses' or account ~* 'Liabilities')
        and account !~ 'Depreciation'
        and account !~ 'Amex'
        and commodity = '$'
    group by
        account
    union all
    select
        'Total' as account,
        sum(case when xtn_month = this_month(%(month)s) - interval '3 year'   then amount else 0 end) as "3 years ago",
        sum(case when xtn_month = this_month(%(month)s) - interval '2 year'   then amount else 0 end) as "2 years ago",
        sum(case when xtn_month = this_month(%(month)s) - interval '1 year'   then amount else 0 end) as "1 year ago",
        sum(case when xtn_month = this_month(%(month)s) - interval '1 month'  then amount else 0 end) as "1 month ago",
        sum(case when xtn_month = this_month(%(month)s)                       then amount else 0 end) as "This month"
    from
        aggregated_accounts_by_month
    where
        account !~ %(bracket)s
        and account !~ '401K'
        and (account ~* 'Income' or account ~* 'Expenses' or account ~* 'Liabilities')
        and account !~ 'Amex'
        and account !~ 'Depreciation'
        and commodity = '$'
) x
where
    "3 years ago" != 0
    or "2 years ago" != 0
    or "1 year ago" != 0
    or "1 month ago" != 0
    or "This month" != 0
order by
    account
;
"""

balance_sheet_query = """
select
    account as "Account",
    "3 years ago",
    "2 years ago",
    "1 year ago",
    "1 month ago",
    "This month"
from (    
    select
        account,
        sum(case when xtn_month <= this_month(%(month)s) - interval '3 year'  then amount else 0 end) as "3 years ago",
        sum(case when xtn_month <= this_month(%(month)s) - interval '2 year'  then amount else 0 end) as "2 years ago",
        sum(case when xtn_month <= this_month(%(month)s) - interval '1 year'  then amount else 0 end) as "1 year ago",
        sum(case when xtn_month <= this_month(%(month)s) - interval '1 month' then amount else 0 end) as "1 month ago",
        sum(case when xtn_month <= this_month(%(month)s)                      then amount else 0 end) as "This month"
    from
        aggregated_accounts_by_month
    where
        account !~ %(bracket)s
        and (account ~* 'Assets' or account ~* 'Liabilities')
        and account !~ '401K'
        and account !~ 'WePay'
        and account !~ 'Schwab'
        and account !~ 'Vanguard'
        and commodity = '$'
    group by
        account
    union all
    select
        'Total' as account,
        sum(case when xtn_month <= this_month(%(month)s) - interval '3 year'  then amount else 0 end) as "3 years ago",
        sum(case when xtn_month <= this_month(%(month)s) - interval '2 year'  then amount else 0 end) as "2 years ago",
        sum(case when xtn_month <= this_month(%(month)s) - interval '1 year'  then amount else 0 end) as "1 year ago",
        sum(case when xtn_month <= this_month(%(month)s) - interval '1 month' then amount else 0 end) as "1 month ago",
        sum(case when xtn_month <= this_month(%(month)s)                      then amount else 0 end) as "This month"
    from
        aggregated_accounts_by_month
    where
        account !~ %(bracket)s
        and (account ~* 'Assets' or account ~* 'Liabilities')
        and account !~ '401K'
        and account !~ 'WePay'
        and account !~ 'Schwab'
        and account !~ 'Vanguard'
        and commodity = '$'
) x
where
    "3 years ago" != 0
    or "2 years ago" != 0
    or "1 year ago" != 0
    or "1 month ago" != 0
    or "This month" != 0
order by
    account
"""

burn_rate_query = """
select
    to_char(current_burn(%(month)s), '99999.99') as "Burn",
    months_at_current_burn(%(month)s) as "Months",
    (this_month(%(month)s) + '1 month'::interval + (months_at_current_burn(%(month)s) || ' months')::interval)::date
"""

parser = argparse.ArgumentParser(description="Run monthly reports")
parser.add_argument('--month', type=str, help="Month to run for", default=datetime.now().strftime('%Y-%m-01'))
parser.add_argument('--open', type=bool, help="Open the report in the browser", default=False)
parser.add_argument('--path', type=str,  help="Where to put the generated files", default=".")
parser.add_argument('--db', type=str,    help="Database connection string", default="")
args = parser.parse_args()

db_connect_string = args.db

income_statement = render_query('income', [
        "Account",
        "3 years ago",
        "2 years ago",
        "1 year ago",
        "1 month ago",
        "This month"], income_statement_query, {'month': args.month, 'bracket': '\\[.*\\]'}, classes=['', 'right', 'right', 'right', 'right', 'right'])

balance_sheet = render_query('balance', [
        "Account",
        "3 years ago",
        "2 years ago",
        "1 year ago",
        "1 month ago",
        "This month"], balance_sheet_query, {'month': args.month, 'bracket': '\\[.*\\]'}, classes=['', 'right', 'right', 'right', 'right', 'right'])

burn_rate = render_query('burn', ['Burn', 'Months', 'Drop Dead'], burn_rate_query, {'month': args.month}, ['center', 'center', 'center'])


page = """
<html>
<head>
<style type="text/css" title="currentStyle"> 
    @import "http://bugsplat.info/static/page.css";
    @import "http://bugsplat.info/static/table.css";
</style> 
<script type="text/javascript" src="http://bugsplat.info/static/jquery.js"></script>
<script type="text/javascript" src="http://bugsplat.info/static/jquery.dataTables.js"></script>
<script type="text/javascript" src="http://bugsplat.info/static/jquery.jqplot.min.js"></script>
<script type="text/javascript" src="http://bugsplat.info/static/jqplot.dateAxisRenderer.min.js"></script>
<script type="text/javascript" src="http://bugsplat.info/static/jqplot.highlighter.min.js"></script>
<script type="text/javascript" src="http://bugsplat.info/static/jqplot.cursor.min.js"></script>
<script type="text/javascript">
$(document).ready(function() { $('#income').dataTable({"bPaginate": false, "bFilter": false}); });
$(document).ready(function() { $('#balance').dataTable({"bPaginate": false, "bFilter": false}); });
$(document).ready(function() { $('#burn').dataTable({"bPaginate": false, "bFilter": false})});
$(document).ready(function(){
  var net_worth_line = :net_worth_data;
  var plot1 = $.jqplot('net_worth_chart', [net_worth_line], {
    title:'',
    axes:{
        xaxis:{
            renderer:$.jqplot.DateAxisRenderer,
            tickOptions:{
                formatString:'%Y-%m'
            }
        },
        yaxis:{
            tickOptions:{
                formatString:'$%.2f'
            }
        }
    },
    highlighter:{show:true},
    series:[{lineWidth:4, markerOptions:{show:false}}]
  });
});
</script>
<title>:month Reports</title>
</head>
<body id="dt_example">
<div id="container">
<h1>:month</h1>
<h2>Balance Sheet</h2>
:balance_sheet
<h2>Net Worth by Month<h2>
<div id="net_worth_chart" style="height:300px; width=650px"></div>
<br />
<h2>Income Statement</h2>
:income_statement
<br /><br />
<h2>Burn Rate</h2>
:burn_rate
</div>
</body>
</html>
"""

net_worth_query = """
select
    xtn_month,
    sum(amount) over (rows unbounded preceding) as total 
from (
    select
        xtn_month,
        sum(amount) as amount
    from
        aggregated_accounts
    where
        commodity = '$'
        and account !~ %(bracket)s
        and (account ~ 'Assets' or account ~ 'Liabilities')
        and account !~ '401K'
        and account !~ 'Schwab' 
        and account !~ 'WePay' 
        and account !~ 'Vanguard' 
        and xtn_month <= this_month(%(month)s)
    group by 
        xtn_month 
    order by 
    xtn_month
) x
order by 
    xtn_month
"""

net_worth_data = '[' + ','.join(["['%s',%s]" % (r[0], r[1]) for r in run_query(net_worth_query, {'bracket': '\\[.*\\]', 'month': args.month})]) + ']'

filename = "%s/%s.html" % (args.path, args.month)
with open(filename, 'w') as f:
    f.write(page.replace(':income_statement', income_statement).\
                replace(':balance_sheet', balance_sheet).\
                replace(':burn_rate', burn_rate).\
                replace(':net_worth_data', net_worth_data).\
                replace(':month', args.month))

import os    
if args.open:
    os.system("open %s" % filename)
