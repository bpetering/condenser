# Any useful bits from from psycopg2 that mysqlclient doesn't have already 

# TODO custom template
# TODO page_size
# TODO fetch
def execute_values(cur, sql, argslist, template=None, page_size=100, fetch=False):
    if template == None:
        template = '(' + ', '.join(["'%s'"] * len(argslist[0])) + ')'
    templated_args = [template % x for x in argslist]
    append = '(' + ','.join(templated_args) + ')'
    sql = sql % append 
    cur.execute(sql)
