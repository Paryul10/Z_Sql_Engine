import os
import sys
import re
import csv
import operator
import copy

metadata = {}
tables_data = {}
ops = {"!=": operator.ne, "=": operator.eq, ">=": operator.ge,
       ">": operator.gt, "<": operator.lt, "<=": operator.le}


def display_res(table):
    print(','.join(table['info']))
    for row in table['data']:
        print(','.join([str(x) for x in row]))


def cartesian_prod(table1, table2):
    prod_table = []
    for row1 in table1['data']:
        for row2 in table2['data']:
            prod_table.append(row1 + row2)

    return prod_table


def validateformat(query):
    if bool(re.match('^select.*from.*', query)) is False:
        return 0
    else:
        return 1


def check_table(table):
    if table not in metadata:
        return 0
    else:
        return 1


def processQuery(fields, tables, flags_aggr, where_arr, conds):
    tables_data = {}
    res_table = {}
    res_table['info'] = []
    res_table['data'] = []
    single_table = 1

    # Read data from tables into dictionary
    for i in range(len(tables)):
        tablename = tables[i]
        # print(tablename)
        # print(metadata[tablename])
        path = 'files/' + tables[i] + '.csv'

        with open(path, 'r') as csvfile:
            csvreader = csv.reader(csvfile)
            data = []
            for row in csvreader:
                data.append(row)

            tables_data[tablename] = {}
            tables_data[tablename]['name'] = tablename
            tables_data[tablename]['info'] = metadata[tablename]
            tables_data[tablename]['data'] = data

        # print(tables_data)

    # lis = []
    for t in tables:
        for c in metadata[t]:
            res_table['info'].append(t + '.' + c.upper())
            # res_table['info'].append((t+ '.' + c.upper()))
    # res_table['info'].append(lis)

    if len(tables) > 1:
        # do cartesian product
        res_table['data'] = cartesian_prod(
            tables_data[tables[0]], tables_data[tables[1]])
        if(len(tables) > 2):
            for i in range(2, len(tables)):
                res_table['data'] = cartesian_prod(
                    res_table, tables_data[tables[i]])

        single_table = 0
    else:
        res_table = tables_data[tables[0]]
        for i in range(len(res_table['info'])):
            res_table['info'][i] = res_table['name']+'.'+res_table['info'][i]

    if (where_arr[0] == 0):
        if len(fields) == 1:
            # * , one column, aggregate functions
            if(fields[0] == "*"):

                if(flags_aggr[4] == 1):
                    seen = set()
                    no_dups = []
                    for lst in res_table['data']:
                        # convert to hashable type
                        current = tuple(lst)
                        # If element not in seen, add it to both
                        if current not in seen:
                            no_dups.append(lst)
                            seen.add(current)
                    # print(no_dups)
                    res_table['data'] = no_dups

                display_res(res_table)
            else:
                # one column
                tablename = tables[0]
                coln = fields[0]
                if ('.' not in fields[0]):
                    coln = str(fields[0]).upper()
                    coln = tables[0] + '.' + coln
                else:
                    col_name = coln.split('.')[1]
                    tab_name = coln.split('.')[0]
                    coln = tab_name + '.' + col_name.upper()
                idx = 0
                is_exist = 0
                # HANDLE CASE FOR - EXTRACT ONE COLUMN GIVEN 2 TABLES
                headers = tables_data[tablename]['info']
                for i in range(len(tables_data[tablename]['info'])):
                    if headers[i] == coln:
                        idx = i
                        is_exist = 1

                if(is_exist == 0):
                    print("Terminating: Column Does not exist")
                    sys.exit(1)

                colns = [coln]
                res_table['info'] = colns
                rows = []
                rrs = []
                data_table = tables_data[tablename]['data']
                for i in range(len(data_table)):
                    temp = []
                    temp.append(data_table[i][idx])
                    rows.append(temp)
                    rrs.append(data_table[i][idx])
                res_table['data'] = rows

                if(flags_aggr[4] == 1):
                    seen = set()
                    no_dups = []
                    for lst in res_table['data']:
                        # convert to hashable type
                        current = tuple(lst)
                        # If element not in seen, add it to both
                        if current not in seen:
                            no_dups.append(lst)
                            seen.add(current)
                    print(no_dups)
                    res_table['data'] = no_dups

                # aggregate check
                sum = 0
                min = 1000000
                max = -1000000
                if (flags_aggr[0] == 0 and flags_aggr[1] == 0 and flags_aggr[2] == 0 and flags_aggr[3] == 0):
                    display_res(res_table)
                elif (flags_aggr[0] == 1):
                    for i in range(len(rrs)):
                        sum = sum + int(rrs[i])
                    print("sum("+coln+")")
                    print(sum)
                elif (flags_aggr[1] == 1):
                    for i in range(len(rrs)):
                        sum = sum + int(rrs[i])
                    print("avg("+coln+")")
                    print(sum/float(len(rrs)))
                elif (flags_aggr[2] == 1):
                    for i in range(len(rrs)):
                        if (int(rrs[i]) > int(max)):
                            max = (rrs[i])
                    print("max("+coln+")")
                    print(max)
                elif (flags_aggr[3] == 1):
                    print("min("+coln+")")
                    for i in range(len(rrs)):
                        if (int(rrs[i]) < int(min)):
                            min = (rrs[i])
                    print(min)
        else:
            exist_flag = 1
            if len(tables) == 1:
                for i in range(len(fields)):
                    col = tables[0] + '.' + fields[i].upper()
                    if col not in metadata[tables[0]]:
                        exist_flag = 0
                    if ('.' in fields[i]):
                        coln = fields[i]
                        col_name = coln.split('.')[1]
                        tab_name = coln.split('.')[0]
                        fields[i] = tab_name + '.' + col_name.upper()
                    else:
                        # print(fields)
                        # check if column exists
                        fields[i] = tables[0] + '.' + fields[i].upper()
            else:
                for i in range(len(fields)):
                    if ('.' in fields[i]):
                        coln = fields[i]
                        col_name = coln.split('.')[1]
                        tab_name = coln.split('.')[0]
                        fields[i] = tab_name + '.' + col_name.upper()
                    else:
                        # print(fields)
                        for table in metadata:
                            if (fields[i].upper() in metadata[table]):
                                fields[i] = table + '.' + fields[i].upper()
                            else:
                                exist_flag = 0

            if exist_flag == 0:
                print("Column does not exist")
                sys.exit(1)
            # print(fields)

            indices = []

            for i in range(len(fields)):
                for j in range(len(res_table['info'])):
                    if (fields[i] == res_table['info'][j]):
                        indices.append(j)

            # print(indices)
            ans_table = {}
            ans_table['info'] = []
            ans_table['data'] = []
            heads = []
            for i in indices:
                heads.append(res_table['info'][i])

            ans_table['info'] = heads

            for i in range(len(res_table['data'])):
                row = []
                for j in indices:
                    row.append(res_table['data'][i][j])
                # print(row)

                ans_table['data'].append(row)

            # print(ans_table)
            if(flags_aggr[4] == 1):
                seen = set()
                no_dups = []
                for lst in ans_table['data']:
                        # convert to hashable type
                    current = tuple(lst)
                    # If element not in seen, add it to both
                    if current not in seen:
                        no_dups.append(lst)
                        seen.add(current)
                ans_table['data'] = no_dups
            display_res(ans_table)
    else:                           ## where is there

        ans_table = {}
        ans_table['info'] = []
        ans_table['data'] = []

        
        indices = []
        # print(fields)
        if len(fields) == 1 and (fields[0] == "*"):
            if where_arr[1] == 5:
                for j in range(len(res_table['info'])):
                    indices.append(j)
            else:
                for j in range(len(res_table['info'])):
                    if (res_table['info'][j] != conds[0][0]):
                        indices.append(j)

        else:
            for i in range(len(fields)):
                fields[i] = parseCol(fields[i], tables)
                for j in range(len(res_table['info'])):
                    if (fields[i] == res_table['info'][j]):
                        indices.append(j)

        heads = []
        for i in indices:
            heads.append(res_table['info'][i])

        ans_table['info'] = heads

        if (where_arr[1] == 5 or where_arr[1] == 6):
            # exectue query type 5
            check_col_arr = []
            if (where_arr[2] == 1):
                # only one condition
                if(where_arr[1] == 5):
                    for i in range(len(res_table['info'])):
                        for j in range(len(conds)):
                            if (res_table['info'][i] == conds[j][0]):
                                check_col_arr.append(i)
                                
                else:
                    for i in range(len(res_table['info'])):
                        if(res_table['info'][i] == conds[0][0]):
                            check_col_arr.append(i)
                    for i in range(len(res_table['info'])):
                        if(res_table['info'][i] == conds[0][2]):
                            check_col_arr.append(i)

                for i in range(len(res_table['data'])):
                    row = []
                    if(where_arr[1] == 5):
                        if (ops[conds[0][1]](int(res_table['data'][i][check_col_arr[0]]), int(conds[0][2]))):
                            for j in indices:
                                row.append(res_table['data'][i][j])
                            ans_table['data'].append(row)
                    else:
                        if (ops[conds[0][1]](int(res_table['data'][i][check_col_arr[0]]), int(res_table['data'][i][check_col_arr[1]]))):
                            for j in indices:
                                row.append(res_table['data'][i][j])
                            ans_table['data'].append(row)

            elif (where_arr[3] == 1):
                # and
                for i in range(len(res_table['info'])):
                    for j in range(len(conds)):
                        if (res_table['info'][i] == conds[j][0]):
                            check_col_arr.append(i)

                for i in range(len(res_table['data'])):
                    row=[]
                    # print(res_table['data'][i])
                    if (ops[conds[0][1]](int(res_table['data'][i][check_col_arr[0]]), int(conds[0][2])) and ops[conds[1][1]](int(res_table['data'][i][check_col_arr[1]]), int(conds[1][2]))):
                        for j in indices:
                            row.append(res_table['data'][i][j])
                        ans_table['data'].append(row)
            elif (where_arr[4] == 1):
                # or
                for i in range(len(res_table['info'])):
                    for j in range(len(conds)):
                        if (res_table['info'][i] == conds[j][0]):
                            check_col_arr.append(i)

                for i in range(len(res_table['data'])):
                    row=[]
                    if (ops[conds[0][1]](int(res_table['data'][i][check_col_arr[0]]), int(conds[0][2])) or ops[conds[1][1]](int(res_table['data'][i][check_col_arr[1]]), int(conds[2][2]))):
                        for j in indices:
                            row.append(res_table['data'][i][j])
                        ans_table['data'].append(row)

            # print(ans_table)
            if(flags_aggr[4] == 1):
                seen=set()
                no_dups=[]
                for lst in ans_table['data']:
                    # convert to hashable type
                    current=tuple(lst)
                    # If element not in seen, add it to both
                    if current not in seen:
                        no_dups.append(lst)
                        seen.add(current)
                # print(no_dups)
                ans_table['data']=no_dups

            rrs=[]
            idx=indices[0]
            data_table=ans_table['data']
            for i in range(len(data_table)):
                rrs.append(data_table[i][idx])

            coln=res_table['info'][idx]
            sum=0
            min=1000000
            max=-1000000
            if (flags_aggr[0] == 0 and flags_aggr[1] == 0 and flags_aggr[2] == 0 and flags_aggr[3] == 0):
                display_res(ans_table)
            elif (flags_aggr[0] == 1):
                for i in range(len(rrs)):
                    sum=sum + int(rrs[i])
                print("sum("+coln+")")
                print(sum)
            elif (flags_aggr[1] == 1):
                for i in range(len(rrs)):
                    sum=sum + int(rrs[i])
                print("avg("+coln+")")
                print(sum/float(len(rrs)))
            elif (flags_aggr[2] == 1):
                for i in range(len(rrs)):
                    if (int(rrs[i]) > int(max)):
                        max=(rrs[i])
                print("max("+coln+")")
                print(max)
            elif (flags_aggr[3] == 1):
                print("min("+coln+")")
                for i in range(len(rrs)):
                    if (int(rrs[i]) < int(min)):
                        min=(rrs[i])
                print(min)
        else:
            # execute quesry type 6
            pass


def parseCol(col, tables):
    if ('.' in col):
        coln=col
        col_name=coln.split('.')[1].strip()
        tab_name=coln.split('.')[0].strip()

        if col_name.upper() not in metadata2[tab_name]:
            print("Invalid Attribute")
            sys.exit(1)

        if tab_name not in tables:
            print("Invalid Table Name")
            sys.exit(1)

        col=tab_name + '.' + col_name.upper()
    else:
        col_exist=True
        for table in tables:
            if (col.upper() in metadata2[table]):
                col=table + '.' + col.upper()
                col_exist=True
                break
            else:
                col_exist=False

        if col_exist == False:
            print("parse_col_error: Column does not exist")
            sys.exit(1)

    return col


def parseCondiditon(cond, tables):

    cond_arr=[]
    if ">=" in cond:
        cond_arr.append(cond.split(">=")[0].strip())
        cond_arr.append(">=")
        cond_arr.append(cond.split(">=")[1].strip())
    elif "<=" in cond:
        cond_arr.append(cond.split("<=")[0].strip())
        cond_arr.append("<=")
        cond_arr.append(cond.split("<=")[1].strip())
    elif ">" in cond:
        cond_arr.append(cond.split(">")[0].strip())
        cond_arr.append(">")
        cond_arr.append(cond.split(">")[1].strip())
    elif "<" in cond:
        cond_arr.append(cond.split("<")[0].strip())
        cond_arr.append("<")
        cond_arr.append(cond.split("<")[1].strip())
    elif "=" in cond:
        cond_arr.append(cond.split("=")[0].strip())
        cond_arr.append("=")
        cond_arr.append(cond.split("=")[1].strip())
    else:
        print("Invalid Operator")
        sys.exit(1)

    cond_arr[0]=parseCol(cond_arr[0], tables)
    return cond_arr


def parseQuery(query):
    query=query.lower()
    # print(query)
    if validateformat(query) == 0:
        print('Invalid Sql Query')
        print('Format : Select * from table_name')

    flags_aggr=[0, 0, 0, 0, 0]  # sum , avg , max , min , distinct

    # what field/fields to display
    fields=query.split('from')[0]
    fields=fields.replace('select', '')

    if "distinct" in fields:
        flags_aggr[4]=1
        fields=fields.replace('distinct', '').strip()


    fields=fields.strip().split(',')
    if(len(fields) == 1):
        if bool(re.match('^(sum)\(.*\)', fields[0])):
        # if "sum" in fields[0]:
            flags_aggr[0]=1
            fields[0]=fields[0].replace('sum', '').strip().strip('()')
        if bool(re.match('^(avg)\(.*\)', fields[0])):
            flags_aggr[1]=1
            fields[0]=fields[0].replace('avg', '').strip().strip('()')
        if bool(re.match('^(max)\(.*\)', fields[0])):
            flags_aggr[2]=1
            fields[0]=fields[0].replace('max', '').strip().strip('()')
        if bool(re.match('^(min)\(.*\)', fields[0])):
            flags_aggr[3]=1
            fields[0]=fields[0].replace('min', '').strip().strip('()')



    # what table/tables
    tables=query.split('where')[0]
    tables=tables.split('from')[1]
    tables=tables.strip()
    tables=tables.split(',')

    for i in range(len(fields)):
        fields[i]=fields[i].strip()

    for i in range(len(tables)):
        tables[i]=tables[i].strip()

    for table in tables:
        if (check_table(table) == 0):
            print("Terminating: ", table, " does not exist")
            return

    # check where , using re.match. If there is where, call process condition function after parsing condition
    # where_exists , query type , only one condition , and condition , or condition
    where_arr=[0, 5, 0, 0, 0]
    conds=[]
    # conds['cond1'][0] = col_name , conds['cond1'][1] = opearator , conds['cond1'][2] = value
    if bool(re.match('^select.*from.*where.*', query)):
        where_arr[0]=1
        # check for query type 5 or 6
        # assuming 5 , will do for 6.
        cond_str=query.split('where')[1]
        # check for single cond, and , or conditions
        if("and" in cond_str):

            cond1=cond_str.split('and')[0].strip()
            cond2=cond_str.split('and')[1].strip()

            conds.append(parseCondiditon(cond1, tables))
            conds.append(parseCondiditon(cond2, tables))

            where_arr[3]=1

        elif("or" in cond_str):
            cond1=cond_str.split('or')[0].strip()
            cond2=cond_str.split('or')[1].strip()

            conds.append(parseCondiditon(cond1, tables))
            conds.append(parseCondiditon(cond2, tables))
            where_arr[4]=1
        else:
            where_arr[2]=1
            cond1=cond_str.split('and')[0].strip()
            conds.append(parseCondiditon(cond1, tables))

            try:
                val=int(conds[0][2])
            except:
                where_arr[1]=6
                conds[0][2]=parseCol(conds[0][2], tables)

        # print(conds)

    processQuery(fields, tables, flags_aggr, where_arr, conds)


def readmetadata():
    try:
        with open("files/metadata.txt", "r") as md:
            lines=md.readlines()
            # print(lines)
            tot_lines=len(lines)
            i=0
            while i < tot_lines:
                lines[i]=lines[i].strip()
                i += 1
            # print(lines)
            i=0
            while i < tot_lines:
                if lines[i] == '<begin_table>':
                    i=i + 1
                    entry=[]
                    while (lines[i] != '<end_table>' and i < tot_lines):
                        entry.append(lines[i])
                        i=i + 1

                    Tablename=entry[0]
                    columns=entry[1:]
                    # print(Tablename,columns)
                    metadata[Tablename]=columns
                    i += 1

            print(metadata)
            md.close()

    except FileNotFoundError:
        print("E404:Meta data file not found")


if __name__ == "__main__":
    print ("Welcome to ZENgine")
    
    print("------------Metadata--------------")
    readmetadata()
    print("----------------------------------")
    metadata2=copy.deepcopy(metadata)
    command=''
    if len(sys.argv) < 2:
        print("Please Enter a Query")
        print("Usage: python3 20171083.py ''query'';")
        sys.exit(1)
    else:
        command=sys.argv
        query=command[1]
        if query[-1] != ';':
            print('Semicolon Not Provided, but our parser is smart.')
        query=query[:-1].strip()
        parseQuery(query)
