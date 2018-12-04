from Table import make_table

def simple_join(tables, column_pairs):
    for i in range(len(tables) - 1):
        t1, t2 = tables[i], tables[i+1]
        c1, c2 = column_pairs[i]
        if not t2.has_index(c2) and t1.has_index(c1):
            t1, t2 = t2, t1
            c1, c2 = c2, c1
        out_columns = ['%s.%s' % (t1.get_name(), c) for c in t1.get_columns()] + \
                      ['%s.%s' % (t2.get_name(), c) for c in t2.get_columns()]
        index_columns = ['%s.%s' % (t1.get_name(), c) for c in t1.get_columns() if t1.has_index(c)] + \
                        ['%s.%s' % (t2.get_name(), c) for c in t2.get_columns() if t2.has_index(c)]
        result = make_table('result', column_list=out_columns, indexes=index_columns)
        for index1, value1 in t1.iterate_column(c1):
            row1 = t1.get_row(index1)
            for index2, value2 in t2.iterate_column(c2):
                if value1 == value2:
                    row2 = t2.get_row(index2)
                    result.insert_list(row1 + row2)
        return result
