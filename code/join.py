from Table import make_table

def two_table_simple_join(t1, t2, c1, c2):
    # define join algorithms, both scan and index
    def join_without_index(t1, t2, c1, c2):
        for index1, value1 in t1.iterate_column(c1):
            row1 = t1.get_row(index1)
            for index2, value2 in t2.iterate_column(c2):
                if value1 == value2:
                    row2 = t2.get_row(index2)
                    yield row1 + row2
    def join_with_index(t1, t2, c1, c2):
        for index1, value1 in t1.iterate_column(c1):
            row1 = t1.get_row(index1)
            for index2 in t2.iterate_index(c2, value1):
                row2 = t2.get_row(index2)
                yield row1 + row2

    # figure out order to join tables and whether or not to use index
    join_function = None
    if t1.has_index(c1) and t2.has_index(c2):
        join_function = join_with_index
        if t1.get_count() > t2.get_count():
            t1, t2 = t2, t1
            c1, c2 = c2, c1
    elif t1.has_index(c1):
        join_function = join_with_index
        t1, t2 = t2, t1
        c1, c2 = c2, c1
    elif t2.has_index(c2):
        join_function = join_with_index
    else:
        join_function = join_without_index

    # compute output columns and index columns, creating an output table
    out_columns = t1.get_columns(prepend_table_name=True) + \
                  t2.get_columns(prepend_table_name=True)
    index_columns = [c for c in t1.get_columns(prepend_table_name=True) if t1.has_index(c)] + \
                    [c for c in t2.get_columns(prepend_table_name=True) if t2.has_index(c)]
    result = make_table('result', column_list=out_columns, indexes=index_columns)

    # join rows and return result table
    for row in join_function(t1, t2, c1, c2):
        result.insert_list(row)
    return result
