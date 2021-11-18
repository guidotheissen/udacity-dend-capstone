def check_not_empty(df, table_name):
    """Count number of entries to ensure completeness of data.
    :param df: spark dataframe to check counts on
    :param table_name: corresponding name of table
    """
    total_count = df.count()

    if total_count == 0:
        print(f"<Not Empty> quality check failed for {table_name} with zero records!")
    else:
        print(f"<Not Empty> quality check passed for {table_name} with {total_count:,} records.")


def check_all_lines_loaded(df, file_name, hasHeaderLine):
    num_lines = sum(1 for line in open(file_name, encoding='utf8'))
    num_rows = df.count()
    if (hasHeaderLine):
        num_lines = num_lines -1
    if (num_lines == num_rows):
        print(f"<All lines loaded> quality check passed!")
    else:
        print(f"<All lines loaded> quality check NOT passed! Number of lines: {num_lines}, number of rows: {num_rows}")


def check_existence(df, expression):
    any_expression = 'ANY('+expression+') as chk'
    check = df.selectExpr(any_expression)
    if check.collect()[0][0]:
        print(f"<Check existence> quality check succeeeded")
    else:
        print(f"<Check existence> quality check FAILED!")


def performQualityChecks(df, file_name, hasHeaderLine):
    check_not_empty(df, file_name)
    check_all_lines_loaded(df, file_name, hasHeaderLine)
