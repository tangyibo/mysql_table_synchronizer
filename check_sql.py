from mysql_utl import MysqlUtil


class CheckSQL:
    def __init__(self, db1, db2):
        self.__db1 = db1
        self.__db2 = db2
        self.__mysql_util1 = MysqlUtil(db1)
        self.__mysql_util2 = MysqlUtil(db2)

    def check_table(self, is_execute):
        """
        和标准库对比检测是否缺少某些表
        :param is_execute:
        :return:
        """

        # 获得标准库中所有表
        source_tables = self.__mysql_util1.get_tables()

        # 获得目标数据库中的所有表
        target_tables = self.__mysql_util2.get_tables()
        target_names = []
        for t_t in target_tables:
            target_names.append(t_t['table_name'])

        losing_tables = []
        for s_t in source_tables:
            table_name = s_t['table_name']
            if table_name in target_names:
                self.check_column(table_name, is_execute)
            else:
                # print('this schema does not has {0}'.format(table_name))
                losing_tables.append(table_name)
                source_columns = self.__mysql_util1.get_columns(table_name)
                self.__mysql_util2.create_table(s_t, is_execute, source_columns)
                print()

        if len(losing_tables) > 0:
            print('this schema does not have these tables:')
            print(losing_tables)

    def check_column(self, table, is_execute):
        """
        和标准库中的表对比，查看某些字段是否不同，会打印这些字段，并生成修改字段的sql
        如果is_excuse设为true，则会更新这些字段
        :param table:
        :param is_execute:
        :return:
        """

        losing_columns = []
        change_columns = []
        additional_columns = []

        source_columns = self.__mysql_util1.get_columns(table)
        target_columns = self.__mysql_util2.get_columns(table)

        source_col_names = []
        for sc in source_columns:
            source_col_names.append(sc["column_name"])

        target_col_names = []
        for t_c in target_columns:
            target_col_names.append(t_c['column_name'])
            if not t_c['column_name'] in source_col_names:
                # 比源多出的字段
                additional_columns.append(t_c)
                self.__mysql_util2.update_column(t_c, "drop", table, is_execute)

        for s_c in source_columns:
            # print(s_c)
            column_name = s_c['column_name']
            # 是否缺少字段
            if column_name in target_col_names:
                # 字段是否相同，包含类型，长度，默认值，备注
                if s_c not in target_columns:
                    change_columns.append(column_name)

                    for d_t in target_columns:
                        if s_c['column_name'] == d_t['column_name']:
                            pass
                            # print(s_c)
                            # print(d_t)

                    # 更新不同的字段
                    self.__mysql_util2.update_column(s_c, 'update', table, is_execute)
            else:
                losing_columns.append(column_name)

                # 新增缺失的字段
                self.__mysql_util2.update_column(s_c, 'add', table, is_execute)

        # if len(losing_columns) > 0:
        #     print('-- this table {0} does not have these columns:'.format(table))
        #     print("-- ", end="")
        #     print(losing_columns)
        #     print("\n")
        #
        # if len(change_columns) > 0:
        #     print('-- this table {0} is different from target columns:'.format(table))
        #     print("-- ", end="")
        #     print(change_columns)
        #     print("\n")
        if len(losing_columns) > 0 or len(change_columns) > 0 or len(additional_columns) > 0:
            print()
