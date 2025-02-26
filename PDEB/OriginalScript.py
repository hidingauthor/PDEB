import time


class OriginalScript:

    def __init__(self, fc):
        self.fc = fc
        self.sqls_in_fds = list()

    # A sample intermediate result from the parser [Existing System]
    def compile(self):
        self.sqls_in_fds.append("CREATE VIRTUAL TABLE vir_item AT <remote path>.item;") #please provide the remote path
        self.fc.add_to_dropables(1, "vir_item")
        self.sqls_in_fds.append("CREATE VIRTUAL TABLE vir_store_sales AT <remote path>.store_sales;") #please provide the remote path
        self.fc.add_to_dropables(1, "vir_store_sales")
        self.sqls_in_fds.append("CREATE VIRTUAL TABLE vir_date_dim AT <remote path>.date_dim;") #please provide the remote path
        self.fc.add_to_dropables(1, "vir_date_dim")
        self.sqls_in_fds.append("CREATE VIRTUAL TABLE vir_customer AT <remote path>.customer;") #please provide the remote path
        self.fc.add_to_dropables(1, "vir_customer")


        ss2 =   "CREATE OR REPLACE PROCEDURE tmp_procedure_loop()" \
                 " AS BEGIN\n" \
                 "  DECLARE i INT := 0;\n" \
                 "  DECLARE k INT := 5;\n" \
                 "  date    =  SELECT d_year, d_date, d_date_sk FROM vir_date_dim;\n" \
                 "  sale    =  SELECT ss_item_sk, ss_sold_date_sk, ss_customer_sk, ss_quantity FROM vir_store_sales;\n" \
                 "  item    =  SELECT i_item_desc, i_item_sk, i_color FROM vir_item WHERE i_color = 'red';\n" \
                 "  saleItem = SELECT ss_item_sk sk1, i_item_desc, i_item_sk, ss_customer_sk, ss_quantity FROM :sale, :item WHERE ss_item_sk = i_item_sk;\n" \
                 "  saleDate = SELECT ss_item_sk sk2, d_year, d_date FROM :sale, :date WHERE ss_sold_date_sk = d_date_sk;\n" \
                 "  saleItemDate = SELECT d_year, i_item_desc, i_item_sk, d_date, ss_customer_sk, ss_quantity FROM :saleDate, :saleItem WHERE sk1 = sk2;\n" \
                 "  WHILE :i < :k DO\n" \
                 "      aggItems = SELECT d_year, i_item_desc, i_item_sk, ss_customer_sk, ss_quantity, d_date, COUNT(*) cnt FROM :saleItemDate GROUP BY ss_customer_sk, ss_quantity, i_item_desc, i_item_sk, d_date, d_year;\n" \
                 "      i = :i + 1;\n" \
                 "  END WHILE;\n" \
                 "  result = SELECT count(*) cnt FROM :aggItems, vir_customer WHERE ss_customer_sk = c_customer_sk AND ss_quantity < 100;\n" \
                 "  SELECT * FROM :result;" \
                 "END;"


        self.fc.add_to_dropables(1, "tmp_procedure_loop")
        self.sqls_in_fds.append(ss2)
        self.sqls_in_fds.append("call tmp_procedure_loop();")

    def execute_plan(self):
        start_time = time.time() * 1000
        for sql in self.sqls_in_fds:
            self.fc.execute_query(1, sql, True)
        end_time = time.time() * 1000
        exe_time = end_time - start_time
        self.fc.drop_dropables()
        end_time = time.time() * 1000
        exe_drop_time = end_time - start_time
        print("Execution time: {0} {1}".format(round(exe_time, 3), round(exe_drop_time, 3)))
