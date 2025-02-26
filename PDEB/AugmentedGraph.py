# import pyhdb
import copy

from PrintLog import PrintLog
from Statement import Statement
from ExecutionBlock import ExecutionBlock
from ControlRegion import ControlRegion
from Variable import Variable


class AugmentedGraph:
    id = "S"
    root_st_id = -1
    statements = dict()
    control_regions = dict()
    execution_blocks = dict()
    bfs_reverse_seq = list()
    post_order_seq = list()
    scheduled_ebs = list()

    def __init__(self):
        pass

    def create_graph(self, script_id):
        self.root_st_id = self.create_graph2()
        # get traversal order for the statement graph
        self.create_level_order_traversal_sequence(self.root_st_id)
        self.bfs_reverse_seq.reverse()
        PrintLog.print_log("BFS Reverse: " + str(self.bfs_reverse_seq))
        # First consider that the whole statement graph in a parent EB
        ebp = ExecutionBlock(1, 0, 1, 1, 0)
        ebp.add_new_statements(list(self.statements.keys()))
        self.execution_blocks[1] = ebp
        # construct the simple execution block inside the parent EB
        self.construct_simple_execution_block(ebp, self.bfs_reverse_seq)
        self.estimate_cost_of_statements()

    #A sample intermediate result from the parser
    def create_graph2(self):
        # stid, crid, ebid, fnid, is_declarative, hbt, nr, size, out_var, base_tables, expr
        s_1 = Statement(1, 1, 1, 1, False, False, 0, 0, Variable("i", "INT", "0", []), [], "DECLARE i INT := 0;")
        s_2 = Statement(2, 1, 1, 1, False, False, 0, 0, Variable("k", "INT", "2", []), [], "DECLARE k INT := 2;")
        s_3 = Statement(3, 1, 1, 2, True, True, 10200, 571,Variable("item", "table var", "i_item_desc varchar(200), i_item_sk integer, i_color char(20)", []), ["item"], "SELECT i_item_desc, i_item_sk, i_color FROM item WHERE i_color = 'red'")
        s_4 = Statement(4, 1, 1, 4, True, True, 28800991, 4608159, Variable("sale", "table var", "ss_item_sk integer, ss_sold_date_sk integer, ss_customer_sk integer, ss_quantity integer", []), ["store_sales"], "SELECT ss_item_sk, ss_sold_date_sk, ss_customer_sk, ss_quantity FROM store_sales")
        s_5 = Statement(5, 1, 1, 1, True, True, 73049, 6355, Variable("date", "table var", "d_year integer, d_date date, d_date_sk integer", []), ["date_dim"], "SELECT d_year, d_date, d_date_sk FROM date_dim")
        s_6 = Statement(6, 1, 1, 1, True, False, 645811, 232492, Variable("saleItem", "table var", "sk1 integer, i_item_desc varchar(200), i_item_sk integer, ss_customer_sk integer, ss_quantity integer", ["sale", "item"]), [], "SELECT ss_item_sk sk1, i_item_desc, i_item_sk, ss_customer_sk, ss_quantity FROM :sale, :item  WHERE ss_item_sk = i_item_sk")
        s_7 = Statement(7, 1, 1, 1, True, False, 27504814, 2392919, Variable("saleDate", "table var", "sk2 integer, d_year integer, d_date date", ["sale", "date"]), [], "SELECT ss_item_sk sk2, d_year, d_date FROM :sale, :date  WHERE ss_sold_date_sk = d_date_sk")
        s_8 = Statement(8, 1, 1, 1, True, False, 219080516, 47709917, Variable("saleItemDate", "table var", "d_year integer, i_item_desc varchar(200), i_item_sk integer, d_date date, ss_customer_sk integer, ss_quantity integer", ["saleDate", "saleItem"]), [], "SELECT d_year, i_item_desc, i_item_sk, d_date, ss_customer_sk, ss_quantity FROM :saleDate, :saleItem WHERE sk1 = sk2")
        s_9 = Statement(9, 2, 1, 1, False, False, 0, 0, Variable("", "", "", ["i", "k"]), [], "WHILE :i < :k DO")
        s_10 = Statement(10, 2, 1, 1, True, False, 178861374, 40348611, Variable("aggItems", "table var", "d_year integer, i_item_desc varchar(200), i_item_sk integer, ss_customer_sk integer, ss_quantity integer, d_date date, cnt BIGINT", ["saleItemDate"]), [], "SELECT d_year, i_item_desc, i_item_sk, ss_customer_sk, ss_quantity, d_date, COUNT(*) cnt FROM :saleItemDate GROUP BY ss_customer_sk, ss_quantity, i_item_desc, i_item_sk, d_date, d_year")
        s_11 = Statement(11, 2, 1, 1, False, False, 0, 0, Variable("i", "INT", "", ["i"]), [], "i = :i + 1;")
        s_12 = Statement(12, 2, 1, 1, False, False, 0, 0, Variable("", "", "", []), [], "END WHILE;")
        s_13 = Statement(13, 3, 1, 3, True, True, 495490, 1936, Variable("customer", "table var", "c_customer_sk integer", []), ["customer"], "SELECT c_customer_sk FROM customer")
        s_14 = Statement(14, 3, 1, 1, True, False, 1, 8, Variable("result", "table var", "cnt BIGINT", ["aggItems", "customer"]), [], "SELECT count(*) cnt FROM :aggItems, :customer WHERE ss_customer_sk = c_customer_sk AND ss_quantity < 100")
        s_15 = Statement(15, 3, 1, 1, True, False, 0, 0, Variable("", "", "", ["result"]), [], "SELECT * FROM :result")
        self.statements[s_1.id] = s_1
        self.statements[s_2.id] = s_2
        self.statements[s_3.id] = s_3
        self.statements[s_4.id] = s_4
        self.statements[s_5.id] = s_5
        self.statements[s_6.id] = s_6
        self.statements[s_7.id] = s_7
        self.statements[s_8.id] = s_8
        self.statements[s_9.id] = s_9
        self.statements[s_10.id] = s_10
        self.statements[s_11.id] = s_11
        self.statements[s_12.id] = s_12
        self.statements[s_13.id] = s_13
        self.statements[s_14.id] = s_14
        self.statements[s_15.id] = s_15

        # estimate_costs_of_statements()
        for sid in self.statements:
            stmt = self.statements.get(sid)
            for csid in self.statements:
                cstmt = self.statements.get(csid)
                if stmt.vars.out_var_name in cstmt.vars.in_var_names:
                    stmt.consumers.append(csid)
                    cstmt.producers.append(sid)

        # region_id, parent_eb_id, l_count, o_l_count
        self.control_regions[1] = ControlRegion(1, 1, 1, 1, "SEQ")
        self.control_regions[2] = ControlRegion(2, 1, 2, 1, "LOOP")
        self.control_regions[3] = ControlRegion(3, 1, 1, 1, "SEQ")
        for sid in self.statements:
            self.control_regions.get(self.statements.get(sid).ctrl_reg_id).add_statement(sid)
        return 15

    def estimate_cost_of_statements(self):
        for sid in self.bfs_reverse_seq:
            self.statements.get(sid).estimate_cost(self)

    def create_post_order_traversal_sequence(self, root_id):
        if root_id:
            root = self.statements.get(root_id)
            if root.producers:
                for psid in root.producers:
                    self.create_post_order_traversal_sequence(psid)
            if root_id not in self.post_order_seq:
                self.post_order_seq.append(root_id)

    def create_level_order_traversal_sequence(self, root):
        # Base Case
        if root is None:
            return
        # Create an empty queue for level order traversal
        queue = list()
        # Enqueue Root and initialize height
        queue.append(root)
        while len(queue) > 0:
            # PrintLog.print(queue[0])
            sid = queue.pop(0)
            statement = self.statements.get(sid)
            # Print front of queue and remove it from queue
            if sid not in self.bfs_reverse_seq:
                self.bfs_reverse_seq.append(sid)
            if statement.producers:
                for psid in statement.producers:
                    queue.append(psid)

    def construct_simple_execution_block(self, parent_eb, statements_order):
        tmp_eb_id = parent_eb.id
        for tmp_stmt_id in statements_order:
            stmt_target = self.statements.get(tmp_stmt_id)
            if stmt_target.has_base_tab:
                if parent_eb.id == stmt_target.eb_id:
                    tmp_eb_id = stmt_target.fed_id
                    # tmp_eb_id + 1
                    stmt_target.eb_id = tmp_eb_id
                    self.statements[tmp_stmt_id] = stmt_target
                    parent_eb.remove_statement(tmp_stmt_id)
                    #print(parent_eb)
                    if tmp_eb_id not in self.execution_blocks:
                        #print()
                        eb = ExecutionBlock(tmp_eb_id, parent_eb.id, stmt_target.fed_id, stmt_target.ctrl_reg_id, stmt_target.size)
                        self.execution_blocks[tmp_eb_id] = eb
                        eb.add_new_statement(tmp_stmt_id)
                        eb.c_ebs.append(parent_eb.id)
                        eb.cost = eb.get_estimated_cost(self.control_regions, self.statements)
                    else:
                        #print(tmp_eb_id)
                        eb = self.execution_blocks.get(tmp_eb_id)
                        eb.add_new_statement(tmp_stmt_id)
                        eb.c_ebs.append(parent_eb.id)
                        eb.cost = eb.get_estimated_cost(self.control_regions, self.statements)
                        #print(self.execution_blocks.get(tmp_stmt_id))

    def get_producer_ebs_ids_of_statement(self, stmt_target):
        ebs = list()
        if stmt_target.producers:
            for tmp_stmt_id in stmt_target.producers:
                eb_id = self.statements.get(tmp_stmt_id).eb_id
                # if statement and its producer EB in same parent EB
                if eb_id not in ebs:
                    ebs.append(eb_id)
        return ebs

    def get_consumer_ebs_ids_of_statement(self, stmt_target):
        ebs = list()
        if stmt_target.consumers:
            for tmp_stmt_id in stmt_target.consumers:
                eb_id = self.statements.get(tmp_stmt_id).eb_id
                # if statement and its consumer EB in same parent EB
                if eb_id not in ebs:
                    ebs.append(eb_id)
        return ebs

    def get_sibling_producer_ebs_ids_of_statement(self, parent_eb, stmt_target):
        ebs = list()
        if stmt_target.producers:
            for tmp_stmt_id in stmt_target.producers:
                # print(tmp_stmt_id)
                eb_id = self.statements.get(tmp_stmt_id).eb_id
                # print("eb: ", eb_id, "tmp: ", tmp_stmt_id)
                # if statement and its producer EB in same parent EB
                if parent_eb.id == self.execution_blocks.get(eb_id).parent_id and eb_id != stmt_target.eb_id and eb_id not in ebs:
                    ebs.append(eb_id)
        return ebs

    def get_sibling_consumer_ebs_ids_of_statement(self, parent_eb, stmt_target):
        ebs = list()
        if stmt_target.consumers:
            for tmp_stmt_id in stmt_target.consumers:
                eb_id = self.statements.get(tmp_stmt_id).eb_id
                # if statement and its consumer EB in same parent EB
                if parent_eb.id == self.execution_blocks.get(eb_id).parent_id and eb_id not in ebs:
                    ebs.append(eb_id)
        return ebs

    def get_sibling_producer_ebs_ids_of_eb(self, parent_eb, eb_target):
        ebs = list()
        if eb_target.statements:
            for tmp_stmt_id in eb_target.statements:
                tmp_stmt = self.statements.get(tmp_stmt_id)
                if tmp_stmt.producers:
                    for prd_sid in tmp_stmt.producers:
                        prd_stmt = self.statements.get(prd_sid)
                        eb_id = prd_stmt.eb_id
                        # if statement and its producer EB in same parent EB
                        if parent_eb.id == self.execution_blocks.get(
                                eb_id).parent_id and eb_id != eb_target.id and eb_id not in ebs:
                            # PrintLog.print("tmp_stmt_id: {0}, prd_sid: {1}, eb_id: {2}".format(tmp_stmt_id, prd_sid, eb_id))
                            ebs.append(eb_id)
        return ebs

    def get_sibling_consumer_ebs_ids_of_eb(self, parent_eb, eb_target):
        ebs = list()
        if eb_target.statements:
            for tmp_stmt_id in eb_target.statements:
                tmp_stmt = self.statements.get(tmp_stmt_id)
                if tmp_stmt.consumers:
                    for csm_sid in tmp_stmt.consumers:
                        csm_stmt = self.statements.get(csm_sid)
                        eb_id = csm_stmt.eb_id
                        # if statement and its producer EB in same parent EB
                        if parent_eb.id == self.execution_blocks.get(
                                eb_id).parent_id and eb_id != eb_target.id and eb_id not in ebs:
                            # PrintLog.print("tmp_stmt_id: {0}, prd_sid: {1}, eb_id: {2}".format(tmp_stmt_id, prd_sid, eb_id))
                            ebs.append(eb_id)
        return ebs

    def get_execution_block(self, eb_sink_id):
        # print(eb_sink_id)
        for eb_id in self.execution_blocks:
            if eb_id == eb_sink_id:
                return self.execution_blocks.get(eb_id)

    def update_for_sinked_statement(self, parent_eb, eb_sink, stmt_target):
        # remove stmt from existing EB and then insert to new
        parent_eb.remove_statement(stmt_target.id)
        # update statement with its new EB ID
        stmt_target.eb_id = eb_sink.id
        # set the federation node ID of the EB to statement
        stmt_target.fed_id = eb_sink.fed_id
        # update statement dictionary
        self.statements[stmt_target.id] = stmt_target
        # add sinked statement to EB
        eb_sink.add_new_statement(stmt_target.id)
        # update the cost of eb_sink
        eb_sink.cost = eb_sink.get_estimated_cost(self.control_regions, self.statements)
        self.estimate_cost_of_statements()

    def update_for_sinked_region(self, parent_eb, eb_sink, region_target):
        # PrintLog.print(eb_sink.id)
        for sid in region_target.statements:
            self.update_for_sinked_statement(parent_eb, eb_sink, self.statements.get(sid))


    def get_copy(self, new_plan_id):
        new_copy = AugmentedGraph()
        new_copy.root_st_id = self.root_st_id

        new_copy.statements = copy.deepcopy(self.statements)
        new_copy.control_regions = copy.deepcopy(self.control_regions)
        new_copy.execution_blocks = copy.deepcopy(self.execution_blocks)
        new_copy.bfs_reverse_seq = copy.deepcopy(self.bfs_reverse_seq)
        new_copy.post_order_seq = copy.deepcopy(self.post_order_seq)
        new_copy.id = new_plan_id
        return new_copy

    def get_tmp_id(self, tmp_stmt_id, prd_eb_id):
        tmp_id = "S"
        for sid in self.bfs_reverse_seq:
            if sid == tmp_stmt_id:
                return tmp_id + "_s" + str(sid) + "e" + str(prd_eb_id)
            else:
                tmp_id += "_s" + str(sid) + "e" + str(self.statements.get(sid).eb_id)

    def rearrange_ebs(self):
        new_ebs = dict()
        running_fd = 0
        running_eb_id = 0
        for sid, statement in self.statements.items():
            if running_fd != statement.fed_id:
                running_fd = statement.fed_id
                running_eb_id = statement.eb_id
                # running_eb_id + 1
                if running_eb_id in new_ebs:
                    running_eb_id = max(self.execution_blocks, key=int) + 1
                    self.execution_blocks[running_eb_id] = copy.deepcopy(self.execution_blocks.get(statement.eb_id))
                    self.execution_blocks.get(running_eb_id).id = running_eb_id
                new_ebs[running_eb_id] = list()
            statement.eb_id = running_eb_id
            self.execution_blocks.get(running_eb_id).fed_id = statement.fed_id
            new_ebs.get(running_eb_id).append(sid)
        removable_ebs = list()
        for eb_id in self.execution_blocks:
            if eb_id not in new_ebs or not self.execution_blocks.get(eb_id):
                removable_ebs.append(eb_id)
            else:
                self.execution_blocks.get(eb_id).statements = copy.deepcopy(new_ebs.get(eb_id))
        # PrintLog.print_log(removable_ebs)
        for eb_id in removable_ebs:
            del self.execution_blocks[eb_id]
        removable_ebs.clear()
        self.scheduled_ebs = list(new_ebs.keys())

    def push_down_imperative(self):
        removable_ebs = list()
        sinkable_imperatives = dict()
        for sid, statement in self.statements.items():
            if not statement.is_declarative:
                c_eb_ids = self.get_consumer_ebs_ids_of_statement(statement)
                if len(c_eb_ids) == 1 and statement.eb_id not in c_eb_ids:
                    eb = self.execution_blocks.get(statement.eb_id)
                    # PrintLog.print_log("{0}@eb{2}: {3} {1}".format(sid, c_eb_ids, statement.eb_id, eb.statements))
                    eb.statements.remove(sid)

                    if not eb.statements:
                        removable_ebs.append(eb.id)
                    eb_sink = self.execution_blocks.get(c_eb_ids[0])
                    statement.fed_id = eb_sink.fed_id
                    statement.eb_id = eb_sink.id
                    self.statements[sid] = statement
                    if eb_sink.id not in sinkable_imperatives:
                        sinkable_imperatives[eb_sink.id] = list()
                    sinkable_imperatives.get(eb_sink.id).append(sid)
        if removable_ebs:
            # PrintLog.print_log(removable_ebs)
            for eb_id in removable_ebs:
                self.scheduled_ebs.remove(eb_id)
                del self.execution_blocks[eb_id]
            removable_ebs.clear()


        if sinkable_imperatives:
            for eb_id in sinkable_imperatives:
                sids = sinkable_imperatives.get(eb_id)
                if sids:
                    for sid in reversed(sids):
                        self.execution_blocks.get(eb_id).statements.insert(0, sid)


    def merge_ebs(self):
        deletable_eb_ids = list()

        for eb_id, eb in self.execution_blocks.items():
            eb.update_input_output(self)
            if eb.c_ebs and len(eb.c_ebs) == 1:
                c_eb = self.execution_blocks.get(eb.c_ebs[0])
                if eb.fed_id == c_eb.fed_id:
                    for sid in reversed(eb.statements):
                        self.statements.get(sid).eb_id = c_eb.id
                    # print("Move {0}: {1} to {2}:{3}".format(eb_id, eb.statements, c_eb.id, c_eb.statements))
                    insert_at = 0
                    for sid in c_eb.statements:
                        if self.statements.get(sid).expr.startswith("DECLARE"):
                            insert_at = insert_at + 1
                        else:
                            break
                    for sid in reversed(eb.statements):
                        if sid not in c_eb.statements:
                            c_eb.statements.insert(insert_at, sid)
                    deletable_eb_ids.append(eb_id)
        for eb_id in deletable_eb_ids:
            self.scheduled_ebs.remove(eb_id)
            del self.execution_blocks[eb_id]
        deletable_eb_ids.clear()
        for eb_id, eb in self.execution_blocks.items():
            eb.update_input_output(self)
            if eb.p_ebs and len(eb.p_ebs) == 1:
                p_eb = self.execution_blocks.get(eb.p_ebs[0])
                if eb.fed_id == p_eb.fed_id:
                    for sid in eb.statements:
                        self.statements.get(sid).eb_id = p_eb.id
                    p_eb.statements = p_eb.statements + eb.statements
                    deletable_eb_ids.append(eb_id)
        for eb_id in deletable_eb_ids:
            self.scheduled_ebs.remove(eb_id)
            del self.execution_blocks[eb_id]
        for eb_id, eb in self.execution_blocks.items():
            eb.update_input_output(self)
            eb = self.execution_blocks.get(eb_id)

    def plan_normalization(self):
        self.rearrange_ebs()
        self.push_down_imperative()
        self.merge_ebs()
