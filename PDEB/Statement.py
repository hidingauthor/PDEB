class Statement:

    def __init__(self, stid, crid, ebid, fnid, is_declarative, hbt, nr, size, vars, base_tables, expr):
        self.id = stid
        self.ctrl_reg_id = crid
        self.eb_id = ebid
        self.fed_id = fnid
        self.has_base_tab = hbt
        self.expr = expr
        self.is_declarative = is_declarative
        self.nRows = nr
        self.size = size
        self.base_tables = base_tables
        self.vars = vars
        self.trnsfrType = -1
        self.cost = 0
        self.consumers = list()
        self.producers = list()


    def estimate_cost(self, ag):
        cost_of_producers = 0
        if self.producers:
            for prd_sid in self.producers:
                prd_stmt = ag.statements.get(prd_sid)
                if prd_stmt.fed_id != self.fed_id:
                    # PrintLog.print("\t\t2I-> PEB: {0}, CEB: {1}, PST: {2}, CST: {3}, Size: {4}".format(prd_stmt.eb_id, prd_eb_target.id, prd_stmt.id, stmt_target.id, prd_stmt.size))
                    cost_of_producers += prd_stmt.size
                    # if consumer is in loop, then producers are called in every iteration
                cost_of_producers += prd_stmt.cost
            control_region = ag.control_regions.get(self.ctrl_reg_id)
            cost_of_producers = cost_of_producers * control_region.loop_count * control_region.outer_loop_count
            self.cost = cost_of_producers

    def __str__(self):
        return "cr: {1}, eb: {2}, fd: {3}, cr: {8}, nr: {4: >9}, size: {5: >8}, cost: {6: >9}, p: {9: <10}, c: {10: <8}, sid: {0: >2}, \t{7}".format(self.id, self.ctrl_reg_id, self.eb_id, self.fed_id, self.nRows, self.size, self.cost, self.expr, self.ctrl_reg_id, str(self.producers), str(self.consumers))