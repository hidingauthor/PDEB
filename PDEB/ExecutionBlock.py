class ExecutionBlock:

    def __init__(self, eb_id, parent_id, fn_id, ctrl_region_id, cost):
        self.id = eb_id
        self.parent_id = parent_id
        self.statements = list()
        self.ctrl_reg_id = ctrl_region_id
        self.fed_id = fn_id
        self.cost = cost
        self.p_ebs = list()
        self.c_ebs = list()
        self.input_tabular_sids = list()
        self.output_tabular_sids = list()
        self.input_scalar_sids = list()
        self.output_scalar_sids = list()
        # self.out_cost = out_cost
        self.executable_sqls = list()

    def add_new_statements(self, stmt_ids):
        self.statements = self.statements + stmt_ids
        # update cost

    def add_new_statement(self, stmt_id):
        self.statements.append(stmt_id)
        # update cost

    def remove_statement(self, stmt_id):
        if stmt_id in self.statements:
            self.statements.remove(stmt_id)
        # update cost

    def __str__(self):
        return "id: {0}, parent_id: {1}, ctrlRgnID: {2}, fdNodeId: {3}, statements: {4}, cost: {5}".format(self.id, self.parent_id, self.ctrl_reg_id, self.fed_id, self.statements, self.cost)

    def get_estimated_cost(self, control_regions, statement_maps):
        eb_cost = 0
        # transferred as input & output
        for tmp_stmt_id in self.statements:
            # data transferred from other EB to peb_id as input
            cost_of_producers = 0
            st_producers = statement_maps.get(tmp_stmt_id).producers
            if st_producers:
                # c2psg.get(tmp_stmt_id) contains the producers of tmp_stmt_id
                for prd_sid in st_producers:
                    prd_stmt = statement_maps.get(prd_sid)
                    if prd_stmt.fed_id != self.fed_id:
                        cost_of_producers += prd_stmt.size
                # if consumer(tmp_stmt_id) is in loop, then producers are called in every iteration
                control_region = control_regions.get(statement_maps.get(tmp_stmt_id).ctrl_reg_id)
                cost_of_producers = cost_of_producers * control_region.loop_count * control_region.outer_loop_count

            # data transferred to other EB from eb_id as output
            num_calls = 0
            st_consumers = statement_maps.get(tmp_stmt_id).consumers
            if st_consumers:
                # p2csg[tmp_stmt_id] contains the consumers of tmp_stmt_id
                for csm_sid in st_consumers:
                    csm_stmt = statement_maps.get(csm_sid)
                    if csm_stmt.fed_id != self.fed_id:
                        # print("\tO-> PEB: {0}, CEB: {1}, PST: {2}, CST: {3}, Size: {4}".format(eb_id, csm_stmt.eb_id, tmp_stmt_id, csm_sid, statement_maps.get(tmp_stmt_id).size))
                        # if csm_stmt.fed_id != statement_maps.get(tmp_stmt_id).fed_id:
                        # if consumer(csm_stmt) is in loop, then tmp_stmt_id is called in every iteration
                        control_region = control_regions.get(csm_stmt.ctrl_reg_id)
                        num_calls = num_calls + control_region.loop_count * control_region.outer_loop_count
            stmt_id_cost = statement_maps.get(tmp_stmt_id).size
            cost_of_consumers = stmt_id_cost * num_calls
            tmp_cost = cost_of_producers + cost_of_consumers
            eb_cost = eb_cost + tmp_cost
        return eb_cost

    def update_input_output(self, ag):
        #print(self.id)
        self.input_tabular_sids.clear()
        self.output_tabular_sids.clear()
        self.input_scalar_sids.clear()
        self.output_scalar_sids.clear()
        self.executable_sqls.clear()
        self.p_ebs.clear()
        self.c_ebs.clear()
        for sid in self.statements:
            statement = ag.statements.get(sid)
            if statement.producers:
                for psid in statement.producers:
                    pstatement = ag.statements.get(psid)
                    if pstatement.eb_id != self.id:
                        if pstatement.is_declarative and psid not in self.input_tabular_sids:
                            self.input_tabular_sids.append(psid)
                        elif not pstatement.is_declarative and psid not in self.input_scalar_sids:
                            self.input_scalar_sids.append(psid)
                        if pstatement.eb_id not in self.p_ebs:
                            self.p_ebs.append(pstatement.eb_id)
            if statement.consumers:
                for csid in statement.consumers:
                    cstatement = ag.statements.get(csid)
                    if cstatement.eb_id != self.id:
                        if ag.statements.get(sid).is_declarative and sid not in self.output_tabular_sids:
                            self.output_tabular_sids.append(sid)
                        elif not ag.statements.get(sid).is_declarative and sid not in self.output_scalar_sids:
                            self.output_scalar_sids.append(sid)
                        if cstatement.eb_id not in self.c_ebs:
                            self.c_ebs.append(cstatement.eb_id)
