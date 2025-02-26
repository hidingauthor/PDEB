##############################################################
############### Sink Operator #######################
##############################################################

from PlanWrite import PlanWrite

class ForwardGreedyApproach:

    def __init__(self, ag, fc, parent_fd):
        self.ag = ag
        self.fc = fc
        self.parent_fd = parent_fd
        self.pw = PlanWrite()

    def plan_enumeration(self):
        self.enumerate(self.ag.execution_blocks.get(self.parent_fd))
        # PrintLog.print_log("With complex EBs: ")
        # for stmt in self.ag.statements:
        #     PrintLog.print_log(self.ag.statements.get(stmt))
        self.ag.plan_normalization()
        # PrintLog.print_log("With Normalized Plan: ")
        # for eb_id in self.ag.scheduled_ebs:
        #     for stmt in self.ag.execution_blocks.get(eb_id).statements:
        #         PrintLog.print_log(self.ag.statements.get(stmt))
        self.pw.rewrite_code(self.ag, self.fc)

    def execute_plan(self):
        self.pw.execute_plan(self.ag, self.fc)

    def enumerate(self, parent_eb):

        cost_before_sinking = self.ag.statements.get(self.ag.root_st_id).cost
        for tmp_stmt_id in self.ag.bfs_reverse_seq:
            stmt_target = self.ag.statements.get(tmp_stmt_id)
            if self.ag.statements.get(tmp_stmt_id).consumers and parent_eb.id == stmt_target.eb_id:
                prd_ebs_ids = self.ag.get_sibling_producer_ebs_ids_of_statement(parent_eb, stmt_target)
                # handle to sink region. It's not done yet
                eb_sink_id = self.get_most_beneficial_eb_id_to_sink(cost_before_sinking, self.ag, stmt_target, parent_eb, prd_ebs_ids)
                if eb_sink_id != 0:
                    # get EB object by its ID
                    if eb_sink_id > 0:
                        # Sink the statement
                        eb_sink = self.ag.get_execution_block(eb_sink_id)
                        self.ag.update_for_sinked_statement(parent_eb, eb_sink, stmt_target)
                    else:
                        # Sink the region that contains the statement
                        eb_sink = self.ag.get_execution_block(-eb_sink_id)
                        self.ag.update_for_sinked_region(parent_eb, eb_sink, self.ag.control_regions.get(stmt_target.ctrl_reg_id))
                    # Consider recursive sinking

    def get_most_beneficial_eb_id_to_sink(self, cost_before_sinking, ag, stmt_target, parent_eb, prd_ebs_ids):
        # consider that both stmt_target and ebs should be in same region
        benefit = 0
        eb_id_selected = 0
        region = ag.control_regions.get(stmt_target.ctrl_reg_id)
        # cost_before_sinking = cost_before_sinking * region.loop_count * region.outer_loop_count
        for prd_eb_id in prd_ebs_ids:
            cost_after_sinking = self.forward_search(ag, stmt_target, parent_eb, prd_eb_id)
            tmp_benefit = cost_before_sinking - cost_after_sinking
            if benefit < tmp_benefit:
                benefit = tmp_benefit
                eb_id_selected = prd_eb_id
        if eb_id_selected != 0 and region.id != ag.execution_blocks.get(eb_id_selected).ctrl_reg_id and region.type != "SEQ":
            return -eb_id_selected
        return eb_id_selected

    # noinspection PyMethodMayBeStatic
    def forward_search(self, ag, stmt_target, parent_eb, prd_eb_id):
        # if not ag.consumers(stmt_target.id):
        if stmt_target.id == 15:
            return stmt_target.cost

        region = ag.control_regions.get(stmt_target.ctrl_reg_id)
        copy_ag = ag.get_copy("x")
        copy_eb_sink = copy_ag.get_execution_block(prd_eb_id)
        copy_parent_eb = copy_ag.execution_blocks.get(self.parent_fd)
        if region.id != ag.execution_blocks.get(prd_eb_id).ctrl_reg_id and region.type != "SEQ":
            # sink entire region of stmt_target
            region_target = copy_ag.control_regions.get(stmt_target.ctrl_reg_id)
            copy_ag.update_for_sinked_region(copy_parent_eb, copy_eb_sink, region_target)
            # now consider the consumers of all statements in sinked region for sinking
            for rsid in region_target.statements:
                if copy_ag.statements.get(rsid).consumers:
                    for sid in copy_ag.statements.get(rsid).consumers:
                        new_prd_ebs_ids = copy_ag.get_sibling_producer_ebs_ids_of_statement(parent_eb, copy_ag.statements.get(sid))
                        for new_prd_eb_id in new_prd_ebs_ids:
                            return self.forward_search(copy_ag, copy_ag.statements.get(sid), parent_eb, new_prd_eb_id)
        else:
            copy_stmt_target = copy_ag.statements.get(stmt_target.id)
            copy_ag.update_for_sinked_statement(copy_parent_eb, copy_eb_sink, copy_stmt_target)
            # now consider the consumers of "copy_stmt_target" for sinking
            if copy_ag.statements.get(stmt_target.id).consumers:
                for sid in copy_ag.statements.get(stmt_target.id).consumers:
                    new_prd_ebs_ids = copy_ag.get_sibling_producer_ebs_ids_of_statement(parent_eb, copy_ag.statements.get(sid))
                    for new_prd_eb_id in new_prd_ebs_ids:
                        return self.forward_search(copy_ag, copy_ag.statements.get(sid), parent_eb, new_prd_eb_id)
        return 9999999999

    # noinspection PyMethodMayBeStatic
    def get_cost_before_sinking_statement(self, ag, stmt_target):
        cost = 0
        # consider all consumers of stmt_target
        if ag.statements.get(stmt_target.id).consumers:
            for c_sid in ag.statements.get(stmt_target.id).consumers:
                c_stmt = ag.statements.get(c_sid)
                cost += c_stmt.cost
        return cost

    # noinspection PyMethodMayBeStatic
    def get_cost_before_sinking_region(self, ag, ctrl_reg_id):
        cost = 0
        statements = ag.control_regions.get(ctrl_reg_id)
        if statements:
            for sid in statements:
                cost += self.get_cost_before_sinking_statement(ag, ag.statements.get(sid))
        return cost
