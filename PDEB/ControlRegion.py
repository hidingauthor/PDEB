class ControlRegion:
    def __init__(self, region_id, parent_eb_id, l_count, o_l_count, type):
        self.id = region_id
        self.parent_eb_id = parent_eb_id
        self.statements = list()
        self.loop_count = l_count
        self.outer_loop_count = o_l_count
        self.type = type

    def add_statements(self, stmt_ids):
        self.statements = self.statements + stmt_ids
        # update cost

    def add_statement(self, stmt_id):
        self.statements.append(stmt_id)

    def remove_statement(self, stmt_id):
        self.statements.remove(stmt_id)
        # update cost

    def __str__(self):
        return "id: {0}, parent_eb_id: {1}, loop_count: {2}, outer_loop_count: {3}, type: {4}".format(self.id, self.parent_eb_id, self.loop_count, self.outer_loop_count, self.type)
