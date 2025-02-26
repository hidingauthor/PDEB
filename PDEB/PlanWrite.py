##############################################################
############### ProgramRewrite Operator #######################
##############################################################

import time
import copy
from PrintLog import PrintLog
from ExecutionThread import ExecutionThread


class PlanWrite:
    def __init__(self):
        self.sqls_in_fds = dict()

    def add_sql_to_sqls_in_fds(self, tmp_stmt_id, federation_id, dml):
        if tmp_stmt_id not in self.sqls_in_fds:
            self.sqls_in_fds[tmp_stmt_id] = dict()
        if federation_id not in self.sqls_in_fds.get(tmp_stmt_id):
            self.sqls_in_fds.get(tmp_stmt_id)[federation_id] = list()
        if dml not in self.sqls_in_fds.get(tmp_stmt_id).get(federation_id):
            self.sqls_in_fds.get(tmp_stmt_id).get(federation_id).append(dml)

    # noinspection PyMethodMayBeStatic
    def replaces_variables(self, replaceables, pfnid, expr):
        expr = expr + " "
        for r_info in replaceables:
            if pfnid == r_info[0]:
                expr = expr.replace(r_info[1], r_info[2])
        return expr.strip()

    def create_procedure(self, ag, fc, last_statement, loop_name, statements, in_vars, out_vars, replaceables):
        # remove a statement if there is no consumer or producer outside loop
        for sid in in_vars:
            if ag.statements.get(sid).ctrl_reg_id == last_statement.ctrl_reg_id:
                in_vars.remove(sid)
        for sid in out_vars:
            has_consumer_outside = False
            for csid in ag.statements.get(sid).consumers:
                if ag.statements.get(csid).ctrl_reg_id != last_statement.ctrl_reg_id:
                    has_consumer_outside = True
                    break
            if not has_consumer_outside:
                out_vars.remove(sid)
        is_cal_view_proc = False
        statement_for_calc = None
        parameters = list()

        input_place_holder = list()
        for sid in in_vars:
            statement = ag.statements.get(sid)
            if statement.is_declarative:
                continue
            if not ag.statements.get(sid).producers:
                consumers_of_sid = ag.statements.get(sid).consumers
                if consumers_of_sid:
                    all_consumers_are_in_same_federation = True
                    fed_id = ag.statements.get(consumers_of_sid[0]).fed_id
                    for csid in consumers_of_sid:
                        if fed_id != ag.statements.get(csid).fed_id:
                            all_consumers_are_in_same_federation = False
                    if all_consumers_are_in_same_federation:
                        statement.eb_id = ag.control_regions.get(fed_id).parent_eb_id
                        statement.fed_id = fed_id
                        statements.insert(0, [statement.id, statement.expr])
                continue
            else:
                var = "IN " + statement.vars.out_var_name + " " + statement.vars.out_var_type
                parameters.append(var)
                input_place_holder.append("'PLACEHOLDER'=('$$" + statement.vars.out_var_name + "$$', '" + statement.vars.out_var_ddl + "')")

        output_place_holder = list()
        if len(out_vars) == 1:
            statement = ag.statements.get(out_vars[0])
            if not statement.is_declarative:
                var = "OUT " + statement.vars.out_var_name + " " + statement.vars.out_var_type
            else:
                # create calc view
                is_cal_view_proc = True
                var = "OUT " + statement.vars.out_var_name + " TABLE(" + statement.vars.out_var_ddl + ")"
                statement_for_calc = statement
            parameters.append(var)
        elif len(out_vars) > 1:
            for sid in out_vars:
                statement = ag.statements.get(sid)
                if not statement.is_declarative:
                    continue
                # create out tables
                out_table_ddl = "CREATE TABLE " + statement.vars.out_var_name + "(" + statement.vars.out_var_ddl + ")"
                fc.add_to_dropables(statement.fed_id, statement.vars.out_var_name)
                self.add_sql_to_sqls_in_fds(statement.id, statement.fed_id, out_table_ddl)
                output_place_holder.append("'PLACEHOLDER'=('$$" + statement.vars.out_var_name + "$$', '?')")
                var = "OUT " + statement.vars.out_var_name
                parameters.append(var)
        parameters = ", ".join(parameters)

        proc_place_holder = ""
        place_holders = input_place_holder + output_place_holder
        if len(place_holders) > 0:
            proc_place_holder = ", ".join(place_holders)
            proc_place_holder = "(" + proc_place_holder + ")"

        procedure_statements = list()
        procedure_name = "procedure_for_reg_" + str(last_statement.ctrl_reg_id) + "_" + loop_name
        fc.add_to_dropables(last_statement.fed_id, procedure_name)
        procedure_statements.append("CREATE OR REPLACE PROCEDURE " + procedure_name + "(" + parameters + ") AS \n BEGIN")
        for st_info in statements:
            # PrintLog.print(st_info[1])
            procedure_statements.append("\t"+st_info[1])
        procedure_statements.append(" END;")
        procedure_ddl = "\n".join(procedure_statements)
        self.add_sql_to_sqls_in_fds(last_statement.id, last_statement.fed_id, procedure_ddl)

        return replaceables

    def create_procedure_from_eb(self, ag, fc, eb, replaceables):
        is_cal_view_proc = False
        statement_for_calc = None
        parameters = list()
        input_place_holder = list()
        for sid in eb.input_tabular_sids:
            statement = ag.statements.get(sid)
            if not statement.is_declarative:
                var = "IN " + statement.vars.out_var_name + " " + statement.vars.out_var_type
                parameters.append(var)
                input_place_holder.append("'PLACEHOLDER'=('$$" + statement.vars.out_var_name + "$$', '" + statement.vars.out_var_ddl + "')")
        output_place_holder = list()
        if len(eb.output_tabular_sids) == 1:
            statement = ag.statements.get(eb.output_tabular_sids[0])
            if not statement.is_declarative:
                var = "OUT " + statement.vars.out_var_name + " " + statement.vars.out_var_type
            else:
                # create calc view
                is_cal_view_proc = True
                var = "OUT " + statement.vars.out_var_name + " TABLE(" + statement.vars.out_var_ddl + ")"
                statement_for_calc = statement
            parameters.append(var)

        parameters = ", ".join(parameters)
        proc_place_holder = ""
        place_holders = input_place_holder + output_place_holder
        if len(place_holders) > 0:
            proc_place_holder = ", ".join(place_holders)
            proc_place_holder = "(" + proc_place_holder + ")"
        procedure_statements = list()
        procedure_name = "procedure_for_reg_" + str(eb.ctrl_reg_id) + "_eb" + str(eb.id)
        fc.add_to_dropables(eb.fed_id, procedure_name)
        procedure_statements.append("CREATE OR REPLACE PROCEDURE " + procedure_name + "(" + parameters + ") AS \n BEGIN")
        for sid in eb.statements:
            # PrintLog.print(st_info[1])
            statement = ag.statements.get(sid)
            tmp_sql = self.replaces_variables(replaceables, eb.fed_id, statement.expr)

            if statement.is_declarative and not tmp_sql.strip().endswith(";"):
                tmp_sql = tmp_sql + ";"
            if statement.is_declarative and len(statement.vars.out_var_name) > 0:
                procedure_statements.append("\t{0} = {1}".format(statement.vars.out_var_name, tmp_sql))
            else:
                procedure_statements.append("\t" + tmp_sql)
        procedure_statements.append(" END;")
        procedure_ddl = "\n".join(procedure_statements)
        eb.executable_sqls.append(procedure_ddl)
        if is_cal_view_proc:
            calc_view_name = procedure_name + "_view"
            fc.add_to_dropables(eb.fed_id, calc_view_name)
            sql_ddl = "CREATE OR REPLACE COLUMN VIEW "+calc_view_name+" TYPE CALCULATION WITH PARAMETERS ('PROCEDURE_NAME'='"+procedure_name.upper()+"');"
            eb.executable_sqls.append(sql_ddl)

            for cebid in eb.c_ebs:
                ceb = ag.execution_blocks.get(cebid)

                # at local
                local_calc_view = "local_" + calc_view_name
                fc.add_to_dropables(eb.fed_id, local_calc_view)
                ddl_view = "CREATE VIEW " + str(local_calc_view) + " AS (SELECT * FROM " + calc_view_name + proc_place_holder + ");"
                eb.executable_sqls.append(ddl_view)

                # at remote
                vir_tab_var_vt = "vir_" + str(statement_for_calc.vars.out_var_name) + "_view"
                fc.add_to_dropables(ceb.fed_id, vir_tab_var_vt)
                ddl_vtab = "CREATE VIRTUAL TABLE " + vir_tab_var_vt + " AT " + fc.federations.get(eb.fed_id) + local_calc_view + ";"
                ceb.executable_sqls.append(ddl_vtab)
                replaceables.append([ceb.fed_id, ":" + statement_for_calc.vars.out_var_name + " ", vir_tab_var_vt + " "])
                replaceables.append([ceb.fed_id, ":" + statement_for_calc.vars.out_var_name + ",", vir_tab_var_vt + ","])

        elif not statement_for_calc:
            eb.executable_sqls.append("call "+procedure_name+"();")
        return replaceables

    def rewrite_code(self, ag, fc):
        replaceables = list()
        control_region_queue = list()
        current_region = 1

        loop_braces = list()
        loop_statements = dict()
        loop_in_vars = dict()
        loop_out_vars = dict()
        control_region_queue.append(current_region)
        key = ""
        for eb_id in ag.scheduled_ebs:
            eb = ag.execution_blocks.get(eb_id)
            if len(eb.output_tabular_sids) <= 1 and len(eb.statements) > 1:
                self.create_procedure_from_eb(ag, fc, eb, replaceables)
            else:
                # if any of consumers is not in same federation, then
                #   (1) create a view from that statement
                #   (2) create a virtual table for that view in remote place
                for psid in eb.statements:
                    statement = ag.statements.get(psid)
                    tmp_sql = self.replaces_variables(replaceables, eb.fed_id, statement.expr)
                    if not statement.is_declarative or ag.control_regions.get(statement.ctrl_reg_id).type != "SEQ":
                        # PrintLog.print(statement.expr)
                        tmp_sql = tmp_sql.strip()
                        if tmp_sql.startswith("WHILE"):
                            key = "WHILE" + str(len(loop_braces))
                            loop_statements[key] = list()
                            loop_statements.get(key).append([psid, tmp_sql])
                            loop_in_vars[key] = list()
                            if statement.producers:
                                loop_in_vars[key] = statement.producers
                            loop_out_vars[key] = list()
                            if statement.consumers:
                                loop_out_vars[key] = [psid]
                            loop_braces.append(key)
                            continue
                        if tmp_sql.startswith("FOR"):
                            key = "FOR" + str(len(loop_braces))
                            loop_statements[key] = list()
                            loop_statements.get(key).append([psid, tmp_sql])
                            loop_in_vars[key] = list()
                            if statement.producers:
                                loop_in_vars[key] = statement.producers
                            loop_out_vars[key] = list()
                            if statement.consumers:
                                loop_out_vars[key] = [psid]
                            loop_braces.append(key)
                            continue
                        if tmp_sql.startswith("END WHILE"):
                            loop_statements.get(key).append([psid, tmp_sql])
                            replaceables = self.create_procedure(ag, fc, statement, loop_braces.pop(), loop_statements.get(key), list(set(loop_in_vars.get(key))), list(set(loop_out_vars.get(key))), replaceables)
                            # add_sql_to_sqls_in_fds(psid, pfnid, )
                            if len(loop_braces):
                                key = loop_braces[0]
                            continue
                        if not tmp_sql.endswith(";"):
                            tmp_sql = tmp_sql + ";"
                        if loop_statements.get(key):
                            if len(statement.vars.out_var_name) > 0 and statement.is_declarative:
                                tmp_sql = statement.vars.out_var_name + " = " + tmp_sql
                            loop_statements.get(key).append([psid, tmp_sql])
                            if statement.producers:
                                loop_in_vars[key] = loop_in_vars.get(key) + statement.producers
                            if statement.consumers:
                                loop_out_vars.get(key).append(psid)
                        continue
                    # PrintLog.print("Variable: {0}, expr: {1}".format(tab_var, tmp_sql))
                    # if any of consumers is not in same federation, then
                    #   (1) create a view from that statement
                    #   (2) create a virtual table for that view in remote place
                    if statement.consumers:
                        tab_var = statement.vars.out_var_name
                        tab_var_vt = str(tab_var) + "_view"
                        ddl_view = "CREATE VIEW " + str(tab_var_vt) + " AS (" + tmp_sql + ");"
                        eb.executable_sqls.append(ddl_view)
                        for csid in statement.consumers:
                            c_eb = ag.execution_blocks.get(ag.statements.get(csid).eb_id)
                            if eb.fed_id != c_eb.fed_id:
                                vir_tab_var_vt = "vir_" + str(tab_var) + "_view"
                                ddl_vtab = "CREATE VIRTUAL TABLE "+vir_tab_var_vt+" AT "+fc.federations.get(eb.fed_id)+tab_var_vt+";"
                                if ddl_vtab not in c_eb.executable_sqls:
                                    c_eb.executable_sqls.append(ddl_vtab)
                                replaceables.append([c_eb.fed_id, ":" + tab_var + " ", vir_tab_var_vt + " "])
                                replaceables.append([c_eb.fed_id, ":" + tab_var + ",", vir_tab_var_vt + ","])
                                fc.add_to_dropables(eb.fed_id, tab_var_vt)
                                fc.add_to_dropables(c_eb.fed_id, vir_tab_var_vt)
                            else:
                                # if there is consumer in same EB/Federatioin Node.
                                # add_sql_to_sqls_in_fds(psid, pfnid, statement.expr)
                                replaceables.append([eb.fed_id, ":" + tab_var+" ", tab_var_vt+" "])
                                replaceables.append([eb.fed_id, ":" + tab_var+",", tab_var_vt+","])
                                fc.add_to_dropables(eb.fed_id, tab_var_vt)
                    else:
                        if statement.is_declarative and not tmp_sql.strip().endswith(";"):
                            tmp_sql = tmp_sql + ";"
                        eb.executable_sqls.append(tmp_sql)
        # scheduled EBS
        scheduled_ebs = copy.deepcopy(ag.scheduled_ebs)
        ag.scheduled_ebs.clear()
        parallel_ebs = list()
        for eb_id in scheduled_ebs:
            eb = ag.execution_blocks.get(eb_id)
            if not eb.executable_sqls:
                continue
            if not eb.p_ebs:
                parallel_ebs.append(eb_id)
                continue
            else:
                if len(parallel_ebs) > 0:
                    ag.scheduled_ebs.append(parallel_ebs)
                parallel_ebs = list()
                parallel_ebs.append(eb_id)
        if len(parallel_ebs) > 0:
            ag.scheduled_ebs.append(parallel_ebs)
        PrintLog.print_log("\nRe-written codes:")
        PrintLog.print_log(ag.scheduled_ebs)
        for eb_ids in ag.scheduled_ebs:
            for eb_id in eb_ids:
                eb = ag.execution_blocks.get(eb_id)
                if eb:
                    PrintLog.print_log("\neb{0}@fn{1}:\n{2}".format(eb.id, eb.fed_id, "\n".join(eb.executable_sqls)))
        PrintLog.print_log()


    def execute_plan(self, ag, fc):
        # PrintLog.print_log(ag.scheduled_ebs)
        start_time = time.time() * 1000
        for eb_ids in ag.scheduled_ebs:
            for eb_id in eb_ids:
                eb = ag.execution_blocks.get(eb_id)
                if eb:
                    PrintLog.print_log("\neb{0}@fn{1}:\n{2}".format(eb.id, eb.fed_id, "\n".join(eb.executable_sqls)))
            if len(eb_ids) > 1:
                threads = []
                for eb_id in eb_ids:
                    thrd = ExecutionThread(ag, fc, eb_id)
                    thrd.start()
                    threads.append(thrd)
                for t in threads:
                    t.join()
                threads.clear()
            elif eb_ids:
                eb = ag.execution_blocks.get(eb_ids[0])
                if eb and eb.executable_sqls:
                    executable_sqls = eb.executable_sqls
                    for sql in executable_sqls:
                        fc.execute_query(eb.fed_id, sql, True)
                        # end_time = time.time() * 1000
                        # exe_time = end_time - start_time
                        # print("Execution time: {0}".format(round(exe_time, 3)))
        end_time = time.time() * 1000
        exe_time = end_time - start_time
        fc.drop_dropables()
        end_time = time.time() * 1000
        exe_drop_time = end_time - start_time
        print("Execution time: {0} {1}".format(round(exe_time, 3), round(exe_drop_time, 3)))
