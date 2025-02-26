class Variable:
    def __init__(self, out_name, out_var_type, out_ddl, in_var_names):
        self.out_var_name = out_name
        self.out_var_type = out_var_type
        self.out_var_ddl = out_ddl
        self.in_var_names = in_var_names
