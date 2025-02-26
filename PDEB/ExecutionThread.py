import threading
import time


class ExecutionThread (threading.Thread):

    def __init__(self, ag, fc, eb_id):
        threading.Thread.__init__(self)
        self.ag = ag
        self.fc = fc
        self.eb_id = eb_id

    def run(self):
        eb = self.ag.execution_blocks.get(self.eb_id)
        start_time = time.time() * 1000
        if eb and eb.executable_sqls:
            for sql in eb.executable_sqls:
                self.fc.execute_query(eb.fed_id, sql, True)
            end_time = time.time() * 1000
            exe_time = end_time - start_time
            print("Execution time EB: {0}".format(round(exe_time, 3)))
