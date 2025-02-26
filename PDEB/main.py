from AugmentedGraph import AugmentedGraph
from FederationConnector import FederationConnector
from ForwardGreedyApproach import ForwardGreedyApproach
from OriginalScript import OriginalScript
import time

def using_algorithm(alg_type, casg, connections, parent_fd):
    if alg_type == "FG":
        start_time = int(round(time.time() * 1000000))
        fgreedy = ForwardGreedyApproach(casg, connections, parent_fd)
        fgreedy.plan_enumeration()
        end_time = int(round(time.time() * 1000000))
        compile_time = end_time - start_time
        print("Compile Time: " + str(round(compile_time, 3)))
        # fgreedy.execute_plan()

    elif alg_type == "ORG":
        start_time = int(round(time.time() * 1000000))
        org_script = OriginalScript(connections)
        org_script.compile()
        end_time = int(round(time.time() * 1000000))
        compile_time = end_time - start_time
        print("Compile Time: " + str(round(compile_time, 3)))
        org_script.execute_plan()


pfd = 1
fc = FederationConnector()
fc.create_federation_connection()
ag = AugmentedGraph()
ag.create_graph(3)
using_algorithm("FG", ag, fc, pfd)
fc.disconnect_federation()

