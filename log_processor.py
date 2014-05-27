import threading
import time
import sys

import nodes


class Coordinator():
    """
    Coordinates log processing algorithm between server nodes.

    1) Starts map tasks in all nodes
    2) Waits for all map tasks to finish
    3) Starts reduce tasks in all nodes, mediating which node becomes responsible for each user log consolidation
    """

    def __init__(self, nodes_list):

        # All active nodes
        self.nodes_list = nodes_list

        # Dictionary that stores in which server each user is being consolidated
        self.users_owner = {}

        self.map_tasks_finished = 0
        self.reduce_tasks_finished = 0

        # Thread Simulation events and locks
        self.map_finished_event = threading.Event()
        self.reduce_finished_event = threading.Event()
        self.map_lock = threading.RLock()
        self.acquire_ownership_lock = threading.RLock()
        self.reduce_lock = threading.RLock()

    def run(self):
        while not self.all_nodes_are_running():
            safe_print('COORDINATOR > waiting for nodes...')
            time.sleep(1)
        safe_print('COORDINATOR > is ready to begin')

        for node in self.nodes_list:
            node.start_map_task(self)

        self.map_finished_event.wait()
        safe_print('COORDINATOR > All nodes finished map task')

        for node in self.nodes_list:
            node.start_reduce_task()

        self.reduce_finished_event.wait()
        safe_print('COORDINATOR > All nodes finished reduce task')
        safe_print('--------------------------------------------------------')
        safe_print('Here is the final user logs distribution:')
        for user_id in self.users_owner:
            safe_print('%s -> %s' % (user_id, self.users_owner[user_id].id))

    def all_nodes_are_running(self):
        return reduce(lambda a, b: a and b, [node.running for node in self.nodes_list])

    ##############################################
    # Inter-thread API
    ##############################################
    def map_finished(self, node):
        """
        Called by a node when it finishes a map task.
        """
        with self.map_lock:
            safe_print('COORDINATOR > processing %s map...' % node.id)
            self.map_tasks_finished += 1
            if self.map_tasks_finished == len(self.nodes_list):
                self.map_finished_event.set()
            safe_print('COORDINATOR > processing %s map: DONE!' % node.id)

    def acquire_ownership(self, node, user_id):
        """
        Called by a node that wants to run reduce task on a given user.
        Returns True if ownership is given to the caller, or False otherwise.
        """
        with self.acquire_ownership_lock:
            if user_id in self.users_owner:
                return False
            else:
                self.users_owner[user_id] = node
                return True

    def get_nodes_with_user_logs(self, caller_node, user_id):
        """
        Returns which nodes found a given user in its map task.
        Coordinator mediates this since nodes don't know about each other.
        """
        result = []
        for node in self.nodes_list:
            if node.id != caller_node.id:
                if node.has_user_logs(user_id):
                    result.append(node)

        return result

    def reduce_finished(self, node):
        with self.reduce_lock:
            safe_print('COORDINATOR > processing %s reduce...' % node.id)
            self.reduce_tasks_finished += 1
            if self.reduce_tasks_finished == len(self.nodes_list):
                self.reduce_finished_event.set()
            safe_print('COORDINATOR > processing %s reduce: DONE!' % node.id)


def safe_print(s):
    """
    Beautiful multithread print
    """
    sys.stdout.write(s + '\n')


if __name__ == "__main__":

    server_A = nodes.Node('server_A')
    t = threading.Thread(target=server_A.run)
    t.daemon = True
    t.start()

    server_B = nodes.Node('server_B')
    t = threading.Thread(target=server_B.run)
    t.daemon = True
    t.start()

    server_C = nodes.Node('server_C')
    t = threading.Thread(target=server_C.run)
    t.daemon = True
    t.start()

    server_D = nodes.Node('server_D')
    t = threading.Thread(target=server_D.run)
    t.daemon = True
    t.start()

    try:
        coordinator = Coordinator([server_A, server_B, server_C, server_D])
        coordinator.run()
    except KeyboardInterrupt:
        sys.exit()


		
		
	


