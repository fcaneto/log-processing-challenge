import threading
import os
import shutil
import unittest

from log_processor import safe_print
import util
import log_parser


class Node():
    """
    Simulates a server node as a thread
    """

    def __init__(self, server_id):
        self.running = False
        self.id = server_id

        # Stores users found in this node during map task
        self.users_found = set()

        # Thread simulation events
        self.map_start_event = threading.Event()
        self.reduce_start_event = threading.Event()

    def run(self):
        self.running = True
        safe_print('[%s] > Running...' % self.id)
        self.map_start_event.wait()
        safe_print('[%s] > Woke up to run map task' % self.id)

        # Resets output directories from previous run
        shutil.rmtree(os.path.join(self.id, 'buckets'), ignore_errors=True)
        shutil.rmtree(os.path.join(self.id, 'processed_logs'), ignore_errors=True)

        self.do_map()
        self.coordinator.map_finished(self)

        safe_print("[%s] > So wake me up when maps are over..." % self.id)
        self.reduce_start_event.wait()
        safe_print('[%s] > Woke up to run reduce task' % self.id)

        self.do_reduce()
        safe_print('[%s] > Reduce is over' % self.id)
        self.coordinator.reduce_finished(self)


    def do_map(self):
        """
        Process log file in chunks, splitting it in separate files for each user_id,
        preserving original sorting.

        Returns all the user_ids found.

        ps: processing only one input log file
        """
        CHUNK_SIZE = 200 # in bytes

        with open(self.input_file_path()) as input_file:
            current_chunk = 0
            end_of_file = False

            while not end_of_file:
                # Processing one chunk
                buckets = {}
                current_chunk += 1
                current_file_position = 0

                while current_file_position < CHUNK_SIZE * current_chunk and not end_of_file:
                    # Processing one line
                    line = input_file.readline()
                    current_file_position = input_file.tell()

                    end_of_file = not line
                    if not end_of_file:
                        user_id = log_parser.get_user_id(line)
                        self.users_found.add(user_id)
                        if user_id in buckets:
                            buckets[user_id].append(line)
                        else:
                            buckets[user_id] = [line]

                # append each bucket line to bucket file
                for user_id in buckets:
                    with open(self.bucket_file_path(user_id), 'a') as bucket_file:
                        for line in buckets[user_id]:
                            bucket_file.write(line)

    def do_reduce(self):
        """
        Tries to consolidate all users that it found locally.
        Maybe other node gets a given user first, but coordinator controls racing conditions.

        For each user being consolidated:
            1. The node downloads the user bucket produced by map tasks in all other nodes
            2. And merges the sorted buckets line by line in the final output file
        """
        for user_id in self.users_found:
            if self.coordinator.acquire_ownership(self, user_id):
                safe_print('[%s] > Got %s' % (self.id, user_id))

                # get files from other server
                other_nodes = self.coordinator.get_nodes_with_user_logs(self, user_id)
                for node in other_nodes:
                    safe_print('[%s] > Downloading bucket %s from %s' % (self.id, user_id, node.id))
                    self.get_bucket_from(node, user_id)

                # Merges all them in the output file, comparing line by line
                files_to_be_merged = [self.bucket_file_path(user_id)]
                for node in other_nodes:
                    files_to_be_merged.append(self.bucket_file_path(user_id, original_node=node))
                util.merge_files(files_to_be_merged, self.output_file_path(user_id), self.compare_lines)

    ###########################################
    # Inter-thread API
    ###########################################
    def start_map_task(self, coordinator):
        """
        Called by the coordinator, this method wakes node up.
        """
        self.coordinator = coordinator
        self.map_start_event.set()

    def start_reduce_task(self):
        """
        When called by the coordinator, this method wakes the node up again,
        to execute its reduce task.
        """
        self.reduce_start_event.set()

    def has_user_logs(self, user_id):
        return user_id in self.users_found

    ###########################################
    # Helper methods
    ###########################################
    def compare_lines(self, line1, line2):
        datetime1 = log_parser.get_date_time(line1)
        datetime2 = log_parser.get_date_time(line2)

        if datetime1 > datetime2:
            return -1
        elif datetime1 == datetime2:
            return 0
        else:
            return 1

    def input_file_path(self):
        """
        <self.id>/logs/input.log
        """
        return os.path.join(self.id, 'logs', 'input.log')

    def bucket_path(self):
        return os.path.join(self.id, 'buckets')

    def bucket_file_path(self, user_id, original_node=None):
        """
        <self.id>/buckets/<original_node.id>-<user_id>.log

        use original_node in case of a download bucket file, e.g. server_A/buckets/server_B-user-X.log
        """
        bucket_dir = self.bucket_path()
        if not os.path.exists(bucket_dir):
            os.makedirs(bucket_dir)
        if not original_node:
            original_node = self
        return os.path.join(bucket_dir, '%s-%s.log' % (original_node.id, user_id))

    def output_file_path(self, user_id):
        """
        <self.id>/processed_logs/<user_id>.log
        """
        output_dir = os.path.join(self.id, 'processed_logs')
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        return os.path.join(output_dir, '%s.log' % user_id)

    def get_bucket_from(self, node, user_id):
        # simply moves the file from one node directory to the other
        shutil.move(node.bucket_file_path(user_id), self.bucket_path())