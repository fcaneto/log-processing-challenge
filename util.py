import unittest
import os


def merge_files(file_paths_to_be_merged, output_file_path, compare_function):
    """
    Utility functions to merge files line by line, in ascending order

    compare_function must accept two strings as parameters and return:
        -1: if the first string is bigger
        0: if strings are equal
        1: if second string is bigger
    """
    # TODO: Optimize in case there is only one file as input

    if not file_paths_to_be_merged or not output_file_path or not compare_function:
        return

    files = []
    for path in file_paths_to_be_merged:
        files.append(open(path))

    with open(output_file_path, 'w') as output_file:

        current_lines_in_each_file = []
        for bucket_file in files:
            current_lines_in_each_file.append(bucket_file.readline().strip())

        while not all_files_were_read(current_lines_in_each_file):
            # Compares current line from each file...
            min_line_index = None
            min_line = None
            for i, line in enumerate(current_lines_in_each_file):
                if line:
                    if min_line_index is None:
                        min_line_index = i
                        min_line = line
                    elif compare_function(min_line, line) < 0:
                        min_line_index = i
                        min_line = line

            # ... write the selected line to output ...
            output_file.write('%s\n' % min_line)

            # ... and advance file of the selected line
            current_lines_in_each_file[min_line_index] = files[min_line_index].readline()


def all_files_were_read(current_lines_in_each_file):
    """
    Returns True if all lines in current_lines list are empty
    """
    return not reduce(lambda line1, line2: line1 or line2, current_lines_in_each_file)

########################################
# Test Cases
########################################
class AllFilesReadTestCase(unittest.TestCase):
    def test_True(self):
        sample = [None]
        self.assertTrue(all_files_were_read(sample))
        sample = ['']
        self.assertTrue(all_files_were_read(sample))
        sample = [None, '']
        self.assertTrue(all_files_were_read(sample))

    def test_False(self):
        sample = ['line1']
        self.assertFalse(all_files_were_read(sample))

        sample = ['line1', None]
        self.assertFalse(all_files_were_read(sample))

        sample = ['line1', 'line2']
        self.assertFalse(all_files_were_read(sample))


class MergeFilesTestCase(unittest.TestCase):
    def setUp(self):
        self.odds = 'fixture1'
        self.even = 'fixture2'
        self.random = 'fixture3'
        self.empty = 'fixture4'
        self.output = 'output'

        with open(self.odds, 'w') as fixture:
            fixture.write('1\n')
            fixture.write('5\n')
            fixture.write('9\n')
        with open(self.even, 'w') as fixture:
            fixture.write('2\n')
            fixture.write('6\n')
            fixture.write('10\n')
        with open(self.random, 'w') as fixture:
            fixture.write('1\n')
            fixture.write('4\n')
            fixture.write('6\n')
            fixture.write('7\n')
            fixture.write('10\n')
            fixture.write('11\n')
        with open(self.empty, 'w') as fixture:
            fixture.write('')

    def compare_function(self, a, b):
        a = int(a.strip())
        b = int(b.strip())
        if a > b:
            return -1
        elif a == b:
            return 0
        else:
            return 1

    def test_one_file(self):
        merge_files([self.odds], self.output, self.compare_function)
        with open(self.output) as output:
            lines = [line.strip() for line in output.readlines()]

        self.assertEqual(lines, ['1', '5', '9'])

    def test_one_empty_file(self):
        merge_files([self.odds, self.empty], self.output, self.compare_function)
        with open(self.output) as output:
            lines = [line.strip() for line in output.readlines()]

        self.assertEqual(lines, ['1', '5', '9'])

    def test_all_files(self):
        merge_files([self.odds, self.even, self.random], self.output, self.compare_function)
        with open(self.output) as output:
            lines = [line.strip() for line in output.readlines()]

        self.assertEqual(lines, ['1', '1', '2', '4', '5', '6', '6', '7', '9', '10', '10', '11'])

    def tearDown(self):
        os.remove(self.odds)
        os.remove(self.even)
        os.remove(self.random)
        os.remove(self.empty)
        os.remove(self.output)


if __name__ == "__main__":
    unittest.main()