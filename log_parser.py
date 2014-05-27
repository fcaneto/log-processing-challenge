import unittest
import datetime
import re
import threading

re_user_id = re.compile('userid=(?P<user_id>[a-z0-9-]*)')
re_date_time = re.compile('\[(?P<date_time>.*)\]')

lock = threading.RLock()


def get_user_id(log_entry):
    return re_user_id.search(log_entry).group('user_id')


def get_date_time(log_entry):
    '''
    Simplification: ignores timezone info
    '''
    datetime_txt = re_date_time.search(log_entry).group('date_time')[:-6]
    with lock:
        # this locks avoids a strptime multithread bug in Mac OSX
        date_time = datetime.datetime.strptime(datetime_txt, "%d/%b/%Y:%H:%M:%S")
    return date_time


class ParserTestCase(unittest.TestCase):
    def test_get_user_id(self):
        log_entry = '177.126.180.83 - - [15/Aug/2013:13:54:38 -0300] "GET /meme.jpg HTTP/1.1" 200 2148 "-" "userid=5352b590-05ac-11e3-9923-c3e7d8408f3a"'
        user_id = '5352b590-05ac-11e3-9923-c3e7d8408f3a'
        self.assertEqual(user_id, get_user_id(log_entry))

    def test_get_date_time(self):
        log_entry = '177.126.180.83 - - [15/Aug/2013:13:54:38 -0300] "GET /meme.jpg HTTP/1.1" 200 2148 "-" "userid=5352b590-05ac-11e3-9923-c3e7d8408f3a"'
        log_datetime = get_date_time(log_entry)

        self.assertEqual(2013, log_datetime.year)
        self.assertEqual(8, log_datetime.month)
        self.assertEqual(15, log_datetime.day)
        self.assertEqual(13, log_datetime.hour)
        self.assertEqual(54, log_datetime.minute)
        self.assertEqual(38, log_datetime.second)


if __name__ == '__main__':
    unittest.main()