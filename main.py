from datetime import datetime
from loguru import logger

import dateparser
import glob
import io
import threading

class TimeoutFinder():
    def __init__(self, logs_dir):
        self.logs_dir = logs_dir

        # In minutes to consider a timeout with short playtime
        self.timeout_threshold = 7

        self.timeouts = {}

    @staticmethod
    def get_player_name_from_line(line):
        #'L 03/16/2020 - 17:16:16: "beamerboy1221<180><STEAM_0:0:181817410><>" disconnected (reason "beamerboy1221 timed out")\r\n'

        # ['L 03/16/2020 - 17:16:16: ', 'beamerboy1221<180><STEAM_0:0:181817410><>', ' disconnected (reason ', 'beamerboy1221 timed out', ')\r\n']
        player_name = line.split('"')

        # 'beamerboy1221<180><STEAM_0:0:181817410><>'
        player_name = player_name[1]

        # 'beamerboy1221'
        player_name = player_name.split("<")[0]

        return player_name

    @staticmethod
    def get_datetime_for_timestamp(timestamp):
        """
        Parse timestamp into datetime object
        """

        return dateparser.parse(timestamp)

    @staticmethod
    def split_work_into_chunks(work, chunks=10):
        for i in range(0, len(work), chunks):
            yield work[i:i + chunks]

    @staticmethod
    def get_timestamp_from_line(line):
        """
        Given a line of expected format, return the date time
        """

        # 'L 03/16/2020 - 17:16:16: "beamerboy1221<180><STEAM_0:0:181817410><>" disconnected (reason "beamerboy1221 timed out")\r\n'

        # ['L 03/16/2020 - 17:16:16: ', 'beamerboy1221<180><STEAM_0:0:181817410><>', ' disconnected (reason ', 'beamerboy1221 timed out', ')\r\n']
        timestamp = line.split('"')

        # 'L 03/16/2020 - 17:16:16: '
        timestamp = timestamp[0]

        # '03/16/2020 - 17:16:16'
        timestamp = timestamp[2:-2]

        return timestamp

    @staticmethod
    def find_enter_for_timeout(player_name, lines):
        """
        Given a player and a set of lines, look back through the lines to find an associated enter event
        """

        # We reverse it so we can get the most recent entered event from the timeout
        for lookback_line in reversed(lines):
            if ' entered the game\r\n' in lookback_line:
                if player_name in lookback_line:
                    logger.debug(lookback_line)

                    return lookback_line

    def get_timestamp_diff(self, enter, timeout):
        """
        Return difference between two given timestamps, in minutes
        """

        enter = self.get_datetime_for_timestamp(enter)
        timeout = self.get_datetime_for_timestamp(timeout)

        diff = timeout - enter

        diff_seconds = diff.total_seconds()
        diff_minutes = diff_seconds / 60.0

        return diff_minutes

    def find_timeouts_for_file(self, filename):
        """
        1. Find these:
        13:40:15: "ShyAdvocate<23><STEAM_0:1:149613238><>" disconnected (reason "ShyAdvocate timed out")

        2. Then find these:
        13:38:18: "ShyAdvocate<23><STEAM_0:1:149613238><>" entered the game

        3. Check if the diff between the two events is ~7m
        """
        self.timeouts[filename] = []

        loglines = []

        with io.open(filename, "r", encoding="utf-8") as log:
            loglines = log.readlines()

        for i, line in enumerate(loglines):
            if ' timed out")' in line:
                logger.debug("Found timeout")

                player_name = self.get_player_name_from_line(line)
                timeout_timestamp = self.get_timestamp_from_line(line)

                # Only search through logs up to this point in the file
                enter_line = self.find_enter_for_timeout(player_name, loglines[:i])

                if not enter_line:
                    logger.warning("Couldn't find an associated connection for timeout: '{}'".format(line))
                    continue

                enter_timestamp = self.get_timestamp_from_line(enter_line)

                event_diff = self.get_timestamp_diff(enter_timestamp, timeout_timestamp)

                if event_diff < self.timeout_threshold:
                    logger.info("Found a short-playtime timeout event ({}): {}".format(event_diff, line))
                    self.timeouts[filename].append(line)

    def find_timeouts_for_chunk(self, chunk, chunk_number):
        for filename in chunk:
            self.find_timeouts_for_file(filename)

    def find_timeouts(self):
        glob_pattern = "{}/*.log".format(self.logs_dir)

        filenames = glob.glob(glob_pattern)

        strings = []

        for chunk_number, chunk in enumerate(self.split_work_into_chunks(filenames, 15)):
            string = threading.Thread(target=self.find_timeouts_for_chunk, args=(chunk,chunk_number,))
            strings.append(string)

        for string in strings:
            string.start()

        for string in strings:
            string.join()

if __name__ == "__main__":
    # Get log dir as param
    TimeoutFinder("/home/steam/Code/servers/garrysmod/log/console").find_timeouts()
