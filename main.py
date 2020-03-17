import glob
import dateparser
from datetime import datetime
from loguru import logger

class TimeoutFinder():
    def __init__(self, logs_dir):
        self.logs_dir = logs_dir

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
        return dateparser.parse(timestamp)

    def get_timestamp_diff(self, enter, timeout):
        enter = self.get_datetime_for_timestamp(enter)
        timeout = self.get_datetime_for_timestamp(timeout)

        diff = timeout - enter

        diff_seconds = diff.total_seconds()
        diff_minutes = diff_seconds / 60.0

        return diff_minutes

    @staticmethod
    def get_timestamp_from_line(line):
        # 'L 03/16/2020 - 17:16:16: "beamerboy1221<180><STEAM_0:0:181817410><>" disconnected (reason "beamerboy1221 timed out")\r\n'

        # ['L 03/16/2020 - 17:16:16: ', 'beamerboy1221<180><STEAM_0:0:181817410><>', ' disconnected (reason ', 'beamerboy1221 timed out', ')\r\n']
        timestamp = line.split('"')

        # 'L 03/16/2020 - 17:16:16: '
        timestamp = timestamp[0]

        # '03/16/2020 - 17:16:16'
        timestamp = timestamp[2:-2]

        return timestamp

    def find_timeouts_for_file(self, filename):
        """
        1. Find these:
        13:40:15: "ShyAdvocate<23><STEAM_0:1:149613238><>" disconnected (reason "ShyAdvocate timed out")

        2. Then find these:
        13:38:18: "ShyAdvocate<23><STEAM_0:1:149613238><>" entered the game

        3. Check if the diff between the two events is ~7m
        """

        filename = "gmodserver-console.log"

        timeouts = []

        loglines = []

        with open(filename, "rb") as log:
            loglines = log.readlines()

        # Decode all the lines so we can do stuff with them easily

        loglines = [str(l.decode("utf-8")) for l in loglines]

        for i, line in enumerate(loglines):
            if ' timed out")' in line:
                logger.debug("Found timeout")

                player_name = self.get_player_name_from_line(line)
                timeout_timestamp = self.get_timestamp_from_line(line)

                # Loop through lines up to this point, in reverse
                # We reverse it so we can get the most recent entered event from the timeout
                for lookback_line in reversed(loglines[:i]):
                    if ' entered the game\r\n' in lookback_line:
                        if player_name in lookback_line:
                            logger.debug(lookback_line)
                            enter_timestamp = self.get_timestamp_from_line(lookback_line)

                            event_diff = self.get_timestamp_diff(enter_timestamp, timeout_timestamp)

                            if event_diff < 7:
                                logger.info("Found a short-playtime timeout event ({}): {}".format(event_diff, line))
                                timeouts.append(line)

                            # Always break after the first one found because we only want the most recent join
                            break
        
        def get_timeouts_for_file(self):
            glob_pattern = "{}/*.log".format(self.logs_dir)

            filenames = glob.glob(glob_pattern)

            for filename in filenames:
                print(filename)

if __name__ == "__main__":
    # Get log dir as param
    TimeoutFinder("/home/steam/Code/servers/garrysmod/log/console").find_timeouts()
