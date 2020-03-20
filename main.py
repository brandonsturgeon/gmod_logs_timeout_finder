from datetime import datetime
from loguru import logger

import dateparser
import glob
import io
import json
import re
import subprocess
import threading
import time

class TimeoutFinder():
    def __init__(self, logs_dir, tmp_dir):
        self.logs_dir = logs_dir
        self.tmp_dir = tmp_dir

        # In minutes to consider a timeout with short playtime
        self.timeout_threshold = 7

        self.timeouts = {}

    @staticmethod
    def get_player_name_from_line(line):
        """
        Using regular expressions, returns player name from line
        """
        pattern = r': \"(.*)<\d+><STEAM_\d:\d:\d+><>\".*$'

        matches = re.findall(pattern, line)

        return matches[0]

    @staticmethod
    def get_datetime_for_timestamp(timestamp):
        """
        Parse timestamp into datetime object
        """

        return dateparser.parse(timestamp)

    @staticmethod
    def split_work_into_chunks_of_size(work, chunks):
        for i in range(0, len(work), chunks):
            yield work[i:i + chunks]

    @staticmethod
    def get_timestamp_from_line(line):
        """
        Given a line of expected format, return the date time
        """

        pattern = r'(\d\d\/\d\d\/\d\d\d\d - \d\d:\d\d:\d\d):'

        matches = re.findall(pattern, line)

        timestamp = matches[0]
        logger.debug("Found '{}' from '{}'".format(timestamp, line))

        return timestamp

    @staticmethod
    def find_enter_for_timeout(player_name, lines):
        """
        Given a player and a set of lines, look back through the lines to find an associated enter event
        """

        # We reverse it so we can get the most recent entered event from the timeout
        for lookback_line in reversed(lines):
            pattern = r': \"(.*)<\d+><STEAM_\d:\d:\d+><>\" entered the game$'
            matches = re.findall(pattern, lookback_line)

            if len(matches) > 0:
                if matches[0] == player_name:
                    logger.debug("Found match in enter line:")
                    logger.debug(lookback_line)

                    return lookback_line

    def get_timestamp_diff(self, given_enter, given_timeout):
        """
        Return difference between two given timestamps, in minutes
        """

        enter = self.get_datetime_for_timestamp(given_enter)
        timeout = self.get_datetime_for_timestamp(given_timeout)

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
        self.timeouts[filename] = {
            "timeouts": [],
            "enters": []
        }

        loglines = []

        logger.info("Opening '{}' for read".format(filename))
        with io.open(filename, "r", encoding="ISO-8859-1") as log:
            loglines = log.read()

        timeouts_pattern = re.compile('^(.+\" disconnected \(reason \".+\ timed out\"\)$)', re.M)
        timeouts = re.findall(timeouts_pattern, loglines)

        enters_pattern = re.compile('^(.+\" entered the game$)', re.M)
        enters = re.findall(enters_pattern, loglines)
        self.timeouts[filename]["enters"] = enters

        events = timeouts + enters
        events = sorted(events)

        for i, line in enumerate(events):
            if ' timed out")' in line:
                player_name = self.get_player_name_from_line(line)
                timeout_timestamp = self.get_timestamp_from_line(line)

                # Only search through logs up to this point in the file
                enter_line = self.find_enter_for_timeout(player_name, events[:i])

                if not enter_line:
                    logger.warning("Couldn't find an associated connection for timeout: '{}'".format(line))
                    continue

                enter_timestamp = self.get_timestamp_from_line(enter_line)

                event_diff = self.get_timestamp_diff(enter_timestamp, timeout_timestamp)

                if event_diff < self.timeout_threshold:
                    logger.info("Found a short-playtime timeout event ({}): {}".format(event_diff, line))
                    self.timeouts[filename]["timeouts"].append(line)

    def find_timeouts_for_chunk(self, chunk, chunk_number):
        for filename in chunk:
            self.find_timeouts_for_file(filename)

    def chunk_by_day(self, filepaths):
        chunks_by_day = {}

        for filepath in filepaths:
            # 'gmodserver-console-2019-10-23-17:20:20.log'
            filename = filepath.split("/")[-1]

            # ['gmodserver', 'console', '2019', '10', '23', '17:20:20.log']
            day_key = filename.split("-")

            # ['2019', '10', '23']
            day_key = day_key[2:5]

            # '2019-10-23'
            day_key = "-".join(day_key)

            chunks_by_day[day_key] = chunks_by_day.get(day_key, []) + [filepath]

        return chunks_by_day

    def combine_day_chunks(self, day_chunks):
        for day, files in day_chunks.items():
            sorted_files = sorted(files)
            sorted_files = " ".join(sorted_files)

            output_file = f"{self.tmp_dir}/{day}.log"

            command = "cat {} >> {}".format(sorted_files, output_file)

            # We have to do shell here because there's no way to redirect output with subprocess normally
            subprocess.check_output(command, shell=True)

    def get_existing_logs(self):
        glob_pattern = "{}/*.log".format(self.logs_dir)
        filenames = glob.glob(glob_pattern)

        return filenames

    def create_combined_logs(self):
        existing_logs = self.get_existing_logs()
        existing_log_chunks = self.chunk_by_day(existing_logs)
        self.combine_day_chunks(existing_log_chunks)

    def get_combined_logs(self):
        #self.create_combined_logs()

        glob_pattern = "{}/*.log".format(self.tmp_dir)
        filenames = glob.glob(glob_pattern)

        return sorted(filenames)

    def find_timeouts(self):
        strings = []

        filenames = self.get_combined_logs()

        number_of_chunks = 2.0

        chunk_size = round(len(filenames) / number_of_chunks)

        for chunk_number, chunk in enumerate(self.split_work_into_chunks_of_size(filenames, chunk_size)):
            logger.info("Starting thread {}".format(chunk_number))
            string = threading.Thread(target=self.find_timeouts_for_chunk, args=(chunk,chunk_number,))
            strings.append(string)

        for string in strings:
            string.start()

        for string in strings:
            string.join()

        #for filename in filenames:
        #    self.find_timeouts_for_file(filename)

        sorted_filenames = sorted(self.timeouts.keys())

        for filename in sorted_filenames:
            timeouts = self.timeouts[filename]["timeouts"]
            total_timeouts = len(timeouts)

            enters = self.timeouts[filename]["enters"]
            total_enters = len(enters)

            if total_enters == 0:
                logger.warning("Total enters was 0 for {}".format(filename))
                logger.warning(self.timeouts[filename])

                continue

            percent_timeouts = (float(total_timeouts) / float(total_enters)) * 100
            percent_timeouts = round(percent_timeouts, 2)

            logger.info("{}: {}/{} ({}%)".format(filename, total_timeouts, total_enters, percent_timeouts))

        for filename in filenames:
            logger.debug("Removing {}".format(filename))
            #os.remove(filename)

if __name__ == "__main__":
    # Get log dir as param
    TimeoutFinder("/home/steam/Code/servers/garrysmod/log/console", "/media/storage/steam/log/tmp").find_timeouts()
