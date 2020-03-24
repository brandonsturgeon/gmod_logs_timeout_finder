"""
Searches all LGSM garry's mod logs in a given directory and
writes a json file containing the "enter" and "short-playtime-timeout" events by day

Author: Brandon Sturgeon
"""

from itertools import islice

import glob
import json
import re
import time

from loguru import logger
import dateparser

class TimeoutFinder():
    """
    Finds and parses all logs in a given log directory, then
    outputs all data by-day in a json file to a given output directory
    """

    def __init__(self, logs_dir, tmp_dir, output_dir):
        self.logs_dir = logs_dir
        self.tmp_dir = tmp_dir
        self.output_dir = output_dir

        # In minutes to consider a timeout with short playtime
        self.timeout_threshold = 7

        self.timeouts = {}

        self.patterns = {
            "enters": re.compile(r'^(.+\" entered the game$)', re.M),
            "timeouts": re.compile(r'^(.+\" disconnected \(reason \".+\ timed out\"\)$)', re.M),
            "timestamp_from_line": re.compile(r'(\d\d\/\d\d\/\d\d\d\d - \d\d:\d\d:\d\d):'),
            "steam_id_from_line": re.compile(r': \".*<\d+><(STEAM_\d:\d:\d+)><>\".*$'),
            "steam_id_for_enter": re.compile(r': \".*<\d+><(STEAM_\d:\d:\d+)><>\" entered the game$')
        }

    @staticmethod
    def get_datetime_for_timestamp(timestamp):
        """
        Parse timestamp into datetime object
        """

        return dateparser.parse(timestamp)


    def get_steam_id_from_line(self, line):
        """
        Using regular expressions, returns player steam id from line
        """

        matches = re.findall(self.patterns["steam_id_from_line"], line)

        if len(matches) == 0:
            logger.error("0 Player Steam ID matches for '{}'".format(line))
            return None

        return matches[0]

    def get_timestamp_from_line(self, line):
        """
        Given a line of expected format, return the date time
        """

        matches = re.findall(self.patterns["timestamp_from_line"], line)

        if len(matches) == 0:
            logger.error("0 Timestamp matches for '{}'".format(line))
            return None

        timestamp = matches[0]

        return timestamp

    def find_enter_for_timeout(self, player_steam_id, lines):
        """
        Given a steam id and a set of lines,
        look back through the lines to find an associated enter event
        """

        # We reverse it so we can get the most recent entered event from the timeout
        for lookback_line in reversed(lines):
            matches = re.findall(self.patterns["steam_id_for_enter"], lookback_line)

            if len(matches) > 0:
                if matches[0] == player_steam_id:
                    return lookback_line

    @staticmethod
    def get_date_from_filepath(filepath):
        """
        Given a full filepath, return the date in YYYY-MM-DD
        """

        # 'gmodserver-console-2019-10-23-17:20:20.log'
        filename = filepath.split("/")[-1]

        # ['gmodserver', 'console', '2019', '10', '23', '17:20:20.log']
        day_key = filename.split("-")

        # ['2019', '10', '23']
        day_key = day_key[2:5]

        # '2019-10-23'
        day_key = "-".join(day_key)

        return day_key

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
    
    def find_events_in_lines(self, lines):
        """
        Given a list of lines, return all timeouts and enters
        """

        # Only get the lines that start with L,
        # those are the only ones we want, and
        # it makes for a smaller regex search
        lines = [line for line in lines if line[0] == "L"]

        line_block = "".join(lines)

        timeouts = re.findall(self.patterns["timeouts"], line_block)
        enters = re.findall(self.patterns["enters"], line_block)

        return enters, timeouts

    def find_events_in_log(self, file_object, chunk_size):
        """
        Searches through the given file_objects in chunks of chunk_size
        Returns all found events
        """

        events = []
        all_enters = []

        while True:
            pieces = list(islice(file_object, chunk_size))

            if len(pieces) == 0:
                logger.info("Hit end of file, beginning processing")
                break

            enters, timeouts = self.find_events_in_lines(pieces)

            all_enters += enters
            events += timeouts + enters

        return events, all_enters

    def handle_timeouts_in_events(self, events, file_date):
        """
        Given a list of events, loop through them, then identify and process timeouts
        """

        for i, line in enumerate(events):
            if ' timed out")' not in line:
                continue

            steam_id = self.get_steam_id_from_line(line)
            timeout_timestamp = self.get_timestamp_from_line(line)

            # Only search through logs up to this point in the file
            enter_line = self.find_enter_for_timeout(steam_id, events[:i])

            if not enter_line:
                logger.warning(
                    "Couldn't find an associated connection for timeout: '{}'".format(line)
                )

                continue

            enter_timestamp = self.get_timestamp_from_line(enter_line)

            event_diff = self.get_timestamp_diff(enter_timestamp, timeout_timestamp)

            if event_diff < self.timeout_threshold:
                self.timeouts[file_date]["timeouts"].append(line)

    def find_timeouts_for_fileset(self, file_date, fileset):
        """
        1. Find these:
        13:40:15: "ShyAdvocate<23><STEAM_0:1:149613238><>" disconnected (reason "ShyAdvocate timed out")

        2. Then find these:
        13:38:18: "ShyAdvocate<23><STEAM_0:1:149613238><>" entered the game

        3. Check if the diff between the two events is ~7m
        """

        self.timeouts[file_date] = {
            "timeouts": [],
            "enters": []
        }

        events = []

        # In number of lines
        chunk_size = 10000

        logger.info("Opening {} files for date {}".format(len(fileset), file_date))

        for filepath in sorted(fileset):
            logger.info("Opening '{}' for read in chunks of {} lines".format(filepath, chunk_size))

            with open(filepath, "r", encoding="ISO-8859-1") as log:
                log_events, log_enters = self.find_events_in_log(log, chunk_size)

                events += log_events
                self.timeouts[file_date]["enters"] += log_enters

        events = sorted(events)

        self.handle_timeouts_in_events(events, file_date)

    def chunk_by_day(self, filepaths):
        """
        Chunk given filepaths by date
        """

        chunks_by_day = {}

        for filepath in filepaths:
            day_key = self.get_date_from_filepath(filepath)

            # The current log won't have a date in the file name
            if day_key:
                chunks_by_day[day_key] = chunks_by_day.get(day_key, []) + [filepath]

        return chunks_by_day

    def get_existing_logs(self):
        """
        Searches through self.logs dir and returns all .log files
        """

        glob_pattern = "{}/*.log".format(self.logs_dir)
        filenames = glob.glob(glob_pattern)

        return filenames

    def logs_by_day(self):
        """
        Returns filepaths chunked by day
        """

        glob_pattern = "{}/*.log".format(self.logs_dir)
        existing_logs = glob.glob(glob_pattern)

        existing_log_chunks = self.chunk_by_day(existing_logs)

        return existing_log_chunks

    def find_timeouts(self):
        """
        Loops through each available log, processes them,
        and then runs them through the timeouts finder function,
        then prints and saves output
        """

        logs_by_day = self.logs_by_day()

        start = time.time()
        for file_date in sorted(logs_by_day.keys()):
            self.find_timeouts_for_fileset(file_date, logs_by_day[file_date])
        end = time.time()
        duration = round(end - start, 4)
        logger.info("Finished processing all files. Took {} seconds".format(duration))

        sorted_filenames = sorted(self.timeouts.keys())

        output_file = "{}/{}-short-playtime-timeouts.json".format(
            self.output_dir,
            round(time.time())
        )

        with open(output_file, "w") as output:
            logger.info("Writing data to {}..".format(output_file))
            data = json.dumps(self.timeouts, sort_keys=True, indent=4)
            output.write(data)

        for file_date in sorted_filenames:
            timeouts = self.timeouts[file_date]["timeouts"]
            total_timeouts = len(timeouts)

            enters = self.timeouts[file_date]["enters"]
            total_enters = len(enters)

            if total_enters == 0:
                logger.warning("Total enters was 0 for {}".format(file_date))
                logger.warning(self.timeouts[file_date])

                continue

            percent_timeouts = (float(total_timeouts) / float(total_enters)) * 100
            percent_timeouts = round(percent_timeouts, 2)

            logger.info("{}: {}/{} ({}%)".format(file_date, total_timeouts, total_enters, percent_timeouts))

if __name__ == "__main__":
    # TODO: Get arguments to use

    TimeoutFinder(
        "/home/steam/Code/servers/garrysmod/log/console",
        "/media/storage/steam/log/tmp",
        "/home/steam/Code/small_scripts/gmod_logs_timeout_finder2/output"
    ).find_timeouts()
