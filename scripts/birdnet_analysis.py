import logging
import os
import os.path
import re
import shutil
import signal
import sys
import threading
from queue import Queue
from subprocess import CalledProcessError

import inotify.adapters
from inotify.constants import IN_CLOSE_WRITE
from utils.analysis import load_global_model, run_analysis
from utils.classes import ParseFileName
from utils.helpers import ANALYZING_NOW, get_settings, get_wav_files
from utils.reporting import (
    apprise,
    bird_weather,
    extract_detection,
    heartbeat,
    summary,
    update_json_file,
    write_to_db,
    write_to_file,
)

shutdown = False

log = logging.getLogger(__name__)


def sig_handler(sig_num, curr_stack_frame):
    global shutdown
    log.info("Caught shutdown signal %d", sig_num)
    shutdown = True


def main():
    load_global_model()
    conf = get_settings()
    i = inotify.adapters.Inotify()
    i.add_watch(os.path.join(conf["RECS_DIR"], "StreamData"), mask=IN_CLOSE_WRITE)

    backlog = get_wav_files()

    report_queue = Queue()
    thread = threading.Thread(target=handle_reporting_queue, args=(report_queue,))
    thread.start()

    log.info("backlog is %d", len(backlog))
    for file_name in backlog:
        process_file(file_name, report_queue)
        if shutdown:
            break
    log.info("backlog done")

    empty_count = 0
    for event in i.event_gen():
        if shutdown:
            break

        if event is None:
            if empty_count > (conf.getint("RECORDING_LENGTH") * 2 + 30):
                log.error("no more notifications: restarting...")
                break
            empty_count += 1
            continue

        (_, type_names, path, file_name) = event
        if re.search(".wav$", file_name) is None:
            continue
        log.debug("PATH=[%s] FILENAME=[%s] EVENT_TYPES=%s", path, file_name, type_names)

        file_path = os.path.join(path, file_name)
        if file_path in backlog:
            # if we're very lucky, the first event could be for the file in the backlog that finished
            # while running get_wav_files()
            backlog = []
            continue

        process_file(file_path, report_queue)
        empty_count = 0

    # we're all done
    report_queue.put(None)
    thread.join()
    report_queue.join()


def process_file(file_name, report_queue):
    try:
        if os.path.getsize(file_name) == 0:
            os.remove(file_name)
            return
        log.info("Analyzing %s", file_name)
        with open(ANALYZING_NOW, "w") as analyzing:
            analyzing.write(file_name)
        file = ParseFileName(file_name)
        detections = run_analysis(file)
        # we join() to make sure te reporting queue does not get behind
        if not report_queue.empty():
            log.warning("reporting queue not yet empty")
        report_queue.join()
        report_queue.put((file, detections))
    except BaseException as e:
        stderr = e.stderr.decode("utf-8") if isinstance(e, CalledProcessError) else ""
        log.exception(f"Unexpected error: {stderr}", exc_info=e)


def handle_reporting_queue(queue):
    while True:
        msg = queue.get()
        # check for signal that we are done
        if msg is None:
            break

        file, detections = msg
        try:
            update_json_file(file, detections)
            for detection in detections:
                detection.file_name_extr = extract_detection(file, detection)
                log.info(
                    "%s;%s",
                    summary(file, detection),
                    os.path.basename(detection.file_name_extr),
                )
                write_to_file(file, detection)
                write_to_db(file, detection)
            apprise(file, detections)
            bird_weather(file, detections)
            heartbeat()
            # os.remove(file.file_name)
            # move processed recording to Archived (instead of deleting) and enforce size cap
            conf = get_settings()
            archive_dir = os.path.join(conf["RECS_DIR"], "Archived")
            os.makedirs(archive_dir, exist_ok=True)
            dst = os.path.join(archive_dir, os.path.basename(file.file_name))
            try:
                shutil.move(file.file_name, dst)
                log.info("Moved processed file to Archived: %s", dst)
            except Exception:
                log.exception("Failed to move %s to %s", file.file_name, archive_dir)

            # ARCHIVE_MAX_MB (0 = unlimited)
            try:
                archive_max_mb = int(conf.get("ARCHIVE_MAX_MB", 0))
            except Exception:
                archive_max_mb = 0

            if archive_max_mb and archive_max_mb > 0:
                limit = archive_max_mb * 1024 * 1024
                entries = [
                    os.path.join(archive_dir, f)
                    for f in os.listdir(archive_dir)
                    if os.path.isfile(os.path.join(archive_dir, f))
                ]
                entries.sort(key=lambda p: os.path.getmtime(p))
                total = sum(os.path.getsize(p) for p in entries)
                removed = []
                while total > limit and entries:
                    oldest = entries.pop(0)
                    try:
                        sz = os.path.getsize(oldest)
                        os.remove(oldest)
                        removed.append(oldest)
                        total -= sz
                    except Exception:
                        log.exception("Failed to remove archived file %s", oldest)
                if removed:
                    log.info(
                        "Removed %d archived files to enforce ARCHIVE_MAX_MB=%dMB",
                        len(removed),
                        archive_max_mb,
                    )
        except BaseException as e:
            stderr = (
                e.stderr.decode("utf-8") if isinstance(e, CalledProcessError) else ""
            )
            log.exception(f"Unexpected error: {stderr}", exc_info=e)

        queue.task_done()

    # mark the 'None' signal as processed
    queue.task_done()
    log.info("handle_reporting_queue done")


def setup_logging():
    logger = logging.getLogger()
    formatter = logging.Formatter("[%(name)s][%(levelname)s] %(message)s")
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    global log
    log = logging.getLogger("birdnet_analysis")


if __name__ == "__main__":
    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)

    setup_logging()

    main()
