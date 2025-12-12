import os
import json
import subprocess
from pathlib import Path
import threading
import queue
import time

input_dir = '/Volumes/Athena/river-lib/small_lib'

worker_count = 8
converted_extensions = ['avif', 'jxl', 'webp']
valid_extensions = ['png', 'jpg', 'jpeg', 'gif']

print_log_lock = threading.Lock()

def safe_print(*a, **b):
    with print_log_lock:
        print(*a, **b)

def purge(image_dir):
    args = ['rm', '-r', image_dir]
    stderr = None

    result = subprocess.run(args, stdout=subprocess.DEVNULL, stderr=stderr)
    if result.returncode != 0:
        return None

def process_one(dir_path, index, total_count, name):
    files = [f.path for f in os.scandir(dir_path) if not f.is_dir()]
    metadata_file = [a for a in files if os.path.basename(a) == 'metadata.json'][0]
    with open(metadata_file, 'r') as file:
        metadata = json.load(file)

    extension = metadata['ext']
    image_name = metadata['name'] + '.' + extension
    safe_print(f'[{name}] processing {image_name}')

    is_bad = False
    if extension in converted_extensions:
        safe_print(f'[{name}] {extension} is already converted, purging')
        is_bad = True

    if extension not in valid_extensions:
        safe_print(f'[{name}] {extension} is not a valid extension, purging')
        is_bad = True

    if is_bad:
        result = purge(dir_path)

def work(name, queue, total_count):
    while True:
        index, image_dir = queue.get()
        did_convert = process_one(image_dir, index, total_count, name)
        queue.task_done()

def start_work(image_dirs):
    q = queue.Queue()
    total_count = len(image_dirs)

    workers = []
    for i in range(worker_count):
        workerThread = threading.Thread(target=work, args=[f'W{i:02d}', q, total_count], daemon=True)
        workers.append(workerThread)
        workerThread.start()

    for index, image_dir in enumerate(image_dirs):
        q.put([index, image_dir])

    q.join()
    safe_print('all work completed')

def main():
    image_dirs = [f.path for f in os.scandir(input_dir) if f.is_dir()]
    start_work(image_dirs)

main()
