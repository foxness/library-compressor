import os
import json
import subprocess
from pathlib import Path
import threading
import queue
import time

source_dir = '/Volumes/Athena/river-lib/small_lib copy'
converted_extensions = ['avif', 'jxl', 'webp']
valid_extensions = ['png', 'jpg', 'jpeg', 'gif']

converted_lock = threading.Lock()
print_log_lock = threading.Lock()

log_path = '/Volumes/Athena/river-lib/conversion.log'
conversion_log = ""

def get_size(dir_path):
    # -ks for size in kilobytes
    # -ms for size in megabytes
    result = subprocess.run(['du', '-ks', dir_path], capture_output=True, text=True)
    return int(result.stdout.split('\t')[0])

def convert(path):
    path = Path(path)
    new_path = path.with_suffix('.avif').resolve()
    path = path.resolve()
    encode_result = subprocess.run(['avifenc', path, new_path], stdout = subprocess.DEVNULL)
    if encode_result.returncode != 0:
        # handle the error
        return None

    os.remove(path)

    return new_path

def process_one(dir_path, index, total_count, name):
    files = [f.path for f in os.scandir(dir_path) if not f.is_dir()]
    metadata_file = [a for a in files if os.path.basename(a) == 'metadata.json'][0]
    with open(metadata_file, 'r') as file:
        metadata = json.load(file)

    extension = metadata['ext']
    image_name = metadata['name'] + '.' + extension
    safe_print(f'[{name}] processing {image_name}')

    if extension in converted_extensions:
        safe_print(f'[{name}] {extension} is already converted, skipping')

        return False

    if extension not in valid_extensions:
        safe_print(f'[{name}] {extension} is not a valid extension, skipping')

        return False

    path = [a for a in files if os.path.basename(a) == image_name][0]
    size = os.path.getsize(path)

    new_path = convert(path)
    if new_path == None:
        safe_print(f'[{name}] error during conversion, skipping')

        return False

    new_size = os.path.getsize(new_path)

    metadata['ext'] = 'avif'
    metadata['size'] = new_size
    with open(metadata_file, 'w') as file:
        json.dump(metadata, file)

    reduction = (1 - (new_size / size)) * -100
    index += 1
    progress = (index / total_count) * 100
    readable_size = human_size(size, False)
    readable_new_size = human_size(new_size, False)

    to_print = f"[{name}] converted.\t" \
    f"old size: {readable_size},\t" \
    f"new size: {readable_new_size},\t" \
    f"reduction: {reduction:.2f}%,\t" \
    f"progress: {index}/{total_count} {progress:.2f}%"
    safe_print(to_print)

    return True

def worker(name, queue, total_count):
    while True:
        index, image_dir = queue.get()

        did_convert = process_one(image_dir, index, total_count, name)
        if did_convert:
            with converted_lock:
                global converted_count
                converted_count += 1

        queue.task_done()

def safe_print(*a, **b):
    with print_log_lock:
        global conversion_log
        conversion_log += f'{a[0]}\n'
        print(*a, **b)

def start_work(image_dirs):
    q = queue.Queue()
    total_count = len(image_dirs)

    worker_count = 5
    workers = []
    for i in range(worker_count):
        workerThread = threading.Thread(target=worker, args=[f'Worker {i + 1}', q, total_count], daemon=True)
        workers.append(workerThread)
        workerThread.start()

    for index, image_dir in enumerate(image_dirs):
        q.put([index, image_dir])

    q.join()
    safe_print('all work completed')

def human_size(size, source_is_kilobytes):
    return f'{size / 1024:.2f}' + ('mb' if source_is_kilobytes else 'kb')

def main():
    start = time.time()
    safe_print(f'starting conversion of {source_dir}')

    size = get_size(source_dir)
    image_dirs = [f.path for f in os.scandir(source_dir) if f.is_dir()]

    total_count = len(image_dirs)
    global converted_count
    converted_count = 0

    start_work(image_dirs)

    new_size = get_size(source_dir)
    reduction = (1 - (new_size / size)) * -100

    end = time.time()
    elapsed = end - start

    safe_print(f'converted {converted_count} files out of {total_count}')
    safe_print(f'old size: {human_size(size, True)}, new size: {human_size(new_size, True)}, reduction: {reduction:.2f}%')
    safe_print(f'finished in {elapsed:.2f}s, speed: {(total_count / elapsed):.2f} files/s')

    with open(log_path, 'w') as file:
        file.write(conversion_log)

main()
