import os
import json
import subprocess
from pathlib import Path
import threading
import queue
import time

source_dir = '/Volumes/Athena/river-lib/small_lib_avif_73'

worker_count = 8
encoder_thread_count = None

# optimal for jxl: w8 e4

default_img_format = 'avif'
base_jxl_distance = 2
base_avif_quality = 73

converted_extensions = ['avif', 'jxl', 'webp']
valid_extensions = ['png', 'jpg', 'jpeg', 'gif']

converted_lock = threading.Lock()
print_log_lock = threading.Lock()

log_dir = '/Volumes/Athena/river-lib/'
conversion_log = ""

def get_log_name():
    q = base_jxl_distance if default_img_format == 'jxl' else base_avif_quality
    e = f'_e{encoder_thread_count}' if encoder_thread_count != None else ''
    return f'log_{default_img_format}_{q}_w{worker_count}{e}.log'

def get_jxl_base_args(iteration):
    distance = base_jxl_distance + iteration
    args = ['cjxl', '--lossless_jpeg=0', '-d', str(distance)]
    if encoder_thread_count != None:
        args += [f'--num_threads={encoder_thread_count}']

    return args

def get_avif_base_args(iteration):
    quality = base_avif_quality - (iteration * 10)
    args = ['avifenc', '-q', str(quality)]
    if encoder_thread_count != None:
        args += ['-j', str(encoder_thread_count)]

    return args

def get_size(dir_path):
    # -ks for size in kilobytes
    # -ms for size in megabytes
    result = subprocess.run(['du', '-ks', dir_path], capture_output=True, text=True)
    return int(result.stdout.split('\t')[0])

def convert(path, name):
    img_format = default_img_format
    old_size = os.path.getsize(path)

    new_size = None
    iteration = 0
    max_iterations = 1

    while True:
        if iteration == max_iterations:
            safe_print(f'[{name}] mission failed, we\'ll get them next time (i tried {iteration} time{'' if iteration == 1 else 's'})')
            return None

        old_path = Path(path)
        new_path = old_path.with_suffix(f'.{img_format}').resolve()
        old_path = old_path.resolve()

        args = None
        stderr = None
        if img_format == 'avif':
            args = get_avif_base_args(iteration) + [old_path, new_path]

        elif img_format == 'jxl':
            args = get_jxl_base_args(iteration) + [old_path, new_path]
            stderr = subprocess.DEVNULL

        if iteration != 0:
            safe_print(f'[{name}] retrying with args [{' '.join(args[:-2])}]')

        encode_result = subprocess.run(args, stdout=subprocess.DEVNULL, stderr=stderr)
        if encode_result.returncode != 0:
            # handle the error
            return None

        new_size = os.path.getsize(new_path)
        if new_size < old_size:
            break

        safe_print(f'[{name}] new size ({human_size(new_size, False)}) is bigger than old size ({human_size(old_size, False)})')
        iteration += 1

    os.remove(path)
    return [new_path, img_format, old_size, new_size]

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

    result = convert(path, name)
    if result == None:
        safe_print(f'[{name}] error during conversion, skipping')

        return False

    new_path, img_format, old_size, new_size = result

    metadata['ext'] = img_format
    metadata['size'] = new_size
    with open(metadata_file, 'w') as file:
        json.dump(metadata, file)

    reduction = (1 - (new_size / old_size)) * -100
    index += 1
    progress = (index / total_count) * 100
    readable_old_size = human_size(old_size, False)
    readable_new_size = human_size(new_size, False)

    to_print = f"[{name}] done.\t" \
    f"old: {readable_old_size},\t" \
    f"new: {readable_new_size},\t" \
    f"r: {reduction:.2f}%,\t" \
    f"{index}/{total_count} {progress:.2f}%"
    safe_print(to_print)

    return True

def work(name, queue, total_count):
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

    workers = []
    for i in range(worker_count):
        workerThread = threading.Thread(target=work, args=[f'W{i:02d}', q, total_count], daemon=True)
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

    log_path = log_dir + get_log_name()
    with open(log_path, 'w') as file:
        file.write(conversion_log)

main()
