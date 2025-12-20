import os
import json
import subprocess
from pathlib import Path
import threading
import queue
import time

source_dir = '/Volumes/Athena/river-lib/small_lib_jxl_lossless_fight'

default_img_format = 'jxl'
jxl_measure_is_quality = True
jxl_quality = 90
jxl_distance = 2
avif_quality = 80

worker_count = 8
encoder_thread_count = None
# optimal for jxl: w8 e4

converted_extensions = ['avif', 'jxl', 'webp']
valid_extensions = ['png', 'jpg', 'jpeg', 'gif']

converted_lock = threading.Lock()
print_log_lock = threading.Lock()

log_dir = '/Volumes/Athena/river-lib/'
conversion_log = ""

def get_log_name():
    q = (jxl_quality if jxl_measure_is_quality else jxl_distance) if default_img_format == 'jxl' else avif_quality
    e = f'_e{encoder_thread_count}' if encoder_thread_count != None else ''
    return f'log_{default_img_format}_{q}_w{worker_count}{e}.log'

def get_jxl_base_args(source_format, use_lossless_jpg, iteration):
    args = ['cjxl']

    add_quality = True
    match source_format:
        case 'jpg' | 'jpeg':
            args += [f'--lossless_jpeg={1 if use_lossless_jpg else 0}']
            if use_lossless_jpg:
                add_quality = False

    if add_quality:
        if jxl_measure_is_quality:
            quality = jxl_quality - (iteration * 10)
            args += ['-q', str(quality)]
        else:
            distance = jxl_distance + iteration
            args += ['-d', str(distance)]

    if encoder_thread_count != None:
        args += [f'--num_threads={encoder_thread_count}']

    return args

def get_avif_base_args(iteration):
    quality = avif_quality - (iteration * 10)
    args = ['avifenc', '-q', str(quality)]
    if encoder_thread_count != None:
        args += ['-j', str(encoder_thread_count)]

    return args

def get_size(dir_path):
    # -ks for size in kilobytes
    # -ms for size in megabytes
    result = subprocess.run(['du', '-ks', dir_path], capture_output=True, text=True)
    return int(result.stdout.split('\t')[0])

def human_size(size, source_is_kilobytes):
    return f'{size / 1024:.2f}' + ('mb' if source_is_kilobytes else 'kb')

def safe_print(*a, **b):
    with print_log_lock:
        global conversion_log
        conversion_log += f'{a[0]}\n'
        print(*a, **b)

def jxl_fight(jpg_path, name):
    old_path = Path(jpg_path)

    lossy_name = f'{old_path.stem}_lossy.jxl'
    lossless_name = f'{old_path.stem}_lossless.jxl'
    final_name = f'{old_path.stem}.jxl'

    lossy_path = old_path.with_name(lossy_name).resolve()
    lossless_path = old_path.with_name(lossless_name).resolve()
    final_path = old_path.with_name(final_name).resolve()

    lossy_args = get_jxl_base_args('jpg', False, 0)
    lossless_args = get_jxl_base_args('jpg', True, 0)

    lossy_args += [jpg_path, lossy_path]
    lossless_args += [jpg_path, lossless_path]

    lossy_size = None
    lossless_size = None

    lossy_result = subprocess.run(lossy_args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    lossy_fail = lossy_result.returncode != 0
    if lossy_fail:
        if os.path.isfile(lossy_path):
            os.remove(lossy_path)
    else:
        lossy_size = os.path.getsize(lossy_path)

    lossless_result = subprocess.run(lossless_args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    lossless_fail = lossless_result.returncode != 0
    if lossless_fail:
        if os.path.isfile(lossless_path):
            os.remove(lossless_path)
    else:
        lossless_size = os.path.getsize(lossless_path)

    if lossy_fail and lossless_fail:
        safe_print(f'[{name}] this is an epic fail, aborting')
        return None
    elif lossy_fail and not lossless_fail:
        safe_print(f'[{name}] lossless won because lossy errored')
        os.rename(lossless_path, final_path)
        return [final_path, lossless_size]
    elif lossless_fail and not lossy_fail:
        safe_print(f'[{name}] lossy won because lossless errored')
        os.rename(lossy_path, final_path)
        return [final_path, lossy_size]

    winner = 'lossless'
    winner_path = lossless_path
    winner_size = lossless_size
    loser_path = lossy_path
    loser_size = lossy_size

    if lossy_size < lossless_size:
        winner = 'lossy'
        winner_path = lossy_path
        winner_size = lossy_size
        loser_path = lossless_path
        loser_size = lossless_size

    readable_winner_size = human_size(winner_size, False)
    readable_loser_size = human_size(loser_size, False)

    difference = (1 - (winner_size / loser_size)) * 100
    safe_print(f'[{name}] {winner} won because it was {difference:.2f}% smaller [{readable_winner_size} vs {readable_loser_size}]')

    os.remove(loser_path)
    os.rename(winner_path, final_path)
    return [final_path, winner_size]

def convert(path, name):
    img_format = default_img_format
    old_size = os.path.getsize(path)

    old_path = Path(path)
    source_format = old_path.suffix.lower()[1:]
    new_path = old_path.with_suffix(f'.{img_format}').resolve()

    new_size = None
    iteration = 0
    max_iterations = 1

    while True:
        if iteration == max_iterations:
            safe_print(f'[{name}] mission failed, we\'ll get them next time (i tried {iteration} time{'' if iteration == 1 else 's'})')
            os.remove(new_path)
            return 'compression_fail'

        if (source_format == 'jpg' or source_format == 'jpeg') and img_format == 'jxl' and iteration == 0:
            jxl_fight_result = jxl_fight(path, name)
            if jxl_fight_result == None:
                return None

            new_path, new_size = jxl_fight_result
        else:
            args = None
            stderr = None
            if img_format == 'avif':
                args = get_avif_base_args(iteration)

            elif img_format == 'jxl':
                args = get_jxl_base_args(source_format, False, iteration)
                stderr = subprocess.DEVNULL

            args += [path, new_path]

            if iteration != 0:
                safe_print(f'[{name}] retrying with args [{' '.join(args[:-2])}]')

            encode_result = subprocess.run(args, stdout=subprocess.DEVNULL, stderr=stderr)
            if encode_result.returncode != 0:
                if os.path.isfile(new_path):
                    os.remove(new_path)

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

    paths = [a for a in files if os.path.basename(a) == image_name]
    if not paths:
        safe_print(f'[{name}] could not find the image, skipping')
        return False

    path = paths[0]

    result = convert(path, name)
    match result:
        case 'compression_fail':
            safe_print(f'[{name}] new size was bigger, skipping')
            return False
        case None:
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

    safe_print(f'finished in {elapsed:.2f}s, {(total_count / elapsed):.2f} files/s')
    safe_print(f'converted {converted_count} files out of {total_count} ({(converted_count / total_count):.2%})')
    safe_print(f'old size: {human_size(size, True)}, new size: {human_size(new_size, True)}, reduction: {reduction:.2f}%')

    log_path = log_dir + get_log_name()
    with open(log_path, 'w') as file:
        file.write(conversion_log)

main()
