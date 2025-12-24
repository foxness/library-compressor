import os
import json
import subprocess
from pathlib import Path
import threading
import queue
import time

source_dir = '/Volumes/Athena/river-lib/small_lib_test'

# --- conversion parameters ---

force_img_format = None
master_quality = 85

jxl_fighting_enabled = True # pick best between lossy and lossless
jxl_measure_is_quality = True
jxl_quality = master_quality if master_quality != None else 85
jxl_distance = 2

avif_quality = master_quality if master_quality != None else 85

# --- multithreading ---

worker_count = 8
encoder_thread_count = None
# optimal for jxl: w8 e4

# --- extensions ---

converted_extensions = ['avif', 'jxl', 'webp']
valid_extensions = ['png', 'jpg', 'jpeg', 'gif']

# --- locks ---

converted_lock = threading.Lock()
print_log_lock = threading.Lock()
jxl_win_count_lock = threading.Lock()
format_win_count_lock = threading.Lock()
outcome_lock = threading.Lock()

# --- counters ---

jxl_fight_count = 0
jxl_lossless_win_count = 0
format_fight_count = 0
format_jxl_win_count = 0

outcomes = {
    'jxl-lossy': 0,
    'jxl-lossless': 0,
    'avif': 0,
    'already-converted': 0,
    'invalid-extension': 0,
    'compression-fail': 0,
    'conversion-error': 0,
    'invalid-directory': 0
}

# --- logging ---

log_dir = '/Volumes/Athena/river-lib/'
conversion_log = ""

def get_outcome_text(outcomes):
    result = '\n'

    outcome_count = sum(list(outcomes.values()))
    result += f'Total: {outcome_count}\n\n'

    for outcome, count in outcomes.items():
        ratio = count / outcome_count
        result += f'{outcome}:\t{count}\t{ratio:.2%}\n'

    return result.rstrip()

def get_log_name():
    q = (jxl_quality if jxl_measure_is_quality else jxl_distance) if force_img_format == 'jxl' else avif_quality
    e = f'_e{encoder_thread_count}' if encoder_thread_count != None else ''
    f = f'_{force_img_format}' if force_img_format != None else ''
    return f'log{f}_{q}_w{worker_count}{e}.log'

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
    global jxl_fight_count, jxl_lossless_win_count

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
        with jxl_win_count_lock:
            jxl_fight_count += 1
            jxl_lossless_win_count += 1
        safe_print(f'[{name}] lossless won because lossy errored')
        os.rename(lossless_path, final_path)
        return [final_path, lossless_size]
    elif lossless_fail and not lossy_fail:
        with jxl_win_count_lock:
            jxl_fight_count += 1
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

    with jxl_win_count_lock:
        jxl_fight_count += 1
        if winner == 'lossless':
            jxl_lossless_win_count += 1

    winner_type = f'jxl-{winner}'

    os.remove(loser_path)
    os.rename(winner_path, final_path)
    return [final_path, winner_size, winner_type]

def convert_to_jxl(path, name):
    img_format = 'jxl'

    old_path = Path(path)
    source_format = old_path.suffix.lower()[1:]
    new_path = old_path.with_suffix(f'.{img_format}').resolve()
    new_size = None
    winner_type = None

    if jxl_fighting_enabled and (source_format == 'jpg' or source_format == 'jpeg'):
        jxl_fight_result = jxl_fight(path, name)
        if jxl_fight_result == None:
            return None

        new_path, new_size, winner_type = jxl_fight_result
    else:
        args = get_jxl_base_args(source_format, False, 0)
        args += [path, new_path]

        encode_result = subprocess.run(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if encode_result.returncode != 0:
            if os.path.isfile(new_path):
                os.remove(new_path)

            return None

        new_size = os.path.getsize(new_path)
        winner_type = 'jxl-lossy'

    return [new_path, new_size, winner_type]

def convert_to_avif(path, name):
    img_format = 'avif'

    new_path = Path(path).with_suffix(f'.{img_format}').resolve()
    new_size = None

    args = get_avif_base_args(0)
    args += [path, new_path]

    encode_result = subprocess.run(args, stdout=subprocess.DEVNULL)
    if encode_result.returncode != 0:
        if os.path.isfile(new_path):
            os.remove(new_path)

        return None

    new_size = os.path.getsize(new_path)
    return [new_path, new_size]

def convert_to_best(path, name):
    global format_fight_count, format_jxl_win_count

    old_size = os.path.getsize(path)
    win_type = 'forced' if force_img_format != None else None

    conversion_jxl = None
    conversion_avif = None

    if force_img_format != 'avif':
        conversion_jxl = convert_to_jxl(path, name)

    if force_img_format != 'jxl':
        conversion_avif = convert_to_avif(path, name)

    jxl_fail = conversion_jxl == None
    avif_fail = conversion_avif == None

    jxl_path = None
    jxl_size = None
    jxl_winner_type = None
    avif_path = None
    avif_size = None

    if not jxl_fail:
        jxl_path, jxl_size, jxl_winner_type = conversion_jxl

    if not avif_fail:
        avif_path, avif_size = conversion_avif

    winner = None
    if jxl_fail and avif_fail:
        return None
    elif jxl_fail and not avif_fail:
        winner = 'avif'
        safe_print(f'[{name}] avif won because jxl errored')
        if win_type == None:
            win_type = 'error'
    elif avif_fail and not jxl_fail:
        winner = 'jxl'
        safe_print(f'[{name}] jxl won because avif errored')
        if win_type == None:
            win_type = 'error'
    else:
        winner = 'jxl' if jxl_size <= avif_size else 'avif'
        win_type = 'fair'

    winner_path = None
    winner_size = None
    loser_path = None
    loser_size = None
    winner_type = None
    if winner == 'jxl':
        winner_path = jxl_path
        winner_size = jxl_size
        loser_path = avif_path
        loser_size = avif_size
        winner_type = jxl_winner_type
    elif winner == 'avif':
        winner_path = avif_path
        winner_size = avif_size
        loser_path = jxl_path
        loser_size = jxl_size
        winner_type = 'avif'

    if win_type == 'fair':
        os.remove(loser_path)

    if win_type != 'forced':
        with format_win_count_lock:
            format_fight_count += 1
            if winner == 'jxl':
                format_jxl_win_count += 1

    readable_old_size = human_size(old_size, False)
    readable_winner_size = human_size(winner_size, False)

    if win_type == 'fair':
        readable_loser_size = human_size(loser_size, False)
        win_diff = (1 - (winner_size / loser_size)) * 100
        safe_print(f'[{name}] {winner} won because it was {win_diff:.2f}% smaller [{readable_winner_size} vs {readable_loser_size}]')

    if winner_size >= old_size:
        new_diff = winner_size / old_size - 1
        source_format = Path(path).suffix.lower()[1:]
        text = f'[{name}] mission failed, converted {winner} is {(new_diff):.2%} bigger than old {source_format}' \
            f' ({readable_winner_size} vs {readable_old_size})'
        safe_print(text)

        os.remove(winner_path)
        return 'compression_fail'

    os.remove(path)
    return [winner_path, winner, old_size, winner_size, winner_type]

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
        return 'already-converted'

    if extension not in valid_extensions:
        safe_print(f'[{name}] {extension} is not a valid extension, skipping')
        return 'invalid-extension'

    paths = [a for a in files if os.path.basename(a) == image_name]
    if not paths:
        safe_print(f'[{name}] could not find the image, skipping')
        return 'invalid-directory'

    path = paths[0]

    result = convert_to_best(path, name)
    match result:
        case 'compression_fail':
            safe_print(f'[{name}] new size was bigger, skipping')
            return 'compression-fail'
        case None:
            safe_print(f'[{name}] error during conversion, skipping')
            return 'conversion-error'

    new_path, img_format, old_size, new_size, winner_type = result

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

    return winner_type

def work(name, queue, total_count):
    while True:
        index, image_dir = queue.get()

        outcome = process_one(image_dir, index, total_count, name)
        with outcome_lock:
            outcomes[outcome] += 1

        if outcome in ['jxl-lossy', 'jxl-lossless', 'avif']:
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
    size = get_size(source_dir)
    image_dirs = [f.path for f in os.scandir(source_dir) if f.is_dir()]

    total_count = len(image_dirs)
    global converted_count
    converted_count = 0

    start = time.time()
    safe_print(f'starting conversion of {source_dir}')

    start_work(image_dirs)

    end = time.time()
    elapsed = end - start

    new_size = get_size(source_dir)
    reduction = (1 - (new_size / size)) * -100

    safe_print(f'converted {converted_count} files out of {total_count} ({(converted_count / total_count):.2%})')
    safe_print(f'old size: {human_size(size, True)}, new size: {human_size(new_size, True)}, reduction: {reduction:.2f}%')

    if force_img_format == None and format_fight_count != 0:
        safe_print(f'jxl wins: {(format_jxl_win_count / format_fight_count):.2%} ({format_jxl_win_count}/{format_fight_count})')

    if jxl_fighting_enabled and jxl_fight_count != 0:
        safe_print(f'jxl lossless wins: {(jxl_lossless_win_count / jxl_fight_count):.2%} ({jxl_lossless_win_count}/{jxl_fight_count})')

    safe_print(get_outcome_text(outcomes))
    safe_print(f'\nfinished in {elapsed:.2f}s, {(total_count / elapsed):.2f} files/s')

    log_path = log_dir + get_log_name()
    with open(log_path, 'w') as file:
        file.write(conversion_log)

main()
