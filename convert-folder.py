import os
import subprocess
from pathlib import Path
import threading
import queue
import time

import conversion

source_dir = '/Volumes/Athena/screenconvtest/gen0_jpg'
output_dir = '/Volumes/Athena/screenconvtest/gen0_jpg_out'

log_dir = '/Volumes/Athena/screenconvtest'

# --- conversion parameters ---

force_img_format = 'avif'
master_quality = 70

# if the lossy version saves less than this % of space,
# we keep the smallest lossless version instead
lossy_throwaway_threshold = 0.1

jxl_try_lossless_transcode = True # pick best between lossy and lossless
jxl_measure_is_quality = True
jxl_quality = master_quality if master_quality != None else 85
jxl_distance = 2

avif_quality = master_quality if master_quality != None else 85

# --- multithreading ---

worker_count = 8
encoder_thread_count = 4
# optimal for jxl: w8 e4

# --- extensions ---

converted_extensions = ['avif', 'jxl', 'webp']
valid_extensions = ['png', 'jpg', 'jpeg']

# --- locks ---

print_log_lock = threading.Lock()
jxl_win_count_lock = threading.Lock()
outcome_lock = threading.Lock()
fail_counter_lock = threading.Lock()
reduction_records_lock = threading.Lock()
disparity_records_lock = threading.Lock()

# --- counters ---

# counting jxl lossless wins even if they
# dont end up winning vs avif in the end
jxl_fight_count = 0
jxl_lossless_win_count = 0

outcomes = {
    'jxl-lossless': 0,
    'jxl-lossy': 0,
    'avif': 0,

    'already-converted': 0,
    'invalid-extension': 0,
    'compression-fail': 0,
    'threshold-fail': 0,
    'conversion-error': 0
}

success_outcomes = [
    'jxl-lossless',
    'jxl-lossy',
    'avif'
]

fail_counter = {}

max_reduction_records = 50
reduction_records = []

max_disparity_records = 50
disparity_records = []

# --- logging ---

conversion_log = ""

def get_outcome_text(outcomes):
    result = '\n'

    outcome_count = sum(list(outcomes.values()))
    result += f'Total outcomes: {outcome_count}\n\n'

    for outcome, count in outcomes.items():
        ratio = count / outcome_count
        outcome_str = f'{outcome}:'
        result += f'{outcome_str:<23} {count:>6} {ratio:>8.2%}\n'

    return result.rstrip()

def get_fail_counter_text(fail_counter):
    result = '\n'

    converted_count = sum([outcomes[a] for a in success_outcomes])
    fail_count = sum(list(fail_counter.values()))
    result += f'Total fails: {fail_count}\n\n'

    fail_types = set([k.split('|')[1] for k in fail_counter.keys()])
    output_formats = set([k.split('|')[0] for k in fail_counter.keys()])

    for fail_type in fail_types:
        fail_type_count = sum([fail_counter[a] for a in fail_counter.keys() if fail_type in a])

        ratio = fail_type_count / fail_count
        fail_str = f'{fail_type}:'
        result += f'{fail_str:<23} {fail_type_count:>6} {ratio:>8.2%}\n'

        for output_format in output_formats:
            key = f'{output_format}|{fail_type}'
            format_count = fail_counter[key] if key in fail_counter else 0

            ratio = format_count / fail_type_count
            overall_ratio = format_count / converted_count
            format_str = f'    {output_format}:'
            result += f'{format_str:<23} {format_count:>6} {ratio:>8.2%}  all: {overall_ratio:>6.2%}\n'

        result += '\n'

    return result.rstrip()

def get_reduction_record_text(reduction_records):
    result = '\nReduction records:\n\n'

    for record in reduction_records:
        name, input_format, output_format, old_size, new_size, reduction = record

        readable_old_size = human_size(old_size, False)
        readable_new_size = human_size(new_size, False)

        result += f'{name} {input_format} {output_format} {readable_old_size} {readable_new_size} {-reduction:>8.2%}\n'

    return result.rstrip()

def get_disparity_record_text(disparity_records):
    result = '\nDisparity records:\n\n'

    for record in disparity_records:
        name, input_format, winner, winner_size, loser_size, disparity = record

        loser = 'jxl-lossy' if winner == 'avif' else 'avif'

        readable_winner_size = human_size(winner_size, False)
        readable_loser_size = human_size(loser_size, False)

        result += f'{name} {input_format} [{winner} {readable_winner_size}] [{loser} {readable_loser_size}] {-disparity:>8.2%}\n'

    return result.rstrip()

def get_log_name():
    name = Path(source_dir).stem
    q = (jxl_quality if jxl_measure_is_quality else jxl_distance) if force_img_format == 'jxl' else avif_quality
    e = f'_e{encoder_thread_count}' if encoder_thread_count != None else ''
    f = f'_{force_img_format}' if force_img_format != None else ''
    return f'{name}_log{f}_{q}_w{worker_count}{e}.log'

def get_size(dir_path):
    # -ks for size in kilobytes
    # -ms for size in megabytes

    assert os.path.exists(dir_path)

    result = subprocess.run(['du', '-ks', dir_path], capture_output=True, text=True)
    return int(result.stdout.split('\t')[0])

def human_size(size, source_is_kilobytes):
    return f'{size / 1024:.2f}' + ('mb' if source_is_kilobytes else 'kb')

def safe_print(*a, **b):
    with print_log_lock:
        global conversion_log
        conversion_log += f'{a[0]}\n'
        print(*a, **b)

def passes_lossy_threshold(old_size, new_size):
    return new_size < old_size * (1 - lossy_throwaway_threshold)

def size_comparison(size_a, size_b, is_kilobytes):
    a_diff = size_a / size_b - 1

    readable_size_a = human_size(size_a, is_kilobytes)
    readable_size_b = human_size(size_b, is_kilobytes)
    vs_text = f'{readable_size_a} vs {readable_size_b}'

    return [a_diff, vs_text]

def copy_file_times(old_file, new_file):
    creation_time = os.stat(old_file).st_birthtime
    modification_time = os.path.getmtime(old_file)

    os.utime(new_file, (creation_time, modification_time))

def handle_errors(convertable, name):
    if convertable.return_code == 0:
        return

    convertable.fail = 'conversion-error'
    safe_print(f'[{name}] {convertable.output_format} errored: {convertable.fail}')

    if os.path.isfile(convertable.temp_path):
        os.remove(convertable.temp_path)

def filter_by_size(old_size, convertables, name):
    for convertable in convertables:
        if convertable.fail != None:
            continue

        convertable.size = os.path.getsize(convertable.temp_path)
        is_lossless = convertable.output_format == 'jxl-lossless'

        passes_original = convertable.size < old_size
        passes_threshold = is_lossless or (passes_original and passes_lossy_threshold(old_size, convertable.size))

        if passes_threshold:
            continue

        diff, vs_text = size_comparison(convertable.size, old_size, False)

        if passes_original:
            convertable.fail = 'threshold-fail'
            safe_print(f'[{name}] converted {convertable.output_format} is only {(-diff):.2%} smaller than old {convertable.input_format} ({vs_text}) [{convertable.fail}]')
        else:
            convertable.fail = 'compression-fail'
            safe_print(f'[{name}] converted {convertable.output_format} is {diff:.2%} bigger than old {convertable.input_format} ({vs_text}) [{convertable.fail}]')

        os.remove(convertable.temp_path)

def filter_losers(convertables, name):
    candidates = [a for a in convertables if a.fail == None]
    fails = [a for a in convertables if a.fail != None]

    fail_text = ', '.join([f'{a.output_format} ({a.fail})' for a in fails])

    if not candidates:
        safe_print(f'[{name}] no winners today{'' if not fails else f', fails: {fail_text}'}')
        return [None, fails]

    size_sorted = sorted(candidates, key=lambda a: a.size)
    winner = size_sorted[0]
    losers = size_sorted[1:]

    list_text = ', '.join([f'[{a.output_format} {human_size(a.size, False)}]' for a in size_sorted])
    count = len(size_sorted)
    safe_print(f'[{name}] {count} candidate{'' if count == 1 else 's'}: {list_text}{'' if not fails else f', fails: {fail_text}'}')

    for loser in losers:
        loser.fail = 'loser'
        os.remove(loser.temp_path)

    return [winner, fails + losers]

def convert_image(path, output_dir, name):
    conversions = [conversion.avif_conversion, conversion.jxl_lossy_conversion]

    if force_img_format != None:
        match force_img_format:
            case 'jxl-lossy':
                conversions = [conversion.jxl_lossy_conversion]
            case 'jxl-lossless':
                conversions = [conversion.jxl_lossless_conversion]
            case 'avif':
                conversions = [conversion.avif_conversion]

    old_path = Path(path)
    old_size = os.path.getsize(path)
    input_format = old_path.suffix[1:].lower()

    if (input_format == 'jpg' or input_format == 'jpeg') and jxl_try_lossless_transcode and force_img_format == None:
        conversions.append(conversion.jxl_lossless_conversion)

    params = conversion.ConversionParameters(
        jxl_quality,
        jxl_distance,
        jxl_measure_is_quality,
        avif_quality,
        encoder_thread_count
    )

    convertables = []
    for conv in conversions:
        convertable = conv(params, path, input_format, output_dir, name)
        handle_errors(convertable, name)

        convertables.append(convertable)

    filter_by_size(old_size, convertables, name)
    winner, fails = filter_losers(convertables, name)

    if winner == None:
        return [None, fails, old_size]

    os.rename(winner.temp_path, winner.final_path)
    copy_file_times(path, winner.final_path)

    return [winner, fails, old_size]

def print_result(winner, old_size, index, total_count, name):
    reduction = (1 - (winner.size / old_size)) * -100
    index += 1
    progress = (index / total_count) * 100
    readable_old_size = human_size(old_size, False)
    readable_new_size = human_size(winner.size, False)

    input_format_text = f"{winner.input_format}:"
    output_format_text = f"{winner.output_format}:"

    done_text = f"[{name}] done.   "
    input_text = f"{input_format_text:<5}{readable_old_size:>10},"
    output_text = f"{output_format_text:<15}{readable_new_size:>10},"
    reduction_text = f"r: {reduction:>7.2f}%,"
    progress_text = f"{index}/{total_count} {progress:.2f}%"

    to_print = f'{done_text}{input_text:<19}{output_text:<30}{reduction_text:<15}{progress_text}'
    safe_print(to_print)

def add_reduction_record(winner, fail_items, old_size):
    reduction_records_lock.acquire()
    global reduction_records

    items = [winner] + fail_items
    for item in items:
        if item == None or item.size == None:
            continue

        reduction = 1 - item.size / old_size

        name = Path(item.final_path).stem
        new_item = [name, item.input_format, item.output_format, old_size, item.size, reduction]

        has_space = len(reduction_records) < max_reduction_records
        is_record = not has_space and (reduction_records[-1][-1] < reduction)

        if has_space or is_record:
            if is_record:
                reduction_records.pop()

            reduction_records.append(new_item)
            reduction_records = sorted(reduction_records, key=lambda a: -a[-1])

    reduction_records_lock.release()

def add_disparity_record(winner, fail_items):
    disparity_records_lock.acquire()
    global disparity_records

    items = [winner] + fail_items
    items = [a for a in items if a is not None]

    avif_items = [a for a in items if a.output_format == 'avif']
    jxl_items = [a for a in items if a.output_format == 'jxl-lossy']

    avif_size = avif_items[0].size if avif_items else None
    jxl_size = jxl_items[0].size if jxl_items else None

    if avif_size != None and jxl_size != None:
        winner_size = min(avif_size, jxl_size)
        loser_size = max(avif_size, jxl_size)
        winner = 'avif' if avif_size < jxl_size else 'jxl-lossy'

        disparity = 1 - winner_size / loser_size

        item = avif_items[0]
        name = Path(item.final_path).stem
        new_item = [name, item.input_format, winner, winner_size, loser_size, disparity]

        has_space = len(disparity_records) < max_disparity_records
        is_record = not has_space and (disparity_records[-1][-1] < disparity)

        if has_space or is_record:
            if is_record:
                disparity_records.pop()

            disparity_records.append(new_item)
            disparity_records = sorted(disparity_records, key=lambda a: -a[-1])

    disparity_records_lock.release()

def handle_result(result, index, total_count, name):
    winner, fail_items, old_size = result

    add_reduction_record(winner, fail_items, old_size)
    add_disparity_record(winner, fail_items)

    with fail_counter_lock:
        for item in fail_items:
            if item.fail == 'loser':
                continue

            key = f'{item.output_format}|{item.fail}'
            if key not in fail_counter:
                fail_counter[key] = 0

            fail_counter[key] += 1

    if winner == None:
        fail_priority = ['threshold-fail', 'compression-fail', 'conversion-error']

        fails = [a.fail for a in fail_items]
        for fail in fail_priority:
            if fail in fails:
                safe_print(f'[{name}] fail outcome: {fail}')
                return fail

        assert False

    print_result(winner, old_size, index, total_count, name)
    return winner.output_format

def process_one(image_path, index, total_count, output_dir, name):
    path = Path(image_path)
    safe_print(f'[{name}] processing {path.name}')

    extension = path.suffix[1:].lower()
    if extension in converted_extensions:
        safe_print(f'[{name}] {extension} is already converted, skipping')
        return 'already-converted'

    if extension not in valid_extensions:
        safe_print(f'[{name}] {extension} is not a valid extension, skipping')
        return 'invalid-extension'

    result = convert_image(image_path, output_dir, name)
    return handle_result(result, index, total_count, name)

def work(name, queue, total_count, output_dir):
    while True:
        index, image = queue.get()

        outcome = process_one(image, index, total_count, output_dir, name)
        with outcome_lock:
            outcomes[outcome] += 1

        queue.task_done()

def start_work(images, output_dir):
    q = queue.Queue()
    total_count = len(images)

    workers = []
    for i in range(worker_count):
        workerThread = threading.Thread(target=work, args=[f'W{i:02d}', q, total_count, output_dir], daemon=True)
        workers.append(workerThread)
        workerThread.start()

    for index, image in enumerate(images):
        q.put([index, image])

    q.join()
    safe_print('\nall work completed')

def main():
    size = get_size(source_dir)
    source_images = [f.path for f in os.scandir(source_dir) if not f.is_dir()]
    source_images = sorted(source_images)

    total_count = len(source_images)

    start = time.time()
    safe_print(f'starting conversion of {source_dir}')

    start_work(source_images, output_dir)

    end = time.time()
    elapsed = end - start

    new_size = get_size(output_dir)
    reduction = (1 - (new_size / size)) * -100

    converted_count = sum([outcomes[a] for a in success_outcomes])
    safe_print(f'converted {converted_count} files out of {total_count} ({(converted_count / total_count):.2%})')
    safe_print(f'old size: {human_size(size, True)}, new size: {human_size(new_size, True)}, reduction: {reduction:.2f}%')

    if jxl_try_lossless_transcode and jxl_fight_count != 0:
        safe_print(f'jxl lossless wins: {(jxl_lossless_win_count / jxl_fight_count):.2%} ({jxl_lossless_win_count}/{jxl_fight_count})')

    safe_print(get_outcome_text(outcomes))
    safe_print(get_fail_counter_text(fail_counter))
    safe_print(get_reduction_record_text(reduction_records))

    if disparity_records:
        safe_print(get_disparity_record_text(disparity_records))

    safe_print(f'\nfinished in {elapsed:.2f}s, {(total_count / elapsed):.2f} files/s')

    log_path = os.path.join(log_dir, get_log_name())
    with open(log_path, 'w') as file:
        file.write(conversion_log)

main()
