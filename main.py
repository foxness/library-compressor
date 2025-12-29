import os
import json
import subprocess
from pathlib import Path
import threading
import queue
import time

source_dir = '/Volumes/Athena/river-lib/small_lib copy'

# --- conversion parameters ---

force_img_format = None
master_quality = 85

# if the lossy version saves less than this % of space,
# we keep the smallest lossless version instead
lossy_throwaway_threshold = 0.03

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

print_log_lock = threading.Lock()
jxl_win_count_lock = threading.Lock()
outcome_lock = threading.Lock()
fail_counter_lock = threading.Lock()

# --- counters ---

# counting jxl lossless wins even if they
# dont end up winning vs avif in the end
jxl_fight_count = 0
jxl_lossless_win_count = 0

outcomes = {
    'jxl-lossless': 0,
    'jxl-lossy': 0,
    'avif': 0,

    'no-metadata': 0,
    'already-converted': 0,
    'no-image': 0,
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

fail_counter = {
    'threshold-fail': 0,
    'compression-fail': 0,
    'conversion-error': 0
}

# --- logging ---

log_dir = '/Volumes/Athena/river-lib/'
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

    fail_count = sum(list(fail_counter.values()))
    result += f'Total fails: {fail_count}\n\n'

    for fail, count in fail_counter.items():
        ratio = count / fail_count
        fail_str = f'{fail}:'
        result += f'{fail_str:<23} {count:>6} {ratio:>8.2%}\n'

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

def passes_lossy_threshold(old_size, new_size):
    return new_size < old_size * (1 - lossy_throwaway_threshold)

def get_extension(img_format):
    return img_format.split('-')[0]

def size_comparison(size_a, size_b, is_kilobytes):
    a_diff = size_a / size_b - 1

    readable_size_a = human_size(size_a, is_kilobytes)
    readable_size_b = human_size(size_b, is_kilobytes)
    vs_text = f'{readable_size_a} vs {readable_size_b}'

    return [a_diff, vs_text]

def jxl_fight(jpg_path, name, old_size):
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
    lossy_fail_type = None
    if lossy_fail:
        lossy_fail_type = 'error'
        if os.path.isfile(lossy_path):
            os.remove(lossy_path)
    else:
        lossy_size = os.path.getsize(lossy_path)
        if not passes_lossy_threshold(old_size, lossy_size):
            lossy_fail = True
            lossy_fail_type = 'threshold'
            os.remove(lossy_path)
            safe_print(f'[{name}] jxl lossy didn\'t pass threshold')

    lossless_result = subprocess.run(lossless_args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    lossless_fail = lossless_result.returncode != 0
    if lossless_fail:
        if os.path.isfile(lossless_path):
            os.remove(lossless_path)
    else:
        lossless_size = os.path.getsize(lossless_path)

    if lossy_fail and lossless_fail:
        safe_print(f'[{name}] this is an epic fail, aborting')
        return 'jxl-lossy-threshold-fail' if lossy_fail_type == 'threshold' else 'conversion-error'
    elif lossy_fail and not lossless_fail:
        with jxl_win_count_lock:
            jxl_fight_count += 1
            jxl_lossless_win_count += 1

        os.rename(lossless_path, final_path)
        if lossy_fail_type == 'error':
            safe_print(f'[{name}] lossless won because lossy errored')
            return [final_path, lossless_size, 'jxl-lossless-technical']
        elif lossy_fail_type == 'threshold':
            safe_print(f'[{name}] lossless won because lossy failed threshold')
            return [final_path, lossless_size, 'jxl-lossless-threshold']
    elif lossless_fail and not lossy_fail:
        with jxl_win_count_lock:
            jxl_fight_count += 1
        safe_print(f'[{name}] lossy won because lossless errored')

        os.rename(lossy_path, final_path)
        return [final_path, lossy_size, 'jxl-lossy-technical']

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

def convert_to_jxl(path, name, old_size):
    output_format = 'jxl'

    old_path = Path(path)
    source_format = old_path.suffix.lower()[1:]
    new_path = old_path.with_suffix(f'.{output_format}').resolve()
    new_size = None
    winner_type = None

    if jxl_fighting_enabled and (source_format == 'jpg' or source_format == 'jpeg'):
        jxl_fight_result = jxl_fight(path, name, old_size)
        match jxl_fight_result:
            case 'conversion-error' | 'jxl-lossy-threshold-fail':
                return jxl_fight_result

        new_path, new_size, winner_type = jxl_fight_result
    else:
        args = get_jxl_base_args(source_format, False, 0)
        args += [path, new_path]

        encode_result = subprocess.run(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if encode_result.returncode != 0:
            if os.path.isfile(new_path):
                os.remove(new_path)

            return 'conversion-error'

        new_size = os.path.getsize(new_path)
        if passes_lossy_threshold(old_size, new_size):
            winner_type = 'jxl-lossy'
        else:
            os.remove(new_path)
            return 'jxl-lossy-threshold-fail'

    return [new_path, new_size, winner_type]

def convert_to_avif(path, name, old_size):
    output_format = 'avif'

    new_path = Path(path).with_suffix(f'.{output_format}').resolve()
    new_size = None

    args = get_avif_base_args(0)
    args += [path, new_path]

    encode_result = subprocess.run(args, stdout=subprocess.DEVNULL)
    if encode_result.returncode != 0:
        if os.path.isfile(new_path):
            os.remove(new_path)

        return 'conversion-error'

    new_size = os.path.getsize(new_path)
    if not passes_lossy_threshold(old_size, new_size):
        os.remove(new_path)
        return 'avif-threshold-fail'

    return [new_path, new_size]

def convert_to_best(path, name):
    old_size = os.path.getsize(path)
    win_type = 'forced' if force_img_format != None else None

    conversion_jxl = None
    conversion_avif = None

    if force_img_format != 'avif':
        conversion_jxl = convert_to_jxl(path, name, old_size)

    if force_img_format != 'jxl':
        conversion_avif = convert_to_avif(path, name, old_size)

    jxl_fail = isinstance(conversion_jxl, str)
    avif_fail = isinstance(conversion_avif, str)
    jxl_fail_type = conversion_jxl if jxl_fail else None
    avif_fail_type = conversion_avif if avif_fail else None

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
        if jxl_fail_type == 'jxl-lossy-threshold-fail' or avif_fail_type == 'avif-threshold-fail':
            return 'threshold-fail'
        return 'conversion-error'
    elif jxl_fail and not avif_fail:
        winner = 'avif'

        if jxl_fail_type == 'jxl-lossy-threshold-fail':
            safe_print(f'[{name}] avif won because jxl failed threshold')
            if win_type == None:
                win_type = 'threshold'
        else:
            safe_print(f'[{name}] avif won because jxl errored')
            if win_type == None:
                win_type = 'error'
    elif avif_fail and not jxl_fail:
        winner = 'jxl'

        if avif_fail_type == 'avif-threshold-fail':
            safe_print(f'[{name}] jxl won because avif failed threshold')
            if win_type == None:
                win_type = 'threshold'
        else:
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

    readable_old_size = human_size(old_size, False)
    readable_winner_size = human_size(winner_size, False)

    if win_type == 'fair':
        readable_loser_size = human_size(loser_size, False)
        win_diff = (1 - (winner_size / loser_size)) * 100
        safe_print(f'[{name}] {winner} won because it was {win_diff:.2f}% smaller [{readable_winner_size} vs {readable_loser_size}]')
    elif win_type == 'error' and not winner_type.endswith('-technical'):
        winner_type += '-technical'
    elif win_type == 'threshold' and not winner_type.endswith('-threshold'):
        winner_type += '-threshold'

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
    metadata_file = [a for a in files if os.path.basename(a) == 'metadata.json']
    if not metadata_file:
        safe_print(f'[{name}] couldn\'t find metadata file, skipping')
        return 'no-metadata'

    metadata_file = metadata_file[0]

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
        return 'no-image'

    path = paths[0]

    result = convert_to_best(path, name)
    match result:
        case 'compression_fail':
            safe_print(f'[{name}] new size was bigger, skipping')
            return 'compression-fail'
        case 'conversion-error':
            safe_print(f'[{name}] error during conversion, skipping')
            return 'conversion-error'
        case 'threshold-fail':
            safe_print(f'[{name}] everyone failed the threshold or errored, skipping')
            return 'threshold-fail'

    new_path, output_format, old_size, new_size, winner_type = result

    metadata['ext'] = output_format
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

class Convertable:
    def __init__(self, input_format, output_format, temp_path, final_path, return_code):
        self.input_format = input_format
        self.output_format = output_format
        self.temp_path = temp_path
        self.final_path = final_path
        self.return_code = return_code
        self.fail = None
        self.size = None

def avif_conversion(path, input_format, name):
    output_format = 'avif'

    old_path = Path(path)
    extension = get_extension(output_format)

    temp_name = f'{old_path.stem}_{output_format}.{extension}'
    final_name = f'{old_path.stem}.{extension}'

    temp_path = old_path.with_name(temp_name).resolve()
    final_path = old_path.with_name(final_name).resolve()

    args = get_avif_base_args(0)
    args += [path, temp_path]

    encode_result = subprocess.run(args, stdout=subprocess.DEVNULL)
    return_code = encode_result.returncode

    return Convertable(input_format, output_format, temp_path, final_path, return_code)

def jxl_lossy_conversion(path, input_format, name):
    output_format = 'jxl-lossy'

    old_path = Path(path)
    extension = get_extension(output_format)

    temp_name = f'{old_path.stem}_{output_format}.{extension}'
    final_name = f'{old_path.stem}.{extension}'

    temp_path = old_path.with_name(temp_name).resolve()
    final_path = old_path.with_name(final_name).resolve()

    args = get_jxl_base_args(input_format, False, 0)
    args += [path, temp_path]

    encode_result = subprocess.run(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return_code = encode_result.returncode

    return Convertable(input_format, output_format, temp_path, final_path, return_code)

def jxl_lossless_conversion(path, input_format, name):
    output_format = 'jxl-lossless'

    old_path = Path(path)
    extension = get_extension(output_format)

    temp_name = f'{old_path.stem}_{output_format}.{extension}'
    final_name = f'{old_path.stem}.{extension}'

    temp_path = old_path.with_name(temp_name).resolve()
    final_path = old_path.with_name(final_name).resolve()

    args = get_jxl_base_args(input_format, True, 0)
    args += [path, temp_path]

    encode_result = subprocess.run(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return_code = encode_result.returncode

    return Convertable(input_format, output_format, temp_path, final_path, return_code)

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

    list_text = ', '.join([f'{a.output_format} ({human_size(a.size, False)})' for a in size_sorted])
    count = len(size_sorted)
    safe_print(f'[{name}] {count} candidate{'' if count == 1 else 's'}: {list_text}{'' if not fails else f', fails: {fail_text}'}')

    for loser in losers:
        loser.fail = 'loser'
        os.remove(loser.temp_path)

    return [winner, fails + losers]

def convert_to_best_new(path, name):
    conversions = [avif_conversion, jxl_lossy_conversion]

    old_path = Path(path)
    old_size = os.path.getsize(path)
    input_format = old_path.suffix.lower()[1:]

    if (input_format == 'jpg' or input_format == 'jpeg') and jxl_fighting_enabled:
        conversions.append(jxl_lossless_conversion)

    convertables = []
    for conversion in conversions:
        convertable = conversion(path, input_format, name)
        handle_errors(convertable, name)

        convertables.append(convertable)

    filter_by_size(old_size, convertables, name)
    winner, fails = filter_losers(convertables, name)

    if winner == None:
        return [None, fails, None]

    os.remove(path)
    os.rename(winner.temp_path, winner.final_path)

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

def handle_result(result, metadata, metadata_file, index, total_count, name):
    winner, fails, old_size = result

    fails = [a.fail for a in fails]
    with fail_counter_lock:
        for fail in fails:
            if fail != 'loser':
                fail_counter[fail] += 1

    if winner == None:
        fail_priority = ['threshold-fail', 'compression-fail', 'conversion-error']

        for fail in fail_priority:
            if fail in fails:
                safe_print(f'[{name}] fail outcome: {fail}')
                return fail

        assert False

    metadata['ext'] = get_extension(winner.output_format)
    metadata['size'] = winner.size
    with open(metadata_file, 'w') as file:
        json.dump(metadata, file)

    print_result(winner, old_size, index, total_count, name)
    return winner.output_format

def process_one_new(dir_path, index, total_count, name):
    files = [f.path for f in os.scandir(dir_path) if not f.is_dir()]
    metadata_file = [a for a in files if os.path.basename(a) == 'metadata.json']
    if not metadata_file:
        safe_print(f'[{name}] couldn\'t find metadata file, skipping')
        return 'no-metadata'

    metadata_file = metadata_file[0]

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
        return 'no-image'

    path = paths[0]

    result = convert_to_best_new(path, name)
    return handle_result(result, metadata, metadata_file, index, total_count, name)

def work(name, queue, total_count):
    while True:
        index, image_dir = queue.get()

        outcome = process_one_new(image_dir, index, total_count, name)
        with outcome_lock:
            outcomes[outcome] += 1

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
    safe_print('\nall work completed')

def main():
    size = get_size(source_dir)
    image_dirs = [f.path for f in os.scandir(source_dir) if f.is_dir()]

    total_count = len(image_dirs)

    start = time.time()
    safe_print(f'starting conversion of {source_dir}')

    start_work(image_dirs)

    end = time.time()
    elapsed = end - start

    new_size = get_size(source_dir)
    reduction = (1 - (new_size / size)) * -100

    converted_count = sum([outcomes[a] for a in success_outcomes])
    safe_print(f'converted {converted_count} files out of {total_count} ({(converted_count / total_count):.2%})')
    safe_print(f'old size: {human_size(size, True)}, new size: {human_size(new_size, True)}, reduction: {reduction:.2f}%')

    if jxl_fighting_enabled and jxl_fight_count != 0:
        safe_print(f'jxl lossless wins: {(jxl_lossless_win_count / jxl_fight_count):.2%} ({jxl_lossless_win_count}/{jxl_fight_count})')

    safe_print(get_outcome_text(outcomes))
    safe_print(get_fail_counter_text(fail_counter))
    safe_print(f'\nfinished in {elapsed:.2f}s, {(total_count / elapsed):.2f} files/s')

    log_path = log_dir + get_log_name()
    with open(log_path, 'w') as file:
        file.write(conversion_log)

main()
