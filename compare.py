import os
import json
import subprocess
from pathlib import Path
import threading
import queue
import time
import random

source_dir = '/Volumes/Athena/river-lib/riverLibrary.library/images'
output_dir = '/Volumes/Athena/river-lib/compare_output'

image_count = 20
encoder_thread_count = None

converted_extensions = ['avif', 'jxl', 'webp']
valid_extensions = ['png', 'jpg', 'jpeg', 'gif']

print_lock = threading.Lock()

def get_jxl_base_args(quality):
    args = ['cjxl', '--lossless_jpeg=0', '-q', str(quality)]
    if encoder_thread_count != None:
        args += [f'--num_threads={encoder_thread_count}']

    return args

def get_avif_base_args(quality):
    args = ['avifenc', '-q', str(quality)]
    if encoder_thread_count != None:
        args += ['-j', str(encoder_thread_count)]

    return args

def convert(path, output_dir, img_format, quality):
    old_path = Path(path)
    new_name = f'{shortened(old_path.stem)}_{img_format}{quality}.{img_format}'
    new_path = os.path.join(output_dir, new_name)

    args = None
    stderr = None
    if img_format == 'avif':
        args = get_avif_base_args(quality) + [path, new_path]

    elif img_format == 'jxl':
        args = get_jxl_base_args(quality) + [path, new_path]
        stderr = subprocess.DEVNULL

    encode_result = subprocess.run(args, stdout=subprocess.DEVNULL, stderr=stderr)
    if encode_result.returncode != 0:
        # handle the error
        return False

    return True

def shortened(name):
    max_len = 8
    return name if len(name) <= max_len else name[:max_len]

def convert_many(path):
    iteration_count = 7

    old_path = Path(path)
    dir_name = shortened(old_path.stem)

    image_dir = None
    i = 0
    while True:
        image_dir = os.path.join(output_dir, dir_name)
        if not os.path.exists(image_dir):
            break
        dir_name = f'{dir_name}{i}'
        i += 1

    result = subprocess.run(['mkdir', image_dir], stdout=subprocess.DEVNULL)

    new_orig_name = f'orig{old_path.suffix}'
    new_orig_path = os.path.join(image_dir, new_orig_name)

    result = subprocess.run(['cp', path, new_orig_path], stdout=subprocess.DEVNULL)
    for img_format in ['jxl', 'avif']:
        for i in range(iteration_count):
            quality = 90 - i * 5
            convert(path, image_dir, img_format, quality)
            print(f'{dir_name} {img_format}{quality}')

def process_one(dir_path):
    files = [f.path for f in os.scandir(dir_path) if not f.is_dir()]
    metadata_file = [a for a in files if os.path.basename(a) == 'metadata.json'][0]
    with open(metadata_file, 'r') as file:
        metadata = json.load(file)

    extension = metadata['ext']
    image_name = metadata['name'] + '.' + extension
    safe_print(f'processing {image_name}')

    if extension in converted_extensions:
        safe_print(f'{extension} is already converted, skipping')

        return False

    if extension not in valid_extensions:
        safe_print(f'{extension} is not a valid extension, skipping')

        return False

    path = [a for a in files if os.path.basename(a) == image_name][0]
    convert_many(path)

    return True

def safe_print(*a, **b):
    with print_lock:
        print(*a, **b)

def main():
    image_dirs = [f.path for f in os.scandir(source_dir) if f.is_dir()]

    converted_images = 0
    while converted_images < image_count:
        image_dir = random.choice(image_dirs)
        image_dirs.remove(image_dir)

        did_process = process_one(image_dir)
        if did_process:
            converted_images += 1

main()
