import os
import json
import subprocess
from pathlib import Path
import time

source_dir = '/Volumes/Athena/river-lib/tiny_lib copy'
converted_extensions = ['avif', 'jxl', 'webp']
valid_extensions = ['png', 'jpg', 'jpeg', 'gif']

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

    subprocess.run(['rm', path], stdout = subprocess.DEVNULL)
    return new_path

def process_one(dir_path, index, total_count):
    files = [f.path for f in os.scandir(dir_path) if not f.is_dir()]
    metadata_file = [a for a in files if os.path.basename(a) == 'metadata.json'][0]
    with open(metadata_file, 'r') as file:
        metadata = json.load(file)

    extension = metadata['ext']
    image_name = metadata['name'] + '.' + extension
    print(f'processing {image_name}')

    if extension in converted_extensions:
        print(f'{extension} is already converted, skipping')
        return False

    if extension not in valid_extensions:
        print(f'{extension} is not a valid extension, skipping')
        return False

    path = [a for a in files if os.path.basename(a) == image_name][0]
    size = os.path.getsize(path)

    new_path = convert(path)
    if new_path == None:
        print('error during conversion, skipping')
        return False

    new_size = os.path.getsize(new_path)

    metadata['ext'] = 'avif'
    metadata['size'] = new_size
    with open(metadata_file, 'w') as file:
        json.dump(metadata, file)

    reduction = (1 - (new_size / size)) * -100
    index += 1
    progress = (index / total_count) * 100
    print(f'converted.\told size: {size},\tnew size: {new_size},\treduction: {reduction:.2f}%,\tprogress: {index}/{total_count} {progress:.2f}%')

    return True

def main():
    start = time.time()

    size = get_size(source_dir)
    image_dirs = [f.path for f in os.scandir(source_dir) if f.is_dir()]

    total_count = len(image_dirs)
    converted_count = 0
    for index, image_dir in enumerate(image_dirs):
        did_convert = process_one(image_dir, index, total_count)
        if did_convert:
            converted_count += 1

    new_size = get_size(source_dir)
    reduction = (1 - (new_size / size)) * -100

    end = time.time()
    elapsed = end - start

    print(f'converted {converted_count} files')
    print(f'old size: {size / 1024:.2f}mb, new size: {new_size / 1024:.2f}mb, reduction: {reduction:.2f}%')
    print(f'finished in {elapsed:.2f}s, speed: {(total_count / elapsed):.2f} files/s')

main()
