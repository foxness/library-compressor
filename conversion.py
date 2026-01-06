import os
import subprocess
from pathlib import Path

class Convertable:
    def __init__(self, input_format, output_format, temp_path, final_path, return_code):
        self.input_format = input_format
        self.output_format = output_format
        self.temp_path = temp_path
        self.final_path = final_path
        self.return_code = return_code
        self.fail = None
        self.size = None

class ConversionParameters:
    def __init__(self, jxl_quality, jxl_distance, jxl_measure_is_quality, avif_quality, encoder_thread_count):
        self.jxl_quality = jxl_quality
        self.jxl_distance = jxl_distance
        self.jxl_measure_is_quality = jxl_measure_is_quality
        self.avif_quality = avif_quality
        self.encoder_thread_count = encoder_thread_count

def get_jxl_base_args(params, source_format, use_lossless_jpg, iteration=0):
    args = ['cjxl']

    add_quality = True
    match source_format:
        case 'jpg' | 'jpeg':
            args += [f'--lossless_jpeg={1 if use_lossless_jpg else 0}']
            if use_lossless_jpg:
                add_quality = False

    if add_quality:
        if params.jxl_measure_is_quality:
            quality = params.jxl_quality - (iteration * 10)
            args += ['-q', str(quality)]
        else:
            distance = params.jxl_distance + iteration
            args += ['-d', str(distance)]

    if params.encoder_thread_count != None:
        args += [f'--num_threads={params.encoder_thread_count}']

    return args

def get_avif_base_args(params, iteration=0):
    quality = params.avif_quality - (iteration * 10)
    args = ['avifenc', '-q', str(quality)]
    if params.encoder_thread_count != None:
        args += ['-j', str(params.encoder_thread_count)]

    return args

def avif_conversion(params, path, input_format, output_dir, name):
    output_format = 'avif'

    old_path = Path(path)
    extension = get_extension(output_format)

    temp_name = f'{old_path.stem}_{output_format}.{extension}'
    final_name = f'{old_path.stem}.{extension}'

    temp_path = os.path.join(output_dir, temp_name)
    final_path = os.path.join(output_dir, final_name)

    args = get_avif_base_args(params)
    args += [path, temp_path]

    encode_result = subprocess.run(args, stdout=subprocess.DEVNULL)
    return_code = encode_result.returncode

    return Convertable(input_format, output_format, temp_path, final_path, return_code)

def jxl_lossy_conversion(params, path, input_format, output_dir, name):
    output_format = 'jxl-lossy'

    old_path = Path(path)
    extension = get_extension(output_format)

    temp_name = f'{old_path.stem}_{output_format}.{extension}'
    final_name = f'{old_path.stem}.{extension}'

    temp_path = os.path.join(output_dir, temp_name)
    final_path = os.path.join(output_dir, final_name)

    args = get_jxl_base_args(params, input_format, False)

    args += [path, temp_path]

    encode_result = subprocess.run(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return_code = encode_result.returncode

    return Convertable(input_format, output_format, temp_path, final_path, return_code)

def jxl_lossless_conversion(params, path, input_format, output_dir, name):
    output_format = 'jxl-lossless'

    old_path = Path(path)
    extension = get_extension(output_format)

    temp_name = f'{old_path.stem}_{output_format}.{extension}'
    final_name = f'{old_path.stem}.{extension}'

    temp_path = os.path.join(output_dir, temp_name)
    final_path = os.path.join(output_dir, final_name)

    args = get_jxl_base_args(params, input_format, True)

    args += [path, temp_path]

    encode_result = subprocess.run(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return_code = encode_result.returncode

    return Convertable(input_format, output_format, temp_path, final_path, return_code)

def get_extension(img_format):
    return img_format.split('-')[0]
