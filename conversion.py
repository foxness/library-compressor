
def get_jxl_base_args(
        source_format,
        use_lossless_jpg,
        jxl_measure_is_quality,
        jxl_quality,
        jxl_distance,
        encoder_thread_count=None,
        iteration=0
    ):

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

def get_avif_base_args(avif_quality, encoder_thread_count=None, iteration=0):
    quality = avif_quality - (iteration * 10)
    args = ['avifenc', '-q', str(quality)]
    if encoder_thread_count != None:
        args += ['-j', str(encoder_thread_count)]

    return args
