import re
import sys
import json
import time
import pathlib
import hashlib
import logging
import functools
import subprocess
import collections


class BuildError(Exception):
    def __init__(self, package, code, message):
        self.src_name = package.secpath + '/' + package.directory
        self.package_name = package.name
        self.code = code
        self.message = message


class MetadataError(BuildError):
    # 1xx
    pass


class BuildProcessError(BuildError):
    # 2xx
    pass


class BuildResultError(BuildError):
    # 3xx
    pass


class DependencyError(BuildError):
    # 4xx
    pass


class MultipleBuildError(Exception):
    pass


class BuildWarning(UserWarning):
    def __init__(self, package, code, message):
        self.src_name = package.secpath + '/' + package.directory
        self.package_name = package.name
        self.code = code
        self.message = message


class MetadataWarning(BuildWarning):
    # 1xx
    pass


class BuildProcessWarning(BuildWarning):
    # 2xx
    pass


class BuildResultWarning(BuildWarning):
    # 3xx
    pass


class DependencyWarning(BuildWarning):
    # 4xx
    pass


class ANSIEscapes:
    ANSI_RST = '\033[0m'
    ANSI_BOLD = '\033[1m'
    ANSI_ULINE = '\033[4m'
    ANSI_RED = '\033[91m'
    ANSI_BLNK = '\033[5m'
    ANSI_CYAN = '\033[36m'
    ANSI_LT_CYAN = '\033[96m'
    ANSI_GREEN = '\033[32m'
    ANSI_YELLOW = '\033[93m'
    ANSI_BLUE = '\033[34m'
    ANSI_BROWN = '\033[33m'


def comma_list(l, color=None):
    if color:
        return ', '.join('%s%s%s' % (color, x, ANSIEscapes.ANSI_RST) for x in l)
    else:
        return ', '.join(l)


def touch(filename):
    pathlib.Path(filename).touch()


def hashtag(s, length=5):
    return hashlib.blake2b(s, digest_size=length).hexdigest()


def chksum_file(self, hash_type, target_file):
    hash_type = hash_type.lower()
    if hash_type in hashlib.algorithms_available:
        hash_obj = hashlib.new(hash_type)
    else:
        raise NotImplementedError(
            'Unsupported hash type %s! Currently supported: %s' % (
            hash_type, ' '.join(sorted(hashlib.algorithms_available))))
    with open(target_file, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b''):
            hash_obj.update(chunk)
    return hash_obj.hexdigest()


def retry(tries: int, delay=1, backoff=2, exc=Exception, raises=True):
    """Retries a function or method with exponential backoff.

    `delay` sets the initial delay in seconds.
    `backoff` sets the factor by which the delay should lengthen
    after each failure. `backoff` must be greater than 1,
    or else it isn't really a backoff.
    `tries` must be at least 0, and `delay` greater than 0.
    `exc` is the Exception to catch.
    If raises = False, then log an exception without raising the error.
    """

    def deco_retry(f):
        @functools.wraps(f)
        def f_retry(*args, **kwargs):
            for t in range(tries):
                try:
                    rv = f(*args, **kwargs)
                except exc as ex:
                    if t == tries - 1:
                        if raises:
                            raise
                        else:
                            logging.exception(ex)
                            return
                    else:
                        time.sleep(delay * (backoff ** t))
                        continue
                break
            return rv
        return f_retry
    return deco_retry


def uniq(seq):  # Dave Kirby
    # Order preserving
    """
    An order preserving de-duplicator by Dave Kirby

    :param seq: The list you want to de-duplicate
    :returns: De-duplicated list
    """
    seen = set()
    return [x for x in seq if x not in seen and not seen.add(x)]


class CircularDependencyError(ValueError):
    def __init__(self, data):
        s = 'Circular dependencies exist among these items: {%s}' % (
            ', '.join('{!r}:{!r}'.format(key, value)
            for key, value in sorted(data.items())))
        super(CircularDependencyError, self).__init__(s)
        self.data = data


def toposort(data):
    if not data:
        return
    # normalize data
    #data = {k: data.get(k, set()).difference(set((k,))) for k in
        #functools.reduce(set.union, data.values(), set(data.keys()))}
    # we don't need this
    data = data.copy()
    while True:
        ordered = set(item for item, dep in data.items() if not dep)
        if not ordered:
            break
        yield sorted(ordered)
        data = {item: (dep - ordered) for item, dep in data.items()
                if item not in ordered}
    if data:
        raise CircularDependencyError(data)


class FileRemover:
    def __init__(self):
        self.weak_references = dict()  # weak_ref -> filepath to remove

    def cleanup_once_done(self, response, filepath):
        wr = weakref.ref(response, self._do_cleanup)
        self.weak_references[wr] = filepath

    def _do_cleanup(self, wr):
        filepath = self.weak_references[wr]
        # shutil.rmtree(filepath, ignore_errors=True)
        os.unlink(filepath)


def merge_dicts(*dict_args):
    '''
    Given any number of dicts, shallow copy and merge into a new dict, \
    precedence goes to key value pairs in latter dicts.
    '''
    result = {}
    for dictionary in dict_args:
        result.update(dictionary)
    return result


def get_arch_name():
    """
    Detect architecture of the host machine

    :returns: architecture name
    """
    import platform
    uname_var = platform.machine() or platform.processor()
    if uname_var in ['x86_64', 'amd64']:
        return 'amd64'
    elif uname_var == 'aarch64':
        return 'arm64'  # FIXME: Don't know ...
    elif uname_var in ['armv7a', 'armv7l', 'armv8a', 'armv8l']:
        return 'armel'    # FIXME: Don't know too much about this...
    elif uname_var == 'mips64':
        return 'mips64el'  # FIXME: How about big endian...
    elif uname_var == 'mips':
        return 'mipsel'   # FIXME: This too...
    elif uname_var == 'ppc':
        return 'powerpc'
    elif uname_var == 'ppc64':
        return 'ppc64'
    elif uname_var == 'riscv64':
        return 'riscv64'
    else:
        return None
    return None


def err_msg(desc=None):
    """
    Print error message

    :param desc: description of the error message
    """
    if desc is None:
        print('\n')
        logging.error('Error occurred!')
    else:
        print('\n')
        logging.error(
            'Error occurred:\033[93m {} \033[0m'.format(desc))


def full_line_banner(msg, char='-'):
    """
    Print a full line banner with customizable texts

    :param msg: message you want to be printed
    """
    import shutil
    bars_count = int((shutil.get_terminal_size().columns - len(msg) - 2) / 2)
    bars = char*bars_count
    return ' '.join((bars, msg, bars))


def acbs_terminate(exit_code):
    sys.exit(exit_code)


def time_this(desc_msg, vars_ctx=None):
    def time_this_func(func):
        def dec_main(*args, **kwargs):
            import time
            now_time = time.time()
            ret = func(*args, **kwargs)
            time_span = time.time() - now_time
            if vars_ctx:
                if not vars_ctx.get('timings'):
                    vars_ctx.set('timings', [time_span])
                else:
                    vars_ctx.get('timings').append(time_span)
            logging.info(
                '>>>>>>>>> %s: %s' % (desc_msg, human_time(time_span)))
            return ret
        return dec_main
    return time_this_func


def human_time(full_seconds):
    """
    Convert time span (in seconds) to more friendly format

    :param seconds: Time span in seconds (decimal is acceptable)
    """
    import datetime
    out_str_tmp = '{}'.format(
        datetime.timedelta(seconds=full_seconds))
    out_str = out_str_tmp.replace(
        ':', ('{}:{}'.format(ANSIEscapes.ANSI_GREEN, ANSIEscapes.ANSI_RST)))
    return out_str


def format_column(data):
    output = ''
    col_width = max(len(str(word)) for row in data for word in row)
    for row in data:
        output = '%s%s\n' % (
            output, ('\t'.join(str(word).ljust(col_width) for word in row)))
    return output


def format_packages(*packages):
    return ', '.join('\033[36m%s\033[0m' % p for p in packages)


class ExternalLogFilter(logging.Filter):
    def filter(self, record):
        return record.name[0] != '_'


class JSONLogFormatter(logging.Formatter):
    def format(self, record):
        record.message = record.msg
        if record.exc_info:
            # Cache the traceback text to avoid converting it multiple times
            # (it's constant anyway)
            if not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)
        msg = collections.OrderedDict(record.msg)
        msg['timestamp'] = record.created
        msg.move_to_end('timestamp', last=False)
        if record.exc_text:
            msg['exception'] = record.exc_text.strip()
        return json.dumps(msg)


class ACBSColorFormatter(logging.Formatter):
    """
    ABBS-like format logger formatter class
    """

    def format(self, record):
        # FIXME: Let's come up with a way to simplify this ****
        lvl_map = {
            'WARNING': '{}WARN{}'.format(ANSIEscapes.ANSI_BROWN, ANSIEscapes.ANSI_RST),
            'INFO': '{}INFO{}'.format(ANSIEscapes.ANSI_LT_CYAN, ANSIEscapes.ANSI_RST),
            'DEBUG': '{}DEBUG{}'.format(ANSIEscapes.ANSI_GREEN, ANSIEscapes.ANSI_RST),
            'ERROR': '{}ERROR{}'.format(ANSIEscapes.ANSI_RED, ANSIEscapes.ANSI_RST),
            'CRITICAL': '{}CRIT{}'.format(ANSIEscapes.ANSI_YELLOW, ANSIEscapes.ANSI_RST)
            # 'FATAL': '{}{}WTF{}'.format(ANSIEscapes.ANSI_BLNK, ANSIEscapes.ANSI_RED,
            # ANSIEscapes.ANSI_RST),
        }
        if record.levelno in (logging.WARNING, logging.ERROR, logging.CRITICAL,
                              logging.INFO, logging.DEBUG):
            record.colorlevelname = lvl_map[record.levelname]
        return super(ACBSColorFormatter, self).format(record)


class ACBSTextLogFormatter(logging.Formatter):
    """
    Formatter class for stripping color codes
    """
    re_ansi = re.compile('\x1B\\[([0-9]{1,2}(;[0-9]{1,2})?)?[mGK]')

    def format(self, record):
        record.msg = self.re_ansi.sub('', record.msg)
        return super().format(record)

