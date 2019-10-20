import re
import sys
import enum
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


class Colors(enum.Enum):
    ANSI_RST = '\033[0m'
    ANSI_RED = '\033[91m'
    ANSI_BLNK = '\033[5m'
    ANSI_CYAN = '\033[36m'
    ANSI_LT_CYAN = '\033[96m'
    ANSI_GREEN = '\033[32m'
    ANSI_YELLOW = '\033[93m'
    ANSI_BLUE = '\033[34m'
    ANSI_BROWN = '\033[33m'


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


def list2str(list_in, sep=' '):
    """
    A simple conversion function to format `list` to `string` with given \
    seperator

    :param list_in: A list that needed to be formatted
    :param sep: Seperator, default is a single non-breaking space `' '`
    :returns: A formatted string
    :raises TypeError: `list_in` must be of `list` type
    """
    return sep.join(map(str, list_in))


def gen_laundry_list(items):
    """
    Generate a laundry list for Bash to interpret

    :param items: An array representing objects that needed to be collected \
    and interpreted
    :returns: A string which is a small Bash snipplet for interpreting.
    """
    # You know what, 'laundry list' can be a joke in somewhere...
    str_out = '\n\n'
    for i in items:
        str_out += 'echo \"%s\"=\"${%s}\"\n' % (i, i)
        # For example: `echo "VAR"="${VAR}"\n`
    return str_out


def test_progs(cmd, display=False):
    """
    Test if the given external program can run without flaw

    :param cmd: A list, the command-line arguments
    :param display: Whether to be displayed in the terminal
    :returns: Whether the external program exited successfully
    """
    try:
        if display is False:
            # _ = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
            proc = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            _, _ = proc.communicate()
            # Maybe one day we'll need its output...?
        else:
            subprocess.check_call(cmd)
    except Exception:
        return False

    return True


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


def str_split_to_list(str_in, sep=' '):
    """
    A simple stupid function to split strings

    :param str_in: A string to be splitted
    :param sep: Seperator
    :returns: A list
    """
    return list(filter(None, str_in.split(sep)))


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


def group_match(pattern_list, string, logic_method):
    """
    Match multiple patterns in one go.

    :param pattern_list: A list contains patterns to be used
    :param string: A string to be tested against
    :param logic_method: 1= OR'ed logic, 2= AND'ed logic
    :returns: Boolean, the test result
    :raises ValueError: pattern_list should be a `list` object, and \
    logic_method should be 1 or 2.
    """
    import re
    if not isinstance(pattern_list, list):
        raise ValueError()
    if logic_method == 1:
        for i in pattern_list:
            if re.match(i, string):
                return True
        return False
    elif logic_method == 2:
        for i in pattern_list:
            if not re.match(i, string):
                return False
        return True
    else:
        raise ValueError('...')
        return False


def full_line_banner(msg, char='-'):
    """
    Print a full line banner with customizable texts

    :param msg: message you want to be printed
    """
    import shutil
    bars_count = int((shutil.get_terminal_size().columns - len(msg) - 2) / 2)
    bars = char*bars_count
    return ' '.join((bars, msg, bars))


def random_msg():

    return ''


def sh_executor(sh_file, function, args, display=False):
    """
    Execute specified functions in external shell scripts with given args

    :param file: The full path to the script file
    :param function: The function need to be excute_code
    :param: args: The arguments that need to be passed to the function
    :param display: Wether return script output or display on screen
    :returns: Return if excution succeeded or return output per requested
    :raise FileNotFoundError: If script file doesn't exist, raise this.
    """
    with open(sh_file, 'rt') as f:
        sh_code = f.read()
    excute_code = '%s\n%s %s\n' % (sh_code, function, args)
    if display:
        try:
            subprocess.check_call(excute_code, shell=True)
        except subprocess.CalledProcessError:
            return False
        return True
    else:
        outs, errs = subprocess.Popen(
            ('bash',), stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE).communicate(excute_code.encode('utf-8'))
    return outs.decode('utf-8')


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
        ':', ('{}:{}'.format(Colors.ANSI_GREEN, Colors.ANSI_RST)))
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


class ACBSVariables(object):

    buffer = {}

    def __init__(self):
        return

    @classmethod
    def get(cls, var_name):
        return cls.buffer.get(var_name)

    @classmethod
    def set(cls, var_name, value):
        cls.buffer[var_name] = value
        return


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
            'WARNING': '{}WARN{}'.format(Colors.ANSI_BROWN, Colors.ANSI_RST),
            'INFO': '{}INFO{}'.format(Colors.ANSI_LT_CYAN, Colors.ANSI_RST),
            'DEBUG': '{}DEBUG{}'.format(Colors.ANSI_GREEN, Colors.ANSI_RST),
            'ERROR': '{}ERROR{}'.format(Colors.ANSI_RED, Colors.ANSI_RST),
            'CRITICAL': '{}CRIT{}'.format(Colors.ANSI_YELLOW, Colors.ANSI_RST)
            # 'FATAL': '{}{}WTF{}'.format(Colors.ANSI_BLNK, Colors.ANSI_RED,
            # Colors.ANSI_RST),
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

