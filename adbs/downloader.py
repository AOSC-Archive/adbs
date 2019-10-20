import os
import shutil
import logging
import contextlib
import subprocess
import concurrent.futures

from . import utils

logger = logging.getLogger('acbs.downloader')


class BaseDownloader:
    program_name = None
    default_params = ()
    priority = 0

    @classmethod
    def is_available(cls):
        return bool(shutil.which(cls.program_name))

    @classmethod
    def dispatch(cls, method):
        impl = getattr(cls, '_dispatch_cached', None)
        if impl is None:
            for impl in sorted(cls.__subclasses__(), key=lambda x: -x.priority):
                if impl.is_available():
                    cls._dispatch_cached = impl
                    break
            else:
                raise NotImplementedError('no available implementation found')
        return getattr(impl, method)

    def run(self, cmd, cwd=None, input=None):
        return subprocess.run(
            (self.program_name,) + self.default_params + cmd,
            check=True, cwd=cwd, input=input
        )

    def close(self):
        pass


class FileDownloader(BaseDownloader):
    def get_file(self, url, filename):
        return self.dispatch('get_file')(url, filename)


class BatchFileDownloader(BaseDownloader):
    def __init__(self, max_concurrent=8, executor=None):
        self.max_concurrent = max_concurrent
        self.executor = executor

    def get_files(self, files):
        """
        Get all the files specified.
        `files` is a list of tuples: [(filename1, url1), (filename2, url2)]
        """
        return self.dispatch('get_files')(files)


class WgetDownloader(FileDownloader):
    program_name = 'wget'
    default_params = ('-c',)
    priority = 1

    def get_file(self, url, filename):
        self.run(('-O', filename, url))


class AxelDownloader(FileDownloader):
    program_name = 'axel'
    default_params = ('-n', '4', '-a')
    priority = 3

    def get_file(self, url, filename):
        self.run(('-o', filename, url))


class Aria2Downloader(BatchFileDownloader):
    program_name = 'aria2c'
    default_params = ()
    priority = 2

    def get_files(self, files):
        inputfile = []
        for filename, url in files:
            inputfile.append(url)
            inputfile.append(' out=' + filename)
        inputfile.append('')
        inputstr = '\n'.join(inputfile).encode()
        self.run(('-i', '-'), input=inputstr)


class MultiprocessDownloader(BatchFileDownloader):
    downloader = FileDownloader
    priority = 1

    def __init__(self, max_concurrent=8, executor=None):
        self.max_concurrent = max_concurrent
        if executor is None:
            self._own_executor = True
            self.executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=max_concurrent)
        else:
            self._own_executor = False
            self.executor = executor

    def get_files(self, files):
        futures = []
        for filename, url in files:
            futures.append(self.executor.submit(
                self.downloader.get_file, url, filename))
        done, not_done = concurrent.futures.wait(futures)
        for future in done:
            # raise errors
            future.result()

    def close(self):
        if self._own_executor:
            self.executor.shutdown()


class VCSDownloader(BaseDownloader):
    def get_repo(self, path, url, checkout=None, branch=None):
        if os.path.isdir(path):
            self.clone(url, path)
        else:
            self.update(path)
        if checkout or branch:
            self.checkout(checkout, branch)

    def clone(self, url, path):
        raise NotImplementedError

    def update(self, path):
        raise NotImplementedError

    def checkout(self, path, name=None, branch=None):
        # branch may be ignored by some vcs.
        raise NotImplementedError


class SVNDownloader(VCSDownloader):
    program_name = 'svn'

    def clone(self, url, path):
        self.run(('co', url, path))

    def update(self, path):
        self.run(('up',), path)

    def checkout(self, path, name=None, branch=None):
        if branch:
            self.run(('switch', branch), path)
        if name:
            self.run(('up', '-r' + name), path)


class GitDownloader(VCSDownloader):
    program_name = 'git'

    def clone(self, url, path):
        self.run(('clone', '--recursive', url, path))

    def update(self, path):
        self.run(('fetch', '--all', '--recurse-submodules=on-demand'), path)

    def checkout(self, path, name=None, branch=None):
        self.run(('checkout', '--recurse-submodules', name or branch), path)


class MercurialDownloader(VCSDownloader):
    program_name = 'hg'

    def clone(self, url, path):
        self.run(('clone', url, path))

    def update(self, path):
        self.run(('update',), path)

    def checkout(self, path, name=None, branch=None):
        self.run(('checkout', name or branch), path)


class FossilDownloader(VCSDownloader):
    program_name = 'fossil'

    def clone(self, url, path):
        os.mkdir(path)
        self.run(('clone', url, '.fossil'), path)
        self.run(('open', '.fossil'), path)

    def update(self, path):
        self.run(('update',), path)

    def checkout(self, path, name=None, branch=None):
        self.run(('update', name or branch), path)


class BazaarBaseDownloader(VCSDownloader):
    def clone(self, url, path):
        return self.dispatch('_clone')(url, path)

    def update(self, path):
        return self.dispatch('_update')(path)

    def checkout(self, path, name=None, branch=None):
        return self.dispatch('_checkout')(path, name, branch)

    def _clone(self, url, path):
        self.run(('branch', url, path))

    def _update(self, path):
        self.run(('update',), path)

    def _checkout(self, path, name=None, branch=None):
        if branch:
            self.run(('switch', branch), path)
        if name:
            self.run(('revert', '-r' + name), path)


class BazaarDownloader(BazaarBaseDownloader):
    program_name = 'bzr'
    priority = 1


class BreezyDownloader(BazaarBaseDownloader):
    # Breezy is a maintained fork of Bazaar
    program_name = 'brz'
    priority = 2


VCS_MAP = {
    'SVN': SVNDownloader(),
    'GIT': GitDownloader(),
    'HG': MercurialDownloader(),
    'BZR': BazaarBaseDownloader(),
    'FSL': FossilDownloader(),
}


def download_name(package, url, tarball=True):
    if tarball:
        return '%s_%s_%s' % (package.name, package.version, utils.hashtag(url))
    else:
        return '%s_%s' % (package.name, utils.hashtag(url))


def fetch_src(packages, path, max_concurrent=8):
    tasks = []  # (package, dirname, task)
    result_paths = []  # (package, path, istarball)
    tarballs = []
    for package in packages:
        if package.spec.get('DUMMYSRC'):
            continue
        url = package.spec.get('SRCTBL')
        if url:
            fname = os.path.join(path, download_name(package, url, True))
            tarballs.append((fname, url))
            result_paths.append((package, fname, True))
            if os.path.isfile(fname + '.failed'):
                with contextlib.suppress(FileNotFoundError):
                    os.remove(fname + '.failed')
                with contextlib.suppress(FileNotFoundError):
                    os.remove(fname)
            continue
        for srctype, downloader in VCS_MAP.items():
            url = package.spec.get(srctype + 'SRC')
            if url is None:
                continue
            dirname = os.path.join(path, download_name(package, url, False))
            checkout = package.spec.get(srctype + 'CO')
            branch = package.spec.get(srctype + 'BRCH')
            tasks.append((package, dirname,
                (downloader.get_repo, dirname, url, checkout, branch)))
            result_paths.append((package, dirname, False))
            break
        else:
            raise utils.MetadataError(package, 101, 'no source defined')
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrent) as executor:
        batch_downloader = BatchFileDownloader(max_concurrent, executor)
        if tarballs:
            tasks.append((
                None, dirname, (batch_downloader.get_files, tarballs), None))
        futures = [(package, dirname, executor.submit(*task))
                   for package, dirname, task in tasks]
        for package, dirname, future in futures:
            try:
                future.result()
            except Exception:
                if package:
                    logger.exception('%s download error' % (
                        package.secpath + '/' + package.directory))
                    if dirname and os.path.isdir(dirname):
                        shutil.rmtree(dirname)
                else:
                    logger.exception('tarballs download error')
        done, not_done = concurrent.futures.wait(futures)
        try:
            for future in done:
                future.result()
        except Exception:
            logger.exception('download error')
            # check results later
    excs = []
    for package, path, istarball in result_paths:
        if istarball:
            if not os.path.isfile(path):
                excs.append(utils.BuildProcessError(
                    package, 201, 'failed to get tarball'))
                continue
            hash_type = 'sha256'
            target_hash = None
            chksum_spec = package.spec.get('CHKSUM')
            if chksum_spec:
                hash_type, hash_value = chksum_spec.split('::')
            target_hash = utils.chksum_file(hash_type, path)
            if chksum_spec is None:
                excs.append(utils.MetadataError(package, 113,
                    'no checksum found for tarball. fix: CHKSUM=%s::%s' % (
                    hash_type, target_hash)))
                utils.touch(path + '.failed')
                continue
            elif hash_value != target_hash:
                excs.append(utils.BuildProcessError(package, 201,
                    'tarball checksum mismatch. fix: CHKSUM=%s::%s' % (
                    hash_type, target_hash)))
                utils.touch(path + '.failed')
                continue
        else:
            if not os.path.isdir(path):
                excs.append(utils.BuildProcessError(
                    package, 201, 'failed to get vcs source'))
    if excs:
        raise utils.MultipleBuildError(excs)

