#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import typing
import sqlite3
import traceback
import collections
import logging
import logging.handlers

from . import utils
from . import abbsmeta


class PackageBuildResult(typing.NamedTuple):
    src_name: str  # base-devel/llvm
    package_name: str # llvm-runtime
    exit_code: int
    digest: str
    issues: list
    start: float
    build_time: float


class BuildManager:
    def __init__(self, tree, log_dir, cache_dir, build_dir,
                 keep_build_dir, log_level=logging.INFO):
        self.tree = tree
        self.log_dir = log_dir
        self.cache_dir = cache_dir
        self.build_dir = build_dir
        self.keep_build_dir = keep_build_dir
        self.log_level = log_level

        os.makedirs(log_dir, exist_ok=True)
        os.makedirs(build_dir, exist_ok=True)
        meta_dir = os.path.join(cache_dir, 'meta')
        os.makedirs(meta_dir, exist_ok=True)

        self.install_logger(self.log_level)
        self.logger = logging.getLogger('manager')
        tree_fullpath = os.path.abspath(tree.rstrip('/'))
        dbfile = os.path.join(meta_dir, '%s-%s.db' % (
            os.path.basename(tree_fullpath), utils.hashtag(tree_fullpath))
        self.dbfile = dbfile
        self.db = None

    def install_logger(self, level=logging.INFO):
        root_logger = logging.getLogger()
        root_logger.setLevel(0)  # Set to lowest to bypass the initial filter
        str_handler = logging.StreamHandler()
        str_handler.setLevel(level)
        str_handler.addFilter(utils.ExternalLogFilter())
        str_handler.setFormatter(utils.ACBSColorFormatter(
            '[%(colorlevelname)s]: %(message)s'))
        root_logger.addHandler(str_handler)
        log_file_handler = logging.FileHandler(
            os.path.join(self.log_loc, 'adbs.log'))
        log_file_handler.setLevel(level)
        log_file_handler.addFilter(utils.ExternalLogFilter())
        log_file_handler.setFormatter(utils.ACBSTextLogFormatter(
            '%(asctime)s [%(levelname).1s:%(name).8s] %(message)s'))
        root_logger.addHandler(log_file_handler)
        for logger_name in ('_package_result', '_recipe_result'):
            result_logger = logging.getLogger(logger_name)
            result_log_handler = logging.FileHandler(
                os.path.join(self.log_loc, '%s.log' % logger_name))
            result_log_handler.setLevel(level)
            result_log_handler.setFormatter(utils.JSONLogFormatter())
            result_logger.addHandler(result_log_handler)

    def build(self, names):
        if self.db:
            self.db.close()
        repo = abbsmeta.LocalRepo(tree, dbfile)
        repo.update()
        self.db = repo.db
        self.db.enable_load_extension(True)
        self.db.execute("SELECT load_extension('./mod_vercomp.so')")
        self.db.enable_load_extension(False)

        recipe = SuiteBuildRecipe(self.tree, self.db, names)
        self.logger.info('Preparing the build recipe for %s',
            utils.comma_list(names))
        recipe.prepare()

    def clean_build(self):
        shutil.rmtree(self.build_dir)


class SuiteBuildRecipe:
    def __init__(self, tree, db, names, revdeps=False, solve_circular=True):
        self.tree = tree
        self.db = db
        self.names = names
        self.revdeps = revdeps
        self.solve_circular = solve_circular

    def find_package(self, name, try_group=True):
        cur = self.db.cursor()
        category = section = directory = None
        namespl = name.split('/', 1)
        if len(namespl) == 1:
            directory = name
        else:
            secpath, directory = namespl
            if any(secpath.startswith(x) for x in abbsmeta.abbs_categories):
                category, section = secpath.split('-', 1)
            else:
                category, section = None, secpath
        sql = """
            SELECT
              coalesce(category || '-', '') || section || '/' || directory
              AS src_name
            FROM packages
            WHERE directory=?
        """
        params = [directory]
        if section:
            sql += ' AND section=?'
            params.append(section)
        if category:
            sql += ' AND category=?'
            params.append(category)
        row = cur.execute(sql, params).fetchone()
        if row:
            return row[0], False
        if section == 'groups' or (try_group and section is None):
            return directory, True
        raise ValueError('source package %s not found' % name)

    def find_packages(self, names, group_path=()):
        src_packages = []
        groups = []
        for name in names:
            result, isgroup = self.find_package(name, not group_path)
            if isgroup:
                groups.append(result)
            else:
                src_packages.append(result)
        for group in groups:
            if group in group_path:
                raise ValueError(
                    'group dependency loop found: %s -> %s' % (group_path, group))
            groupfile = os.path.join(self.tree, 'groups', directory)
            if os.path.isfile(groupfile):
                with open(groupfile, 'r', encoding='utf-8') as f:
                    names = [ln.strip() for ln in f]
                src_packages.extend(self.find_packages(names, group_path + (group,)))
            else:
                raise ValueError('source package %s not found' % group)
        return src_packages

    def install_deps(self):
        cur.execute("""
SELECT d.package, d.dependency
FROM package_dependencies d
INNER JOIN v_packages v2 ON v2.name=d.dependency
AND compare_dpkgrel(v2.full_version, d.relop, d.version)
WHERE d.relationship IN ('PKGDEP', 'BUILDDEP') AND d.package!=d.dependency

        
        """)


    sobreaks = []
    circular = None
    cur = self.repo.db.cursor()
    cur.execute("SELECT dep_package, deplist FROM v_so_breaks_dep "
        "WHERE package=%s", (name,))
    res = {k: set(v) for k, v in cur}
    try:
        for level in utils.toposort(res):
            sobreaks.append(level)
    except utils.CircularDependencyError as ex:
        circular = ex.data
    sobreaks.reverse()
    if circular:
        circular = sorted(circular.keys())



        ...

    def get_sources(self):
        ...

    def build_list(self):
        ...
        # yield group

    #input_names: list
    #src_packages: list
    #dependencies: dict
    #installed: list
    #build_list: list


class PackageBuilder:
    def __init__(self, manager, package):
        self.manager = manager
        self.package = package

        self.start_time = None
        self.build_time = None

        self.result_logger = logging.getLogger('_result')

    def log_result(self, exit_code=0, digest='', issues=()):
        ####
        msg = {}
        if sys.exc_info()[0]:
            self.result_logger.exception(msg)
        else:
            self.result_logger.info(msg)

    def init(self):
        sys.excepthook = self.acbs_except_hdr
        print(utils.full_line_banner(
            'Welcome to ACBS - {}'.format(self.acbs_version), '='))
        if self.isdebug:
            str_verbosity = logging.DEBUG
        else:
            str_verbosity = logging.INFO
        try:
            for dir_loc in [self.dump_loc, self.tmp_loc, self.conf_loc,
                            self.log_loc]:
                if not os.path.isdir(dir_loc):
                    os.makedirs(dir_loc)
        except Exception:
            raise IOError('\033[93mFailed to make work directory\033[0m!')
        self.__install_logger(str_verbosity)
        Misc().dev_utilz_warn()
        forest_file = os.path.join(self.conf_loc, 'forest.conf')
        if os.path.exists(os.path.join(self.conf_loc, 'forest.conf')):
            self.tree_loc = os.path.abspath(parse_acbs_conf(forest_file, self.tree))
            if not self.tree_loc:
                raise ACBSConfError('Tree not found!')
        else:
            self.tree_loc = os.path.abspath(write_acbs_conf(forest_file))

        # @LoaderHelper.register('after_build_finish')
        # def fortune():
        #     Fortune().get_comment()

        LoaderHelper.callback('after_init')


    def build(self):
        LoaderHelper.callback('before_build_init')
        pkgs_to_build = []
        for pkg in self.pkgs_name:
            matched_pkg = Finder(
                pkg, search_path=self.tree_loc).acbs_pkg_match()
            if isinstance(matched_pkg, list):
                logging.info('Package build list found: \033[36m%s (%s)\033[0m' %
                             (os.path.basename(pkg), len(matched_pkg)))
                pkgs_to_build.extend(matched_pkg)
            elif not matched_pkg:
                raise ACBSGeneralError(
                    'No valid candidate package found for %s.' %
                    utils.format_packages(pkg))
            else:
                pkgs_to_build.append(matched_pkg)
        for pkg in pkgs_to_build:
            self.pkgs_que.update(pkg)
            self.build_pkg_group(pkg)
        print(utils.full_line_banner('Build Summary:', '='))
        self.print_summary()
        LoaderHelper.callback('after_build_finish')
        return 0

    def print_summary(self):
        i = 0
        group_name = None
        prev_group_name = None
        accum = 0.0

        def swap_vars(prev_group_name):
            ACBSVariables.get('timings').insert(i, accum)
            self.pkgs_done.insert(i, prev_group_name)
            prev_group_name = group_name

        for it in self.pkgs_done:
            if it.find('::') > -1:
                group_name, sub_name = it.split('::')
                if prev_group_name and (group_name != prev_group_name):
                    swap_vars(prev_group_name)
                if sub_name == 'autobuild':
                    self.pkgs_done.remove(it)
                else:
                    accum += ACBSVariables.get('timings')[i]
                i += 1
        if group_name:
            swap_vars(group_name)        
        if self.download_only:
            x = [[name, 'Downloaded'] for name in self.pkgs_done]
        else:
            x = [[name, utils.human_time(time)] for name, time in zip(
                self.pkgs_done, ACBSVariables.get('timings'))]
        print(utils.format_column(x))
        return

    def build_main(self, pkg_data):
        # , target, tmp_dir_loc=[], skipbuild=False, groupname=None
        skipbuild = self.download_only
        pkg_name = pkg_data.pkg_name
        if not skipbuild:
            try_build = Dependencies().process_deps(
                pkg_data.build_deps, pkg_data.run_deps, pkg_name)
            if try_build:
                logging.info('Dependencies to build: ' +
                    utils.format_packages(try_build))
                if set(try_build).intersection(self.pending):
                    # Suspect this is dependency loop
                    err_msg = 'Dependency loop: %s' % '<->'.join(self.pending)
                    utils.err_msg(err_msg)
                    raise ACBSGeneralError(err_msg)
                self.new_build_thread(pkg_name, try_build)
        repo_ab_dir = pkg_data.ab_dir()
        if not skipbuild:
            ab3 = Autobuild(pkg_data.temp_dir, repo_ab_dir, pkg_data)
            ab3.copy_abd()
            ab3.timed_start_ab3()
        self.pkgs_done.append(
            pkg_data.directory if pkg_data.subdir == 'autobuild'
            else '%s::%s' % (pkg_data.directory, pkg_data.subdir))

    def build_pkg_group(self, directory):
        logging.info('Start building ' + utils.format_packages(directory))
        os.chdir(self.tree_loc)
        pkg_group = ACBSPackageGroup(directory, rootpath=self.tree_loc)
        #pkg_type_res = Finder.determine_pkg_type(directory)
        #if isinstance(pkg_type_res, dict):
            #return self.build_pkg_group1(pkg_type_res, directory)  # FIXME
        logging.info('Downloading sources...')
        src_fetcher = SourceFetcher(
            pkg_group.pkg_name, pkg_group.abbs_data, self.dump_loc)
        pkg_group.src_name = src_fetcher.fetch_src()
        pkg_group.src_path = self.dump_loc
        pkg_group.temp_dir = SourceProcessor(
            pkg_group, self.dump_loc, self.tmp_loc).process()
        subpkgs = pkg_group.subpackages()
        isgroup = (len(subpkgs) > 1)
        if isgroup:
            logging.info('Package group\033[36m({})\033[0m detected: '
                'contains: {}'.format(
                len(subpkgs), utils.format_packages(*(p.pkg_name for p in subpkgs))))
        for pkg_data in subpkgs:
            print(utils.full_line_banner('%s::%s' % (directory, pkg_data.pkg_name)))
            self.build_main(pkg_data)
        return 0

    def new_build_thread(self, current_pkg, try_build):
        def slave_thread_build(pkg, shared_error):
            logging.debug(
                'New build thread started for ' + utils.format_packages(pkg))
            try:
                new_build_instance = BuildCore(
                    **self.acbs_settings, pkgs_name=[pkg], init=False,
                    pending=self.pending + (current_pkg,))
                new_build_instance.tree_loc = self.tree_loc
                new_build_instance.shared_error = shared_error
                new_build_instance.build()
            except Exception as ex:
                shared_error.set()
                raise
            return
        from multiprocessing import Process, Event, Lock
        self.shared_error = Event()
        for sub_pkg in list(try_build):
            dumb_mutex = Lock()
            dumb_mutex.acquire()
            sub_thread = Process(
                target=slave_thread_build, args=(sub_pkg, self.shared_error))
            sub_thread.start()
            sub_thread.join()
            dumb_mutex.release()
            if self.shared_error.is_set():
                raise ACBSGeneralError(
                    'Sub-build process building {} \033[93mfailed!\033[0m'.format(
                    utils.format_packages(sub_pkg)))

    def acbs_except_hdr(self, type, value, tb):
        if self.isdebug:
            sys.__excepthook__(type, value, tb)
        else:
            print()
            logging.fatal('Oops! \033[93m%s\033[0m: %s' % (
                str(type.__name__), str(value)))
        logging.error('Traceback:\n' + ''.join(traceback.format_tb(tb)))


def main(args):
    return 0

if __name__ == '__main__':
    import sys
    sys.exit(main(sys.argv))
