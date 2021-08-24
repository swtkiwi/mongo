import os
import pprint
import pymongo
import random
import shutil
import sys
import threading
import time

from buildscripts.resmokelib.core import process
from buildscripts.resmokelib.testing.hooks import interface

def validate(mdb, logger, orig_port):
    for db in mdb.database_names():
        for coll in mdb.get_database(db).list_collection_names():
            resp = mdb.get_database(db).command({"validate": coll}, check=False)
            if "fsmdb" in db or coll == "transactions":
                logger.info("DBG. Port: {} Resp: {}".format(orig_port, resp))
            if resp['ok'] != 1.0 or resp['valid'] != True:
                if 'code' in resp and resp['code'] == 166:
                    pass
                else:
                    logger.info("DBG. FAILURE!\nCmdLineOpts: {}\nValidate Response: {}",
                                pprint.pformat(mdb.admin.command("getCmdLineOpts")),
                                pprint.pformat(resp))
                    return False
    return True

class BGJob(threading.Thread):
    def __init__(self, hook):
        threading.Thread.__init__(self, name="SimulateCrashes")
        self.daemon = True
        self._hook = hook
        self._lock = threading.Lock()
        self._is_alive = True
        self.backup_num = 0
        self.found_error = False

    def run(self):
        while True:
            with self._lock:
                if self.is_alive == False:
                    break

            self._hook.pause_and_copy(self.backup_num)
            if not self._hook.validate_all(self.backup_num):
                self.found_error = True
                self._hook.running_test.fixture.teardown()
                self.is_alive = False
                return
            time.sleep(random.randint(1, 5))
            self.backup_num += 1

    def kill(self):
        with self._lock:
            self.is_alive = False

class SimulateCrashHook(interface.Hook):
    IS_BACKGROUND = True

    def __init__(self, hook_logger, fixture):
        interface.Hook.__init__(self, hook_logger, fixture, "Simulate crashes by sigstopping and copying the datafiles with DirectI/O")
        self.logger = hook_logger
        self.last_validate_port = 19000

    def has_failed(self):
        return self.found_error

    def before_suite(self, test_report):
        """Test runner calls this exactly once before they start running the suite."""
        shutil.rmtree('./tmp', ignore_errors=True)

        self.logger.info("DBG. Starting the SimulateCrashes thread.")
        self._background_job = BGJob(self)
        self._background_job.start()

    def pause_and_copy(self, backup_num):
        self.logger.info("DBG. Taking snapshot #{}".format(backup_num))
        cpy = [x for x in self.fixture.nodes]
        random.shuffle(cpy)

        for node in cpy:
            node.mongod.pause()

            try:
                for tup in os.walk(node._dbpath, followlinks=True):
                    if tup[0].endswith('/diagnostic.data') or tup[0].endswith('/_tmp'):
                        continue
                    for filename in tup[-1]:
                        if 'Preplog' in filename:
                            continue
                        fqfn = '/'.join([tup[0], filename])
                        self.copy_file(node._dbpath, fqfn, './tmp/d{}/{}'.format(node.port, backup_num))
            finally:
                node.mongod.resume()


    def copy_file(self, root, fqfn, new_root):
        # in_fd = os.open(fqfn, os.O_RDONLY | os.O_DIRECT)
        in_fd = os.open(fqfn, os.O_RDONLY)
        in_bytes = os.stat(in_fd).st_size

        rel = fqfn[len(root):]
        os.makedirs(new_root + '/journal', exist_ok=True)
        out_fd = os.open(new_root + rel, os.O_WRONLY | os.O_CREAT)
        os.sendfile(out_fd, in_fd, 0, in_bytes)
        os.close(out_fd)
        os.close(in_fd)

    def validate_all(self, backup_num):
        for node in self.fixture.nodes:
            if self.last_validate_port >= 20000:
                self.last_validate_port = 19000
            validate_port = self.last_validate_port
            self.last_validate_port += 1

            path = "tmp/d{}/{}".format(node.port, backup_num)
            self.logger.info("DBG. Starting to validate. DBPath: {} Port: {}".format(path, validate_port))
            mdb = process.Process(self.logger,
                                  ['./mongod',
                                   '--dbpath', path,
                                   '--port', str(validate_port),
                                   '--logpath', 'tmp/validate.log'])
            mdb.start()
            client = pymongo.MongoClient('localhost:{}'.format(validate_port))
            is_valid = validate(client, self.logger, node.port)
            mdb.stop()
            if not is_valid:
                return False
            shutil.rmtree(path, ignore_errors=True)
            return True

    def after_suite(self, test_report):
        """Invoke by test runner calls this exactly once after all tests have finished executing.
        Be sure to reset the behavior back to its original state so that it can be run again.
        """
        self._background_job.kill()

        self._background_job.join()

    def before_test(self, test, test_report):
        """Each test will call this before it executes."""
        self.running_test = test

    def after_test(self, test, test_report):
        """Each test will call this after it executes."""
        pass