import os
import time
import threading
from datetime import datetime

from watchdog.events import PatternMatchingEventHandler
from watchdog.observers import Observer

from elasticsearch_dsl import Document, Keyword, Date, Integer
from elasticsearch_dsl.connections import connections

ELASTIC_ADDRESS = ['https://vpc-hailotech-elk-3zq2kqyel3qe54wzfr6v6jacse.eu-central-1.es.amazonaws.com:443']


class HostStatus(Document):
    host = Keyword()
    time = Date()
    jobs = Integer()

    class Index:
        name = 'dist_compilation_status'

    class Meta:
        doc_type = '_doc'


class JobsList:
    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(JobsList, cls).__new__(cls)
            cls.instance.jobs_per_host = {}
            cls.instance.jobs_lock = threading.Lock()
            cls.instance.host_per_job = {}
            cls.instance.host_lock = threading.Lock()
        return cls.instance

    def update_elastic(self, host_id):
        connections.create_connection(hosts=ELASTIC_ADDRESS)
        current_status = HostStatus(
            host=host_id,
            time=datetime.now(),
            jobs=len(self.jobs_per_host[host_id])
        )
        current_status.save()

    def update_all(self):
        tmp_hosts = self.jobs_per_host.keys().copy()
        for host in tmp_hosts:
            self.update_elastic(host)

    def insert_local_job(self, job_id, host_id):
        self.jobs_lock.acquire()
        if host_id not in self.jobs_per_host.keys():
            self.jobs_per_host[host_id] = []
        self.jobs_per_host[host_id].append(job_id)
        self.jobs_lock.release()
        self.host_lock.acquire()
        self.host_per_job[job_id] = host_id
        self.host_lock.release()

    def insert_remote_job(self, job_id, host_id):
        self.jobs_lock.acquire()
        if host_id not in self.jobs_per_host.keys():
            self.jobs_per_host[host_id] = []
        self.jobs_per_host[host_id].append(job_id)
        self.jobs_lock.release()
        self.host_lock.acquire()
        self.host_per_job[job_id] = host_id
        self.host_lock.release()

    def remove_local_job(self, job_id):
        if job_id in self.host_per_job.keys():
            host_id = self.host_per_job[job_id]
            self.jobs_lock.acquire()
            self.jobs_per_host[host_id].remove(job_id)
            self.jobs_lock.release()

    def remove_remote_job(self, job_id):
        if job_id in self.host_per_job.keys():
            host_id = self.host_per_job[job_id]
            self.jobs_lock.acquire()
            self.jobs_per_host[host_id].remove(job_id)
            self.jobs_lock.release()


def local_created_on_created(event):
    print("Local job created")
    os.remove(event.src_path)
    job_id, host_id = event.src_path.split('|')[1], event.src_path.split('|')[2]
    jobs_list = JobsList()
    jobs_list.insert_local_job(job_id, host_id)


def local_done_on_created(event):
    print("Local job done")
    os.remove(event.src_path)
    job_id = event.src_path.split('|')[1]
    jobs_list = JobsList()
    jobs_list.remove_local_job(job_id)


def remote_created_on_created(event):
    print("Remote job created")
    os.remove(event.src_path)
    job_id, host_id = event.src_path.split('|')[1], event.src_path.split('|')[2]
    jobs_list = JobsList()
    jobs_list.insert_local_job(job_id, host_id)


def remote_done_on_created(event):
    print("Remote job done")
    os.remove(event.src_path)
    job_id = event.src_path.split('|')[1]
    jobs_list = JobsList()
    jobs_list.remove_remote_job(job_id)


def main():
    # Create handler with patterns
    local_created_handler = PatternMatchingEventHandler(['LOCAL_JOB_BEGIN|*'])
    local_done_handler = PatternMatchingEventHandler(['LOCAL_JOB_DONE|*'])
    remote_created_handler = PatternMatchingEventHandler(['JOB_BEGIN|*'])
    remote_done_handler = PatternMatchingEventHandler(['JOB_DONE|*'])

    # Define on_created behaviors
    local_created_handler.on_created = local_created_on_created
    local_done_handler.on_created = local_done_on_created
    remote_created_handler.on_created = remote_created_on_created
    remote_done_handler.on_created = remote_done_on_created

    # Create observers
    path = "../builddir/.logs/"
    if not os.path.isdir(path):
        os.mkdir(path)
    local_created_observer = Observer()
    local_done_observer = Observer()
    remote_created_observer = Observer()
    remote_done_observer = Observer()

    # Schedule observers
    local_created_observer.schedule(local_created_handler, path)
    local_done_observer.schedule(local_done_handler, path)
    remote_created_observer.schedule(remote_created_handler, path)
    remote_done_observer.schedule(remote_done_handler, path)

    # Start observers
    local_created_observer.start()
    local_done_observer.start()
    remote_created_observer.start()
    remote_done_observer.start()

    try:
        while True:
            time.sleep(1)
            jobs_list = JobsList()
            jobs_list.update_all()
    except KeyboardInterrupt:
        local_created_observer.stop()
        local_done_observer.stop()
        remote_created_observer.stop()
        remote_done_observer.stop()
        local_created_observer.join()
        local_done_observer.join()
        remote_created_observer.join()
        remote_done_observer.join()


if __name__ == "__main__":
    main()
