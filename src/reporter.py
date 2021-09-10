import os
import time
from datetime.datetime import now

from watchdog.events import PatternMatchingEventHandler
from watchdog.observers import Observer


class JobsList:
    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(JobsList, cls).__new__(cls)
            cls.instance.jobs_per_host = {}
            cls.instance.host_per_job = {}
        return cls.instance

    def insert_local_job(self, job_id, host_id):
        if host_id not in self.jobs.keys():
            self.jobs[host_id] = []
        self.jobs[host_id].append(job_id)
        self.host_per_job[job_id] = host_id

    def insert_remote_job(self, job_id, host_id):
        if host_id not in self.jobs.keys():
            self.jobs[host_id] = []
        self.jobs[host_id].append(job_id)
        self.host_per_job[job_id] = host_id

    def remove_local_job(self, job_id):
        if job_id in self.host_per_job.keys():
            host_id = self.host_per_job[job_id]
            self.jobs[host_id].remove(job_id)

    def remove_remote_job(self, job_id):
        if job_id in self.host_per_job.keys():
            host_id = self.host_per_job[job_id]
            self.jobs[host_id].remove(job_id)


def local_created_on_created(event):
    job_id, host_id = event.src_path.split('|')[1], event.src_path.split('|')[2]
    jobs_list = JobsList()
    jobs_list.insert_local_job(job_id, host_id)
    os.remove(event.src_path)


def local_done_on_created(event):
    job_id = event.src_path.split('|')[1]
    jobs_list = JobsList()
    jobs_list.remove_local_job(job_id)
    os.remove(event.src_path)


def remote_created_on_created(event):
    job_id, host_id = event.src_path.split('|')[1], event.src_path.split('|')[2]
    jobs_list = JobsList()
    jobs_list.insert_local_job(job_id, host_id)
    os.remove(event.src_path)


def remote_done_on_created(event):
    job_id = event.src_path.split('|')[1]
    jobs_list = JobsList()
    jobs_list.remove_remote_job(job_id)
    os.remove(event.src_path)


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
    path = "../builddir/"
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
            time.sleep(0.1)
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
