import os
import glob
import time
from datetime import datetime

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
    def __init__(self):
        self.jobs_per_host = {}
        self.host_per_job = {}
        self.total_created = 0
        self.total_done = 0
        self.need_update = False

    def update_elastic(self, host_id):
        connections.create_connection(hosts=ELASTIC_ADDRESS)
        current_status = HostStatus(
            host=host_id,
            time=datetime.now(),
            jobs=self.jobs_per_host[host_id]
        )
        current_status.save()

    def update_all(self):
        if self.need_update:
            tmp_hosts = list(self.jobs_per_host.keys())
            for host in tmp_hosts:
                self.update_elastic(host)

    def print_status(self):
        tmp_status = self.jobs_per_host.copy()
        for host in tmp_status:
            print(f'{host}: {tmp_status[host]}')
        print(f'TOTAL CREATED: {self.total_created}')
        print(f'TOTAL DONE:    {self.total_done}')

    def insert_job(self, job_id, host_id):
        if host_id not in self.jobs_per_host.keys():
            self.jobs_per_host[host_id] = 0
        self.jobs_per_host[host_id] += 1
        self.host_per_job[job_id] = host_id
        self.total_created += 1
        self.need_update = True

    def remove_job(self, job_id):
        if job_id in self.host_per_job.keys():
            host_id = self.host_per_job.pop(job_id)
            self.jobs_per_host[host_id] = max(self.jobs_per_host[host_id] - 1, 0)
            self.total_done += 1
            self.need_update = True
        else:
            print("JOB NOT FOUND!", job_id)


def job_begin(filename, jobs_list):
    print("job created")
    os.remove(filename)
    job_id, host_id = filename.replace('.txt', '').split('%')[1], filename.replace('.txt', '').split('%')[2]
    jobs_list.insert_job(job_id, host_id)


def job_done(filename, jobs_list):
    print("job done")
    os.remove(filename)
    job_id = filename.replace('.txt', '').split('%')[1]
    jobs_list.remove_job(job_id)


def main():
    path = "../builddir/.logs/"
    if not os.path.isdir(path):
        os.mkdir(path)
    jobs_list = JobsList()
    try:
        while True:
            jobs_list.need_update = False
            time.sleep(10)
            begin_logs = list(filter(os.path.isfile, glob.glob(path + "BEGIN*")))
            begin_logs.sort(key=lambda x: os.path.getmtime(x))
            for log in begin_logs:
                job_begin(log, jobs_list)
            end_logs = list(filter(os.path.isfile, glob.glob(path + "DONE*")))
            end_logs.sort(key=lambda x: os.path.getmtime(x))
            for log in end_logs:
                job_done(log, jobs_list)
            jobs_list.update_all()
            jobs_list.print_status()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
