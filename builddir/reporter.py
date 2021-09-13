import os
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
        self.need_update = {}

    def update_elastic(self, host_id):
        if self.need_update[host_id]:
            connections.create_connection(hosts=ELASTIC_ADDRESS)
            current_status = HostStatus(
                host=host_id,
                time=datetime.now(),
                jobs=self.jobs_per_host[host_id]
            )
            current_status.save()

    def update_all(self):
        tmp_hosts = list(self.jobs_per_host.keys())
        for host in tmp_hosts:
            self.update_elastic(host)

    def update(self, host_name, num_jobs):
        if host_name not in self.jobs_per_host.keys() or self.jobs_per_host[host_name] != num_jobs:
            self.need_update[host_name] = True
        self.jobs_per_host[host_name] = num_jobs

    def cancel_update(self):
        for host in self.need_update.keys():
            self.need_updae[host] = False


def main():
    path = ".logs/"
    if not os.path.isdir(path):
        os.mkdir(path)
    jobs_list = JobsList()
    try:
        while True:
            jobs_list.cancel_update()
            for log in os.listdir(path):
                host_name = log.replace('.txt', '')
                full_filepath = os.path.join(path, log)
                with open(full_filepath, 'r') as f:
                    num_jobs = int(f.read().replace('/n', 0))
                jobs_list.update(host_name, num_jobs)
            jobs_list.update_all()
            time.sleep(10)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
