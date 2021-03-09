import time
import os
import subprocess
import re

import yaml
import json
import docker


# python3 -m pip install -U PyYAML

def return_cpu_load():
    cpu_load = [x / os.cpu_count() * 100 for x in os.getloadavg()][-1]
    return cpu_load


def _return_mem_stat():
    # Memory usage
    total_ram = subprocess.run(['free', '-h'], stdout=subprocess.PIPE).stdout.decode('utf-8')
    used_free_shared_buf_avail = total_ram.split('\n')[1]

    return  used_free_shared_buf_avail

def return_available_memory():
    used_free_shared_buf_avail = _return_mem_stat()
    a = re.split('\s+', used_free_shared_buf_avail)
    available_memory = a[3].split('G')[0]
    return float(available_memory)


class Kelvin:
    def __init__(self, yml_file='kelvin_run.yml'):
        print('Kelvin instaniated', yml_file)
        self.yml_file = yml_file
        self._read_yml()
        self.client = docker.from_env()
    
    def __repr__(self):
        return(json.dumps(etm.etm_parms, indent=2))
        
    def _read_yml(self):
        print(self.yml_file)
        with open(self.yml_file) as file:
            self.etm_parms = yaml.full_load(file)

    def MAIN_etm_runner(self):
        products = self.etm_parms['products']
        start_year = self.etm_parms['start_year']
        end_year = self.etm_parms['end_year']
        for product in products:
            print(product)
            self._event_loop(start_year, end_year, product)

    def _start_container(self, docker_image, docker_full_cmd, name):
        container = self.client.containers.run(docker_image, docker_full_cmd, detach=True, auto_remove=True, name=name)
        # container = self.client.containers.run(docker_image, docker_full_cmd, detach=True, name=name)
        print ( "CONTAINER is ", container.name)
        return(container)

    def _start_etm(self, year, product):
    
        temperatureType=product
        cmd_opt =  ' -y ' + year + ' -t ' + temperatureType
        print(cmd_opt)
        cmd = 'python3 api_etm.py '
        full_cmd = cmd + cmd_opt
        #print(full_cmd)
        docker_image =  "tbutzer/kelvin"
        #print(docker_image)
        name_c = f'etmv1_{year}'
        c = self._start_container(docker_image, full_cmd, name_c)
        print("real name is", c.name, product)
        print("==="*30)

    def _return_num_containers(self):
        client = self.client
        try:
            running_containers = client.containers.list()
        except:
            running_containers=[]
        return(len(running_containers))

    def _event_loop(self, year_to_process, end_year, product):

        MAX_LOAD_LEVEL = self.etm_parms['max_cpu_percent']
        MIN_MEMORY_AVAILABLE =  self.etm_parms['min_memory_available']
        MAX_CONCURRENT_CONTAINERS = self.etm_parms['max_concurrent_containers']

        print(f' {year_to_process} <= {end_year}:')
        while year_to_process <= end_year:
            print('sleeping for 30 .... ')
            time.sleep(30)

            mem_avail = return_available_memory()
            cpu_load = return_cpu_load()
            num_running_containers = self._return_num_containers()
            print(f'mem={mem_avail}, cpu={cpu_load}, num_containers={num_running_containers}')

            cpu = False
            if (cpu_load < MAX_LOAD_LEVEL):
                print("CPU is FINE")
                cpu=True

            mem = False
            if mem_avail > MIN_MEMORY_AVAILABLE:
                print("MEM is FINE")
                mem = True

            containers = False
            if num_running_containers < MAX_CONCURRENT_CONTAINERS:
                containers = True
            else:
                print("MAX Container Level Reached - Chill :-)")


            if (mem and cpu and containers):
                print("OK to Launch")
                self._start_etm(str(year_to_process), product) ### start mosaic container
                print("starting year", year_to_process)
                year_to_process = year_to_process + 1


kelvin_instance=Kelvin()
print(kelvin_instance)

kelvin_instance.MAIN_kelvin_runner()

