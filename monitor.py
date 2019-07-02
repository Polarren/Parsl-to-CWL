import time
import os
from scipy.integrate import odeint
from operator import itemgetter

import parsl
from parsl.app.app import python_app, bash_app
#from parsl.configs.local_threads import config

import logging
from parsl.config import Config
from parsl.executors.threads import ThreadPoolExecutor
from parsl.monitoring import MonitoringHub
from parsl.addresses import address_by_hostname

# Define a configuration for using local threads and pilot jobs
#parsl.set_stream_logger()
FILENAME = 'log_monitor.txt'
parsl.set_file_logger(FILENAME, level=logging.DEBUG)
config = Config(
    executors=[
        ThreadPoolExecutor(
            max_threads=8, 
            label='local_threads'
        )
    ],
    monitoring =MonitoringHub(
        hub_address=address_by_hostname(),
        hub_port=55055,
        logging_level=logging.INFO,
        resource_monitoring_interval=10,
    ),
    strategy=None
)

parsl.load(config)
print( 'Modules Imported!')


# App that generates 
@bash_app
def generate(inputs=[], outputs=[],stderr='stderr.txt'):
    return "echo {inputs[0]} &> {outputs[0]}"

# App that concatenates input files into a single output file
@bash_app
def concat(inputs=[], outputs=[], stdout="stdout.txt", stderr='stderr.txt'):
    return "cat {0} > {1}".format(" ".join(list(map(str, inputs))), outputs[0])


#########################################################################################

# Create 5 files with random numbers
output_files = []
for i in range (5):
     output_files.append(generate(inputs=['random-{}.txt'.format(i)], outputs=[os.path.join(os.getcwd(), 'random-{}.txt'.format(i))]))

# Concatenate the files into a single file
cc = concat(inputs=[i.outputs[0] for i in output_files], outputs=[os.path.join(os.getcwd(), 'all.txt')])
cc.result()
print("Execution Successful.")