# Code from db_manager.py 
import logging
import threading
import queue
import os
import time

from parsl.dataflow.states import States
from parsl.providers.error import OptionalModuleMissing

try:
    import sqlalchemy as sa
    from sqlalchemy import Column, Text, Float, Integer, DateTime, PrimaryKeyConstraint
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.ext.declarative import declarative_base
except ImportError:
    _sqlalchemy_enabled = False
else:
    _sqlalchemy_enabled = True

try:
    from sqlalchemy_utils import get_mapper
except ImportError:
    _sqlalchemy_utils_enabled = False
else:
    _sqlalchemy_utils_enabled = True

WORKFLOW = 'workflow'    # Workflow table includes workflow metadata
TASK = 'task'            # Task table includes task metadata
STATUS = 'status'        # Status table includes task status
RESOURCE = 'resource'    # Resource table includes task resource utilization

# My codes starts here
from parsl.monitoring.db_manager import Database

################################ CWL Data Structure #####################################
# CWL classes for rendering purpose: 
# To write a CWL workflow by machine, I need to 
# store required information somewhere, which is 
# the class defined below as cwl_Workflow
class cwl_File(object):
    def __init__(self,path):
        self.path = path
class cwl_string(str):
    def __init__(self,string):
        self.string=string
    pass
class cwl_array(list):
    def __init__(self, lst):
        self.list = lst
class cwl_CommandLineTool(object):
    def __init__(self, name, basecommand, inputs=[], outputs=[]):
        self.name = name
        self.basecommand = basecommand
        self.inputs = inputs
        self.outputs = outputs

class cwl_step(object):
    def __init__(self, clt, inputs=[], outputs=[], scatter = None):
        self.clt = clt
        self.inputs = inputs
        self.outputs = outputs
        self.scatter = scatter

class cwl_Workflow(object):
    def __init__(self, inputs=[], outputs=[], steps=[], requirements = ['ScatterFeatureRequirement: {}'] ):
        self.requirements = requirements
        self.inputs = inputs
        self.outputs = outputs
        self.steps = steps

################################  Rendering  #####################################
# Rendering procedure:
#  automatically generate a cwl_workflow 
#  by the function render_cwl

# Rendering functions
def render_cwl(filename,cwl_workflow):
    f= open(filename,"w+")
    f.write("#!/usr/bin/env cwl-runner\n\ncwlVersion: v1.0\nclass: Workflow\n\n")
    f.close()
    render_requirements(filename, cwl_workflow)
    render_inputs(filename, cwl_workflow)
    render_outputs(filename, cwl_workflow)
    render_steps(filename, cwl_workflow)
    return

def render_requirements(filename, cwl_workflow):
    f = open(filename, "a+")
    f.write("requirements:\n")
    for requirement in cwl_workflow.requirements:
        f.write('  '+requirement+"\n")
    f.close()
    
def render_inputs(filename, cwl_workflow):
    f = open(filename, "a+")
    f.write("inputs:\n")
    if not cwl_workflow.inputs:
        f.write("  []\n")
    for i in cwl_workflow.inputs:
        idx = cwl_workflow.inputs.index(i)
        if type(i) == cwl_File:
            f.write("  input_{}: File\n".format(idx))
        elif type(i) == cwl_string:
            f.write("  input_{}: string\n".format(idx))
        elif type(i) == cwl_array or type(i) == list:
            if not i:
                continue
            elif type(i[0])==cwl_string:
                f.write("  input_{}: string[]\n".format(idx))
            elif type(i[0])==cwl_File:
                f.write("  input_{}: File[]\n".format(idx))
    f.close()

def render_outputs(filename, cwl_workflow):
    f = open(filename, "a+")
    f.write("outputs:\n")
    if not cwl_workflow.outputs:
        f.write("  []\n")
    for i in cwl_workflow.outputs:
        idx = cwl_workflow.outputs.index(i)
        if type(i) == cwl_File:
            f.write("  output_{}:\ntype: File\n".format(idx))
            f.write("  outputSource: {}\n".format(i.path))
    f.close()
    
def render_steps(filename, cwl_workflow):
    f = open(filename, "a+")
    f.write("\nsteps:\n")
    for step in cwl_workflow.steps:
        idx = cwl_workflow.steps.index(step)
        f.write('  '+step.clt.name+':\n')
        f.write("    run: {}.cwl\n".format(step.clt.name))
        if step.scatter:
            f.write("    scatter: input_0\n")
        f.write("    in:\n")
        for i in range(len(step.clt.inputs)):
            f.write("      input_{0}: {1}\n".format(i,step.inputs[i]))
        f.write("    out:\n")
        for i in range(len(step.clt.outputs)):
            f.write("      [{0}]\n".format(step.outputs[i]))
    f.close()

 
################################  Interpreting  #####################################
# Functions that extract information from Database: monitoring.db
# and fill in the class cwl_Workflow 
# before using it to generate a CWL workflow
def add_indep_steps(tasks, db,  cwl_workflow, clts=[] ):
    # make independent tasks with the same task_func_name just one step by scatter
    # filter independent tasks
    indep_tasks = tasks.filter(db.Task.task_depends=='')
    # make a list of independent function names
    indep_task_func_name_query=tasks.group_by(db.Task.task_func_name).having(db.Task.task_depends=='')
    indep_task_func_name =[]
    for funcs in indep_task_func_name_query:
        indep_task_func_name.append(funcs.task_func_name)
        
    #print(indep_task_func_name)
#     for task in indep_tasks:
#         if task.task_func_name not in indep_task_func_name:
#             indep_task_func_name.append(task.task_func_name)
    # Write cwl_steps for independent tasks
    for func_name in indep_task_func_name:
        idx = indep_task_func_name.index(func_name)
        for clt in clts:
            if clt.name == func_name:
                clt_for_func_name = clt
        inputs = ['input_{}'.format(idx)] #!!!Assumption here: Only one input and one output in a cwl_CommandLineTool
        outputs = ['output_{}'.format(idx)]
        step = cwl_step(clt_for_func_name, inputs=inputs, outputs = outputs, scatter=inputs[0])
        cwl_workflow.steps.append(step)

        
        
def add_dep_steps(tasks, db,  cwl_workflow, clts=[] ):
    # make independent tasks with the same task_func_name just one step by scatter
    # filter independent tasks
    dep_tasks = tasks.filter(db.Task.task_depends!='')
    # make a list of independent function names
    dep_task_func_name_query=tasks.group_by(db.Task.task_func_name).having(db.Task.task_depends!='')
    dep_task_func_name =[]
    depends_on =[]
    for funcs in dep_task_func_name_query:
        dep_task_func_name.append(funcs.task_func_name)
        # Find their corresponding preceding tasks
        depends_on.append(interpret_task_depends(tasks, funcs.task_depends))
    # Write cwl_steps for dependent tasks
    for func_name in dep_task_func_name:
        idx = dep_task_func_name.index(func_name)
        for clt in clts:
            if clt.name == func_name:
                clt_for_func_name = clt
        inputs = ['{}/input_0'.format(depends_on[idx])] #!!!Assumption here: Only one input and one output in a cwl_CommandLineTool
        outputs = ['output_{}'.format(idx)]
        step = cwl_step(clt_for_func_name, inputs=inputs, outputs = outputs, scatter=inputs[0])
        cwl_workflow.steps.append(step)
    
    
# Helper funciton for add_dep_steps
# Given a list of dependent tasks in the form of a string,
# interpret the string and find out the task_func_name it depends on
def interpret_task_depends(tasks, task_depends):
    pre_task_ids=eval('['+task_depends+']')
    #print(pre_task_ids)
    pre_tasks = tasks.filter(db.Task.task_id==str(pre_task_ids[0])) #!!!Assumption here: only depends on one task
    pre_func_name = pre_tasks[0].task_func_name
    return pre_func_name



################################  Example  #####################################

# Auto_workflow_1 generation

# Define CommandLineTools: generate and concat are the two bash_apps I used in monitor.py
# Since I now assume that bash_apps are translated into cwl manually, so
# I need a list of CommandLineTools telling me what are the available cwls
# I declare them here as cwl_CommandLineTools and store them in the variable 'clts'
generate_inputs = [cwl_string('')]
generate_outputs = [cwl_File('')]
generate = cwl_CommandLineTool('generate','echo',generate_inputs, generate_outputs)   

concat_inputs = [cwl_array([cwl_File(''),cwl_File(''),cwl_File('')])]
concat_outputs = [cwl_File('')]
concat = cwl_CommandLineTool('concat','cat',concat_inputs,concat_outputs)

clts = [generate, concat]  

# Query database to prepare rendering
db = Database()
Task = db.session.query(db.Task)


# Define the Workflow

# Determine inputs and outputs of the workflow
# The inputs and outputs of the workflow are supposed to be figured out by the program reading monitoring.db,
# but I'm still working on it, so I temporarily fill them in my cwl_Workflow data type manually
auto_workflow_1_inputs = [[cwl_string('random-0.txt'),cwl_string('random-1.txt'),cwl_string('random-2.txt')]]
auto_workflow_1_outputs = [cwl_File('concat/output_0')]

# Determine steps of the workflow
# Create the cwl_CommandLineTool that will be rendered (written by machine) into a cwl workflow file called auto_workflow_1.cwl
auto_workflow_1 = cwl_Workflow(inputs=auto_workflow_1_inputs, outputs=auto_workflow_1_outputs,steps=[])
add_indep_steps(Task, db, auto_workflow_1, clts) # Add independent steps to the workflow
add_dep_steps(Task, db, auto_workflow_1, clts) # Add dependent steps to the workflow

# Apply the render procedure
render_cwl("auto_workflow_1.cwl",auto_workflow_1)
