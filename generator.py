#dbmanager.py
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

from parsl.monitoring.db_manager import Database



#CWL classes for rendering purpose        
class cwl_File(object):
    def __init__(self,path=''):
        self.path = path
class cwl_string(str):
    def __init__(self,string=''):
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
    def __init__(self, clt, inputs=[], outputs=[],step_idx = 0, indep=True, scatter = None):
        self.clt = clt
        self.inputs = inputs
        self.outputs = outputs
        self.scatter = scatter
        self.indep = indep
        self.step_idx = step_idx
        
class cwl_Workflow(object):
    def __init__(self, inputs=[], outputs=[], steps=[], requirements = [] ):
        self.requirements = requirements
        self.inputs = inputs
        self.input_count = 0
        self.outputs = outputs
        self.steps = steps

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
    if cwl_workflow.requirements:
        f.write("requirements:\n")
    for requirement in cwl_workflow.requirements:
        f.write('  '+requirement+"\n")
    f.close()
    
def render_inputs(filename, cwl_workflow):
    f = open(filename, "a+")
    f.write("inputs:\n")
    if not cwl_workflow.inputs:
        f.write("  []\n")
    idx = 0
    for i in cwl_workflow.inputs:
        #idx = cwl_workflow.inputs.index(i)
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
        idx+=1
    f.close()

def render_outputs(filename, cwl_workflow):
    f = open(filename, "a+")
    f.write("outputs:\n")
    if not cwl_workflow.outputs:
        f.write("  []\n")
    for i in cwl_workflow.outputs:
        idx = cwl_workflow.outputs.index(i)
        if type(i) == cwl_File:
            f.write("  output_{}:\n    type: File\n".format(idx))
            f.write("    outputSource: {}\n".format(i.path))
        else: #Assuming only FIle and File[] can exist
            for j in i:
                f.write("  output_{}:\n    type: File[]\n".format(idx))
                f.write("    outputSource: {}\n".format(j.path))            
    f.close()
    
def render_steps(filename, cwl_workflow):
    f = open(filename, "a+")
    f.write("\nsteps:\n")
    for step in cwl_workflow.steps:
        idx = cwl_workflow.steps.index(step)
        f.write('  step_'+str(step.step_idx)+':\n')
        f.write("    run: {}.cwl\n".format(step.clt.name))
        if step.scatter:
            f.write("    scatter: input_0\n")
        f.write("    in:\n")
        for i in range(len(step.clt.inputs)):
            f.write("      input_{0}: {1}\n".format(i,step.inputs[i]))
        f.write("    out:\n")
        render_step_output(f, step.outputs)
    f.close()
    
def render_step_output(f, outputs):
    f.write("      [")
    for i in range(len(outputs)):
        f.write('{0}'.format(outputs[i]))
        if (i!=len(outputs)-1):
            f.write(',')
    f.write("]\n")

def add_inputs(cwl_workflow):
    workflow_inputs = []
    input_instance = None
    #input_count = 0
    for step in cwl_workflow.steps:
        for i in range(len(step.clt.inputs)):
            if step.inputs[i][0] == 's':
                continue
            if type(step.clt.inputs[i])==cwl_string or type(step.clt.inputs[i])==str:
                input_instance = cwl_string('')
            elif type(step.clt.inputs[i])==cwl_File: # TODO: Consider when input is an array
                input_instance = cwl_File('')
            elif type(step.clt.inputs[i])==list or type(step.clt.inputs[i])==cwl_array:
                if type(step.clt.inputs[i][0])==cwl_string or type(step.clt.inputs[i][0])==str:
                    input_instance = [cwl_string('')]
                elif type(step.clt.inputs[i][0])==cwl_File: # TODO: Consider when input is an array
                    input_instance = [cwl_File('')]
            if step.scatter!=None:
                input_instance = [input_instance]
            workflow_inputs.append(input_instance)  
            #print("Input count: {}".format(input_count));input_count+=1
    cwl_workflow.inputs = workflow_inputs

def add_outputs(cwl_workflow):
    workflow_outputs =[]
    workflow_out_idx = 0
    for step in cwl_workflow.steps:
        step_out_idx = 0
        for i in range(len(step.clt.outputs)):
            #print(type(step.clt.outputs[i]))
            if type(step.clt.outputs[i])==cwl_File: 
                output_instance =cwl_File('step_{0}/output_{1}'.format(step.step_idx,step_out_idx))
                #print('{0}/output_{1}'.format(step.clt.name,out_idx))
                if step.scatter != None:
                    output_instance = [output_instance]
                workflow_outputs.append(output_instance)
            step_out_idx+=1
        workflow_out_idx+=1
    #print(workflow_outputs[0])
    cwl_workflow.outputs = workflow_outputs       

    
def add_indep_steps(tasks, db,  cwl_workflow, clts=[] ):
    # make independent tasks with the same task_func_name just one step by scatter
    # filter independent tasks
    indep_tasks_query = tasks.filter(db.Task.task_depends=='')
    # make a list of independent function names
    indep_task_func_name =[]
    idx = 0
    for task in indep_tasks_query:
        indep_task_func_name.append(task.task_func_name)
        func_name = task.task_func_name
        has_clt = False
        clt_for_func_name = None
        for clt in clts: #check for clt availability
            if clt.name == func_name:
                clt_for_func_name = clt
                has_clt = True
        if has_clt == True:
            inputs = []; outputs = []
            out_idx = 0
            for i in clt_for_func_name.inputs:
                inputs.append('input_{}'.format(idx))
                idx+=1
            for i in clt_for_func_name.outputs:
                outputs.append('output_{}'.format(out_idx))
                out_idx +=1
            step = cwl_step(clt_for_func_name, inputs=inputs, outputs = outputs, step_idx = task.task_id)
            cwl_workflow.steps.append(step)
            cwl_workflow.input_count += len(step.inputs)
        else:
            print("clt {} missing".format(func_name))

        
        
def add_dep_steps(tasks, db,  cwl_workflow, clts=[] ):
    # make independent tasks with the same task_func_name just one step by scatter
    # filter independent tasks
    dep_task_query = tasks.filter(db.Task.task_depends!='')
    # make a list of independent function names
    dep_task_func_name =[]
    depends_on =[]
    for task in dep_task_query:
        dep_task_func_name.append(task.task_func_name)

        func_name = task.task_func_name
        idx = dep_task_func_name.index(func_name)
        has_clt = False
        clt_for_func_name = None
        for clt in clts:
            if clt.name == func_name:
                clt_for_func_name = clt
                has_clt = True    
    
        if has_clt == True:
            inputs = []; outputs = []
            # Find their corresponding preceding tasks
            pre_task_ids = eval('['+task.task_depends+']')
            for i in range(len(clt_for_func_name.inputs)-len(clt_for_func_name.outputs)): # for each input in the command:
                #find if it has preceding tasks, determine the preceding step(task) id, and the required output index
                pre_task_id_and_out_idx = find_pre_task(interpret_task_inputs(task.task_inputs)[i], tasks) #[task_id, out_idx]
                inputs.append('step_{0}/output_{1}'.format(pre_task_id_and_out_idx[0],pre_task_id_and_out_idx[1]))
            for j in range(len(clt_for_func_name.outputs)):
                inputs.append('input_{}'.format(cwl_workflow.input_count))
                outputs.append('output_{}'.format(str(j)))
                cwl_workflow.input_count += 1
            step = cwl_step(clt_for_func_name, inputs=inputs, outputs = outputs,step_idx = task.task_id , indep=False, scatter=None)
            cwl_workflow.steps.append(step)
            
        else:
            print("clt '{}' missing".format(func_name))
    
    
# Helper funciton for add_dep_steps
# Given a list of dependent tasks in the form of a string,
# interpret the string and find out the task_func_name it depends on
# def interpret_task_depends(tasks, task_depends):
#     pre_task_ids=eval('['+task_depends+']')
#     #print(pre_task_ids)
#     for i in range(len(pre_task_ids)):
#         pre_tasks = tasks.filter(db.Task.task_id==str(pre_task_ids[i])) #!!!Assumption here: only depends on one task
#     pre_func_name = pre_tasks[0].task_func_name
#     return pre_func_name

def find_pre_task(input_string, pre_tasks_query):   
    pre_task_id_and_out_idx =['0','0']
    for pre_task in pre_tasks_query:
        task_outputs = eval(pre_task.task_outputs)
       # print(input_string, task_outputs)
        if input_string in task_outputs:
            pre_task_id_and_out_idx[0]=pre_task.task_id
            pre_task_id_and_out_idx[1]=task_outputs.index(input_string)
            break
    return pre_task_id_and_out_idx

# task_inputs interpreter only for DEPENDET tasks
def interpret_task_inputs(task_inputs):
    evaluated_inputs =[]
    try: #success if the inputs are string
        evaluated_inputs = eval(task_inputs)
#         for i in evaluated_inputs:
#             i = cwl_string(i)
    except: # this means inputs are files, and we need to modify task_input
        new_str = ''
        for s in task_inputs:
            if s == '[':
                new_str += "['"
            elif s == ',':
                new_str += "','" 
            elif s == ']':
                new_str += "']"
            elif s == ' ':
                continue
            else:
                new_str += s
        evaluated_inputs = eval(new_str)
#         for i in range(len(evaluated_inputs)):
#             evaluated_inputs[i] = cwl_File(evaluated_inputs[i])
    return evaluated_inputs



# Prepare available cwl_CommandLineTools

echo1 = cwl_CommandLineTool('echo1', 'bash', [cwl_File(), cwl_string()], [cwl_File()])
echo2 = cwl_CommandLineTool('echo2', 'bash', [cwl_File(),cwl_string(),cwl_string(),cwl_string(),cwl_string(),cwl_string()],[cwl_File()])
untar = cwl_CommandLineTool('untar', 'tar', [cwl_File(),cwl_string(),cwl_string()],[cwl_File(),cwl_File()])
concat = cwl_CommandLineTool('concat','cat', [cwl_File(),cwl_File(),cwl_string()],[cwl_File()])
clts = [echo1, echo2, untar, concat]  

# Query database to prepare rendering
db = Database()
# run_id = select_workflow()
Task = db.session.query(db.Task)


# Define the Workflow
    # Determine inputs and outputs of the workflow
# auto_workflow_1_inputs = [[cwl_string('random-0.txt'),cwl_string('random-1.txt'),cwl_string('random-2.txt')]]
# auto_workflow_1_outputs = [cwl_File('concat/output_0')]

    # Determine steps of the workflow
# auto_workflow_1_steps = [cwl_step(generate, inputs=['input_0'], outputs=['output_0'], scatter='input_0'),
#                          cwl_step(concat, inputs=['generate/output_0'], outputs=['output_0'])]
#auto_workflow_1 = cwl_Workflow(inputs=auto_workflow_1_inputs, outputs=auto_workflow_1_outputs,steps=auto_workflow_1_steps)

auto_workflow_1 = cwl_Workflow(inputs=[], outputs=[],steps=[])

add_indep_steps(Task, db, auto_workflow_1, clts)
add_dep_steps(Task, db, auto_workflow_1, clts)
add_inputs(auto_workflow_1)
add_outputs(auto_workflow_1)
render_cwl("auto_workflow.cwl",auto_workflow_1)