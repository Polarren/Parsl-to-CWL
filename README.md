# Parsl-to-CWL
This is a repository where I try to use Parsl's monitoring feature to reproduce a Parsl program's progress in CWL.

## What's going on:
I wrote a script `workflow_generator.py` that can interpret `monitoring.db` produced by a parsl program and rewrite the parsl workflow in CWL. I manually wrote a CWL version of the workflow from `monitor.py` in `'workflow_1/workflow_1.cwl'`, and produced the same result in `'auto_workflow_1.cwl'` using my program `workflow_generator.py`.
The script is not fully developed but it can accomplish some writing tasks of a CWL workflow.

## How you can try the program
1. To understand my code, open `workflow_generator.py` and see how I accomplish this and the problems.
2. To go through the whole procedure, run `monitor.py`, and run `workflow_generator.py`, which would produce the same workflow as has been written in `'workflow_1/workflow_1.cwl'`.

## Description of files
### monitor.py
This is an example Parsl program that generates five .txt files and cat them into another file. 

### workflow_1 (directory)
This directory contains a CWL workflow manually written that reproduces the workflow run in `monitor.py`

### workflow_generator.py
This is the program that does interpreting of monitoring.db and auto-writing of a workflow. 

### monitoring.db
This is the database after running `monitor.py`

## Important Assumptions
1. CWL CommandLineTools are written by human. In my design, CWL_CommandLineTools are cwl translation of bash_apps. Because writing a CWL CommandLineTool is too contingent on the way bash_apps are written, I would assume bash_apps have their CWL translations ready by manual translation.
2. CWL inputs & outputs only contain a single string/file/array. Because it would be complex to handle multiple inputs and outputs, I'm still working on it.

  
