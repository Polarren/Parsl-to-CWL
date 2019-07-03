# Parsl-to-CWL
This is a repository where I try to use Parsl's monitoring feature to reproduce a Parsl program's progress in CWL.

# What's going on:
I wrote a script `workflow_generator.py` that can interpret monitoring.db of a parsl program and rewrite the workflow in CWL. 
The script is not fully developed but can accomplish some writing tasks of a CWL workflow.

# monitor.py
This is an example Parsl program that generates five .txt files and cat them into another file. 

# workflow_1 (directory)
This directory contains a CWL workflow manually written that reproduces what's going on in `monitor.py`

# workflow_generator.py
This is the program that does interpreting of monitoring.db and auto-writing of a workflow. 

# monitoring.db
This is the database after running `monitor.py`
