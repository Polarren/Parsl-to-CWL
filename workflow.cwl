#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: Workflow

inputs:
  input_0: #inputs for echo1.cwl
    type: File
  input_1:
    type: string
  input_2: #inputs for echo2.cwl
    type: File
  input_3:
    type: string
  input_4:
    type: string
  input_5:
    type: string
  input_6:
    type: string
  input_7:
    type: string
  input_8:
    type: string
  input_9:
    type: File
  input_10:
    type: string
  input_11:
    type: string
  input_12:
    type: string
  input_13:
    type: string
outputs:
  output_0:
    type: File
    outputSource: step_0/output_0
  output_1: 
    type: File
    outputSource: step_1/output_0
  output_2:
    type: File
    outputSource: step_2/output_0
  output_3: 
    type: File
    outputSource: step_3/output_0
  output_4:
    type: File
    outputSource: step_3/output_1
  output_5: 
    type: File
    outputSource: step_4/output_0
  output_6:
    type: File
    outputSource: step_5/output_0
steps:
  step_0: #echo1
    run: echo1.cwl
    in: 
      input_0: input_0 
      input_1: input_1
    out:
      [output_0]
  step_1: #echo2
    run: echo2.cwl
    in: 
      input_0: input_2 
      input_1: input_3
      input_2: input_4 
      input_3: input_5
      input_4: input_6 
      input_5: input_7
    out:
      [output_0]  
  step_2: #concat inputs from echo_1 and echo_2
    run: concat.cwl
    in: 
      input_0: step_0/output_0
      input_1: step_1/output_0
      input_2: input_8
    out:
      [output_0] 
  step_3: #extract files from texts.tar
    run: untar.cwl
    in: 
      input_0: input_9
      input_1: input_10
      input_2: input_11
    out:
      [output_0, output_1] 
  step_4: #concat inputs from echo_1 and echo_2
    run: concat.cwl
    in: 
      input_0: step_3/output_0
      input_1: step_3/output_1
      input_2: input_12
    out:
      [output_0] 
  step_5:
    run: concat.cwl
    in: 
      input_0: step_2/output_0
      input_1: step_4/output_0
      input_2: input_13
    out:
      [output_0] 
    
