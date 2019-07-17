#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
baseCommand: bash
inputs:
  input_0:
    type: File
    inputBinding:
      position: 0
  input_1:
    type: string
    inputBinding: 
      position: 1
  input_2:
    type: string
    inputBinding: 
      position: 2
  input_3:
    type: string
    inputBinding: 
      position: 3
  input_4:
    type: string
    inputBinding: 
      position: 4
  input_5:
    type: string
    inputBinding: 
      position: 5
outputs: 
  output_0:
    type: File
    outputBinding:
      glob:  $(inputs.input_5)