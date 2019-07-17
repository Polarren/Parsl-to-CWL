#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
baseCommand: cat
stdout: $(inputs.input_2)
inputs:
  input_0:
    type: File
    inputBinding:
      position: 0
  input_1:
    type: File
    inputBinding:
      position: 1
  input_2:
    type: string
outputs: 
  output_0:
    type: File
    outputBinding:
      glob: $(inputs.input_2)