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
outputs: 
  output_0:
    type: File
    outputBinding:
      glob: $(inputs.input_1)