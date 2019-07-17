#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
baseCommand: [tar, xvf]
inputs:
  input_0:
    type: File
    inputBinding:
      position: 0
  input_1:
    type: string
  input_2:
    type: string
outputs:
  output_0:
    type: File
    outputBinding:
      glob: $(inputs.input_1)
  output_1:
    type: File
    outputBinding:
      glob: $(inputs.input_2)