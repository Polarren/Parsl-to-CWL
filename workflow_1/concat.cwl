#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
# requirements:
#  - class: ShellCommandRequirement
baseCommand: cat
stdout: all.txt
inputs:
  input_0:
    type:
      type: array
      items: File
    inputBinding:
      position: 1
outputs: 
  output_0:
    type: File
    outputBinding:
      glob: "all.txt"