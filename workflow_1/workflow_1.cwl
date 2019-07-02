#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: Workflow

requirements:
  ScatterFeatureRequirement: {}
inputs:
  input_0: string[]
outputs:
  output_0: 
    type: File
    outputSource: concat/output_0

steps:
  generate:
    run: generate.cwl
    scatter: input_0
    in:
      input_0: input_0
    out: 
      [output_0]
  concat:
    run: concat.cwl
    in:
      input_0: generate/output_0
    out:
      [output_0]
