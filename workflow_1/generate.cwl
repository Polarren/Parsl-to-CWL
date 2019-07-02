# #!/usr/bin/env cwl-runner

# cwlVersion: v1.0
# class: CommandLineTool
# requirements: 
#   - class: ShellCommandRequirement
# baseCommand: echo
# arguments: ["($RANDOM)", "&>"]
# inputs: 
#   filename:
#     type: string
#     inputBinding:
#       position: 3
# outputs: 
#   txtfile:
#     type: File
#     outputBinding:
#       glob: "*.txt"

#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
requirements:
  ShellCommandRequirement: {}
  EnvVarRequirement: 
    envDef:
      RANDOM: $(inputs.input_0)
baseCommand: echo
stdout: $(inputs.input_0)
arguments:
 - valueFrom: $RANDOM
   shellQuote: false
#  - valueFrom: "&>"
#    shellQuote: false
inputs:
  input_0:
    type: string
outputs:
  output_0:
    type: File
    outputBinding:
      glob: "*.txt"