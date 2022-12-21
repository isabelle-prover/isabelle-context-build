# Isabelle Context Build
![status](https://github.com/isabelle-prover/isabelle-context-build/actions/workflows/build.yml/badge.svg)
Build tool for Isabelle, with additional execution context (e.g., for distributed builds).

## Setup
Install with: `isabelle components -u <DIR>`. On Windows, use the `Cygwin-Terminal`.

## Usage
`isabelle context_build -?` (nearly regular build options)

## Distributed Build
Distributed builds are implemented for slurm clusters.

Infrastructure Assumptions:
- shared files system for Isabelle and user home (mounted on same paths), writeable by all nodes
- slurm partitions of homogeneous nodes 