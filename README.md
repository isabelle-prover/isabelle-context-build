# Isabelle Planned Build
![status](https://github.com/isabelle-prover/isabelle-context-build/actions/workflows/build.yml/badge.svg)
Build engine for Isabelle for pre-planned builds.

## Setup
Install with: `isabelle components -u <DIR>`. On Windows, use the `Cygwin-Terminal`.

## Available Build Strategies
- `timing_heuristic`: Paths taking longer than 30 minutes (assuming optimal run-time) are built fast, 
  then sessions in short paths are built as parallel as possible until available hosts exceeds jobs.
  At that point, the remaining sessions are built one per host as fast as possible.

## Usage
`isabelle build -o build_engine=<strategy> -H worker:jobs=<threads>,build_engine=<strategy>` 
e.g.
`isabelle build -v -j 0 -o build_engine=timing_heuristic -H localhost:jobs=2,threads=8,build_engine=timing_heuristic HOL`