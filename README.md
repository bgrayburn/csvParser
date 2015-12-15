#csv parser

To run `./run.sh` in project root will run `python csvParse.py ./csvParse.config`.
The steps set in the `processing_steps` part of the config file are run in order. In all cases the values of each step in `processing_steps` can either be a list or a single target (ex. 'col1' or ['col1','col2']).
Running this project will create a sqlite db in the root directory called testParse.csv
