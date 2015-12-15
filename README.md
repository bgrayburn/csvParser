#csvParser

`./run.sh` in project root will run `python csvParse.py ./csvParse.config`.
The steps set in the `processing_steps` part of the config file are run in order. In all cases the values of each step in `processing_steps` can either be a list or a single target (ex. 'col1' or ['col1','col2']).
Running this project will create a sqlite db in the root directory called `testDb.db`.

##ipython notebook
Also include is a ipython notebook which can also be viewed [here](csvParser.ipynb)

##dependencies

I recommend using the latest version of the Anaconda python3 distribution
- pandas
- sqlalchemy
- json

##future

- Currently headers aren't stripped of whitespace before being compared to config, it would be nice to fix this
- More datatypes (ex datetime)
- Seperate out loading, processing, and storing into seperate classes for better modularity
- More input types (ex load from db)
- More output types (ex csv)
- More transform types (ex round)
- Save transformed cols as new cols instead of overwriting (ex col3_float)
- Better error handling
