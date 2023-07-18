# Datalink API samples in Python.

The code assumes Python 3.9, and only requires the added `requests` package.

```
pip install requests
```

Some samples also plot the results on a graph, using `pyplot`:

```
pip install matplotlib
```

## Usage

To use the samples, simply modify the username and password in the sample for your own service api user.

```
username = 'your username here'
password = 'your password here'
```

The util library `datalink_utils.py` is supplied to simplify calls to the api, but is not required.  Direct GET/POST requests to the api can also be used.
