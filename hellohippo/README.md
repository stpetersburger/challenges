# Description
## This repo consists of:

* apps - folder to keep the script, running the code for executing the assigments
  * **hellohippo.py** - is the run file for execution of the [Problem](https://github.com/stpetersburger/challenges/blob/master/hellohippo/Problem.md)
  * entrypoint is folder with the initial data ```data.tar.gz```, unzipped (```tar -xvzf filename.tar.gz```) for:
    * **claims**
    * **pharmacies**
    * **reverts**
    * and **output** folder is used to keep .json files of the executed assignments result
  * utils is the folder with reusable methods

## Running the app
```export PYTHONPATH="${PYTHONPATH}:/{full_path_to}/hellohippo/"```
```python apps/hellohippo.py```
### Arguments
    -path - is the path to folder, where hellohippo repo has been cloned
### Output
    Is in hellohippo/entrypoint/output folder, named as per the number of the assignment

2. ***assignment_2.json***
```[
    {
        "npi": 123456789,
        "ndc": 2323401,
        "fills": 151,
        "reverted": 2,
        "avg_price": 54.3245033113,
        "total_price": 863913.05
    },...]
```
3. ***assigment_3.json***
```[
    {
        "ndc": 2323401,
        "name": [
            {
                "chain": "doctor",
                "avg_price": 15877.846382055
            },
            {
                "chain": "health",
                "avg_price": 11135.9559701493
            },
            {
                "chain": "saint",
                "avg_price": 239.2169412976
            }
        ]
    },...]
```
4. ***assignment_4.json***
```[
    {
        "ndc": 93752910,
        "most_prescribed_quantity": [
            30,
            45
        ]
    },...]
```