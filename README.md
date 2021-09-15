# Evaluation Suite

This repository contains the source code of the evaluation suite of
the [SALSA](https://github.com/ramin-master-thesis/salsa) repository.

## Prerequisites

* Clone the repository on your computer. Create a python virtual environment and run `pip install -r requirements.txt`.
  This will install all the necessary dependencies.
* Download the left-side-index, sampled users, or the already generated baseline (for 500 and 2000 users)
  from [here](https://mega.nz/folder/sVp3FYIb#AKAgrIz4P6W4wAJZjZrfnQ) and place them in the `data` folder.
* The containers of the SALSA repository should be running before starting the evaluation. Please refer to the main
  repository.

## Usage

To run the suite, use the following command:

```shell
python3 -m main ./configs/<partition_count>/<partition_method>.yaml
```

This command will load the config file and run the evaluation. To ease the configuration, all the necessary test
scenarios are configured in the `configs` folder.

After the assessment finishes, the results will be saved in the `output` folder under the partitioning method name.
