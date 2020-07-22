REFER ME !!

# Airflow

The Directory Structure should be :

There should be 1 airflow folder inside which there should be two Folders:

One with the name of dags and the other with the name selenium_scripts.

Your Python Selenium Script and the Python dag file should go in these two separate Folders.

Happy Coding!

# Selenium Plugin

The Selenium plugin will contain a Hook and Operator, Hooks handle external connections and make up the building blocks of an Operator. The operator will execute our task. The plugin folder structure is as follows:
.
├── README.md
├── __init__.py
├── hooks
│ ├── __init__.py
│ └── Selenium_hook.py
└── operators
├── __init__.py
└── Selenium_operator.py

