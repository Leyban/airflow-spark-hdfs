from airflow.models import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def method1():
    """Using the sys path list"""
    import sys
    sys.path.insert(0, '/opt/airflow/files')
    
    print("@@@@@@@@@@@@@@@@@@@@@@@")
    print(sys.path)

    from files import test
    print("nothing")


def method2():
    """Using the os chdir method"""
    import os
    # print the current working directory
    print('Original working directory:', os.getcwd())

    # change the working directory
    os.chdir('/opt/airflow/files')

    # print the updated working directory
    print('Updated working directory:', os.getcwd())

    # now you can import your module
    from files import test


def method3():
    """Using the imp method"""
    import imp

    # load a source module from a file
    files, pathname, description = imp.find_module('files', ['/opt/airflow/files'])
    test = imp.load_module('files', files, pathname, description)


def method4():
    """Using importlib"""
    import importlib

    # import a module using its name as a string
    files = importlib.import_module('files')


def method5():
    """Airflow Satisfaction Guaranteed"""
    from main_airflow.modules import test
    
    test.greet_mom()
    

def check_contents():
    """This function proves that it can in fact detect the contents of the path"""
    # import OS module
    import os
     
    # Get the list of all files and directories
    path = "/opt/airflow/files"
    dir_list = os.listdir(path)
     
    print("Files and directories in '", path, "' :")
     
    # prints all files
    print(dir_list)


with DAG(
    dag_id='split_files',
    start_date=datetime(2023, 7, 24),
    schedule_interval="0 10 * * *",
    catchup=False
) as dag:
    task1 = PythonOperator(
            task_id="path_list",
            python_callable=method1
            )

    task2 = PythonOperator(
            task_id="os_chdir",
            python_callable=method2
            )

    task3 = PythonOperator(
            task_id="imp",
            python_callable=method3
            )

    task4 = PythonOperator(
            task_id="importlib",
            python_callable=method4
            )

    task5 = PythonOperator(
            task_id="airflow_docs",
            python_callable=method5
            )

    check_module = PythonOperator(
            task_id="check_contents",
            python_callable=check_contents
            )

    task1
    task2
    task3
    task4
    task5
    check_module
