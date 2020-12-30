import time
import logging
import datetime
import contextlib
from pathlib import Path
import traceback
import os


def setup_logging(name="logger",
                  filepath=None,
                  stream_log_level="INFO",
                  file_log_level="INFO"):
    logger = logging.getLogger(name)
    logger.setLevel("INFO")
    formatter = logging.Formatter(
        '%(message)s'
    )
    ch = logging.StreamHandler()
    ch.setLevel(getattr(logging, stream_log_level))
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    if filepath is not None:
        fh = logging.FileHandler(filepath)
        fh.setLevel(getattr(logging, file_log_level))
        fh.setFormatter(formatter)
        logger.addHandler(fh)


def print_footer(function_name, status, start):
    print("\n\n-------------------------------------------------------------------------------------------")
    print(datetime.datetime.now())
    if not status:
        print(f"{function_name} Completed Successfully")
    else:
        print(f"{function_name} Failed")
    end = time.time()
    print("Total Runtime: {}s".format(round(end - start, 2)))
    print("-------------------------------------------------------------------------------------------")


def print_header(function_name, args, kwargs):
    print("\n\n-------------------------------------------------------------------------------------------")
    print(datetime.datetime.now())
    print(f"Running function: {function_name}")
    print(f"args: {args}\nkwargs: {kwargs}")
    print("-------------------------------------------------------------------------------------------")


def log_function(log_name =None, output_log_dir=os.getcwd(), error_log_dir=os.getcwd()):
    def log_this(func):
        stdout_log_path = Path(f"{output_log_dir}/{log_name}_OUTPUT.log")
        stderr_log_path = Path(f"{error_log_dir}/{log_name}_ERROR.log")
        if not Path(output_log_dir).exists():
            Path(output_log_dir).mkdir(parents=True)
        if not Path(error_log_dir).exists():
            Path(error_log_dir).mkdir(parents=True)
        def new_function(*args, **kwargs):
            with contextlib.redirect_stdout(open(stdout_log_path, "w")):
                start = time.time()
                print_header(func.__name__, args, kwargs)
                execution_failed = False
                try:
                    output = func(*args, **kwargs)
                except Exception:
                    full_tb = traceback.format_exc()
                    execution_failed = True

                if execution_failed:
                    print(f"\n\n{func.__name__} failed with the following error:\n\n{full_tb}")
                print_footer(func.__name__, execution_failed, start)

            f = open(stdout_log_path, "r")
            print(f.read())
            f.close()
            if execution_failed:
                if stderr_log_path.exists():
                    os.remove(stderr_log_path)
                stdout_log_path.rename(stderr_log_path)
                raise Exception(f"{func.__name__} failed with the following error:\n\n{full_tb}")
            return output

        return new_function

    return log_this


def divideStrByInt(input_str=None):
    print('\nhello\n\nNew line')
    print(input_str)
    print('New line')
    dict_test = {'s': 1}
    q = dict_test['q']
    return


if __name__ == '__main__':
    log_function("default_log2",Path(os.getcwd()+"/output_logs"),Path(os.getcwd()+"/error_logs"))(divideStrByInt)(input_str='input stringg baby')
