import sys
import contextlib
import multiprocessing

def timed(func, time, *args, **kwargs):
    p = multiprocessing.Process(target=func, args=args, kwargs=kwargs)
    p.start()
    p.join(time)
    if p.is_alive():
        p.terminate()
        raise Exception("Timed out!")


@contextlib.contextmanager
def capture(stderr=None, stdout=None):
    if stderr:
        saved_stderr = sys.stderr
        sys.stderr = stderr
    if stdout:
        saved_stdout = sys.stdout
        sys.stdout = stdout
    yield
    if stderr:
        sys.stderr = saved_stderr
    if stdout:
        sys.stdout = saved_stdout
    
