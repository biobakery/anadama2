import sys

def default(run_context):
    return ConsoleReporter(run_context)

class BaseReporter(object):

    def __init__(self, run_context):
        self.run_context = run_context

    def started(self):
        raise NotImplementedError()

    def task_skipped(self, task_no):
        raise NotImplementedError()

    def task_failed(self, task_result):
        raise NotImplementedError()        

    def task_completed(self, task_result):
        raise NotImplementedError()

    def finished(self):
        raise NotImplementedError()


class ReporterGroup(BaseReporter):
    def __init__(self, other_reporters):
        self.reps = other_reporters


    def started(self):
        for r in self.reps:
            r.started()


    def task_skipped(self, task_no):
        for r in self.reps:
            r.task_skipped(task_no)


    def task_failed(self, task_no):
        for r in self.reps:
            r.task_failed(task_no)


    def task_completed(self, task_no):
        for r in self.reps:
            r.task_completed(task_no)


    def finished(self):
        for r in self.reps:
            r.finished()

    

class ConsoleReporter(BaseReporter):
    msg_str = "[{:4}/{:4} - {:4.2f}% complete] {:.51}"

    def _msg(self, msg, fail=False):
        if fail:
            self.failed = True
        s = self.msg_str.format(self.n_complete, self.n_tasks,
                                (float(self.n_complete)/self.n_tasks)*100, msg)
        if self.failed:
            s = "(!)" + s
        print >> sys.stderr, s
        

    def started(self):
        self.n_tasks = len(self.run_context.tasks)
        self.n_complete = 0
        self.failed = False

    
    def task_skipped(self, task_no):
        self._msg(self.run_context.tasks[task_no].name+" (Skipped)")


    def task_failed(self, task_no):
        self._msg(self.run_context.tasks[task_no].name+" (Failed)", fail=True)


    def task_completed(self, task_no):
        self._msg(self.run_context.tasks[task_no].name)


    def finished(self):
        print >> sys.stderr, "Run Finished"



class LoggerReporter(BaseReporter):
    pass



class WebhookReporter(BaseReporter):
    pass
