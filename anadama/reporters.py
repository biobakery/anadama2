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

    def task_started(self, task_no):
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


    def task_started(self, task_no):
        for r in self.reps:
            r.task_started(task_no)


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
    msg_str = "({:.1})[{:3}/{:3} - {:6.2f}%] {:.57}"

    class stats:
        skip = "s"
        fail = "!"
        done = "+"
        start = " "

    def __init__(self, *args, **kwargs):
        super(ConsoleReporter, self).__init__(*args, **kwargs)
        self.failed_results = list()


    def _msg(self, status, msg, c_r=False):
        if c_r:
            self.n_complete += 1
        s = self.msg_str.format(status, self.n_complete, self.n_tasks,
                                (float(self.n_complete)/self.n_tasks)*100, msg)
        if c_r:
            s = "\r" + s + "\n"
        sys.stderr.write(s)
        

    def started(self):
        self.n_tasks = len(self.run_context.tasks)
        self.n_complete = 0
        self.failed = False

    def task_started(self, task_no):
        self._msg(self.stats.start, self.run_context.tasks[task_no].name)
    
    def task_skipped(self, task_no):
        self._msg(self.stats.skip, self.run_context.tasks[task_no].name, True)

    def task_failed(self, task_result):
        name = self.run_context.tasks[task_result.task_no].name
        self.failed_results.append((name, task_result))
        self._msg(self.stats.fail, name, True)

    def task_completed(self, task_result):
        name = self.run_context.tasks[task_result.task_no].name
        self._msg(self.stats.done, name, True)

    def finished(self):
        print >> sys.stderr, "Run Finished"
        for name, result in self.failed_results:
            print >> sys.stderr, "Task {} failed".format(result.task_no)
            print >> sys.stderr, "  Name: "+name
            print >> sys.stderr, "  Original error: "
            for line in result.error.split("\n"):
                print >> sys.stderr, "  "+line



class LoggerReporter(BaseReporter):
    pass



class WebhookReporter(BaseReporter):
    pass
