def default():
    pass

class BaseReporter(object):

    def __init__(self, run_context):
        self.run_context = run_context

    def task_skipped(self, task_no):
        raise NotImplementedError()

    def task_failed(self, task_no):
        raise NotImplementedError()        

    def task_completed(self, task_no):
        raise NotImplementedError()

    def finished(self):
        raise NotImplementedError()


class ReporterGroup(BaseReporter):
    def __init__(self, other_reporters):
        self.reps = other_reporters


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
    


class LoggerReporter(object):
    pass

class FileReporter(object):
    pass

class WebhookReporter(object):
    pass
