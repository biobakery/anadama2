# -*- coding: utf-8 -*-
from . import runners

class DummyPowerup(object):

    def do(self, task, **opts):
        pass

    def add_task(self, task, **opts):
        pass

    def runner(self, ctx, jobs=1, grid_jobs=1):
        return runners.default(ctx, jobs)

