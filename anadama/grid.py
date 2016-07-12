from . import runners

class DummyPowerup(object):

    def do(self, task, **opts):
        pass

    def add_task(self, task, **opts):
        pass

    def runner(self, ctx, n_parallel=1, n_grid_parallel=1):
        return runners.default(ctx, n_parallel)

