import getopt

from doit.cmdparse import CmdParseError
from doit.cmdparse import DefaultUpdate

# monkey patch for doit.cmdparse.CmdParse:parse
def parse(self, in_args):
    params = DefaultUpdate()
    # add default values
    for opt in self.options:
        params.set_default(opt.name, opt.default)
        
        # parse options using getopt
    try:
        opts, args = getopt.gnu_getopt(in_args, self.get_short(),
                                   self.get_long())
    except Exception as error:
        msg = "Error parsing %s: %s (parsing options: %s)"
        raise CmdParseError(msg % (self._type, str(error), in_args))
        
    # update params with values from command line
    for opt, val in opts:
        this, inverse = self.get_option(opt)
        if this.type is bool:
            params[this.name] = not inverse
        elif this.type is list:
            params[this.name].append(val)
        else:
            try:
                params[this.name] = this.type(val)
            except ValueError as exception:
                msg = "Error parsing parameter '%s' %s.\n%s\n"
                raise CmdParseError(msg % (this.name, this.type,
                                           str(exception)))
                
    return params, args
