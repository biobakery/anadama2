# -*- coding: utf-8 -*-
import six
import os
import tempfile
import shutil
import subprocess

try:
    import cPickle as pickle
except ImportError:
    import pickle

class Task(object):
    """A unit of work. 
    
    :param name: The task name; must be unique to all tasks within a workflow.
    :type name: str or unicode
    
    :param actions: The actions to execute; do these and the work is done.
    :type actions: list of callable
    
    :param depends: The list of dependencies. 
    :type depends: list of :class:`anadama2.tracked.Base`
    
    :param targets: The list of targets. The task must produce all of
      these to be a successfully complete task.
    :type targets: list of :class:`anadama2.tracked.Base`
    
    :param task_no: The unique task number. Ordered by declaration,
      not execution.
    :type task_no: int

    :param visible: Whether the task should appear in the console or
      not.
    :type visible: bool

    """
    
    def __init__(self, name, actions, depends, targets, task_no, visible):
        # Set a default task number
        if task_no is None:
            self.task_no="NA"
        else:
            self.task_no=task_no
            
        # Set a default task name if not provided
        if name is None:
            self.name="Task"+str(task_no)
        else:
            self.name=name
            
        self.actions=actions
        self.depends=depends
        self.targets=targets
        self.visible=visible
        
        # get a task description based on the actions or name
        if six.callable(actions[0]):
            # if the first action is a function, use the function name
            command=actions[0].__name__
        else:
            # if the first action is a command, use the executable name
            command=six.u(actions[0]).split(" ")[0]
            
        # if the task name is not set, then use the command name for the description
        if name is None:
            self.description=command
        else:
            self.description=six.u(name)
            
class Document(object):
    """A document that is auto generated from a template. 
    
    :param templates: The document template files (or file)
    :type templates: str or list
    
    :param depends: The list of dependencies. 
    :type depends: list of :class:`anadama2.tracked.Base` or strings
    
    :param targets: The target(s). The document(s) to be generated.
    :type targets: :class:`anadama2.tracked.Base` or string
    
    :param vars: A dictionary of variables used by the template.
    :type vars: dict

    """
    
    def create(self, task):
        
        raise NotImplementedError()
    

class PweaveDocument(Document):
    """ A document using the pweave autogen tool """
    
    def __init__(self, templates=None, depends=None, targets=None, vars=None):
        # allow for a single template or multiple templates
        if templates is not None and isinstance(templates,basestring):
            templates=[templates]
        self.templates=templates
        self.depends=depends
        
        # if targets is a single item, save as a list
        if targets is not None and isinstance(targets,basestring):
            targets=[targets]
            
        self.targets=targets
        self.vars=vars
        
        # check for the required dependencies when using a pweave document
        # only check when creating an instance with a template to run
        if templates is not None:
            try:
                import numpy
            except ImportError:
                sys.exit("Please install numpy for document generation")
                
            try:
                import matplotlib.pyplot as pyplot
            except ImportError:
                sys.exit("Please install matplotlib for document generation")
                
            try:
                output=subprocess.check_output(["pypublish","-h"],stderr=subprocess.STDOUT)
            except EnvironmentError:
                sys.exit("Please instally pweave for document generation")
                
            try:
                output=subprocess.check_output(["pdflatex","--help"],stderr=subprocess.STDOUT)
            except EnvironmentError:
                sys.exit("Please install latex which includes pdflatex for document generation")

    def create(self, task):
        """ Create the documents specified as targets """

        # create the temp file to run with when ready to create the document
        temp_directory = tempfile.mkdtemp(dir=os.path.dirname(self.targets[0]))
        handle, temp_template = tempfile.mkstemp(dir=temp_directory, prefix="template")
        
        # merge the templates into the temp file
        for file in self.templates:
            for line in open(file):
                os.write(handle,line)
                
        os.close(handle)
        
        # if variables are provided, then create a pickled file to store these
        # for use when the document is created
        if self.vars is not None:
            # save the depends in the vars set
            self.vars["depends"]=self.depends
            # create a picked file with the temp template name in the same folder
            pickle.dump(self.vars, open(temp_template+".pkl", "wb"))

        # create the document
        # first move to the directory with the temp output files
        # this will cause all pweave output files to be written to this folder
        # by default the tex output file is written to the same folder as the
        # template file and the pdf files are written to the current working directory
        current_working_directory = os.getcwd()
        os.chdir(temp_directory)
        
        output = subprocess.check_output(["pypublish",temp_template,"-f","pdf"])
        
        # change back to original working directory
        os.chdir(current_working_directory)
        
        # rename to the original target name and location specified
        shutil.copy(temp_template+".pdf",self.targets[0])
        
        # move the figures folder to the output folder
        # remove the original folder if it already exists
        output_dir = os.path.dirname(self.targets[0])
        if os.path.isdir(os.path.join(output_dir,"figures")):
            shutil.rmtree(os.path.join(output_dir,"figures"))
            
        shutil.move(os.path.join(temp_directory,"figures"),os.path.dirname(self.targets[0]))
        
        # remove all of the temp files in the temp folder
        shutil.rmtree(temp_directory)
        
        
    def get_vars(self):
        """ Try to get the variables from the pickled file """
        
        # the pickled file will be the same name as the template name
        # the current working directory will hold the pickle file
        # the current working directory is a temp folder
        
        # find the pickle file
        pickle_file = filter(lambda x: x.endswith(".pkl"),os.listdir("."))
        
        vars = pickle.load(open(pickle_file[0],"rb"))
        
        return vars
            
    
    def read_table(self, file, invert=None, delimiter="\t", only_data_columns=None):
        """ Read the table from a text file with the first line the column
        names and the first column the row names. """
        
        data=[]
        row_names=[]
        with open(file) as file_handle:
            column_names = file_handle.readline().rstrip().split(delimiter)[1:]
            for line in file_handle:
                line=line.rstrip().split(delimiter)
                row_names.append(line[0])
                data.append([float(i) for i in line[1:]])
                
        # remove extra columns if not requested
        if only_data_columns is not None:
            column_names=[column_names[i] for i in only_data_columns]
            new_data=[]
            for row in data:
                new_data.append([row[i] for i in only_data_columns])
            data=new_data
                
        return column_names, row_names, data
        

    def plot_grouped_barchart(self, data, row_labels, column_labels, title, 
        xlabel=None, ylabel=None, legend_title=None, yaxis_in_millions=None):
        """ Plot a grouped barchart """
        
        import numpy
        import matplotlib.pyplot as pyplot
        import matplotlib.ticker as ticker
        
        # create a figure subplot to move the legend
        figure = pyplot.figure()
        subplot=pyplot.subplot(111)
        
        # change the yaxis format if set
        axis = pyplot.gca()
        if yaxis_in_millions:
            axis.get_yaxis().set_major_formatter(ticker.FuncFormatter(lambda value, position: "{:,}".format(int(value/1000000))))
        
        # set the width of the bars as each total group width is one
        bar_start_point = numpy.arange(len(column_labels))
        gap = 0.1
        bar_width = (1.0 - gap) / len(data)
        
        # create the grouped barplots with gap offsets
        barplots=[]
        for i, data_set in enumerate(data):
            barplots.append(subplot.bar(bar_start_point + i*bar_width, data_set, width=bar_width))

        # move the bottom of the figure for larger xaxis labels
        # done first before adjusting the width of the figure
        figure.subplots_adjust(bottom=0.3)    
        
        # reduce the size of the plot to fit in the legend and the xlabels
        subplot_position=subplot.get_position()
        subplot.set_position([subplot_position.x0, subplot_position.y0, 
            subplot_position.width *0.80, subplot_position.height])
        
        # add labels and title
        if xlabel is not None:
            pyplot.xlabel(xlabel)
        if ylabel is not None:
            pyplot.ylabel(ylabel)
            
        pyplot.title(title)
        
        # place the xticks in the middle of each group
        pyplot.xticks(bar_start_point + 0.5, column_labels, fontsize=7, rotation="vertical")
        pyplot.yticks(fontsize=7)
        
        # set the limits on the x axis so the edge gaps are correct
        pyplot.xlim(0-gap,len(column_labels))
        
        subplot.legend(barplots,row_labels,loc="center left", bbox_to_anchor=(1,0.5),
            fontsize=7, title=legend_title, frameon=False)
        
        pyplot.show()  
        
    def plot_stacked_barchart(self, data, row_labels, column_labels, title, 
        xlabel=None, ylabel=None, legend_title=None):
        """ Plot a stacked barchart """
        
        import numpy
        import matplotlib.pyplot as pyplot
        
        figure = pyplot.figure()
        subplot=pyplot.subplot(111)
        bar_plots=[]
        names=[]
        
        # create a plot for each stacked group
        plot_indexes=numpy.arange(len(column_labels))
        y_offset=numpy.array([0.0]*len(column_labels))
        for name, plot_abundance in zip(row_labels, data):
            bar_plots.append(subplot.bar(plot_indexes, plot_abundance, 
                bottom=y_offset, align="center"))
            names.append(name)
            # add to the y_offset which is the bottom of the stacked plot
            y_offset=y_offset+plot_abundance
            
        # move the bottom of the figure for larger xaxis labels
        # done first before adjusting the width of the figure
        figure.subplots_adjust(bottom=0.3)
            
        # reduce the size of the plot to fit in the legend
        subplot_position=subplot.get_position()
        subplot.set_position([subplot_position.x0, subplot_position.y0, 
            subplot_position.width *0.80, subplot_position.height])
            
        # Add the title, labels, and legend
        if xlabel is not None:
            subplot.set_xlabel(xlabel)
        if ylabel is not None:
            subplot.set_ylabel(ylabel)
            
        pyplot.title(title)
        
        pyplot.xticks(plot_indexes, column_labels, fontsize=7, rotation="vertical")
        pyplot.yticks(fontsize=7)
        subplot.legend(bar_plots,names,loc="center left", bbox_to_anchor=(1,0.5),
            fontsize=7, title=legend_title, frameon=False)
        
        pyplot.show()
        
    def show_table(self, data, row_labels, column_labels, title, format_data_comma=None,
                   column_width=0.12):
        """ Plot the data as a table """
        
        import matplotlib.pyplot as pyplot
        
        fig = pyplot.figure()
        # create a subplot and remove frame and x/y axis
        subplot = fig.add_subplot(111, frame_on=False)
        subplot.xaxis.set_visible(False)
        subplot.yaxis.set_visible(False)
        
        # reduce height of the empty plot to as small as possible
        # remove subplot padding
        fig.subplots_adjust(bottom=0.85, wspace=0, hspace=0)
        
        # if the option is set to format the data, add commas
        if format_data_comma:
            format_data=[]
            for row in data:
                format_data.append(["{:,}".format(int(i)) for i in row])
            data=format_data
        
        # create the table
        table = pyplot.table(cellText=data,
            colWidths = [column_width]*len(column_labels),
            rowLabels=row_labels, colLabels=column_labels, loc="bottom")
        
        # set the font size for the table
        table.set_fontsize(8)
        
        # add the title
        pyplot.title(title)
        
        # plot the table
        pyplot.show() 
        
    def write_table(self, column_labels, row_labels, data, file):
        """ Write a table of data to a file """
    
        with open(file, "wb") as file_handle:
            file_handle.write("\t".join(column_labels)+"\n")
            for name, row in zip(row_labels, data):
                file_handle.write("\t".join([name]+[str(i) for i in row])+"\n")
        
    def show_hclust2(self,sample_names,feature_names,data,title):
        """ Create a hclust2 heatmap with dendrogram and show it in the document """
        from matplotlib._png import read_png
        import matplotlib.pyplot as pyplot
        
        # write a file of the data
        handle, hclust2_input_file=tempfile.mkstemp(prefix="hclust2_input",dir=os.getcwd())
        heatmap_file=hclust2_input_file+".png"
        self.write_table(["# "]+sample_names,feature_names,data,hclust2_input_file)
        
        label_font="7"
        # compute the aspect ratio based on the number of samples and features
        aspect_ratio=len(sample_names)/(len(feature_names)*1.0)
        output=subprocess.check_output(["hclust2.py","-i",hclust2_input_file,"-o",heatmap_file,
                                        "--title",title,
                                        "--title_font",str(int(label_font)*2),
                                        "--cell_aspect_ratio",str(aspect_ratio),
                                        "--flabel_size",label_font,"--slabel_size",label_font,
                                        "--colorbar_font_size",label_font])
        # read the heatmap png file
        heatmap=read_png(heatmap_file)
        
        # create a subplot and remove the frame and axis labels
        fig = pyplot.figure()
        subplot = fig.add_subplot(111, frame_on=False)
        subplot.xaxis.set_visible(False)
        subplot.yaxis.set_visible(False)
        
        # show but do not interpolate (as this will make the text hard to read)
        pyplot.imshow(heatmap, interpolation="none")

from .workflow import Workflow

Workflow # pyflakes
