# -*- coding: utf-8 -*-
import os
import tempfile
import shutil
import subprocess
import itertools
import sys

from .helpers import sh
from . import Task

try:
    import cPickle as pickle
except ImportError:
    import pickle
    
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

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
    """A document that is auto generated from a template using Pweave and Pandoc 
    
    :keyword templates: The document template files (or file)
    :type templates: str or list
    
    :keyword depends: The list of dependencies. 
    :type depends: list of :class:`anadama2.tracked.Base` or strings
    
    :keyword targets: The target(s). The document(s) to be generated.
    :type targets: :class:`anadama2.tracked.Base` or string
    
    :keyword vars: A dictionary of variables used by the template.
    :type vars: dict
    
    :keyword table_of_contents: If set add table of contents to reports
    :type table_of_contents: bool

    """
    
    def __init__(self, templates=None, depends=None, targets=None, vars=None, table_of_contents=None):
        # allow for a single template or multiple templates
        if templates is not None and isinstance(templates,str):
            templates=[templates]
        self.templates=templates
        self.depends=depends
        
        self.table_of_contents=table_of_contents
        
        # if targets is a single item, save as a list
        if targets is not None and isinstance(targets,str):
            targets=[targets]
            
        self.targets=targets
        self.vars=vars
        
        # set the max number of x tick labels to be shown on plots
        self.max_labels = 65
        # set the max labels for legends
        self.max_labels_legend = 30
        
        # set the location of the final figures and data folders
        # only create if the document has a target
        if self.targets:
            self.figures_folder = os.path.join(os.path.dirname(self.targets[0]),"figures")
            if not os.path.isdir(self.figures_folder):
                os.makedirs(self.figures_folder)
                
            self.data_folder = os.path.join(os.path.dirname(self.targets[0]),"data")
            if not os.path.isdir(self.data_folder):
                os.makedirs(self.data_folder)
        else:
            # if a target is not provided use the variables to try to find the folder locations
            try:
                self.vars=self.get_vars()
            except (IndexError, EOFError):
                self.vars=None
                
            if self.vars:
                self.figures_folder = os.path.join(os.path.dirname(self.vars["targets"][0]),"figures")
                self.data_folder = os.path.join(os.path.dirname(self.vars["targets"][0]),"data")
        
        # check for the required dependencies when using a pweave document
        # only check when creating an instance with a template to run
        if templates is not None:
            try:
                import numpy
            except ImportError:
                sys.exit("Please install numpy for document generation")
                
            try:
                # set non-interactive backend to simplify server install
                import matplotlib
                matplotlib.use('Agg')
                import matplotlib.pyplot as pyplot
            except ImportError:
                sys.exit("Please install matplotlib for document generation")
                
            try:
                output=subprocess.check_output(["pypublish","-h"],stderr=subprocess.STDOUT)
            except EnvironmentError:
                sys.exit("Please install pweave for document generation")
                
            try:
                output=subprocess.check_output(["pdflatex","--help"],stderr=subprocess.STDOUT)
            except EnvironmentError:
                sys.exit("Please install latex which includes pdflatex for document generation")
                
        # copy over the file dependencies to the data folder
        if self.depends and self.data_folder and os.path.isdir(self.data_folder):
            depends_files = list(filter(lambda x: not isinstance(x,Task), self.depends))
            for data_file in depends_files:
                if data_file:
                    shutil.copy(data_file,os.path.join(self.data_folder,os.path.basename(data_file)))

    def create(self, task):
        """ Create the documents specified as targets """

        # get the template extension
        template_extension=os.path.splitext(self.templates[0])[-1]
            
        # get the report and figure extensions based on the target
        report_filename, report_extension=os.path.splitext(self.targets[0])

        # create the temp file to run with when ready to create the document
        temp_directory = tempfile.mkdtemp(dir=os.path.dirname(self.targets[0]))
        temp_template_basename = os.path.join(temp_directory,os.path.basename(report_filename))
        # keep the extension of the template for pweave auto reader function
        temp_template = temp_template_basename + template_extension
        
        # merge the templates into the temp file
        with open(temp_template,"w") as handle:
            for file in self.templates:
                for line in open(file):
                    handle.write(line)
                
        handle.close()
        
        # if variables are provided, then create a pickled file to store these
        # for use when the document is created
        if self.vars is not None:
            # save the depends in the vars set
            self.vars["depends"]=self.depends
            
            # save the targets
            self.vars["targets"]=self.targets
            
            # change directories/files to the full paths (so no longer required in input)
            # this is because pweave does not have an output folder option (just cwd so a ch is needed)
            for variable in self.vars:
                try:
                    if os.path.isdir(self.vars[variable]) or os.path.isfile(self.vars[variable]):
                        self.vars[variable] = os.path.abspath(self.vars[variable])
                except (TypeError, ValueError, NameError):
                    # ignore lists and None values
                    continue
            # create a picked file with the temp template name in the same folder
            pickle.dump(self.vars, open(temp_template_basename+".pkl", "wb"))

        # create the document
        # first move to the directory with the temp output files
        # this will cause all pweave output files to be written to this folder
        # by default the tex output file is written to the same folder as the
        # template file and the pdf files are written to the current working directory
        current_working_directory = os.getcwd()
        os.chdir(temp_directory)
        
        # get the intermediate output file based on the initial template name
        intermediate_template = temp_template_basename+"."+"md"
            
        # process the template based on the extension type
        temp_report = temp_template_basename+"."+report_extension
        
        # set the pandoc command based on if a table of contents will be included
        pandoc_command="pandoc {0} -o {1} --variable=linkcolor:Blue "+\
            "--variable=toccolor:Blue --latex-engine=pdflatex --standalone" 
        if self.table_of_contents:
            pandoc_command+=" --toc"
        
        # run pweave then pandoc to generate document
        
        # call pweave to use class with fix
        from pweave import PwebPandocFormatter, Pweb
        from pweave.readers import PwebScriptReader

        class PwebPandocFormatterFixedFigures(PwebPandocFormatter):
            def make_figure_string_size(self, figname, width, label, caption = ""):
                # new function to fix figure width string to work with pandoc format
                # only use width if pandoc installed is >= v1.16.0
                if self.new_pandoc:
                    figstring="![%s](%s){ width=%s }\n" % (caption, figname, width)
                else:
                    figstring="![%s](%s)\n" % (caption, figname) 
                
                if caption == "":
                    figstring += "\\"
        
                figstring += "\n"
                
                return figstring

            def formatfigure(self, chunk):
                fignames = chunk['figure']
                if chunk["caption"]:
                    caption = chunk["caption"]
                else:
                    caption = ""
                figstring = ""
                
                # increase default figure size
                if not chunk["width"]:
                    chunk["width"]="100%"
        
                if chunk['caption'] and len(fignames) > 0:
                    if len(fignames) > 1:
                        print("INFO: Only including the first plot in a chunk when the caption is set")
                        figstring = self.make_figure_string_size(fignames[0], chunk["width"], chunk["name"], caption)
                        return figstring
        
                for fig in fignames:
                    # original line which duplicates figures commented out and replaced
                    #figstring += self.make_figure_string(fignames[0], chunk["width"], chunk["name"])
                    figstring += self.make_figure_string_size(fig, chunk["width"], chunk["name"])
        
                return figstring

        # capture stdout messages
        original_stdout = sys.stdout
        sys.stdout = capture_stdout = StringIO()
        
        doc = Pweb(temp_template)
        doc.setformat(Formatter = PwebPandocFormatterFixedFigures)
        doc.detect_reader()
        doc.weave()
        
        sys.stdout = original_stdout
        
        sh(pandoc_command.format(intermediate_template, temp_report),log_command=True)()          
        
        # change back to original working directory
        os.chdir(current_working_directory)
        
        # rename to the original target name and location specified
        shutil.copy(temp_report,self.targets[0])
        
        # move the temp figures files
        temp_figures_folder=os.path.join(temp_directory,"figures")
        for file in os.listdir(temp_figures_folder):
            shutil.move(os.path.join(temp_figures_folder,file),os.path.join(self.figures_folder,file))
        
        # remove all of the temp files in the temp folder
        shutil.rmtree(temp_directory)
        
        # remove the figures folder if it is empty
        if len(os.listdir(self.figures_folder)) == 0:
            os.rmdir(self.figures_folder)
        
        # remove the data folder if it is empty
        if len(os.listdir(self.data_folder)) == 0:
            os.rmdir(self.data_folder)
        
        
    def get_vars(self):
        """ Try to get the variables from the pickled file """
        
        # the pickled file will be the same name as the template name
        # the current working directory will hold the pickle file
        # the current working directory is a temp folder
        
        # find the pickle file
        pickle_file = list(filter(lambda x: x.endswith(".pkl"),os.listdir(".")))
        
        vars = pickle.load(open(pickle_file[0],"rb"))
        
        return vars
            
    
    def read_table(self, file, invert=None, delimiter="\t", only_data_columns=None, format_data=None):
        """ Read the table from a text file with the first line the column
        names and the first column the row names. 
        
        :param file: The file to read
        :type file: str
            
        :keyword invert: Invert the table rows/columns after reading
        :type invert: bool
        
        :keyword delimiter: The delimiter present in the file
        :type delimiter: str
        
        :keyword only_data_columns: Remove the header and row names
        :type only_data_columns: bool
        
        :keyword format_data: A function to use to format the data
        :type format_data: function
        
        """
        
        def try_format_data(function, data):
            """ Try to format the data, except use zero """
            
            try:
                formatted_data=function(data)
            except ValueError:
                formatted_data=function(0)
                
            return formatted_data
            
        
        # if not set, format data to floats
        if format_data is None:
            format_data=float
        
        data=[]
        row_names=[]
        with open(file) as file_handle:
            column_names = file_handle.readline().rstrip().split(delimiter)[1:]
            for line in file_handle:
                line=line.rstrip().split(delimiter)
                row_names.append(line[0])
                data.append([try_format_data(format_data, i) for i in line[1:]])
                
        # remove extra columns if not requested
        if only_data_columns is not None:
            column_names=[column_names[i] for i in only_data_columns]
            new_data=[]
            for row in data:
                new_data.append([row[i] for i in only_data_columns])
            data=new_data
                
        return column_names, row_names, data
    
    def sorted_data_numerical_or_alphabetical(self, data):
        """ Sort the data numerically or alphabetically depending on data type """

        # sort the data alphabetically
        sorted_data = sorted(data)
        
        try:
            # allow for NA in keys
            na=False
            if "NA" in data:
                data.remove("NA")
                na=True
            sorted_data = sorted(data, key=float)
            if na:
                sorted_data+=["NA"]
        except ValueError:
            pass        
        
        return sorted_data

    def add_threshold(self, threshold, color, label):
         """  Adds horizontal line to plot as threshold
            :param threshold:  float
            :param color:  string
            :param label:  string
         """
         import matplotlib.pyplot as pyplot

         pyplot.axhline(threshold, color=color)
         pyplot.text(0, int(threshold) + 50, label)

         pyplot.show()


    def plot_stacked_barchart_grouped(self, grouped_data, row_labels, column_labels_grouped, title, 
        ylabel=None, legend_title=None, legend_style="normal", legend=True, legend_size=7):
        """ Plot a stacked barchart with data grouped into subplots
        
        :param grouped_data: A dict of lists containing the grouped data
        :type data: dict
        
        :param row_labels: The labels for the data rows
        :type row_labels: list
        
        :param column_labels_grouped: The labels for the columns grouped
        :type column_labels: dict
        
        :param title: The title for the plot
        :type title: str
        
        :keyword ylabel: The y-axis label
        :type ylabel: str
        
        :keyword legend_title: The title for the legend
        :type legend_title: str
        
        :keyword legend_style: The font style for the legend
        :type legend_style: str
        
        :keyword legend: Display legend
        :type legend: bool
        
        :keyword legend_size: The font size for the legend
        :type legend_size: int
        
        """
        
        import numpy
        import matplotlib.pyplot as pyplot
        
        total_groups=len(grouped_data.keys())
        figure, group_axis = pyplot.subplots(1, total_groups, sharey=True, gridspec_kw = {'wspace':0.02})

        # create a set of custom colors to prevent overlap
        # get only a set number of items to recycle colors through subplots
        custom_colors=list(itertools.islice(self._custom_colors(total_colors=len(row_labels)),len(row_labels)))

        # create a subplot for each group
        group_number=0
        
        # sort the groups prior to plotting
        sorted_group_names = self.sorted_data_numerical_or_alphabetical(grouped_data.keys())
            
        # get the total number of columns for all groups
        total_columns_all_groups=len(list(itertools.chain.from_iterable(column_labels_grouped.values())))
        
        for group_name in sorted_group_names:
            data = grouped_data[group_name]
            bar_plots=[]
            # create a plot for each stacked group
            column_labels=column_labels_grouped[group_name]
            plot_indexes=numpy.arange(len(column_labels))
            y_offset=numpy.array([0.0]*len(column_labels))
            for plot_abundance, color in zip(data, custom_colors):
                bar_plots.append(group_axis[group_number].bar(plot_indexes, plot_abundance, 
                    bottom=y_offset, align="center", color=color))
                # add to the y_offset which is the bottom of the stacked plot
                y_offset=y_offset+plot_abundance
            
            # set the current axis to this groups plot
            pyplot.sca(group_axis[group_number])    
            
            # Add the title, labels, and legend
            pyplot.title(group_name, size=12)
            # only set the ylabels for the first subplot
            if ylabel is not None and group_number == 0:
                pyplot.ylabel(ylabel)
            else:
                pyplot.tick_params(axis="y",which="both",left="off",labelleft="off")
                
            # only label the x-axis if all subplots can have labels
            if total_columns_all_groups <= self.max_labels:
                # move the bottom of the figure for larger xaxis labels
                figure.subplots_adjust(bottom=0.3)
                pyplot.xticks(plot_indexes, column_labels, fontsize=7, rotation="vertical")
            else:
                pyplot.tick_params(axis="x",which="both",bottom="off",labelbottom="off")
            pyplot.yticks(fontsize=7)
            
            group_number+=1
            
        # add the legend to the last subplot
        if legend:
            # reduce the size of the plot to fit in the legend
            figure.subplots_adjust(right=0.75)
                
            pyplot.legend(bar_plots,row_labels, loc="center left", bbox_to_anchor=(1,0.5),
                title=legend_title, frameon=False, prop={"size":legend_size, "style":legend_style})
            
        figure.suptitle(title, fontsize=14)
        
        pyplot.show()

    def plot_grouped_barchart(self, data, row_labels, column_labels, title, 
        xlabel=None, ylabel=None, legend_title=None, yaxis_in_millions=None):
        """ Plot a grouped barchart 
        
        :param data: A list of lists containing the data
        :type data: list
        
        :param row_labels: The labels for the data rows
        :type row_labels: list
        
        :param column_labels: The labels for the columns
        :type column_labels: list
        
        :param title: The title for the plot
        :type title: str
        
        :keyword xlabel: The x-axis label
        :type xlabel: str
        
        :keyword ylabel: The y-axis label
        :type ylabel: str
        
        :keyword legend_title: The title for the legend
        :type legend_title: str
        
        :keyword yaxis_in_millions: Show the y-axis in millions
        :type yaxis_in_millions: bool
        
        """
        
        import numpy
        import matplotlib.pyplot as pyplot
        import matplotlib.ticker as ticker
        
        # create a figure subplot to move the legend
        figure = pyplot.figure()
        subplot=pyplot.subplot(111)
        
        # create a set of custom colors to prevent overlap
        custom_colors=self._custom_colors(total_colors=len(row_labels))
        
        # change the yaxis format if set
        axis = pyplot.gca()
        if yaxis_in_millions:
            # get the max value to determine if decimals should be shown on the label 
            max_value=max([max(row)for row in data])/1000000.0
            if max_value <= 0.5:
                yaxis_format = lambda value, position: "{:,.3f}".format(float(value/1000000.0))
            elif max_value <= 1:
                yaxis_format = lambda value, position: "{:,.2f}".format(float(value/1000000.0))
            elif max_value <= 5:
                yaxis_format = lambda value, position: "{:,.1f}".format(float(value/1000000.0))
            else:
                yaxis_format = lambda value, position: "{:,}".format(int(value/1000000))
            axis.get_yaxis().set_major_formatter(ticker.FuncFormatter(yaxis_format))
        
        # set the width of the bars as each total group width is one
        bar_start_point = numpy.arange(len(column_labels))
        gap = 0.1
        bar_width = (1.0 - gap) / len(data)
        
        # create the grouped barplots with gap offsets
        barplots=[]
        for i, data_set in enumerate(data):
            barplots.append(subplot.bar(bar_start_point + i*bar_width, data_set,
                width=bar_width, color=next(custom_colors)))   
        
        # add labels and title
        if xlabel is not None and len(column_labels) <= self.max_labels:
            pyplot.xlabel(xlabel)
        if ylabel is not None:
            pyplot.ylabel(ylabel)
            
        pyplot.title(title)
        
        # place the xticks in the middle of each group
        if len(column_labels) <= self.max_labels:
            # move the bottom of the figure for larger xaxis labels
            # done first before adjusting the width of the figure
            figure.subplots_adjust(bottom=0.3) 
            pyplot.xticks(bar_start_point + 0.5, column_labels, fontsize=7, rotation="vertical")
        else:
            pyplot.tick_params(axis="x",which="both",bottom="off",labelbottom="off")
        pyplot.yticks(fontsize=7)
        
        # set the limits on the x axis so the edge gaps are correct
        pyplot.xlim(0-gap,len(column_labels))
        
        # reduce the size of the plot to fit in the legend
        subplot_position=subplot.get_position()
        subplot.set_position([subplot_position.x0, subplot_position.y0, 
            subplot_position.width *0.80, subplot_position.height])
        
        subplot.legend(barplots,row_labels,loc="center left", bbox_to_anchor=(1,0.5),
            fontsize=7, title=legend_title, frameon=False)
        
        pyplot.show()  
        
    def plot_scatter(self, data, title, row_labels, xlabel=None, ylabel=None, trendline=None):
        """ Plot a scatter plot 

        :param data: A list of lists containing the data
        :type data: list
        
        :param title: The title for the plot
        :type title: str
        
        :param row_labels: The labels for the data rows
        :type row_labels: list
        
        :keyword xlabel: The x-axis label
        :type xlabel: str
        
        :keyword ylabel: The y-axis label
        :type ylabel: str
        
        :keyword trendline: Add a trendline to the plot
        :type trendline: bool
        
        """
        
        import numpy
        import matplotlib.pyplot as pyplot
        
        # create a figure subplot to move the legend
        figure = pyplot.figure()
        subplot = pyplot.subplot(111)

        plots=[]
        for x,y in data:
            # add a scatter plot
            plots.append(subplot.scatter(x,y))
            
            if trendline:
                # compute linear least squares polynomial fit, returns vector of coefficients
                coeff = numpy.polyfit(x,y,1)
                trendline_function = numpy.poly1d(coeff)
                # add trendline to the plot
                pyplot.plot(x,trendline_function(x))
        if ylabel:
            pyplot.ylabel(ylabel)
        if xlabel:
            pyplot.xlabel(xlabel)
            
        # reduce the size of the plot to fit in the legend and the xlabels
        subplot_position=subplot.get_position()
        subplot.set_position([subplot_position.x0, subplot_position.y0, 
            subplot_position.width *0.80, subplot_position.height])
        
        subplot.legend(plots,row_labels,loc="center left", bbox_to_anchor=(1,0.5),
            fontsize=7, frameon=False)
            
        pyplot.title(title)

        pyplot.show()        
        
    def plot_barchart(self, data, labels=None, title=None, xlabel=None, ylabel=None):
        """ Plot a barchart 
        
        :param data: A list of lists containing the data
        :type data: list

        :keyword labels: The labels for the data rows
        :type labels: list

        :keyword title: The title for the plot
        :type title: str
        
        :keyword xlabel: The x-axis label
        :type xlabel: str
        
        :keyword ylabel: The y-axis label
        :type ylabel: str
        
        """
        
        import numpy
        import matplotlib.pyplot as pyplot
        
        figure = pyplot.figure()
        
        # check for a list of lists
        # if a list of lists of single items is found convert to a list
        # if lists of multiple items are found then issue error
        if isinstance(data[0], list):
            max_length=max([len(row) for row in data])
            if max_length == 1:
                data_list=[row[0] for row in data]
                data=data_list
            else:
                raise ValueError("Provide data to the AnADAMA2 document.plot_barchart as a list of floats or ints.")

        positions=numpy.arange(len(data))
        pyplot.bar(positions, data, align="center")
        if labels:
            pyplot.xticks(positions, labels, rotation="vertical")
            # move the bottom of the figure for larger xaxis labels
            figure.subplots_adjust(bottom=0.3)
        
        if ylabel:
            pyplot.ylabel(ylabel)
        if xlabel:
            pyplot.xlabel(xlabel)
            
        pyplot.title(title)

        pyplot.show() 
        
    def _custom_colors(self,total_colors):
        """ Get a set of custom colors for a matplotlib plot """
        
        from matplotlib import cm

        # create a set of custom colors
        
        # get the max amount of colors for a few different color maps
        terrain=[cm.terrain(i/7.0) for i in range(7)]
        # don't use the last dark2 color as this overlaps with the first terrain color
        dark=[cm.Dark2(i/8.0) for i in range(7)]
        jet=[cm.jet(i/7.0) for i in range(7)]
        nipy_spectral=[cm.nipy_spectral(i/10.0) for i in range(10)]
        set3=[cm.Set3(i/12.0) for i in range(12)]
        
        # select the total numer of color maps based on the total number of colors
        if total_colors <= 7:
            sets=[terrain]
        elif total_colors <= 14:
            sets=[terrain,dark]
        elif total_colors <= 24:
            sets=[terrain,dark,nipy_spectral]
        else:
            sets=[terrain,dark,nipy_spectral,set3]
        
        # return a mixed set of colors from each set used
        # repeat colors if we run out
        for color_set in itertools.cycle(zip(*sets)):
            for color in color_set:
                yield color

  

    def plot_stacked_barchart(self, data, row_labels, column_labels, title,
        xlabel=None, ylabel=None, legend_title=None, legend_style="normal", legend_size=7):
        """ Plot a stacked barchart
        
        :param data: A list of lists containing the data
        :type data: list
        
        :param row_labels: The labels for the data rows
        :type row_labels: list
        
        :param column_labels: The labels for the columns
        :type column_labels: list
        
        :param title: The title for the plot
        :type title: str
        
        :keyword xlabel: The x-axis label
        :type xlabel: str
        
        :keyword ylabel: The y-axis label
        :type ylabel: str
        
        :keyword legend_title: The title for the legend
        :type legend_title: str
        
        :keyword legend_style: The font style for the legend
        :type legend_style: str
        
        :keyword legend_size: The font size for the legend
        :type legend_size: int
        
        """
        
        import numpy
        import matplotlib.pyplot as pyplot
        
        figure = pyplot.figure()
        subplot=pyplot.subplot(111)
        bar_plots=[]
        names=[]
        
        # create a set of custom colors to prevent overlap
        custom_colors=self._custom_colors(total_colors=len(row_labels))
        
        # create a plot for each stacked group
        plot_indexes=numpy.arange(len(column_labels))
        y_offset=numpy.array([0.0]*len(column_labels))
        for name, plot_abundance, color in zip(row_labels, data, custom_colors):
            bar_plots.append(subplot.bar(plot_indexes, plot_abundance, 
                bottom=y_offset, align="center", color=color))
            names.append(name)
            # add to the y_offset which is the bottom of the stacked plot
            y_offset=y_offset+plot_abundance
            
        # Add the title, labels, and legend
        if xlabel is not None and len(column_labels) <= self.max_labels:
            subplot.set_xlabel(xlabel)
        if ylabel is not None:
            subplot.set_ylabel(ylabel)
            
        pyplot.title(title)
        
        if len(column_labels) <= self.max_labels:
            # move the bottom of the figure for larger xaxis labels
            # done first before adjusting the width of the figure
            figure.subplots_adjust(bottom=0.3)
            # add labels
            pyplot.xticks(plot_indexes, column_labels, fontsize=7, rotation="vertical")
        else:
            pyplot.tick_params(axis="x",which="both",bottom="off",labelbottom="off")
        
        # reduce the size of the plot to fit in the legend
        subplot_position=subplot.get_position()
        subplot.set_position([subplot_position.x0, subplot_position.y0, 
            subplot_position.width *0.80, subplot_position.height])
            
        pyplot.yticks(fontsize=7)
        subplot.legend(bar_plots,names,loc="center left", bbox_to_anchor=(1,0.5),
            title=legend_title, frameon=False, prop={"size":legend_size, "style":legend_style})
        
        pyplot.show()
        
    def show_table(self, data, row_labels, column_labels, title, format_data_comma=None,
                   location="center", font=None):
        """ Plot the data as a table 
        
        :param data: A list of lists containing the data
        :type data: list
        
        :param row_labels: The labels for the data rows
        :type row_labels: list
        
        :param column_labels: The labels for the columns
        :type column_labels: list
        
        :param title: The title for the plot
        :type title: str
        
        :keyword format_data_comma: Format the data as comma delimited
        :type format_data_comma: bool
        
        :keyword location: The location for the text in the cell
        :type location: str
        
        :keyword font: The size of the font
        :type font: int
        
        """
        
        import numpy
        import matplotlib.pyplot as pyplot
        from matplotlib.table import Table
        
        # if the option is set to format the data, add commas
        if format_data_comma:
            format_data = [map(lambda x: "{:,}".format(int(x)),row) for row in data]
        else:
            format_data = [map(str,row) for row in data]
        data=format_data
        
        # create a figure in one subplot
        figure, axis = pyplot.subplots()
        axis.set_axis_off()
        
        # create a new table instance
        table = Table(axis, bbox=[0,0,1,1])
    
        total_rows=len(row_labels)
        total_columns=len(column_labels)
    
        height = 1.0 / total_rows
    
        # get the width of the columns based on
        # the length of the labels and values
        max_width_chars = [max(map(len,row_labels))]
        for i in range(total_columns):
            current_values=[str(value) for value in [column_labels[i]]+[row[i] for row in data]]
            max_width_chars.append(max(map(len,current_values)))
        
        # compute the widths for each column
        total_chars=sum(max_width_chars)*1.0
        column_widths=[i/total_chars for i in max_width_chars]
    
        # add column labels
        for i, label in enumerate(column_labels):
            table.add_cell(0, i+1, width=column_widths[i+1], height=height, text=label, loc=location)
    
        # add row labels
        for i, label in enumerate(row_labels):
            table.add_cell(i+1, 0, width=column_widths[0], height=height, text=label, loc=location)
    
        # Add data
        for i, row in enumerate(data):
            for j, value in enumerate(row):
                table.add_cell(i+1, j+1, width=column_widths[j+1], height=height, text=value, loc=location)               

        axis.add_table(table)

        # set the font size for the table
        # first must turn off the auto set font size
        table.auto_set_font_size(False)
        font_size=8
        if total_columns > 5:
            font_size=7
            
        # use the font if provided
        if font is not None:
            font_size=font
            
        table.set_fontsize(font_size)
    
        # add the title
        pyplot.title(title)
        
        # plot the table
        pyplot.show() 
        
    def write_table(self, column_labels, row_labels, data, file):
        """ Write a table of data to a file 
        
        :param column_labels: The labels for the columns
        :type column_labels: list
        
        :param row_labels: The labels for the data rows
        :type row_labels: list

        :param data: A list of lists containing the data
        :type data: list        

        :param file: The file to write the table to
        :type file: str
        
        """
        
        # if the folder for the table does not exist, then create
        if not os.path.isdir(os.path.dirname(file)):
            os.makedirs(os.path.dirname(file))
    
        with open(file, "wb") as file_handle:
            file_handle.write("\t".join(column_labels)+"\n")
            for name, row in zip(row_labels, data):
                file_handle.write("\t".join([name]+[str(i) for i in row])+"\n")
        
    def show_hclust2(self,sample_names,feature_names,data,title,log_scale=True,zscore=False,metadata_rows=None,method="correlation"):
        """ Create a hclust2 heatmap with dendrogram and show it in the document
        
        :param sample_names: The names of the samples
        :type sample_names: list
        
        :param feature_names: The names of the features
        :type feature_names: list 
        
        :param data: A list of lists containing the data
        :type data: list         
        
        :param title: The title for the plot
        :type title: str
        
        :keyword log_scale: Show the heatmap with the log scale
        :type log_scale: bool
        
        :keyword zscore: Apply the zscore to the data prior to clustering
        :type zscore: bool

        :keyword metadata_rows: A list of metadata rows
        :type metadata_rows: list

        :keyword method: The distance function for features
        :type method: str
        
        """
        
        from matplotlib._png import read_png
        import matplotlib.pyplot as pyplot
        import numpy
        
        # apply zscore if requested
        if zscore:
            from scipy import stats
            
            data = stats.zscore(numpy.array(data),axis=1)
        
        # write a file of the data
        handle, hclust2_input_file=tempfile.mkstemp(prefix="hclust2_input",dir=os.getcwd())
        heatmap_file=hclust2_input_file+".png"
        if metadata_rows:
            metadata_legend_file = hclust2_input_file+"_legend.png"

        self.write_table(["# "]+sample_names,feature_names,data,hclust2_input_file)
        
        # increase the dpi for small text
        dpi=300
        label_font="8"

        # compute the aspect ratio based on the number of samples and features
        aspect_ratio=len(sample_names)/(len(feature_names)*1.0)
        command=["hclust2.py","-i",hclust2_input_file,"-o",heatmap_file,"--title",title,
            "--title_font",str(int(label_font)*2),"--cell_aspect_ratio",str(aspect_ratio),
            "--flabel_size", label_font, "--slabel_size", label_font,
            "--colorbar_font_size",label_font,"--dpi",str(dpi),"--f_dist_f",method]
        if log_scale:
            command+=["--log_scale"]
        if metadata_rows:
            command+=["--metadata_rows",",".join(str(i) for i in metadata_rows)]
            command+=["--legend_file", metadata_legend_file]
            if len(metadata_rows) > 10:
                command+=["--metadata_height","0.8"]
            elif len(metadata_rows) > 4:
                command+=["--metadata_height","0.4"]
            elif len(metadata_rows) > 1:
                command+=["--metadata_height","0.1"]
            
        # if more than the max samples, do not include sample labels on the heatmap
        if len(sample_names) > self.max_labels:
            command+=["--no_slabels"]
            
        # if more than max labels, do not include the feature labels on the heatmap
        if len(feature_names) > self.max_labels:
            command+=["--no_flabels"]
           
        try: 
            output=subprocess.check_output(command)
            # read the heatmap png file
            heatmap=read_png(heatmap_file)
        except (subprocess.CalledProcessError, OSError):
            print("Unable to generate heatmap")
            heatmap=[]

        # create a subplot and remove the frame and axis labels
        # set the figure and increase the dpi for small text

        fig = pyplot.figure(figsize=(8,8),dpi=dpi)

        if metadata_rows:
            subplot1 = pyplot.subplot2grid((4,1),(0,0), rowspan=3, frame_on=False)
            subplot1.xaxis.set_visible(False)
            subplot1.yaxis.set_visible(False)
        else:
            subplot = fig.add_subplot(111, frame_on=False)
            subplot.xaxis.set_visible(False)
            subplot.yaxis.set_visible(False)
        # show but do not interpolate (as this will make the text hard to read)
        pyplot.imshow(heatmap, interpolation="none")

        if metadata_rows:
            heatmap_legend = read_png(metadata_legend_file)
            # metadata legend subplot
            subplot2 = pyplot.subplot2grid((4,1),(3,0), rowspan=1, frame_on=False)
            subplot2.xaxis.set_visible(False)
            subplot2.yaxis.set_visible(False)
            pyplot.imshow(heatmap_legend, interpolation="none")

        pyplot.show()
        # adjust the heatmap to fit in the figure area
        # this is needed to increase the image size (to fit in the increased figure)
        pyplot.tight_layout()
        
    def _run_r(self, commands, args=None):
        """ Run R on the commands providing the arguments """
        
        if args is None:
            args=[]
        
        proc=subprocess.Popen(["R","--vanilla","--quiet","--args"]+args,
            stdin=subprocess.PIPE, stderr=subprocess.PIPE, stdout=subprocess.PIPE)

        out, err = proc.communicate(input="\n".join(commands))
        
    def filter_zero_rows(self, row_names, data):
        """ Filter the rows from the data set that sum to zero 
        
        :param row_names: The names of the rows
        :type row_names: list
        
        :param data: A list of lists containing the data
        :type data: list    
        
        """
         
        new_names=[]
        new_data=[]
        for name, row in zip(row_names, data):
            if sum(row) != 0:
                new_names.append(name)
                new_data.append(row)
            
        return new_names, new_data
    
    def filter_zero_columns(self, column_names, data):
        """ Filter the columns from the data set that sum to zero 
        
        :param column_names: The names of the columns
        :type column_names: list
        
        :param data: A list of lists containing the data
        :type data: list    
        
        """

        import numpy
         
        new_names, new_data = self.filter_zero_rows(column_names, numpy.transpose(data))
        data_temp = []
        for row in numpy.transpose(new_data):
            data_temp.append(list(row))
            
        new_data = data_temp
            
        return new_names, new_data
       
    def compute_pcoa(self, sample_names, feature_names, data, apply_transform):
        """ Use the vegan package in R to compute a PCoA. 
        Input data should be organized with samples as columns and features as rows. 
        Data should be scaled to [0-1] if transform is to be applied.
        
        :param sample_names: The labels for the columns
        :type sample_names: list

        :param feature_names: The labels for the data rows
        :type feature_names: list

        :param data: A list of lists containing the data
        :type data: list
        
        :keyword apply_transform: Arcsin transform to be applied
        :type apply_transform: bool
        """

        r_vegan_pcoa=[
            "library(vegan)",
            "args<-commandArgs(TRUE)",
            "data<-read.table(args[1],sep='\\t',header=TRUE, row.names=1)",
            "data.t<-as.data.frame(t(data))"]
        if apply_transform:
            r_vegan_pcoa+=["pcoa<-capscale(asin(sqrt(data.t))~1,distance='bray')"]
        else:
            r_vegan_pcoa+=["pcoa<-capscale(data.t~1,distance='bray')"]
        r_vegan_pcoa+=[
            "write.table(head(eigenvals(pcoa)/sum(eigenvals(pcoa))),args[2],sep='\\t')",
            "write.table(as.data.frame(scores(pcoa,display='sites')),args[3],sep='\\t')"]
        
        # test that the data is scaled to [0-1]
        if apply_transform:
            for row in data:
                out_of_range=list(filter(lambda x: x < 0 or x > 1, row))
                if len(out_of_range) > 0:
                    raise ValueError("Provide data to the AnADAMA2 document.show_pcoa function in the range of [0-1].")
            
        # test for duplicate feature names
        feature_set=set(feature_names)
        if len(list(feature_set)) < len(feature_names):
            raise ValueError("Do not provide duplicate feature names to document.show_pcoa.")
        
        # test samples are provided as the columns of the data
        if len(data[0]) != len(sample_names):
            raise ValueError("Provide data to the AnADAMA2 document.show_pcoa function in the form of samples as columns.")
        
        # test features are provided as rows of the data
        if len(data) != len(feature_names):
            raise ValueError("Provide data to the AnADAMA2 document.show_pcoa function in the form of features as rows.")
        
        # remove any samples from the data for which all features are zero
        sample_names, data = self.filter_zero_columns(sample_names, data)

        # remove any features from the data for which all samples have zero values
        feature_names, data = self.filter_zero_rows(feature_names, data)

        # write a file of the data
        handle, vegan_input_file=tempfile.mkstemp(prefix="vegan_input",dir=os.getcwd())
        eigenvalues_file=vegan_input_file+".eigen"
        scores_file=vegan_input_file+".scores"
        self.write_table(["# "]+sample_names,feature_names,data,vegan_input_file)
        
        self._run_r(r_vegan_pcoa,[vegan_input_file,eigenvalues_file,scores_file])
        
        # get the x and y labels
        r_run_error=False
        try:
            columns, rows, data = self.read_table(eigenvalues_file)
        except EnvironmentError:
            print("No eigenvalues found")
            data=[[0],[0]]
            r_run_error=True
        pcoa1_x_label=int(data[0][0]*100)
        pcoa2_y_label=int(data[1][0]*100)
        
        # get the scores to plot
        try:
            columns, rows, pcoa_data = self.read_table(scores_file)
        except EnvironmentError:
            print("No scores found")
            r_run_error=True
            columns=[]
            rows=[]
            pcoa_data=[]
       
        # if there were no errors, remove the temp files
        if not r_run_error:
            try:
                os.remove(vegan_input_file)
                os.remove(eigenvalues_file)
                os.remove(scores_file)
            except EnvironmentError:
                print("Warning: Unable to remove temp files")

        return pcoa_data, pcoa1_x_label, pcoa2_y_label
            
    def show_pcoa_multiple_plots(self, sample_names, feature_names, data, title, abundances, legend_title="% Abundance", sample_types="samples", feature_types="species", apply_transform=False):
        """ Use the vegan package in R plus matplotlib to plot a PCoA. 
        Input data should be organized with samples as columns and features as rows. 
        Data should be scaled to [0-1] if transform is to be applied.
        Show multiple PCoA plots as subplots each with coloring based on abundance.
        
        :param sample_names: The labels for the columns
        :type sample_names: list

        :param feature_names: The labels for the data rows
        :type feature_names: list

        :param data: A list of lists containing the data
        :type data: list
        
        :param title: The title for the plot
        :type title: str
        
        :param abundances: The sets of abundance data and names for the subplots
        :type abundances: dict
        
        :keyword legend_title: The title for the legend
        :type legend_title: str

        :keyword sample_types: What type of data are the columns
        :type sample_types: str
        
        :keyword feature_types: What type of data are the rows
        :type feature_types: str
        
        :keyword apply_transform: Arcsin transform to be applied
        :type apply_transform: bool
        """

        import numpy
        import matplotlib.pyplot as pyplot
        from matplotlib import cm

        pcoa_data, pcoa1_x_label, pcoa2_y_label=self.compute_pcoa(sample_names, feature_names, data, apply_transform)         
 
        # create a figure and subplots
        nrows = len(abundances.keys())/2
        figure, axis = pyplot.subplots(nrows=nrows,ncols=2)
        # if needed, modify matrix of axis to list
        reformatted_axis = []
        if isinstance(axis[0],numpy.ndarray):
            for axis_list in axis:
                reformatted_axis+=axis_list.tolist()
            axis=reformatted_axis
        figure.suptitle(title,fontsize=12,y=1.002)      
 
        x_values = [x for x,y in pcoa_data]
        y_values = [y for x,y in pcoa_data]
        for subplot, abundance_name in zip(axis,sorted(abundances.keys())):
            pcoa_plot=subplot.scatter(x_values,y_values,c=abundances[abundance_name],cmap=cm.jet)
            figure.colorbar(pcoa_plot,ax=subplot,label=legend_title)
            subplot.set_title(abundance_name)
            subplot.set(xlabel="PCoA 1 ("+str(pcoa1_x_label)+" %)",ylabel="PCoA 2 ("+str(pcoa2_y_label)+" %)")
            subplot.tick_params(axis="both",bottom="off",labelbottom="off",left="off",labelleft="off")
             
        # adjust spacing between subplots
        figure.tight_layout()   

        pyplot.show()
 


    def show_pcoa(self, sample_names, feature_names, data, title, sample_types="samples", feature_types="species",
                  metadata=None, apply_transform=False, sort_function=None, metadata_type=None):
        """ Use the vegan package in R plus matplotlib to plot a PCoA.
        Input data should be organized with samples as columns and features as rows.
        Data should be scaled to [0-1] if transform is to be applied.

        :param sample_names: The labels for the columns
        :type sample_names: list

        :param feature_names: The labels for the data rows
        :type feature_names: list

        :param data: A list of lists containing the data
        :type data: list

        :param title: The title for the plot
        :type title: str

        :keyword sample_types: What type of data are the columns
        :type sample_types: str

        :keyword feature_types: What type of data are the rows
        :type feature_types: str

        :keyword metadata: Metadata for each sample
        :type metadata: dict

        :keyword metadata_type: Type of metadata (continuous or categorical)
        :type metadata_type: str

        :keyword apply_transform: Arcsin transform to be applied
        :type apply_transform: bool

        :keyword sort_function: The function to sort the plot data
        :type sort_function: lambda
        """

        import matplotlib.pyplot as pyplot
        import matplotlib.colors as mcolors
        import matplotlib.cm as cm
        import matplotlib.patches as mpatches
        import numpy as np

        pcoa_data, pcoa1_x_label, pcoa2_y_label = self.compute_pcoa(sample_names, feature_names, data, apply_transform)

        # create a figure subplot to move the legend
        figure = pyplot.figure()
        subplot = pyplot.subplot(111)
        nancolor="grey"

        # create a set of custom colors to prevent overlap
        if metadata:

            metadata_categories = list(set(metadata.values()))
            custom_colors = self._custom_colors(total_colors=len(metadata_categories))

            if metadata_type == 'con':

                cleaned_array = [value for value in metadata_categories if ~np.isnan(value)]
                normalize = mcolors.Normalize(vmin=min(cleaned_array), vmax=max(cleaned_array))
                colormap = pyplot.get_cmap('jet')
                scalarmappaple = cm.ScalarMappable(norm=normalize, cmap=colormap)
                scalarmappaple.set_array(cleaned_array)

                custom_colors_cont = []
                for value in metadata_categories:
                    if np.isnan(value):
                        custom_colors_cont.append(nancolor)
                    else:
                        custom_colors_cont.append(colormap(normalize(value)))

                colors_by_metadata = dict((key, color) for key, color in zip(metadata_categories, custom_colors_cont))

            else:
                colors_by_metadata = dict((key, color) for key, color in zip(metadata_categories, custom_colors))
                colors_by_metadata["NA"] = nancolor

        else:
            custom_colors = self._custom_colors(total_colors=len(pcoa_data))


        # reduce the size of the plot to fit in the legend
        subplot_position = subplot.get_position()
        subplot.set_position([subplot_position.x0, subplot_position.y0,
                              subplot_position.width * 0.80, subplot_position.height])

        plots = []
        metadata_plots = {}
        for i, (x, y) in enumerate(pcoa_data):
            if metadata:
                if metadata[sample_names[i]] not in metadata_plots:
                    metadata_plots[metadata[sample_names[i]]] = [[x], [y]]
                else:
                    metadata_plots[metadata[sample_names[i]]][0].append(x)
                    metadata_plots[metadata[sample_names[i]]][1].append(y)
            else:
                plots.append(subplot.scatter(x, y, color=next(custom_colors)))

        # order the plots alphabetically or numerically
        if not sort_function:
            metadata_ordered_keys = self.sorted_data_numerical_or_alphabetical(metadata_plots.keys())
        else:
            metadata_ordered_keys = sort_function(metadata_plots.keys())

        for key in metadata_ordered_keys:
                plots.append(subplot.scatter(metadata_plots[key][0], metadata_plots[key][1],
                                         color=colors_by_metadata[key]))


        pyplot.title(title)
        pyplot.xlabel("PCoA 1 (" + str(pcoa1_x_label) + " %)")
        pyplot.ylabel("PCoA 2 (" + str(pcoa2_y_label) + " %)")

        # remove the tick marks on both axis
        pyplot.tick_params(axis="x", which="both", bottom="off", labelbottom="off")
        pyplot.tick_params(axis="y", which="both", left="off", labelleft="off")

        if not metadata and len(sample_names) <= self.max_labels_legend:
            subplot.legend(plots, sample_names, loc="center left", bbox_to_anchor=(1, 0.5),
                           fontsize=7, title="Samples", frameon=False)

        if metadata:
            if metadata_type == 'con':
                subplot.append = pyplot.colorbar(scalarmappaple)
                if nancolor in custom_colors_cont:
                    figure.text(0.24, 0.01, "NA/Unknown values are shown in grey.")

            else:
                if len(metadata_ordered_keys) <= self.max_labels_legend:
                    subplot.legend(plots, metadata_ordered_keys, loc="center left", bbox_to_anchor=(1, 0.5),
                               fontsize=7, frameon=False)


        if apply_transform:
            caption = "\n".join(
                ["Principal coordinate analysis of variance among " + sample_types + ", based on Bray-Curtis ",
                 "dissimilarities between " + feature_types + " profiles of " + sample_types + ".  Filtered " + feature_types + "' relative abundances ",
                 "were arcsin-square root transformed to approximate a normal distribution and down-weigh the effect ",
                 "of highly abundant " + feature_types + " on Bray-Curtis dissimilarities.  Numbers in parenthesis on each axis ",
                 "represent the amount of variance explained by that axis."])
        else:
            caption = "\n".join(
                ["Principal coordinate analysis of variance among " + sample_types + ", based on Bray-Curtis ",
                 "dissimilarities between " + feature_types + " profiles of " + sample_types + ".  Numbers in parenthesis on each axis ",
                 "represent the amount of variance explained by that axis."])

        pyplot.show()

        return caption

