# -*- coding: utf-8 -*-
import os
import tempfile
import shutil
import subprocess
import itertools
import sys

from .helpers import sh

try:
    import cPickle as pickle
except ImportError:
    import pickle

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
    
    def __init__(self, templates=None, depends=None, targets=None, vars=None, table_of_contents=None):
        # allow for a single template or multiple templates
        if templates is not None and isinstance(templates,basestring):
            templates=[templates]
        self.templates=templates
        self.depends=depends
        
        self.table_of_contents=table_of_contents
        
        # if targets is a single item, save as a list
        if targets is not None and isinstance(targets,basestring):
            targets=[targets]
            
        self.targets=targets
        self.vars=vars
        
        # set the max number of x tick labels to be shown on plots
        self.max_labels = 30
        
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
        temp_template = temp_template_basename + "." + template_extension
        
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
        
        # get the intermediate output file based on the initial template type
        if "rst" in template_extension:
            intermediate_template = temp_template_basename+"."+"rst"
        else:
            intermediate_template = temp_template_basename+"."+"md"
            
        # process the template based on the extension type
        temp_report = temp_template_basename+"."+report_extension
        
        # set the pandoc command based on if a table of contents will be included
        pandoc_command="pandoc {0} -o {1} --variable=linkcolor:Blue "+\
            "--variable=toccolor:Blue --latex-engine=pdflatex --standalone" 
        if self.table_of_contents:
            pandoc_command+=" --toc"
        
        # run pweave then pandoc to generate document
        sh("pweave {0} -o {1}".format(temp_template, intermediate_template),log_command=True)()
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
        names and the first column the row names. """
        
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

    def plot_grouped_barchart(self, data, row_labels, column_labels, title, 
        xlabel=None, ylabel=None, legend_title=None, yaxis_in_millions=None):
        """ Plot a grouped barchart """
        
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
            axis.get_yaxis().set_major_formatter(ticker.FuncFormatter(lambda value, position: "{:,}".format(int(value/1000000))))
        
        # set the width of the bars as each total group width is one
        bar_start_point = numpy.arange(len(column_labels))
        gap = 0.1
        bar_width = (1.0 - gap) / len(data)
        
        # create the grouped barplots with gap offsets
        barplots=[]
        for i, data_set in enumerate(data):
            barplots.append(subplot.bar(bar_start_point + i*bar_width, data_set,
                width=bar_width, color=next(custom_colors)))

        # move the bottom of the figure for larger xaxis labels
        # done first before adjusting the width of the figure
        figure.subplots_adjust(bottom=0.3)    
        
        # reduce the size of the plot to fit in the legend and the xlabels
        subplot_position=subplot.get_position()
        subplot.set_position([subplot_position.x0, subplot_position.y0, 
            subplot_position.width *0.80, subplot_position.height])
        
        # add labels and title
        if xlabel is not None and len(column_labels) <= self.max_labels:
            pyplot.xlabel(xlabel)
        if ylabel is not None:
            pyplot.ylabel(ylabel)
            
        pyplot.title(title)
        
        # place the xticks in the middle of each group
        if len(column_labels) <= self.max_labels:
            pyplot.xticks(bar_start_point + 0.5, column_labels, fontsize=7, rotation="vertical")
        else:
            pyplot.tick_params(axis="x",which="both",bottom="off",labelbottom="off")
        pyplot.yticks(fontsize=7)
        
        # set the limits on the x axis so the edge gaps are correct
        pyplot.xlim(0-gap,len(column_labels))
        
        subplot.legend(barplots,row_labels,loc="center left", bbox_to_anchor=(1,0.5),
            fontsize=7, title=legend_title, frameon=False)
        
        pyplot.show()  
        
    def plot_scatter(self, data, title, row_labels, xlabel=None, ylabel=None, trendline=None):
        """ Plot a scatter plot """
        
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
        
    def plot_barchart(self, data, labels, title, xlabel=None, ylabel=None):
        """ Plot a barchart """
        
        import numpy
        import matplotlib.pyplot as pyplot
        
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
        pyplot.xticks(positions, labels, rotation="vertical")
        
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
        xlabel=None, ylabel=None, legend_title=None, legend_style="normal"):
        """ Plot a stacked barchart """
        
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
            
        # move the bottom of the figure for larger xaxis labels
        # done first before adjusting the width of the figure
        figure.subplots_adjust(bottom=0.3)
            
        # reduce the size of the plot to fit in the legend
        subplot_position=subplot.get_position()
        subplot.set_position([subplot_position.x0, subplot_position.y0, 
            subplot_position.width *0.80, subplot_position.height])
            
        # Add the title, labels, and legend
        if xlabel is not None and len(column_labels) <= self.max_labels:
            subplot.set_xlabel(xlabel)
        if ylabel is not None:
            subplot.set_ylabel(ylabel)
            
        pyplot.title(title)
        
        if len(column_labels) <= self.max_labels:
            pyplot.xticks(plot_indexes, column_labels, fontsize=7, rotation="vertical")
        else:
            pyplot.tick_params(axis="x",which="both",bottom="off",labelbottom="off")
        pyplot.yticks(fontsize=7)
        subplot.legend(bar_plots,names,loc="center left", bbox_to_anchor=(1,0.5),
            title=legend_title, frameon=False, prop={"size":7, "style":legend_style})
        
        pyplot.show()
        
    def show_table(self, data, row_labels, column_labels, title, format_data_comma=None,
                   location="center", font=None):
        """ Plot the data as a table """
        
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
        """ Write a table of data to a file """
        
        # if the folder for the table does not exist, then create
        if not os.path.isdir(os.path.dirname(file)):
            os.makedirs(os.path.dirname(file))
    
        with open(file, "wb") as file_handle:
            file_handle.write("\t".join(column_labels)+"\n")
            for name, row in zip(row_labels, data):
                file_handle.write("\t".join([name]+[str(i) for i in row])+"\n")
        
    def show_hclust2(self,sample_names,feature_names,data,title,log_scale=True,zscore=False):
        """ Create a hclust2 heatmap with dendrogram and show it in the document """
        
        from matplotlib._png import read_png
        import matplotlib.pyplot as pyplot
        
        # apply zscore if requested
        if zscore:
            from scipy import stats
            import numpy
            
            data = stats.zscore(numpy.array(data),axis=1)
        
        # write a file of the data
        handle, hclust2_input_file=tempfile.mkstemp(prefix="hclust2_input",dir=os.getcwd())
        heatmap_file=hclust2_input_file+".png"
        self.write_table(["# "]+sample_names,feature_names,data,hclust2_input_file)
        
        label_font="7"
        # compute the aspect ratio based on the number of samples and features
        aspect_ratio=len(sample_names)/(len(feature_names)*1.0)
        command=["hclust2.py","-i",hclust2_input_file,"-o",heatmap_file,"--title",title,
            "--title_font",str(int(label_font)*2),"--cell_aspect_ratio",str(aspect_ratio),
            "--flabel_size",label_font,"--slabel_size",label_font,
            "--colorbar_font_size",label_font]
        if log_scale:
            command+=["--log_scale"]
            
        # if more than the max samples, do not include sample labels on the heatmap
        if len(sample_names) > self.max_labels:
            command+=["--no_slabels"]
            
        # if more than max labels, do not include the feature labels on the heatmap
        if len(feature_names) > self.max_labels:
            command+=["--no_flabels"]
            
        output=subprocess.check_output(command)
        # read the heatmap png file
        heatmap=read_png(heatmap_file)
        
        # create a subplot and remove the frame and axis labels
        # set the figure to square and increase the dpi to the hclust2 default of 150
        # increase the figure size to increase the size of the heatmap
        fig = pyplot.figure(figsize=(8, 8), dpi=150)
        subplot = fig.add_subplot(111, frame_on=False)
        subplot.xaxis.set_visible(False)
        subplot.yaxis.set_visible(False)
        
        # show but do not interpolate (as this will make the text hard to read)
        pyplot.imshow(heatmap, interpolation="none")
        
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
        """ Filter the rows from the data set that sum to zero """
         
        new_names=[]
        new_data=[]
        for name, row in zip(row_names, data):
            if sum(row) != 0:
                new_names.append(name)
                new_data.append(row)
            
        return new_names, new_data
    
    def filter_zero_columns(self, column_names, data):
        """ Filter the columns from the data set that sum to zero """

        import numpy
         
        new_names, new_data = self.filter_zero_rows(column_names, numpy.transpose(data))
        data_temp = []
        for row in numpy.transpose(new_data):
            data_temp.append(list(row))
            
        new_data = data_temp
            
        return new_names, new_data
        
    def show_pcoa(self, sample_names, feature_names, data, title, sample_types="samples", feature_types="species"):
        """ Use the vegan package in R plus matplotlib to plot a PCoA 
        Input data should be organized with samples as columns and features as rows 
        Data should be scaled to [0-1] """
        
        import matplotlib.pyplot as pyplot
        
        r_vegan_pcoa=[
            "library(vegan)",
            "args<-commandArgs(TRUE)",
            "data<-read.table(args[1],sep='\\t',header=TRUE, row.names=1)",
            "data.t<-as.data.frame(t(data))",
            "pcoa<-capscale(asin(sqrt(data.t))~1,distance='bray')",
            "write.table(head(eigenvals(pcoa)/sum(eigenvals(pcoa))),args[2],sep='\\t')",
            "write.table(as.data.frame(scores(pcoa,display='sites')),args[3],sep='\\t')"]
        
        # test that the data is scaled to [0-1]
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
        try:
            columns, rows, data = self.read_table(eigenvalues_file)
        except EnvironmentError:
            raise ValueError("No eigenvalues found in AnADAMA2 document.show_pcoa function. "+
                "Provide data to this function in the form of samples as columns and features as rows.")
        pcoa1_x_label=int(data[0][0]*100)
        pcoa2_y_label=int(data[1][0]*100)
        
        # get the scores to plot
        columns, rows, pcoa_data = self.read_table(scores_file)
        
        # create a figure subplot to move the legend
        figure = pyplot.figure()
        subplot=pyplot.subplot(111)
        
        # create a set of custom colors to prevent overlap
        custom_colors=self._custom_colors(total_colors=len(pcoa_data))
        
        # reduce the size of the plot to fit in the legend
        subplot_position=subplot.get_position()
        subplot.set_position([subplot_position.x0, subplot_position.y0, 
            subplot_position.width *0.80, subplot_position.height])
        
        plots = []
        for x,y in pcoa_data:
            plots.append(subplot.scatter(x,y,color=next(custom_colors)))
        
        pyplot.title(title)
        pyplot.xlabel("PCoA 1 ("+str(pcoa1_x_label)+" %)")
        pyplot.ylabel("PCoA 2 ("+str(pcoa2_y_label)+" %)")

        # remove the tick marks on both axis
        pyplot.tick_params(axis="x",which="both",bottom="off",labelbottom="off")
        pyplot.tick_params(axis="y",which="both",left="off",labelleft="off")
        
        if len(sample_names) <= self.max_labels:
            subplot.legend(plots, sample_names, loc="center left", bbox_to_anchor=(1,0.5),
                fontsize=7, title="Samples", frameon=False)

        caption="\n".join(["Principal coordinate analysis of variance among "+sample_types+", based on Bray-Curtis ", 
            "dissimilarities between "+feature_types+" profiles of "+sample_types+".  Filtered "+feature_types+"' relative abundances ", 
            "were arcsin-square root transformed to approximate a normal distribution and down-weigh the effect ",
            "of highly abundant "+feature_types+" on Bray-Curtis dissimilarities.  Numbers in parenthesis on each axis ",
            "represent the amount of variance explained by that axis."])
        
        pyplot.show()
        
        return caption
    
