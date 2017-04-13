# -*- coding: utf-8 -*-
import os
import tempfile
import shutil
import subprocess
import itertools
import sys

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
            except IndexError:
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
        
        # rename the figure pdf files so their name corresponds to their number
        # in the pdf document (by default their number ordering is what is expected,
        # it just does not represent the exact number of the figure)
        # For example, we could have two figures "template_figure4_1.pdf"
        # and "template_figure5_1.pdf" these correspond to figure1 and figure2.
        temp_figures_folder=os.path.join(temp_directory,"figures")
        figure_number=1
        
        # sort the temp figure files by their number ordering
        try:
            sorted_temp_figures_files=sorted(os.listdir(temp_figures_folder),
                key=lambda file: int(file.split("figure")[1].replace("_1.pdf","")))
        except ValueError:
            sorted_temp_figures_files=sorted(os.listdir(temp_figures_folder))
        
        # move and rename the temp files
        for file in sorted_temp_figures_files:
            new_file=os.path.join(self.figures_folder,"figure_"+str(figure_number)+".pdf")
            shutil.move(os.path.join(temp_figures_folder,file),new_file)
            figure_number+=1
        
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
        pickle_file = filter(lambda x: x.endswith(".pkl"),os.listdir("."))
        
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
        xlabel=None, ylabel=None, legend_title=None):
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
            fontsize=7, title=legend_title, frameon=False)
        
        pyplot.show()
        
    def show_table(self, data, row_labels, column_labels, title, format_data_comma=None,
                   column_width=None):
        """ Plot the data as a table """
        
        import numpy
        import matplotlib.pyplot as pyplot
        
        fig = pyplot.figure()
        # create a subplot and remove frame and x/y axis
        subplot = fig.add_subplot(111, frame_on=False)
        subplot.xaxis.set_visible(False)
        subplot.yaxis.set_visible(False)
        
        # reduce height of the empty plot to as small as possible
        # remove subplot padding
        fig.subplots_adjust(bottom=0.85, wspace=0, hspace=0, left=0.4)
        
        # if the option is set to format the data, add commas
        if format_data_comma:
            format_data=[]
            for row in data:
                format_data.append(["{:,}".format(int(i)) for i in row])
            data=format_data
        
        # compute the column width as the length of the column label or the
        # length of the data in the column which ever is larger
        auto_column_widths=[]
        for column_data, column_name in zip(numpy.transpose(data),column_labels):
            max_len=max([len(str(i)) for i in list(column_data)+[column_name]])
            auto_column_widths.append((max_len+10)/100.0)
            
        # use the column width if provided
        if column_width is not None:
            auto_column_widths=[column_width]*len(column_labels)
        
        # create the table
        table = pyplot.table(cellText=data,
            colWidths = auto_column_widths,
            rowLabels=row_labels, colLabels=column_labels, loc="bottom")
        
        # set the font size for the table
        # first must turn off the auto set font size
        table.auto_set_font_size(False)
        table.set_fontsize(8)
        
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
        command=["hclust2.py","-i",hclust2_input_file,"-o",heatmap_file,"--title",title,
            "--title_font",str(int(label_font)*2),"--cell_aspect_ratio",str(aspect_ratio),
            "--flabel_size",label_font,"--slabel_size",label_font,
            "--colorbar_font_size",label_font,"--log_scale"]
        # if more than the max samples, do not include sample labels on the heatmap
        if len(sample_names) > self.max_labels:
            command+=["--no_slabels"]
            
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
        
    def show_pcoa(self, sample_names, feature_names, data, title):
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
        
        pyplot.show()
