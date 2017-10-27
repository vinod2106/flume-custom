# flume-custom
To run this project use the flume command

bin/flume-ng agent --conf-file $1 --name a1 --conf ./conf/ -Dflume.root.logger=INFO,console



#nf-source 
This branch adds a custom source in flume. This source reads a line and concatenates to the next line.

			#Input
			1 
			2
			3
			
			#Output
			1
			12
			123

#nf-intercept
This branch adds a new  custom interceptor.

#nf-queuesrc
This source reads from an external distributed queue.

#project tracking
https://trello.com/b/j1tragnb/flume-custom-project
