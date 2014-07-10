#!/mnt/speed/usr/bin/Rscript
#This script takes a list of things and returns 
#a subsample from that list with the provided
#number of elements

#.libPaths( c(.libPaths(), "/mnt/iscsi_speed/devel/lib64/R/library") )
args <- commandArgs(TRUE)
data <- read.table(file=args[1],sep=",",header=TRUE)
current_samples <- read.table(file=args[2],header=FALSE)
library(ggplot2)
a <- ggplot(data, aes(x = Mean_target_coverage, y = In_dbSNP, color = Flowcell_ID))
a <- a + geom_point(size=3)
a <- a + theme_bw()
a <- a + theme(axis.line = element_line(colour = 'black'))
a <- a + theme(panel.grid.major = element_blank())
a <- a + theme(panel.grid.minor = element_blank())
a <- a + theme(panel.border = element_blank())
a <- a + theme(panel.background = element_blank())
a <- a + theme(plot.title = element_text(size=24,face="bold"))
a <- a + theme(axis.title.x = element_text(size=20,face="bold"))
a <- a + theme(axis.title.y = element_text(size=20,face="bold"))
a <- a + theme(axis.text.x = element_text(size=20))
a <- a + theme(axis.text.y = element_text(size=20))
a <- a + xlab('Mean read depth')
a <- a + ylab('Percentage variants in dbSNP')

jpeg(args[3])
plot(a)
dev.off()
