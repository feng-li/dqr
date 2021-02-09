
commcost = read.table("commcost_data.csv", header = TRUE)
par(ps = 12, cex = 1, cex.main = 1)
plot(commcost$sample_size, commcost$time_commcost,
     xlab = 'Sample size',
     ylab = 'Time (s) for transferring data from HDFS to the master node', type = 'l', col
     = "blue", lwd = 2)
points(commcost[1, 3], commcost[1, 2], col = 'red', lwd = 2)
dev.copy2pdf(file = 'commcost.pdf')
