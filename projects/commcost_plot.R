commcost = read.table("commcost_data.csv", header = TRUE)

par(ps = 12, cex = 1, cex.main = 1, mfrow = c(1, 2), mar = c(4, 4, 1, 1))

## estimate the communication
y = commcost[2:9, 2]
x = commcost[2:9, 3]
model = lm(y ~ x)

overhead = commcost[1, 2] - commcost[2, 2]

npar_oneshot = 44
npartition = 117
npar_dqr = npar_oneshot + npar_oneshot + 1 + npartition * npar_oneshot ^ 2 + npar_oneshot * npartition

time_all = predict(model, newdata = data.frame(x = c(npar_oneshot, npar_dqr))) # + overhead
names(time_all) = c("One Shot", "DQR")
barplot(time_all, ylab = 'Total communication time (s)', ylim = c(0, 6))
abline(h = overhead, col = "red", lwd = 2)
legend('topright', legend = c("overhead"), col = "red", lty = "solid", box.col = "white", lwd = 2)

commcost_overhead = commcost
commcost_overhead[1, 2] = commcost_overhead[1, 2] - overhead

plot(commcost_overhead$sample_size, commcost_overhead$time_commcost,
     xlab = 'Object length',
     ylab = 'One round communication time (s)', type = 'l', col
     = "blue", lwd = 2)
#points(commcost[1, 3], commcost[1, 2], col = 'red', lwd = 2)

dev.copy2pdf(file = 'commcost.pdf')
