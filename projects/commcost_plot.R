commcost = read.table("commcost_data.csv", header = TRUE)

par(ps = 12, cex = 1, cex.main = 1, mfrow = c(1, 2), mar = c(4, 4, 1, 1.5))

## estimate the communication
y = commcost[2:9, 2]
x = commcost[2:9, 3]
model = lm(y ~ x)

overhead = commcost[1, 2] - commcost[2, 2]

npar_oneshot = 44
npartition = 117
npar_dqr = npar_oneshot + npar_oneshot + 1 + npartition * npar_oneshot ^ 2 + npar_oneshot * npartition

time_all = predict(model, newdata = data.frame(x = c(npar_oneshot, npar_dqr))) # + overhead
names(time_all) = c("One-shot", "One-step")
barplot(time_all, ylab = 'Total communication time (s)', ylim = c(0, 6))
abline(h = overhead, col = "red", lwd = 2)
legend('topright', legend = c(paste("overhead:", round(overhead, 2))), col = "red", lty = "solid", box.col = "white", lwd = 2)

commcost_overhead = commcost
commcost_overhead[1, 2] = commcost_overhead[1, 2] - overhead
commcost_overhead = rbind(c(9, time_all[2], npar_dqr, NA, NA), commcost_overhead)
commcost_overhead = rbind(c(9, time_all[1], npar_oneshot, NA, NA), commcost_overhead)

base = 1e6
plot(commcost_overhead$sample_size / base, commcost_overhead$time_commcost,
     xlab = 'Object length',
     ylab = 'One round communication time (s)', type = 'l',
     # xaxt = "n",
     col = "blue", lwd = 2)
#points(commcost[1, 3], commcost[1, 2], col = 'red', lwd = 2)
mtext(expression(NULL %*% 10^6), side = 1, at = 2.8)

## sciNotation <- function(x, digits = 1) {
##     if (length(x) > 1) {
##         return(append(sciNotation(x[1]), sciNotation(x[-1])))
##     }
##     if (!x) return(0)
##     exponent <- floor(log10(x))
##     base <- round(x / 10^exponent, digits)
##     as.expression(substitute(base%*%10^exponent,
## 			list(base = base, exponent = exponent)))
## }
## axis(1, at = axTicks(1), label = sciNotation(axTicks(1), 1))

dev.copy2pdf(file = '~/workspace/dqr/commcost.pdf')
