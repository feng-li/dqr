Y = read.table('~/workspace/dqr/Y.csv', header = T)

par(mfrow = c(1, 2), mar = c(4, 4, 0.5, 0), las = 1)
options(scipen=999)

hist(exp(Y[, 1]), xlab = "Price (USD)", main = '', breaks = 500, ylab = '', probability = FALSE, xlim = c(0, 100000), ylim = c(0, 400000))
#lines(density(exp(Y[, 1]), adjust = 2))
hist(Y[, 1], xlab = "Log Price (USD)",  main = '', ylab = '', probability = FALSE)
#lines(density(Y[, 1]))
