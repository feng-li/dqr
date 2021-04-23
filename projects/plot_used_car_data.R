Y = read.table('~/workspace/dqr/Y.csv', header = T)

par(mfrow = c(1, 2), mar = c(4, 4, 0.5, 0), las = 1)
library(moments)
options(scipen=999)

sk1 = skewness(exp(Y[,1]))
sk2 = skewness(Y[,1])

ku1 = kurtosis(exp(Y[,1]))
ku2 = kurtosis(Y[,1])

hist(exp(Y[, 1]), xlab = "Price (USD)", main = '', breaks = 500, ylab = '', probability = FALSE, xlim = c(0, 100000), ylim = c(0, 400000))
legend("topright", legend = c(paste("Skewness:", round(sk1, 2)), paste("Kurtosis:", round(ku1, 2))), box.col = "white", col = c(2, 1))
                                        #lines(density(exp(Y[, 1]), adjust = 2))
hist(Y[, 1], xlab = "Log Price (USD)",  main = '', ylab = '', xlim =c(5, 16), probability = FALSE)
legend("topright", legend = c(paste("Skewness:", round(sk2, 2)), paste("Kurtosis:", round(ku2, 2))), box.col = "white", col = c(2, 1))
#lines(density(Y[, 1]))
