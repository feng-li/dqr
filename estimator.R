# load packages
library(plyr)
library(quantreg)

globalEstimate <- function(X, Y, Tau){
  # Return global estimate
  
  # Args
  # X;Y - simulated data
  # Tau - quantile of regression
  
  rq_global <- rq(Y~X, tau = Tau)
  beta_global <- coef(rq_global)[-1]
  
  return(beta_global)
}

oneshotEstimate <- function(X, Y, k, Tau){
  # Return one-hot estimate
  
  # Args
  # X;Y - simulated data
  # k - number of mechine
  # Tau - quantile of regression
  
  N <- dim(X)[1]
  P <- dim(X)[2]
  index <- c(rep(1:k, rep(ceiling(N/k), k)), 
             rep(k, N - ceiling(N/k)*k))
  DData <- as.data.frame(cbind(Y, X, index = index))
  colnames(DData)[1] <- 'Y'
  result <- ddply(DData, .(index), 
                  function(x) coef(rq(Y~.-index, x, tau = Tau))[-1])
  
  return(apply(result, 2, mean)[-1])
}

onestepEstimate <- function(X, Y, k, Tau){
  # Return one step estimate
  
  # Args
  # X;Y - simulated data
  # k - number of mechine
  # Tau - quantile of regression
  
  # Define Gaussian Kernel Function
  G_kernel <- function(u){
    return((2*pi)^(-1/2) * exp(-u^2/2))
  }
  
  # Acquire pilot sample
  N <- dim(X)[1]
  P <- dim(X)[2]
  n <- round(sqrt(N) * log(N))
  index <- sample(1:N, n)
  X_pilot <- X[index,]
  Y_pilot <- Y[index]
  
  # Pilot estimation
  rq_pilot <- rq(Y_pilot~X_pilot, tau = Tau)
  beta_pilot <- coef(rq_pilot)[-1]
  beta_pilot_intercept <- coef(rq_pilot)[1]
  
  # error of pilot sample
  error_in_Q <- Y_pilot - X_pilot %*% beta_pilot - beta_pilot_intercept
  
  # send pilot estimators to branches to get pilot error
  error <- Y - X %*% beta_pilot - beta_pilot_intercept
  
  # estimate kernel density at 0
  IQR <- quantile(error_in_Q)[4] - quantile(error_in_Q)[2]
  A <- min(sd(error_in_Q), IQR/1.34) # 文章里写的1.34是怎么来的。。
  h <- 0.9 * A * n^(-1/5) 
  f0 <- sum(G_kernel(error/h)) / (N*h)
  
  beta_one_step <- beta_pilot + (1/f0) * solve(t(X) %*% X) %*% (t(X) %*% (Tau - 1*(error<=0)))
  
  return(c(beta_pilot, beta_one_step))
}

