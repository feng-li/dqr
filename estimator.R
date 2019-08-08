# load packages
library(plyr)
library(quantreg)

globalEstimate <- function(X, Y, Tau){
  # Return global estimate
  
  # Args
  # X;Y - simulated data
  # Tau - quantile of regression
  
  # fit a quantile regression
  rq_global <- rq(Y~X, tau = Tau)
  
  # skip the intercept
  beta_global <- coef(rq_global)[-1]
  
  return(beta_global)
}

oneshotEstimate <- function(X, Y, k, Tau){
  # Return one-hot estimate
  
  # Args
  # X;Y - simulated data
  # k - number of mechine
  # Tau - quantile of regression
  
  N <- dim(X)[1]  # number of observations
  P <- dim(X)[2]  # number of dependent varaibles
  
  # assign observations to k machines and generate the corresponding index
  index <- c(rep(1:k, rep(ceiling(N/k), k)), 
             rep(k, N - ceiling(N/k)*k))
  
  # combine X, Y and index together
  DData <- as.data.frame(cbind(Y, X, index = index))
  colnames(DData)[1] <- 'Y'
  
  # fit a quantile regression for each machine
  result <- ddply(DData, .(index), 
                  function(x) coef(rq(Y~.-index, x, tau = Tau))[-1])
  
  # return mean of coefficients for all machines
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
  N <- dim(X)[1]                 # number of observations
  P <- dim(X)[2]                 # number of dependent varaibles
  n <- round(sqrt(N) * log(N))   # number of pilot sample
  index <- sample(1:N, n)        # select pilot sample
  X_pilot <- X[index,]           # get X for pilot sample
  Y_pilot <- Y[index]            # get Y for pilot sample
  
  # Pilot estimation
  # fit quantile regression on pilot sample
  rq_pilot <- rq(Y_pilot~X_pilot, tau = Tau)
  # skip the intercept
  beta_pilot <- coef(rq_pilot)[-1]
  beta_pilot_intercept <- coef(rq_pilot)[1]
  
  # error of pilot sample
  error_in_Q <- Y_pilot - X_pilot %*% beta_pilot - beta_pilot_intercept
  
  # send pilot estimators to branches to get pilot error
  error <- Y - X %*% beta_pilot - beta_pilot_intercept
  
  # estimate kernel density at 0
  IQR <- quantile(error_in_Q)[4] - quantile(error_in_Q)[2] # interquantile range
  A <- min(sd(error_in_Q), IQR/1.34)                       # get constant A
  h <- 0.9 * A * n^(-1/5)                                  # calculate bandwidth
  f0 <- sum(G_kernel(error/h)) / (N*h)                     # kernel density
  
  # update pilot estimation to get the one-step estimation
  beta_one_step <- beta_pilot + (1/f0) * solve(t(X) %*% X) %*% (t(X) %*% (Tau - 1*(error<=0)))
  
  return(c(beta_pilot, beta_one_step))
}